// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/fs/hdfs_file_system.h"

#include <fcntl.h>
#include <gen_cpp/PlanNodes_types.h>
#include <limits.h>
#include <stddef.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <mutex>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "gutil/hash/hash.h"
#include "gutil/integral_types.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/hdfs_builder.h"
#include "util/hdfs_util.h"
#include "util/obj_lru_cache.h"
#include "util/slice.h"

namespace doris {
namespace io {

#ifndef CHECK_HDFS_HANDLE
#define CHECK_HDFS_HANDLE(handle)                         \
    if (!handle) {                                        \
        return Status::IOError("init Hdfs handle error"); \
    }
#endif

// Cache for HdfsFileSystemHandle
class HdfsFileSystemCache {
public:
    static int MAX_CACHE_HANDLE;

    static HdfsFileSystemCache* instance() {
        static HdfsFileSystemCache s_instance;
        return &s_instance;
    }

    HdfsFileSystemCache(const HdfsFileSystemCache&) = delete;
    const HdfsFileSystemCache& operator=(const HdfsFileSystemCache&) = delete;

    // This function is thread-safe
    Status get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                          HdfsFileSystemHandle** fs_handle);

private:
    std::mutex _lock;
    std::unordered_map<uint64, std::unique_ptr<HdfsFileSystemHandle>> _cache;

    HdfsFileSystemCache() = default;

    uint64 _hdfs_hash_code(const THdfsParams& hdfs_params, const std::string& fs_name);
    Status _create_fs(const THdfsParams& hdfs_params, const std::string& fs_name, hdfsFS* fs,
                      bool* is_kerberos);
    void _clean_invalid();
    void _clean_oldest();
};

class HdfsFileHandleCache {
public:
    static HdfsFileHandleCache* instance() {
        static HdfsFileHandleCache s_instance;
        return &s_instance;
    }

    HdfsFileHandleCache(const HdfsFileHandleCache&) = delete;
    const HdfsFileHandleCache& operator=(const HdfsFileHandleCache&) = delete;

    FileHandleCache& cache() { return _cache; }

    // try get hdfs file from cache, if not exists, will open a new file, insert it into cache
    // and return the file cache handle.
    Status get_file(const std::shared_ptr<HdfsFileSystem>& fs, const Path& file, int64_t mtime,
                    int64_t file_size, FileHandleCache::Accessor* accessor);

private:
    FileHandleCache _cache;
    HdfsFileHandleCache() : _cache(config::max_hdfs_file_handle_cache_num, 16, 3600 * 1000L) {}
};

Status HdfsFileHandleCache::get_file(const std::shared_ptr<HdfsFileSystem>& fs, const Path& file,
                                     int64_t mtime, int64_t file_size,
                                     FileHandleCache::Accessor* accessor) {
    bool cache_hit;
    std::string fname = file.string();
    RETURN_IF_ERROR(HdfsFileHandleCache::instance()->cache().get_file_handle(
            fs->_fs_handle->hdfs_fs, fname, mtime, file_size, false, accessor, &cache_hit));
    accessor->set_fs(fs);

    return Status::OK();
}

Status HdfsFileSystem::create(const THdfsParams& hdfs_params, const std::string& fs_name,
                              RuntimeProfile* profile, std::shared_ptr<HdfsFileSystem>* fs) {
#ifdef USE_HADOOP_HDFS
    if (!config::enable_java_support) {
        return Status::InternalError(
                "hdfs file system is not enabled, you can change be config enable_java_support to "
                "true.");
    }
#endif
    (*fs).reset(new HdfsFileSystem(hdfs_params, fs_name, profile));
    return (*fs)->connect();
}

HdfsFileSystem::HdfsFileSystem(const THdfsParams& hdfs_params, const std::string& fs_name,
                               RuntimeProfile* profile)
        : RemoteFileSystem("", "", FileSystemType::HDFS),
          _hdfs_params(hdfs_params),
          _fs_handle(nullptr),
          _profile(profile) {
    if (fs_name.empty() && _hdfs_params.__isset.fs_name) {
        _fs_name = _hdfs_params.fs_name;
    } else {
        _fs_name = fs_name;
    }
}

HdfsFileSystem::~HdfsFileSystem() {
    if (_fs_handle != nullptr) {
        if (_fs_handle->from_cache) {
            _fs_handle->dec_ref();
        } else {
            delete _fs_handle;
        }
    }
}

Status HdfsFileSystem::connect_impl() {
    RETURN_IF_ERROR(
            HdfsFileSystemCache::instance()->get_connection(_hdfs_params, _fs_name, &_fs_handle));
    if (!_fs_handle) {
        return Status::IOError("failed to init Hdfs handle with, please check hdfs params.");
    }
    return Status::OK();
}

Status HdfsFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                        const FileWriterOptions* opts) {
    *writer = std::make_unique<HdfsFileWriter>(file, getSPtr());
    return Status::OK();
}

Status HdfsFileSystem::open_file_internal(const FileDescription& fd, const Path& abs_path,
                                          FileReaderSPtr* reader) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(abs_path, _fs_name);

    FileHandleCache::Accessor accessor;
    RETURN_IF_ERROR(HdfsFileHandleCache::instance()->get_file(
            std::static_pointer_cast<HdfsFileSystem>(shared_from_this()), real_path, fd.mtime,
            fd.file_size, &accessor));

    *reader = std::make_shared<HdfsFileReader>(abs_path, _fs_name, std::move(accessor), _profile);
    return Status::OK();
}

Status HdfsFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(dir, _fs_name);
    int res = hdfsCreateDirectory(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (res == -1) {
        return Status::IOError("failed to create directory {}: {}", dir.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_file_impl(const Path& file) {
    return delete_internal(file, 0);
}

Status HdfsFileSystem::delete_directory_impl(const Path& dir) {
    return delete_internal(dir, 1);
}

Status HdfsFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_internal(const Path& path, int is_recursive) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(path, &exists));
    if (!exists) {
        return Status::OK();
    }
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::IOError("failed to delete directory {}: {}", path.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::exists_impl(const Path& path, bool* res) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int is_exists = hdfsExists(_fs_handle->hdfs_fs, real_path.string().c_str());
#ifdef USE_HADOOP_HDFS
    // when calling hdfsExists() and return non-zero code,
    // if root_cause is nullptr, which means the file does not exist.
    // if root_cause is not nullptr, which means it encounter other error, should return.
    // NOTE: not for libhdfs3 since it only runs on MaxOS, don't have to support it.
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        return Status::IOError("failed to check path existence {}: {}", path.native(), root_cause);
    }
#endif
    *res = (is_exists == 0);
    return Status::OK();
}

Status HdfsFileSystem::file_size_impl(const Path& path, int64_t* file_size) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    hdfsFileInfo* file_info = hdfsGetPathInfo(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (file_info == nullptr) {
        return Status::IOError("failed to get file size of {}: {}", path.native(), hdfs_error());
    }
    *file_size = file_info->mSize;
    hdfsFreeFileInfo(file_info, 1);
    return Status::OK();
}

Status HdfsFileSystem::list_impl(const Path& path, bool only_file, std::vector<FileInfo>* files,
                                 bool* exists) {
    RETURN_IF_ERROR(exists_impl(path, exists));
    if (!(*exists)) {
        return Status::OK();
    }

    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _fs_name);
    int numEntries = 0;
    hdfsFileInfo* hdfs_file_info =
            hdfsListDirectory(_fs_handle->hdfs_fs, real_path.c_str(), &numEntries);
    if (hdfs_file_info == nullptr) {
        return Status::IOError("failed to list files/directors {}: {}", path.native(),
                               hdfs_error());
    }
    for (int idx = 0; idx < numEntries; ++idx) {
        auto& file = hdfs_file_info[idx];
        if (only_file && file.mKind == kObjectKindDirectory) {
            continue;
        }
        FileInfo file_info;
        file_info.file_name = file.mName;
        file_info.file_size = file.mSize;
        file_info.is_file = (file.mKind != kObjectKindDirectory);
        files->emplace_back(std::move(file_info));
    }
    hdfsFreeFileInfo(hdfs_file_info, numEntries);
    return Status::OK();
}

Status HdfsFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    Path normal_orig_name = convert_path(orig_name, _fs_name);
    Path normal_new_name = convert_path(new_name, _fs_name);
    int ret = hdfsRename(_fs_handle->hdfs_fs, normal_orig_name.c_str(), normal_new_name.c_str());
    if (ret == 0) {
        LOG(INFO) << "finished to rename file. orig: " << normal_orig_name
                  << ", new: " << normal_new_name;
        return Status::OK();
    } else {
        return Status::IOError("fail to rename from {} to {}: {}", normal_orig_name.native(),
                               normal_new_name.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::rename_dir_impl(const Path& orig_name, const Path& new_name) {
    return rename_impl(orig_name, new_name);
}

Status HdfsFileSystem::upload_impl(const Path& local_file, const Path& remote_file) {
    // 1. open local file for read
    FileSystemSPtr local_fs = global_local_filesystem();
    FileReaderSPtr local_reader = nullptr;
    RETURN_IF_ERROR(local_fs->open_file(local_file, &local_reader));
    int64_t file_len = local_reader->size();
    if (file_len == -1) {
        return Status::IOError("failed to get size of file: {}", local_file.string());
    }

    // 2. open remote file for write
    FileWriterPtr hdfs_writer = nullptr;
    RETURN_IF_ERROR(create_file_impl(remote_file, &hdfs_writer, nullptr));

    constexpr size_t buf_sz = 1024 * 1024;
    char read_buf[buf_sz];
    size_t left_len = file_len;
    size_t read_offset = 0;
    size_t bytes_read = 0;
    while (left_len > 0) {
        size_t read_len = left_len > buf_sz ? buf_sz : left_len;
        RETURN_IF_ERROR(local_reader->read_at(read_offset, {read_buf, read_len}, &bytes_read));
        RETURN_IF_ERROR(hdfs_writer->append({read_buf, read_len}));

        read_offset += read_len;
        left_len -= read_len;
    }

    LOG(INFO) << "finished to write file: " << local_file << ", length: " << file_len;
    return Status::OK();
}

Status HdfsFileSystem::batch_upload_impl(const std::vector<Path>& local_files,
                                         const std::vector<Path>& remote_files) {
    DCHECK(local_files.size() == remote_files.size());
    for (int i = 0; i < local_files.size(); ++i) {
        RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
    }
    return Status::OK();
}

Status HdfsFileSystem::direct_upload_impl(const Path& remote_file, const std::string& content) {
    FileWriterPtr hdfs_writer = nullptr;
    RETURN_IF_ERROR(create_file(remote_file, &hdfs_writer));
    RETURN_IF_ERROR(hdfs_writer->append({content}));
    return Status::OK();
}

Status HdfsFileSystem::upload_with_checksum_impl(const Path& local, const Path& remote_file,
                                                 const std::string& checksum) {
    std::string temp = remote_file.string() + ".part";
    std::string final_file = remote_file.string() + "." + checksum;
    RETURN_IF_ERROR(upload_impl(local, temp));
    return rename_impl(temp, final_file);
}

Status HdfsFileSystem::download_impl(const Path& remote_file, const Path& local_file) {
    // 1. open remote file for read
    FileReaderSPtr hdfs_reader = nullptr;
    FileDescription fd;
    fd.path = remote_file;
    RETURN_IF_ERROR(open_file_internal(fd, remote_file, &hdfs_reader));

    // 2. remove the existing local file if exist
    if (std::filesystem::remove(local_file)) {
        LOG(INFO) << "remove the previously exist local file: " << local_file;
    }

    // 3. open local file for write
    FileSystemSPtr local_fs = global_local_filesystem();
    FileWriterPtr local_writer = nullptr;
    RETURN_IF_ERROR(local_fs->create_file(local_file, &local_writer));

    // 4. read remote and write to local
    LOG(INFO) << "read remote file: " << remote_file << " to local: " << local_file;
    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<char[]> read_buf(new char[buf_sz]);
    size_t write_offset = 0;
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(hdfs_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        RETURN_IF_ERROR(local_writer->write_at(write_offset, {read_buf.get(), read_len}));
        write_offset += read_len;
    }

    return Status::OK();
}

Status HdfsFileSystem::direct_download_impl(const Path& remote_file, std::string* content) {
    // 1. open remote file for read
    FileReaderSPtr hdfs_reader = nullptr;
    FileDescription fd;
    fd.path = remote_file;
    RETURN_IF_ERROR(open_file_internal(fd, remote_file, &hdfs_reader));

    constexpr size_t buf_sz = 1024 * 1024;
    std::unique_ptr<char[]> read_buf(new char[buf_sz]);
    size_t write_offset = 0;
    size_t cur_offset = 0;
    while (true) {
        size_t read_len = 0;
        Slice file_slice(read_buf.get(), buf_sz);
        RETURN_IF_ERROR(hdfs_reader->read_at(cur_offset, file_slice, &read_len));
        cur_offset += read_len;
        if (read_len == 0) {
            break;
        }

        content->insert(write_offset, read_buf.get(), read_len);
        write_offset += read_len;
    }
    return Status::OK();
}

HdfsFileSystemHandle* HdfsFileSystem::get_handle() {
    return _fs_handle;
}

// ************* HdfsFileSystemCache ******************
int HdfsFileSystemCache::MAX_CACHE_HANDLE = 64;

Status HdfsFileSystemCache::_create_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                       hdfsFS* fs, bool* is_kerberos) {
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    *is_kerberos = builder.is_need_kinit();
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::IOError("faield to connect to hdfs {}: {}", fs_name, hdfs_error());
    }
    *fs = hdfs_fs;
    return Status::OK();
}

void HdfsFileSystemCache::_clean_invalid() {
    std::vector<uint64> removed_handle;
    for (auto& item : _cache) {
        if (item.second->invalid() && item.second->ref_cnt() == 0) {
            removed_handle.emplace_back(item.first);
        }
    }
    for (auto& handle : removed_handle) {
        _cache.erase(handle);
    }
}

void HdfsFileSystemCache::_clean_oldest() {
    uint64_t oldest_time = ULONG_MAX;
    uint64 oldest = 0;
    for (auto& item : _cache) {
        if (item.second->ref_cnt() == 0 && item.second->last_access_time() < oldest_time) {
            oldest_time = item.second->last_access_time();
            oldest = item.first;
        }
    }
    _cache.erase(oldest);
}

Status HdfsFileSystemCache::get_connection(const THdfsParams& hdfs_params,
                                           const std::string& fs_name,
                                           HdfsFileSystemHandle** fs_handle) {
    uint64 hash_code = _hdfs_hash_code(hdfs_params, fs_name);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(hash_code);
        if (it != _cache.end()) {
            HdfsFileSystemHandle* handle = it->second.get();
            if (!handle->invalid()) {
                handle->inc_ref();
                *fs_handle = handle;
                return Status::OK();
            }
            // fs handle is invalid, erase it.
            _cache.erase(it);
            LOG(INFO) << "erase the hdfs handle, fs name: " << hdfs_params.fs_name;
        }

        // not find in cache, or fs handle is invalid
        // create a new one and try to put it into cache
        hdfsFS hdfs_fs = nullptr;
        bool is_kerberos = false;
        RETURN_IF_ERROR(_create_fs(hdfs_params, fs_name, &hdfs_fs, &is_kerberos));
        if (_cache.size() >= MAX_CACHE_HANDLE) {
            _clean_invalid();
            _clean_oldest();
        }
        if (_cache.size() < MAX_CACHE_HANDLE) {
            std::unique_ptr<HdfsFileSystemHandle> handle =
                    std::make_unique<HdfsFileSystemHandle>(hdfs_fs, true, is_kerberos);
            handle->inc_ref();
            *fs_handle = handle.get();
            _cache[hash_code] = std::move(handle);
        } else {
            *fs_handle = new HdfsFileSystemHandle(hdfs_fs, false, is_kerberos);
        }
    }
    return Status::OK();
}

uint64 HdfsFileSystemCache::_hdfs_hash_code(const THdfsParams& hdfs_params,
                                            const std::string& fs_name) {
    uint64 hash_code = 0;
    hash_code += Fingerprint(fs_name);
    if (hdfs_params.__isset.user) {
        hash_code += Fingerprint(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code += Fingerprint(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code += Fingerprint(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code += Fingerprint(conf.first);
            hash_code += Fingerprint(conf.second);
        }
    }
    return hash_code;
}
} // namespace io
} // namespace doris
