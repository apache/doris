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

#include "gutil/hash/hash.h"
#include "io/cache/block/cached_remote_file_reader.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/hdfs_builder.h"
#include "service/backend_options.h"
#include "util/hdfs_util.h"
#include "util/stack_util.h"

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
    Status get_connection(const THdfsParams& hdfs_params, HdfsFileSystemHandle** fs_handle);

private:
    std::mutex _lock;
    std::unordered_map<uint64, std::unique_ptr<HdfsFileSystemHandle>> _cache;

    HdfsFileSystemCache() = default;

    uint64 _hdfs_hash_code(const THdfsParams& hdfs_params);
    Status _create_fs(const THdfsParams& hdfs_params, hdfsFS* fs);
    void _clean_invalid();
    void _clean_oldest();
};

Status HdfsFileSystem::create(const THdfsParams& hdfs_params, const std::string& path,
                              std::shared_ptr<HdfsFileSystem>* fs) {
#ifdef USE_HADOOP_HDFS
    if (!config::enable_java_support) {
        return Status::InternalError(
                "hdfs file system is not enabled, you can change be config enable_java_support to "
                "true.");
    }
#endif
    (*fs).reset(new HdfsFileSystem(hdfs_params, path));
    return (*fs)->connect();
}

HdfsFileSystem::HdfsFileSystem(const THdfsParams& hdfs_params, const std::string& path)
        : RemoteFileSystem(path, "", FileSystemType::HDFS),
          _hdfs_params(hdfs_params),
          _fs_handle(nullptr) {
    _namenode = _hdfs_params.fs_name;
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
    RETURN_IF_ERROR(HdfsFileSystemCache::instance()->get_connection(_hdfs_params, &_fs_handle));
    if (!_fs_handle) {
        return Status::IOError("failed to init Hdfs handle with, please check hdfs params.");
    }
    return Status::OK();
}

Status HdfsFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer) {
    *writer = std::make_unique<HdfsFileWriter>(file, getSPtr());
    return Status::OK();
}

Status HdfsFileSystem::open_file_internal(const Path& file, int64_t file_size,
                                          FileReaderSPtr* reader) {
    CHECK_HDFS_HANDLE(_fs_handle);
    int64_t fsize = file_size;
    if (fsize < 0) {
        RETURN_IF_ERROR(file_size_impl(file, &fsize));
    }

    Path real_path = convert_path(file, _namenode);
    auto hdfs_file =
            hdfsOpenFile(_fs_handle->hdfs_fs, real_path.string().c_str(), O_RDONLY, 0, 0, 0);
    if (hdfs_file == nullptr) {
        if (_fs_handle->from_cache) {
            // hdfsFS may be disconnected if not used for a long time
            _fs_handle->set_invalid();
            _fs_handle->dec_ref();
            // retry
            RETURN_IF_ERROR(connect());
            hdfs_file = hdfsOpenFile(_fs_handle->hdfs_fs, file.string().c_str(), O_RDONLY, 0, 0, 0);
            if (hdfs_file == nullptr) {
                return Status::IOError("failed to open {}: {}", file.native(), hdfs_error());
            }
        } else {
            return Status::IOError("failed to open {} from cache: {}", file.native(), hdfs_error());
        }
    }
    *reader = std::make_shared<HdfsFileReader>(
            file, fsize, _namenode, hdfs_file,
            std::static_pointer_cast<HdfsFileSystem>(shared_from_this()));
    return Status::OK();
}

Status HdfsFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(dir, _namenode);
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
    Path real_path = convert_path(path, _namenode);
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::IOError("failed to delete directory {}: {}", path.native(), hdfs_error());
    }
    return Status::OK();
}

Status HdfsFileSystem::exists_impl(const Path& path, bool* res) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _namenode);
    int is_exists = hdfsExists(_fs_handle->hdfs_fs, real_path.string().c_str());
    *res = (is_exists == 0);
    return Status::OK();
}

Status HdfsFileSystem::file_size_impl(const Path& path, int64_t* file_size) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = convert_path(path, _namenode);
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
    Path real_path = convert_path(path, _namenode);
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
    Path normal_orig_name = convert_path(orig_name, _namenode);
    Path normal_new_name = convert_path(new_name, _namenode);
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
    RETURN_IF_ERROR(create_file_impl(remote_file, &hdfs_writer));

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
    RETURN_IF_ERROR(open_file_internal(remote_file, -1, &hdfs_reader));

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
    RETURN_IF_ERROR(open_file_internal(remote_file, -1, &hdfs_reader));

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

Status HdfsFileSystemCache::_create_fs(const THdfsParams& hdfs_params, hdfsFS* fs) {
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(createHDFSBuilder(hdfs_params, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::IOError("faield to connect to hdfs {}: {}", hdfs_params.fs_name,
                               hdfs_error());
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
                                           HdfsFileSystemHandle** fs_handle) {
    uint64 hash_code = _hdfs_hash_code(hdfs_params);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(hash_code);
        if (it != _cache.end()) {
            HdfsFileSystemHandle* handle = it->second.get();
            if (handle->invalid()) {
                hdfsFS hdfs_fs = nullptr;
                RETURN_IF_ERROR(_create_fs(hdfs_params, &hdfs_fs));
                *fs_handle = new HdfsFileSystemHandle(hdfs_fs, false);
            } else {
                handle->inc_ref();
                *fs_handle = handle;
            }
        } else {
            hdfsFS hdfs_fs = nullptr;
            RETURN_IF_ERROR(_create_fs(hdfs_params, &hdfs_fs));
            if (_cache.size() >= MAX_CACHE_HANDLE) {
                _clean_invalid();
                _clean_oldest();
            }
            if (_cache.size() < MAX_CACHE_HANDLE) {
                std::unique_ptr<HdfsFileSystemHandle> handle =
                        std::make_unique<HdfsFileSystemHandle>(hdfs_fs, true);
                handle->inc_ref();
                *fs_handle = handle.get();
                _cache[hash_code] = std::move(handle);
            } else {
                *fs_handle = new HdfsFileSystemHandle(hdfs_fs, false);
            }
        }
    }
    return Status::OK();
}

uint64 HdfsFileSystemCache::_hdfs_hash_code(const THdfsParams& hdfs_params) {
    uint64 hash_code = 0;
    if (hdfs_params.__isset.fs_name) {
        hash_code += Fingerprint(hdfs_params.fs_name);
    }
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
