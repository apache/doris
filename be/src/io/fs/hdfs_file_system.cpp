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
#include "io/fs/hdfs_file_reader.h"
#include "io/hdfs_builder.h"
#include "service/backend_options.h"

namespace doris {
namespace io {

#ifndef CHECK_HDFS_HANDLE
#define CHECK_HDFS_HANDLE(handle)                               \
    if (!handle) {                                              \
        return Status::InternalError("init Hdfs handle error"); \
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

HdfsFileSystem::HdfsFileSystem(const THdfsParams& hdfs_params, const std::string& path)
        : RemoteFileSystem(path, "", FileSystemType::HDFS),
          _hdfs_params(hdfs_params),
          _fs_handle(nullptr) {
    _namenode = _hdfs_params.fs_name;
}

HdfsFileSystem::~HdfsFileSystem() {
    if (_fs_handle && _fs_handle->from_cache) {
        _fs_handle->dec_ref();
    }
}

Status HdfsFileSystem::connect() {
    RETURN_IF_ERROR(HdfsFileSystemCache::instance()->get_connection(_hdfs_params, &_fs_handle));
    if (!_fs_handle) {
        return Status::InternalError("failed to init Hdfs handle with, please check hdfs params.");
    }
    return Status::OK();
}

Status HdfsFileSystem::create_file(const Path& /*path*/, FileWriterPtr* /*writer*/) {
    // auto handle = get_handle();
    // CHECK_HDFS_HANDLE(handle);
    // auto hdfs_file = hdfsOpenFile(handle->hdfs_fs, path.string().c_str(), O_WRONLY, 0, 0, 0);
    // if (hdfs_file == nullptr) {
    //     return Status::InternalError("Failed to create file {}", path.string());
    // }
    // hdfsCloseFile(handle->hdfs_fs, hdfs_file);
    // return Status::OK();
    return Status::NotSupported("Currently not support to create file to HDFS");
}

Status HdfsFileSystem::open_file(const Path& path, FileReaderSPtr* reader) {
    CHECK_HDFS_HANDLE(_fs_handle);
    size_t file_len = 0;
    RETURN_IF_ERROR(file_size(path, &file_len));

    Path real_path = _covert_path(path);
    auto hdfs_file =
            hdfsOpenFile(_fs_handle->hdfs_fs, real_path.string().c_str(), O_RDONLY, 0, 0, 0);
    if (hdfs_file == nullptr) {
        if (_fs_handle->from_cache) {
            // hdfsFS may be disconnected if not used for a long time
            _fs_handle->set_invalid();
            _fs_handle->dec_ref();
            // retry
            RETURN_IF_ERROR(connect());
            hdfs_file = hdfsOpenFile(_fs_handle->hdfs_fs, path.string().c_str(), O_RDONLY, 0, 0, 0);
            if (hdfs_file == nullptr) {
                return Status::InternalError(
                        "open file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                        BackendOptions::get_localhost(), _namenode, path.string(),
                        hdfsGetLastError());
            }
        } else {
            return Status::InternalError("open file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                                         BackendOptions::get_localhost(), _namenode, path.string(),
                                         hdfsGetLastError());
        }
    }
    *reader = std::make_shared<HdfsFileReader>(path, file_len, _namenode, hdfs_file, this);
    return Status::OK();
}

Status HdfsFileSystem::delete_file(const Path& path) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    // The recursive argument `is_recursive` is irrelevant if path is a file.
    int is_recursive = 0;
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::InternalError("Failed to delete file {}", path.string());
    }
    return Status::OK();
}

Status HdfsFileSystem::create_directory(const Path& path) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    int res = hdfsCreateDirectory(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (res == -1) {
        return Status::InternalError("Failed to create directory {}", path.string());
    }
    return Status::OK();
}

Status HdfsFileSystem::delete_directory(const Path& path) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    // delete in recursive mode
    int is_recursive = 1;
    int res = hdfsDelete(_fs_handle->hdfs_fs, real_path.string().c_str(), is_recursive);
    if (res == -1) {
        return Status::InternalError("Failed to delete directory {}", path.string());
    }
    return Status::OK();
}

Status HdfsFileSystem::exists(const Path& path, bool* res) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    int is_exists = hdfsExists(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (is_exists == 0) {
        *res = true;
    } else {
        *res = false;
    }
    return Status::OK();
}

Status HdfsFileSystem::file_size(const Path& path, size_t* file_size) const {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    hdfsFileInfo* file_info = hdfsGetPathInfo(_fs_handle->hdfs_fs, real_path.string().c_str());
    if (file_info == nullptr) {
        return Status::InternalError("Failed to get file size of {}", path.string());
    }
    *file_size = file_info->mSize;
    hdfsFreeFileInfo(file_info, 1);
    return Status::OK();
}

Status HdfsFileSystem::list(const Path& path, std::vector<Path>* files) {
    CHECK_HDFS_HANDLE(_fs_handle);
    Path real_path = _covert_path(path);
    int numEntries = 0;
    hdfsFileInfo* file_info =
            hdfsListDirectory(_fs_handle->hdfs_fs, real_path.string().c_str(), &numEntries);
    if (file_info == nullptr) {
        return Status::InternalError("Failed to list files/directors of {}", path.string());
    }
    for (int idx = 0; idx < numEntries; ++idx) {
        files->emplace_back(file_info[idx].mName);
    }
    hdfsFreeFileInfo(file_info, numEntries);
    return Status::OK();
}

HdfsFileSystemHandle* HdfsFileSystem::get_handle() {
    return _fs_handle;
}

Path HdfsFileSystem::_covert_path(const Path& path) const {
    // if the format of path is hdfs://ip:port/path, replace it to /path.
    // path like hdfs://ip:port/path can't be used by libhdfs3.
    Path real_path(path);
    if (path.string().find(_namenode) != std::string::npos) {
        std::string real_path_str = path.string().substr(_namenode.size());
        real_path = real_path_str;
    }
    return real_path;
}

// ************* HdfsFileSystemCache ******************
int HdfsFileSystemCache::MAX_CACHE_HANDLE = 64;

Status HdfsFileSystemCache::_create_fs(const THdfsParams& hdfs_params, hdfsFS* fs) {
    HDFSCommonBuilder builder = createHDFSBuilder(hdfs_params);
    if (builder.is_need_kinit()) {
        RETURN_IF_ERROR(builder.run_kinit());
    }
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::InternalError("connect to hdfs failed. error: {}", hdfsGetLastError());
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
