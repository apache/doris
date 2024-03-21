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

#include "io/hdfs_util.h"

#include <ostream>

#include "common/logging.h"
#include "gutil/hash/hash.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_builder.h"

namespace doris::io {
namespace {

Status create_hdfs_fs(const THdfsParams& hdfs_params, const std::string& fs_name, hdfsFS* fs) {
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::InternalError("failed to connect to hdfs {}: {}", fs_name, hdfs_error());
    }
    *fs = hdfs_fs;
    return Status::OK();
}

uint64 hdfs_hash_code(const THdfsParams& hdfs_params) {
    uint64 hash_code = 0;
    hash_code ^= Fingerprint(hdfs_params.fs_name);
    if (hdfs_params.__isset.user) {
        hash_code ^= Fingerprint(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code ^= Fingerprint(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code ^= Fingerprint(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (const auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code ^= Fingerprint(conf.first);
            hash_code ^= Fingerprint(conf.second);
        }
    }
    return hash_code;
}

} // namespace

void HdfsHandlerCache::_clean_invalid() {
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

void HdfsHandlerCache::_clean_oldest() {
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

Status HdfsHandlerCache::get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                                        HdfsHandler** fs_handle) {
    uint64 hash_code = hdfs_hash_code(hdfs_params);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(hash_code);
        if (it != _cache.end()) {
            HdfsHandler* handle = it->second.get();
            if (!handle->invalid()) {
                handle->inc_ref();
                *fs_handle = handle;
                return Status::OK();
            }
            // fs handle is invalid, erase it.
            _cache.erase(it);
            LOG(INFO) << "erase the hdfs handle, fs name: " << fs_name;
        }

        // not find in cache, or fs handle is invalid
        // create a new one and try to put it into cache
        hdfsFS hdfs_fs = nullptr;
        RETURN_IF_ERROR(create_hdfs_fs(hdfs_params, fs_name, &hdfs_fs));
        if (_cache.size() >= MAX_CACHE_HANDLE) {
            _clean_invalid();
            _clean_oldest();
        }
        if (_cache.size() < MAX_CACHE_HANDLE) {
            std::unique_ptr<HdfsHandler> handle = std::make_unique<HdfsHandler>(hdfs_fs, true);
            handle->inc_ref();
            *fs_handle = handle.get();
            _cache[hash_code] = std::move(handle);
        } else {
            *fs_handle = new HdfsHandler(hdfs_fs, false);
        }
    }
    return Status::OK();
}

Path convert_path(const Path& path, const std::string& namenode) {
    Path real_path(path);
    if (path.string().find(namenode) != std::string::npos) {
        std::string real_path_str = path.string().substr(namenode.size());
        real_path = real_path_str;
    }
    return real_path;
}

bool is_hdfs(const std::string& path_or_fs) {
    return path_or_fs.rfind("hdfs://") == 0;
}

} // namespace doris::io
