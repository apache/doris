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

#include <bvar/latency_recorder.h>
#include <gen_cpp/cloud.pb.h>

#include <ostream>

#include "common/logging.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_builder.h"
#include "vec/common/string_ref.h"

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

uint64_t hdfs_hash_code(const THdfsParams& hdfs_params, const std::string& fs_name) {
    uint64_t hash_code = 0;
    // The specified fsname is used first.
    // If there is no specified fsname, the default fsname is used
    if (!fs_name.empty()) {
        hash_code ^= crc32_hash(fs_name);
    } else if (hdfs_params.__isset.fs_name) {
        hash_code ^= crc32_hash(hdfs_params.fs_name);
    }

    if (hdfs_params.__isset.user) {
        hash_code ^= crc32_hash(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (const auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code ^= crc32_hash(conf.first);
            hash_code ^= crc32_hash(conf.second);
        }
    }
    return hash_code;
}

} // namespace

namespace hdfs_bvar {
bvar::LatencyRecorder hdfs_read_latency("hdfs_read");
bvar::LatencyRecorder hdfs_write_latency("hdfs_write");
bvar::LatencyRecorder hdfs_create_dir_latency("hdfs_create_dir");
bvar::LatencyRecorder hdfs_open_latency("hdfs_open");
bvar::LatencyRecorder hdfs_close_latency("hdfs_close");
bvar::LatencyRecorder hdfs_flush_latency("hdfs_flush");
bvar::LatencyRecorder hdfs_hflush_latency("hdfs_hflush");
bvar::LatencyRecorder hdfs_hsync_latency("hdfs_hsync");
}; // namespace hdfs_bvar

void HdfsHandlerCache::_clean_invalid() {
    std::vector<uint64_t> removed_handle;
    for (auto& item : _cache) {
        if (item.second.use_count() == 1 && item.second->invalid()) {
            removed_handle.emplace_back(item.first);
        }
    }
    for (auto& handle : removed_handle) {
        _cache.erase(handle);
    }
}

void HdfsHandlerCache::_clean_oldest() {
    uint64_t oldest_time = ULONG_MAX;
    uint64_t oldest = 0;
    for (auto& item : _cache) {
        if (item.second.use_count() == 1 && item.second->last_access_time() < oldest_time) {
            oldest_time = item.second->last_access_time();
            oldest = item.first;
        }
    }
    _cache.erase(oldest);
}

Status HdfsHandlerCache::get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                                        std::shared_ptr<HdfsHandler>* fs_handle) {
    uint64_t hash_code = hdfs_hash_code(hdfs_params, fs_name);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _cache.find(hash_code);
        if (it != _cache.end()) {
            std::shared_ptr<HdfsHandler> handle = it->second;
            if (!handle->invalid()) {
                handle->update_last_access_time();
                *fs_handle = std::move(handle);
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
            auto handle = std::make_shared<HdfsHandler>(hdfs_fs, true);
            handle->update_last_access_time();
            *fs_handle = handle;
            _cache[hash_code] = std::move(handle);
        } else {
            *fs_handle = std::make_shared<HdfsHandler>(hdfs_fs, false);
        }
    }
    return Status::OK();
}

Path convert_path(const Path& path, const std::string& namenode) {
    std::string fs_path;
    if (path.native().find(namenode) != std::string::npos) {
        // `path` is uri format, remove the namenode part in `path`
        // FIXME(plat1ko): Not robust if `namenode` doesn't appear at the beginning of `path`
        fs_path = path.native().substr(namenode.size());
    } else {
        fs_path = path;
    }

    // Always use absolute path (start with '/') in hdfs
    if (fs_path.empty() || fs_path[0] != '/') {
        fs_path.insert(fs_path.begin(), '/');
    }
    return fs_path;
}

bool is_hdfs(const std::string& path_or_fs) {
    return path_or_fs.rfind("hdfs://") == 0;
}

THdfsParams to_hdfs_params(const cloud::HdfsVaultInfo& vault) {
    THdfsParams params;
    auto build_conf = vault.build_conf();
    params.__set_fs_name(build_conf.fs_name());
    if (build_conf.has_user()) {
        params.__set_user(build_conf.user());
    }
    if (build_conf.has_hdfs_kerberos_principal()) {
        params.__set_hdfs_kerberos_principal(build_conf.hdfs_kerberos_principal());
    }
    if (build_conf.has_hdfs_kerberos_keytab()) {
        params.__set_hdfs_kerberos_keytab(build_conf.hdfs_kerberos_keytab());
    }
    std::vector<THdfsConf> tconfs;
    for (const auto& confs : vault.build_conf().hdfs_confs()) {
        THdfsConf conf;
        conf.__set_key(confs.key());
        conf.__set_value(confs.value());
        tconfs.emplace_back(conf);
    }
    params.__set_hdfs_conf(tconfs);
    return params;
}

} // namespace doris::io
