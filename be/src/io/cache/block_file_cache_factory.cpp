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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.cpp
// and modified by Doris

#include "io/cache/block_file_cache_factory.h"

#include <glog/logging.h>
#include <sys/statfs.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "io/cache/file_cache_utils.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"

namespace doris {
class TUniqueId;

namespace io {

FileCacheFactory* FileCacheFactory::instance() {
    return ExecEnv::GetInstance()->file_cache_factory();
}

size_t FileCacheFactory::try_release() {
    int elements = 0;
    for (auto& cache : _caches) {
        elements += cache->try_release();
    }
    return elements;
}

size_t FileCacheFactory::try_release(const std::string& base_path) {
    auto iter = _path_to_cache.find(base_path);
    if (iter != _path_to_cache.end()) {
        return iter->second->try_release();
    }
    return 0;
}

Status FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                           FileCacheSettings file_cache_settings) {
    const auto& fs = global_local_filesystem();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(cache_base_path, &exists));
    if (!exists) {
        RETURN_IF_ERROR(fs->create_directory(cache_base_path));
    } else if (config::clear_file_cache) {
        RETURN_IF_ERROR(fs->delete_directory(cache_base_path));
        RETURN_IF_ERROR(fs->create_directory(cache_base_path));
    }

    struct statfs stat;
    if (statfs(cache_base_path.c_str(), &stat) < 0) {
        LOG_ERROR("").tag("file cache path", cache_base_path).tag("error", strerror(errno));
        return Status::IOError("{} statfs error {}", cache_base_path, strerror(errno));
    }
    size_t disk_total_size = static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize);
    if (disk_total_size < file_cache_settings.total_size) {
        file_cache_settings =
                calc_settings(disk_total_size * 0.9, file_cache_settings.max_query_cache_size);
    }
    auto cache = std::make_unique<BlockFileCacheManager>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    _path_to_cache[cache_base_path] = cache.get();
    _caches.push_back(std::move(cache));
    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    _total_cache_size += file_cache_settings.total_size;
    return Status::OK();
}

BlockFileCacheManagerPtr FileCacheFactory::get_by_path(const UInt128Wrapper& key) {
    return _caches[KeyHash()(key) % _caches.size()].get();
}

BlockFileCacheManagerPtr FileCacheFactory::get_by_path(const std::string& cache_base_path) {
    auto iter = _path_to_cache.find(cache_base_path);
    if (iter == _path_to_cache.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

std::vector<BlockFileCacheManager::QueryFileCacheContextHolderPtr>
FileCacheFactory::get_query_context_holders(const TUniqueId& query_id) {
    std::vector<BlockFileCacheManager::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

void FileCacheFactory::clear_file_caches(bool sync) {
    for (const auto& cache : _caches) {
        if (sync) {
            Status st = cache->clear_file_cache_directly();
            if (st.ok()) {
                LOG_WARNING("").error(st);
            }
        } else {
            cache->clear_file_cache_async();
        }
    }
}

} // namespace io
} // namespace doris
