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

#include "io/cache/block/block_file_cache_factory.h"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "io/cache/block/block_file_cache.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "io/cache/block/block_lru_file_cache.h"
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

void FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                         const FileCacheSettings& file_cache_settings,
                                         Status* status) {
    if (config::clear_file_cache) {
        auto fs = global_local_filesystem();
        bool res = false;
        static_cast<void>(fs->exists(cache_base_path, &res));
        if (res) {
            static_cast<void>(fs->delete_directory(cache_base_path));
        }
    }

    std::unique_ptr<IFileCache> cache =
            std::make_unique<LRUFileCache>(cache_base_path, file_cache_settings);
    *status = cache->initialize();
    if (!status->ok()) {
        return;
    }

    {
        // the create_file_cache() may be called concurrently,
        // so need to protect it with lock
        std::lock_guard<std::mutex> lock(_cache_mutex);
        _path_to_cache[cache_base_path] = cache.get();
        _caches.push_back(std::move(cache));
    }
    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    *status = Status::OK();
    return;
}

CloudFileCachePtr FileCacheFactory::get_by_path(const IFileCache::Key& key) {
    return _caches[KeyHash()(key) % _caches.size()].get();
}

CloudFileCachePtr FileCacheFactory::get_by_path(const std::string& cache_base_path) {
    auto iter = _path_to_cache.find(cache_base_path);
    if (iter == _path_to_cache.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

std::vector<IFileCache::QueryFileCacheContextHolderPtr> FileCacheFactory::get_query_context_holders(
        const TUniqueId& query_id) {
    std::vector<IFileCache::QueryFileCacheContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

} // namespace io
} // namespace doris
