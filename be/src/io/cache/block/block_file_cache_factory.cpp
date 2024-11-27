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
#include "service/backend_options.h"
#include "vec/core/block.h"

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

void FileCacheFactory::get_cache_stats_block(vectorized::Block* block) {
    if (!config::enable_file_cache) {
        return;
    }
    // std::shared_lock<std::shared_mutex> read_lock(_qs_ctx_map_lock);
    TBackend be = BackendOptions::get_local_backend();
    int64_t be_id = be.id;
    std::string be_ip = be.host;

    auto insert_int_value = [&](int col_index, int64_t int_val, vectorized::Block* block) {
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(
                int_val);
        nullable_column->get_null_map_data().emplace_back(0);
    };

    auto insert_string_value = [&](int col_index, std::string str_val, vectorized::Block* block) {
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_val.data(),
                                                                          str_val.size());
        nullable_column->get_null_map_data().emplace_back(0);
    };

    for (auto& cache : _caches) {
        std::map<std::string, double> stats = cache->get_stats();
        for (auto& [k, v] : stats) {
            insert_int_value(0, be_id, block);                     // be id
            insert_string_value(1, be_ip, block);                  // be ip
            insert_string_value(2, cache->get_base_path(), block); // cache path
            insert_string_value(3, k, block);                      // metric name
            insert_string_value(4, std::to_string(v), block);      // metric value
        }
    }
}
} // namespace io
} // namespace doris
