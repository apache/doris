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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.h
// and modified by Doris

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_cache_common.h"
namespace doris {
class TUniqueId;

namespace io {

/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory {
public:
    static FileCacheFactory* instance();

    Status create_file_cache(const std::string& cache_base_path,
                             FileCacheSettings file_cache_settings);

    size_t try_release();

    size_t try_release(const std::string& base_path);

    const std::string& get_cache_path() {
        size_t cur_index = _next_index.fetch_add(1);
        return _caches[cur_index % _caches.size()]->get_base_path();
    }

    [[nodiscard]] size_t get_capacity() const { return _capacity; }

    [[nodiscard]] size_t get_cache_instance_size() const { return _caches.size(); }

    BlockFileCache* get_by_path(const UInt128Wrapper& hash);
    BlockFileCache* get_by_path(const std::string& cache_base_path);
    std::vector<BlockFileCache::QueryFileCacheContextHolderPtr> get_query_context_holders(
            const TUniqueId& query_id);

    /**
     * Clears data of all file cache instances
     *
     * @param sync wait until all data cleared
     * @return summary message
     */
    std::string clear_file_caches(bool sync);

    std::vector<std::string> get_base_paths();

    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::mutex _mtx;
    std::vector<std::unique_ptr<BlockFileCache>> _caches;
    std::unordered_map<std::string, BlockFileCache*> _path_to_cache;
    size_t _capacity = 0;
    std::atomic_size_t _next_index {0}; // use for round-robin
};

} // namespace io
} // namespace doris
