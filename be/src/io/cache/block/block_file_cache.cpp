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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCache.cpp
// and modified by Doris

#include "io/cache/block/block_file_cache.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <utility>

#include "io/cache/block/block_file_cache_fwd.h"
#include "io/cache/block/block_file_cache_settings.h"
#include "vec/common/hex.h"
#include "vec/common/sip_hash.h"

namespace fs = std::filesystem;

namespace doris {
namespace io {

IFileCache::IFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings)
        : _cache_base_path(cache_base_path),
          _max_size(cache_settings.max_size),
          _max_element_size(cache_settings.max_elements),
          _persistent_max_size(cache_settings.persistent_max_size),
          _persistent_max_element_size(cache_settings.persistent_max_elements),
          _max_file_segment_size(cache_settings.max_file_segment_size),
          _max_query_cache_size(cache_settings.max_query_cache_size) {}

std::string IFileCache::Key::to_string() const {
    return vectorized::get_hex_uint_lowercase(key);
}

IFileCache::Key IFileCache::hash(const std::string& path) {
    uint128_t key;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&key));
    return Key(key);
}

std::string IFileCache::get_path_in_local_cache(const Key& key, size_t offset,
                                                bool is_persistent) const {
    auto key_str = key.to_string();
    std::string suffix = is_persistent ? "_persistent" : "";
    if constexpr (USE_CACHE_VERSION2) {
        return fs::path(_cache_base_path) / key_str.substr(0, KEY_PREFIX_LENGTH) / key_str /
               (std::to_string(offset) + suffix);
    } else {
        return fs::path(_cache_base_path) / key_str / (std::to_string(offset) + suffix);
    }
}

std::string IFileCache::get_path_in_local_cache(const Key& key) const {
    auto key_str = key.to_string();
    if constexpr (USE_CACHE_VERSION2) {
        return fs::path(_cache_base_path) / key_str.substr(0, KEY_PREFIX_LENGTH) / key_str;
    } else {
        return fs::path(_cache_base_path) / key_str;
    }
}

std::string IFileCache::get_version_path() const {
    return fs::path(_cache_base_path) / "version";
}

IFileCache::QueryFileCacheContextHolderPtr IFileCache::get_query_context_holder(
        const TUniqueId& query_id) {
    std::lock_guard cache_lock(_mutex);

    if (!_enable_file_cache_query_limit) {
        return {};
    }

    /// if enable_filesystem_query_cache_limit is true,
    /// we create context query for current query.
    auto context = get_or_set_query_context(query_id, cache_lock);
    return std::make_unique<QueryFileCacheContextHolder>(query_id, this, context);
}

IFileCache::QueryFileCacheContextPtr IFileCache::get_query_context(
        const TUniqueId& query_id, std::lock_guard<std::mutex>& cache_lock) {
    auto query_iter = _query_map.find(query_id);
    return (query_iter == _query_map.end()) ? nullptr : query_iter->second;
}

void IFileCache::remove_query_context(const TUniqueId& query_id) {
    std::lock_guard cache_lock(_mutex);
    const auto& query_iter = _query_map.find(query_id);

    if (query_iter != _query_map.end() && query_iter->second.unique()) {
        _query_map.erase(query_iter);
    }
}

IFileCache::QueryFileCacheContextPtr IFileCache::get_or_set_query_context(
        const TUniqueId& query_id, std::lock_guard<std::mutex>& cache_lock) {
    if (query_id.lo == 0 && query_id.hi == 0) {
        return nullptr;
    }

    auto context = get_query_context(query_id, cache_lock);
    if (context) {
        return context;
    }

    auto query_context = std::make_shared<QueryFileCacheContext>(_max_query_cache_size);
    auto query_iter = _query_map.emplace(query_id, query_context).first;
    return query_iter->second;
}

void IFileCache::QueryFileCacheContext::remove(const Key& key, size_t offset, bool is_presistent,
                                               size_t size,
                                               std::lock_guard<std::mutex>& cache_lock) {
    auto record = records.find({key, offset, is_presistent});
    DCHECK(record != records.end());
    lru_queue.remove(record->second, cache_lock);
    records.erase({key, offset, is_presistent});
}

void IFileCache::QueryFileCacheContext::reserve(const Key& key, size_t offset, bool is_presistent,
                                                size_t size,
                                                std::lock_guard<std::mutex>& cache_lock) {
    auto queue_iter = lru_queue.add(key, offset, is_presistent, size, cache_lock);
    records.insert({{key, offset, is_presistent}, queue_iter});
}

} // namespace io
} // namespace doris
