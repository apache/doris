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
#include <sys/resource.h>

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
          _total_size(cache_settings.total_size),
          _max_file_segment_size(cache_settings.max_file_segment_size),
          _max_query_cache_size(cache_settings.max_query_cache_size) {
    _cur_size_metrics =
            std::make_shared<bvar::Status<size_t>>(_cache_base_path.c_str(), "cur_size", 0);
}

std::string IFileCache::Key::to_string() const {
    return vectorized::get_hex_uint_lowercase(key);
}

IFileCache::Key IFileCache::hash(const std::string& path) {
    uint128_t key;
    sip_hash128(path.data(), path.size(), reinterpret_cast<char*>(&key));
    return Key(key);
}

std::string_view IFileCache::cache_type_to_string(CacheType type) {
    switch (type) {
    case CacheType::INDEX:
        return "_idx";
    case CacheType::DISPOSABLE:
        return "_disposable";
    case CacheType::NORMAL:
        return "";
    case CacheType::TTL:
        return "_ttl";
    }
    return "";
}

CacheType IFileCache::string_to_cache_type(const std::string& str) {
    switch (str[0]) {
    case 'i':
        return CacheType::INDEX;
    case 'd':
        return CacheType::DISPOSABLE;
    default:
        DCHECK(false);
    }
    return CacheType::DISPOSABLE;
}

std::string IFileCache::get_path_in_local_cache(const Key& key, size_t offset,
                                                CacheType type) const {
    return get_path_in_local_cache(key) + "/" +
           (std::to_string(offset) + cache_type_to_string(type));
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

void IFileCache::QueryFileCacheContext::remove(const Key& key, size_t offset,
                                               std::lock_guard<std::mutex>& cache_lock) {
    auto pair = std::make_pair(key, offset);
    auto record = records.find(pair);
    DCHECK(record != records.end());
    auto iter = record->second;
    records.erase(pair);
    lru_queue.remove(iter, cache_lock);
}

void IFileCache::QueryFileCacheContext::reserve(const Key& key, size_t offset, size_t size,
                                                std::lock_guard<std::mutex>& cache_lock) {
    auto pair = std::make_pair(key, offset);
    if (records.find(pair) == records.end()) {
        auto queue_iter = lru_queue.add(key, offset, size, cache_lock);
        records.insert({pair, queue_iter});
    }
}

void IFileCache::set_read_only(bool read_only) {
    s_read_only = read_only;
    if (read_only) {
        std::lock_guard lock(s_file_reader_cache_mtx);
        s_file_reader_cache.clear();
        s_file_name_to_reader.clear();
    }
}

std::weak_ptr<FileReader> IFileCache::cache_file_reader(const AccessKeyAndOffset& key,
                                                        std::shared_ptr<FileReader> file_reader) {
    std::weak_ptr<FileReader> wp;
    if (!s_read_only) [[likely]] {
        std::lock_guard lock(s_file_reader_cache_mtx);
        if (s_file_reader_cache.size() >= _max_file_reader_cache_size) {
            s_file_name_to_reader.erase(s_file_reader_cache.back().first);
            s_file_reader_cache.pop_back();
        }
        wp = file_reader;
        s_file_reader_cache.emplace_front(key, std::move(file_reader));
        s_file_name_to_reader.insert(std::make_pair(key, s_file_reader_cache.begin()));
    }
    return wp;
}

void IFileCache::remove_file_reader(const AccessKeyAndOffset& key) {
    std::lock_guard lock(s_file_reader_cache_mtx);
    if (auto iter = s_file_name_to_reader.find(key); iter != s_file_name_to_reader.end()) {
        s_file_reader_cache.erase(iter->second);
        s_file_name_to_reader.erase(key);
    }
}

bool IFileCache::contains_file_reader(const AccessKeyAndOffset& key) {
    std::lock_guard lock(s_file_reader_cache_mtx);
    return s_file_name_to_reader.find(key) != s_file_name_to_reader.end();
}

size_t IFileCache::file_reader_cache_size() {
    std::lock_guard lock(s_file_reader_cache_mtx);
    return s_file_name_to_reader.size();
}

void IFileCache::init() {
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        LOG(FATAL) << "getrlimit() failed with errno: " << errno;
        return;
    }

    _max_file_reader_cache_size =
            std::min((uint64_t)config::file_cache_max_file_reader_cache_size, limit.rlim_max / 3);
    LOG(INFO) << "max file reader cache size is: " << _max_file_reader_cache_size
              << ", resource hard limit is: " << limit.rlim_max
              << ", config file_cache_max_file_reader_cache_size is: "
              << config::file_cache_max_file_reader_cache_size;
    return;
}

} // namespace io
} // namespace doris
