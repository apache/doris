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

#pragma once

#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "util/hash_util.hpp"
#include "util/lru_cache.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {
class InvertedIndexCacheHandle;

class InvertedIndexSearcherCache {
public:
    // The cache key of index_searcher lru cache
    struct CacheKey {
        CacheKey(std::string index_file_path) : index_file_path(std::move(index_file_path)) {}
        std::string index_file_path;
    };

    // The cache value of index_searcher lru cache.
    // Holding an opened index_searcher.
    class CacheValue : public LRUCacheValueBase {
    public:
        IndexSearcherPtr index_searcher;
        size_t size = 0;
        int64_t last_visit_time;

        CacheValue() = default;
        explicit CacheValue(IndexSearcherPtr searcher, size_t mem_size, int64_t visit_time)
                : index_searcher(std::move(searcher)) {
            size = mem_size;
            last_visit_time = visit_time;
        }

        // The sum of the lengths the norms of the non-NULL documents of `field` stand for,
        // computed by `compute` the first time. The index never changes, and merge-on-write
        // scoring needs the sum for every query.
        Status non_null_norm_length_sum(const std::wstring& field,
                                        const std::function<Status(uint64_t*)>& compute,
                                        uint64_t* out) {
            std::lock_guard lock(_norm_length_sums_mutex);
            if (auto it = _norm_length_sums.find(field); it != _norm_length_sums.end()) {
                *out = it->second;
                return Status::OK();
            }
            RETURN_IF_ERROR(compute(out));
            _norm_length_sums.emplace(field, *out);
            return Status::OK();
        }

        // The sum of the lengths the norms of `docs`, ascending rows of the index, stand for,
        // computed by `compute` the first time the field meets that set of rows. Queries at one
        // read version hide the same rows, so they read no norms; a few sets per field are kept
        // for queries at different versions.
        Status hidden_norm_length_sum(const std::wstring& field, std::span<const uint32_t> docs,
                                      const std::function<Status(uint64_t*)>& compute,
                                      uint64_t* out) {
            const RowSetKey key {
                    .fingerprint = HashUtil::xxHash64WithSeed(
                            reinterpret_cast<const char*>(docs.data()), docs.size_bytes(), 0),
                    .count = docs.size(),
                    .first = docs.empty() ? 0 : docs.front(),
                    .last = docs.empty() ? 0 : docs.back()};
            std::lock_guard lock(_norm_length_sums_mutex);
            auto& sums = _hidden_length_sums[field];
            for (const auto& [cached_key, sum] : sums) {
                if (cached_key == key) {
                    *out = sum;
                    return Status::OK();
                }
            }
            RETURN_IF_ERROR(compute(out));
            if (sums.size() == kHiddenSetsPerField) {
                sums.erase(sums.begin());
            }
            sums.emplace_back(key, *out);
            return Status::OK();
        }

    private:
        struct RowSetKey {
            uint64_t fingerprint;
            size_t count;
            uint32_t first;
            uint32_t last;
            bool operator==(const RowSetKey&) const = default;
        };
        static constexpr size_t kHiddenSetsPerField = 4;

        std::mutex _norm_length_sums_mutex;
        std::unordered_map<std::wstring, uint64_t> _norm_length_sums;
        std::unordered_map<std::wstring, std::vector<std::pair<RowSetKey, uint64_t>>>
                _hidden_length_sums;
    };
    // Create global instance of this class.
    // "capacity" is the capacity of lru cache.
    static InvertedIndexSearcherCache* create_global_instance(size_t capacity,
                                                              uint32_t num_shards = 16);

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexSearcherCache* instance() {
        return ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
    }

    InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards);

    void insert(const InvertedIndexSearcherCache::CacheKey& cache_key, CacheValue* cache_value);

    void insert(const InvertedIndexSearcherCache::CacheKey& cache_key, CacheValue* cache_value,
                InvertedIndexCacheHandle* handle);

    // Lookup the given index_searcher in the cache.
    // If the index_searcher is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool lookup(const InvertedIndexSearcherCache::CacheKey& key, InvertedIndexCacheHandle* handle);

    // function `erase` called after compaction remove segment
    Status erase(const std::string& index_file_path);

    void release(Cache::Handle* handle) { _policy->release(handle); }

    int64_t mem_consumption();

private:
    InvertedIndexSearcherCache() = default;

    class InvertedIndexSearcherCachePolicy : public LRUCachePolicy {
    public:
        InvertedIndexSearcherCachePolicy(size_t capacity, uint32_t num_shards,
                                         uint32_t element_count_capacity)
                : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::inverted_index_cache_stale_sweep_time_sec, num_shards,
                                 element_count_capacity, /*enable_prune*/ true,
                                 /*is lru k*/ false) {}
        InvertedIndexSearcherCachePolicy(size_t capacity, uint32_t num_shards,
                                         uint32_t element_count_capacity,
                                         CacheValueTimeExtractor cache_value_time_extractor,
                                         bool cache_value_check_timestamp)
                : LRUCachePolicy(
                          CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE, capacity,
                          LRUCacheType::SIZE, config::inverted_index_cache_stale_sweep_time_sec,
                          num_shards, element_count_capacity, cache_value_time_extractor,
                          cache_value_check_timestamp, /*enable_prune*/ true, /*is lru k*/ false) {}
    };
    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    Cache::Handle* _insert(const InvertedIndexSearcherCache::CacheKey& key, CacheValue* value);

    std::unique_ptr<InvertedIndexSearcherCachePolicy> _policy;
};

using IndexCacheValuePtr = std::unique_ptr<InvertedIndexSearcherCache::CacheValue>;

// A handle for a index_searcher from index_searcher lru cache.
// The handle can ensure that the index_searcher is valid
// and will not be closed while the holder of the handle is accessing the index_searcher.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class InvertedIndexCacheHandle {
public:
    InvertedIndexCacheHandle() = default;
    InvertedIndexCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~InvertedIndexCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            // only after get_index_searcher call this destructor will
            // add `config::index_cache_entry_stay_time_after_lookup_s` on last_visit_time,
            // this is to extend the retention time of the entries hit by lookup.
            ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))->last_visit_time =
                    UnixMillis() + config::index_cache_entry_stay_time_after_lookup_s * 1000;
            _cache->release(_handle);
        }
    }

    InvertedIndexCacheHandle(InvertedIndexCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    InvertedIndexCacheHandle& operator=(InvertedIndexCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    IndexSearcherPtr get_index_searcher() {
        return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))->index_searcher;
    }

    InvertedIndexSearcherCache::CacheValue* get_index_cache_value() {
        return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle));
    }

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexCacheHandle);
};

class InvertedIndexQueryCacheHandle;

class InvertedIndexQueryCache : public LRUCachePolicy {
public:
    using LRUCachePolicy::insert;

    // cache key
    struct CacheKey {
        io::Path index_path;               // index file path
        std::string column_name;           // column name
        InvertedIndexQueryType query_type; // query type
        std::string value;                 // query value

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(index_path.string());
            key_buf.append("/");
            key_buf.append(column_name);
            key_buf.append("/");
            auto query_type_str = query_type_to_string(query_type);
            if (query_type_str.empty()) {
                return "";
            }
            key_buf.append(query_type_str);
            key_buf.append("/");
            key_buf.append(value);
            return key_buf;
        }
    };

    class CacheValue : public LRUCacheValueBase {
    public:
        std::shared_ptr<roaring::Roaring> bitmap;
    };

    // Create global instance of this class
    static InvertedIndexQueryCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        auto* res = new InvertedIndexQueryCache(capacity, num_shards);
        return res;
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static InvertedIndexQueryCache* instance() {
        return ExecEnv::GetInstance()->get_inverted_index_query_cache();
    }

    InvertedIndexQueryCache() = delete;

    InvertedIndexQueryCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_QUERY_CACHE, capacity,
                             LRUCacheType::SIZE, config::inverted_index_cache_stale_sweep_time_sec,
                             num_shards,
                             /*element_count_capacity*/ 0, /*enable_prune*/ true,
                             /*is_lru_k*/ true) {}

    bool lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle);

    void insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                InvertedIndexQueryCacheHandle* handle);
};

class InvertedIndexQueryCacheHandle {
public:
    InvertedIndexQueryCacheHandle() = default;

    InvertedIndexQueryCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~InvertedIndexQueryCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    InvertedIndexQueryCacheHandle(InvertedIndexQueryCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    InvertedIndexQueryCacheHandle& operator=(InvertedIndexQueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    LRUCachePolicy* cache() const { return _cache; }

    std::shared_ptr<roaring::Roaring> get_bitmap() const {
        if (!_cache) {
            return nullptr;
        }
        return ((InvertedIndexQueryCache::CacheValue*)_cache->value(_handle))->bitmap;
    }

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexQueryCacheHandle);
};

} // namespace segment_v2
} // namespace doris
