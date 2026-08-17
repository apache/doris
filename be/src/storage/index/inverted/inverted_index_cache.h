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
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "util/lru_cache.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {
class InvertedIndexCacheHandle;
class InvertedIndexTermBloomFilter;

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
        // A token-exists Bloom Filter entry reuses this same cache (same LRU + memory budget) under
        // a distinct key namespace, so it has no opened searcher: index_searcher is null and the
        // payload is term_bf. A normal searcher entry has term_bf null. The two never share a key.
        std::shared_ptr<InvertedIndexTermBloomFilter> term_bf;
        size_t size = 0;
        int64_t last_visit_time;

        CacheValue() = default;
        explicit CacheValue(IndexSearcherPtr searcher, size_t mem_size, int64_t visit_time)
                : index_searcher(std::move(searcher)) {
            size = mem_size;
            last_visit_time = visit_time;
        }
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

    // Token-exists Bloom Filter sub-API. The BF for a (segment, index) is cached in THIS cache
    // (same LRU + memory budget, no separate cache) under an internal key namespace derived from
    // the same `index_file_key` the searcher entry uses -- callers pass the logical key and never
    // see the namespacing, so it cannot drift between insert/lookup/erase. On a hit the handle
    // exposes the BF via get_term_bf().
    bool lookup_term_bf(const std::string& index_file_key, InvertedIndexCacheHandle* handle);
    void insert_term_bf(const std::string& index_file_key,
                        std::shared_ptr<InvertedIndexTermBloomFilter> bf,
                        InvertedIndexCacheHandle* handle);
    // Cache a negative result: this (segment, index) has no usable "tbf" sub-file (never built or
    // corrupt). This is stable for the immutable segment, so eligible queries can skip re-opening
    // the index. A lookup_term_bf() hit whose get_term_bf() is null IS this negative marker. (Only
    // structural absence is cached negative -- an analyzer-signature mismatch is reader-dependent
    // and must NOT be, or it would deny a valid BF to a matching-analyzer reader on the same key.)
    void insert_term_bf_negative(const std::string& index_file_key,
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

    // Derive the BF entry's key from a (segment, index) identity. The '\x01' byte never occurs in
    // an index file path, so a BF key can never collide with a searcher key. This is the single
    // place that knows the namespace -- insert/lookup/erase all route through it, so the searcher
    // entry and its BF entry stay in lockstep (notably: erase() removes both).
    static std::string term_bf_key(const std::string& index_file_key) {
        return index_file_key + std::string("\x01tbf", 4);
    }

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

    // The token-exists Bloom Filter held by a BF-namespace cache entry (null for searcher entries).
    std::shared_ptr<InvertedIndexTermBloomFilter> get_term_bf() {
        if (_cache == nullptr) {
            return nullptr;
        }
        return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))->term_bf;
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
