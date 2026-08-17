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
#include <string_view>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_bkd_searcher.h"
#include "util/lru_cache.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {
class InvertedIndexCacheHandle;
class IndexFileReader;

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
        std::shared_ptr<IndexFileReader> snii_index_file_reader;
        std::unique_ptr<doris::snii::reader::LogicalIndexReader> snii_logical_reader;
        // The numeric counterpart of snii_logical_reader: an opened SNII-native
        // BKD blob index. A cache entry holds exactly one of the two, decided by
        // which constructor ran.
        std::unique_ptr<doris::snii::bkd::BkdSearcher> snii_bkd_searcher;
        size_t size = 0;
        int64_t last_visit_time;

        CacheValue() = default;
        explicit CacheValue(IndexSearcherPtr searcher, size_t mem_size, int64_t visit_time)
                : index_searcher(std::move(searcher)) {
            size = mem_size;
            last_visit_time = visit_time;
        }
        explicit CacheValue(std::unique_ptr<doris::snii::reader::LogicalIndexReader> logical_reader,
                            size_t mem_size, int64_t visit_time,
                            std::shared_ptr<IndexFileReader> index_file_reader)
                : snii_index_file_reader(std::move(index_file_reader)),
                  snii_logical_reader(std::move(logical_reader)) {
            size = mem_size;
            last_visit_time = visit_time;
        }
        explicit CacheValue(std::unique_ptr<doris::snii::bkd::BkdSearcher> bkd_searcher,
                            size_t mem_size, int64_t visit_time,
                            std::shared_ptr<IndexFileReader> index_file_reader)
                : snii_index_file_reader(std::move(index_file_reader)),
                  snii_bkd_searcher(std::move(bkd_searcher)) {
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

    doris::snii::reader::LogicalIndexReader* get_snii_logical_reader() {
        return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))
                ->snii_logical_reader.get();
    }

    doris::snii::bkd::BkdSearcher* get_snii_bkd_searcher() {
        return ((InvertedIndexSearcherCache::CacheValue*)_cache->value(_handle))
                ->snii_bkd_searcher.get();
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

inline constexpr uint32_t INVERTED_INDEX_QUERY_CACHE_SEMANTICS_VERSION = 1;

// Stable identity shared by result-cache and row-accurate single-flight. It intentionally contains
// no analyzer output or internal plan kind: those are segment-local implementation details below
// the cache lookup.
struct InvertedIndexRawQuerySemantic {
    std::string_view raw_query_bytes;
    InvertedIndexQueryType query_type;
    int32_t slop = 0;
    bool ordered = false;
    int32_t max_expansions = 0;
    uint32_t cache_semantics_version = INVERTED_INDEX_QUERY_CACHE_SEMANTICS_VERSION;
    bool common_grams_query_plan_enabled = false;

    std::string encode() const;
};

class InvertedIndexQueryCache : public LRUCachePolicy {
public:
    using LRUCachePolicy::insert;

    // cache key
    struct CacheKey {
        io::Path index_path;               // index file path
        std::string column_name;           // column name
        InvertedIndexQueryType query_type; // query type
        std::string value;                 // query value

        // Encode to an unambiguous flat binary which can be used as LRUCache's key.
        std::string encode() const;
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
