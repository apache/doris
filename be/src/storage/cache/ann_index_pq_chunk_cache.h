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

#include <stddef.h>
#include <stdint.h>

#include <memory>

#include "runtime/memory/lru_cache_policy.h"
#include "storage/cache/page_cache.h"

namespace doris {

// Dedicated LRU cache for PQ_ON_DISK fixed-size chunk data.
//
// Each cache entry corresponds to one fixed-size chunk of PQ codes in
// ann.pqdata, keyed by (file-prefix, file-size, chunk-byte-offset).
// Chunk size is code_size-aligned to ~64KB so each chunk contains an
// integral number of PQ code vectors.
//
// Zero-copy design: ADC distance computation reads directly from cached
// chunk memory via the _for_each_code_in_bitmap template, without any
// intermediate buffer copy.
//
// Unlike AnnIndexIVFListCache, this cache participates in dynamic capacity
// adjustment (not skipped in CacheManager::for_each_cache_refresh_capacity)
// because chunk miss cost is low (a single 64KB sequential read).
class AnnIndexPqChunkCache {
public:
    // Reuse the same CacheKey format as StoragePageCache for consistency.
    using CacheKey = StoragePageCache::CacheKey;

    class CacheImpl : public LRUCachePolicy {
    public:
        CacheImpl(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::ANN_INDEX_PQ_CHUNK_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::ann_index_pq_chunk_cache_stale_sweep_time_sec, num_shards,
                                 /*element_count_capacity*/ 0, /*enable_prune*/ true,
                                 /*is lru-k*/ false) {}
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    static AnnIndexPqChunkCache* create_global_cache(size_t capacity,
                                                     uint32_t num_shards = kDefaultNumShards);

    // --------------- ctor / dtor ---------------
    AnnIndexPqChunkCache(size_t capacity, uint32_t num_shards);
    ~AnnIndexPqChunkCache() = default;

    AnnIndexPqChunkCache(const AnnIndexPqChunkCache&) = delete;
    AnnIndexPqChunkCache& operator=(const AnnIndexPqChunkCache&) = delete;

    // --------------- operations ---------------

    // Lookup a cached chunk.  Returns true on hit; `handle` is populated and
    // the caller must let it go out of scope (or call reset()) to unpin.
    bool lookup(const CacheKey& key, PageCacheHandle* handle);

    // Insert a chunk into the cache.  On success the cache takes ownership of
    // `page` (via the handle).  Caller must release() the unique_ptr before calling.
    void insert(const CacheKey& key, DataPage* page, PageCacheHandle* handle);

    std::shared_ptr<MemTrackerLimiter> mem_tracker() { return _cache->mem_tracker(); }

private:
    std::unique_ptr<CacheImpl> _cache;
};

} // namespace doris
