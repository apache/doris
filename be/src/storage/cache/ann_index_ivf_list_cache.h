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
#include <string>
#include <utility>

#include "runtime/memory/lru_cache_policy.h"
#include "storage/cache/page_cache.h"

namespace doris {

// Dedicated LRU cache for IVF on-disk inverted list data.
//
// Each cache entry corresponds to one IVF list's codes or ids region,
// keyed by (file-prefix, file-size, region-offset).  This gives perfect
// cache alignment with the IVF access pattern: every search probes a
// fixed set of lists and each list's codes/ids are always the same
// (offset, size) pair, so repeated queries hit the cache without any
// partial-block copies.
//
// Separated from StoragePageCache so that:
//   1. IVF list data does not compete with column-data / index pages.
//   2. Capacity can be tuned independently (default: 70% of physical memory
//      when ann_index_ivf_list_cache_limit == "0").
class AnnIndexIVFListCache {
public:
    // Reuse the same CacheKey format as StoragePageCache for consistency.
    // offset field stores the exact byte offset of the list's codes or ids.
    using CacheKey = StoragePageCache::CacheKey;

    class CacheImpl : public LRUCachePolicy {
    public:
        CacheImpl(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::ANN_INDEX_IVF_LIST_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::ann_index_ivf_list_cache_stale_sweep_time_sec, num_shards,
                                 /*element_count_capacity*/ 0, /*enable_prune*/ true,
                                 /*is lru-k*/ false) {}
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    // --------------- singleton ---------------
    static AnnIndexIVFListCache* instance() { return _s_instance; }

    static AnnIndexIVFListCache* create_global_cache(size_t capacity,
                                                     uint32_t num_shards = kDefaultNumShards);
    static void destroy_global_cache();

    // --------------- ctor / dtor ---------------
    AnnIndexIVFListCache(size_t capacity, uint32_t num_shards);
    ~AnnIndexIVFListCache() = default;

    AnnIndexIVFListCache(const AnnIndexIVFListCache&) = delete;
    AnnIndexIVFListCache& operator=(const AnnIndexIVFListCache&) = delete;

    // --------------- operations ---------------

    // Lookup a cached entry.  Returns true on hit; `handle` is populated and
    // the caller must let it go out of scope (or call reset()) to unpin.
    bool lookup(const CacheKey& key, PageCacheHandle* handle);

    // Insert an entry into the cache.  On success the cache takes ownership of
    // `page` (via the handle).  Caller must release() the unique_ptr.
    void insert(const CacheKey& key, DataPage* page, PageCacheHandle* handle);

    std::shared_ptr<MemTrackerLimiter> mem_tracker() { return _cache->mem_tracker(); }

private:
    static AnnIndexIVFListCache* _s_instance;

    std::unique_ptr<CacheImpl> _cache;
};

} // namespace doris
