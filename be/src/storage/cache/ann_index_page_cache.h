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

// Dedicated LRU cache for ANN index (IVF on disk) data pages.
//
// Separated from StoragePageCache so that:
//   1. ANN index blocks do not compete with column-data / index pages for
//      cache space.
//   2. Capacity can be tuned independently (default: 70% of physical memory
//      when ann_index_page_cache_limit == "0").
//   3. No page-type routing overhead – every entry is an ANN index block.
class AnnIndexDataPageCache {
public:
    // Reuse the same CacheKey format as StoragePageCache for consistency.
    using CacheKey = StoragePageCache::CacheKey;

    class AnnIndexPageCacheImpl : public LRUCachePolicy {
    public:
        AnnIndexPageCacheImpl(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::ANN_INDEX_PAGE_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::ann_index_page_cache_stale_sweep_time_sec, num_shards,
                                 /*element_count_capacity*/ 0, /*enable_prune*/ true,
                                 /*is lru-k*/ false) {}
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    // --------------- singleton ---------------
    static AnnIndexDataPageCache* instance() { return _s_instance; }

    static AnnIndexDataPageCache* create_global_cache(size_t capacity,
                                                      uint32_t num_shards = kDefaultNumShards);
    static void destroy_global_cache();

    // --------------- ctor / dtor ---------------
    AnnIndexDataPageCache(size_t capacity, uint32_t num_shards);
    ~AnnIndexDataPageCache() = default;

    AnnIndexDataPageCache(const AnnIndexDataPageCache&) = delete;
    AnnIndexDataPageCache& operator=(const AnnIndexDataPageCache&) = delete;

    // --------------- operations ---------------

    // Lookup a cached block.  Returns true on hit; `handle` is populated and
    // the caller must let it go out of scope (or call reset()) to unpin.
    bool lookup(const CacheKey& key, PageCacheHandle* handle);

    // Insert a block into the cache.  On success the cache takes ownership of
    // `page` (via the handle).  Caller must release() the unique_ptr.
    void insert(const CacheKey& key, DataPage* page, PageCacheHandle* handle);

    std::shared_ptr<MemTrackerLimiter> mem_tracker() { return _cache->mem_tracker(); }

private:
    static AnnIndexDataPageCache* _s_instance;

    std::unique_ptr<AnnIndexPageCacheImpl> _cache;
};

} // namespace doris
