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

#include "storage/cache/ann_index_ivf_list_cache.h"

#include <glog/logging.h>

#include "common/config.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

AnnIndexIVFListCache* AnnIndexIVFListCache::_s_instance = nullptr;

AnnIndexIVFListCache* AnnIndexIVFListCache::create_global_cache(size_t capacity,
                                                                uint32_t num_shards) {
    DCHECK(_s_instance == nullptr);
    _s_instance = new AnnIndexIVFListCache(capacity, num_shards);
    return _s_instance;
}

void AnnIndexIVFListCache::destroy_global_cache() {
    delete _s_instance;
    _s_instance = nullptr;
}

AnnIndexIVFListCache::AnnIndexIVFListCache(size_t capacity, uint32_t num_shards) {
    _cache = std::make_unique<CacheImpl>(capacity, num_shards);
}

bool AnnIndexIVFListCache::lookup(const CacheKey& key, PageCacheHandle* handle) {
    auto* lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(_cache.get(), lru_handle);
    return true;
}

void AnnIndexIVFListCache::insert(const CacheKey& key, DataPage* page, PageCacheHandle* handle) {
    CachePriority priority = CachePriority::NORMAL;
    auto* lru_handle = _cache->insert(key.encode(), page, page->capacity(), 0, priority);
    DCHECK(lru_handle != nullptr);
    *handle = PageCacheHandle(_cache.get(), lru_handle);
}

} // namespace doris
