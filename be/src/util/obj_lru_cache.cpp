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

#include "util/obj_lru_cache.h"

namespace doris {

ObjLRUCache::ObjLRUCache(int64_t capacity, uint32_t num_shards, LRUCacheType type,
                         CachePolicy::CacheType cache_type)
        : LRUCachePolicy(cache_type, capacity, type,
                         config::common_obj_lru_cache_stale_sweep_time_sec, num_shards,
                         /*element count capacity*/ 0, /*enable prune*/ true,
                         /*is lru-k*/ type == LRUCacheType::NUMBER),
          _enabled(capacity >= num_shards) {}

bool ObjLRUCache::lookup(const ObjKey& key, CacheHandle* handle) {
    if (!_enabled) {
        return false;
    }
    auto* lru_handle = LRUCachePolicy::lookup(key.key);
    if (!lru_handle) {
        // cache miss
        return false;
    }
    *handle = CacheHandle(this, lru_handle);
    return true;
}

void ObjLRUCache::erase(const ObjKey& key) {
    if (_enabled) {
        LRUCachePolicy::erase(key.key);
    }
}

bool ObjLRUCache::exceed_prune_limit() {
    // Keep stale sweeps enabled even for object caches whose byte budget is smaller than
    // the size-cache GC threshold. Expiration must not depend on the current footprint.
    return true;
}

} // namespace doris
