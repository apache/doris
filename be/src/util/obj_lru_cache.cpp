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

ObjLRUCache::ObjLRUCache(int64_t capacity, uint32_t num_shards) {
    _enabled = (capacity > 0);
    if (_enabled) {
        _cache = std::unique_ptr<Cache>(
                new_lru_cache("ObjLRUCache", capacity, LRUCacheType::NUMBER, num_shards));
    }
}

bool ObjLRUCache::lookup(const ObjKey& key, CacheHandle* handle) {
    if (!_enabled) {
        return false;
    }
    auto lru_handle = _cache->lookup(key.key);
    if (!lru_handle) {
        // cache miss
        return false;
    }
    *handle = CacheHandle(_cache.get(), lru_handle);
    return true;
}

void ObjLRUCache::erase(const ObjKey& key) {
    if (_enabled) {
        _cache->erase(key.key);
    }
}

} // namespace doris
