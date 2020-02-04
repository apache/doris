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

#include "olap/page_cache.h"

namespace doris {

// This should only be used in unit test. 1GB
static StoragePageCache s_ut_cache(1073741824);

StoragePageCache* StoragePageCache::_s_instance = &s_ut_cache;

void StoragePageCache::create_global_cache(size_t capacity) {
    if (_s_instance == &s_ut_cache) {
        _s_instance = new StoragePageCache(capacity);
    }
}

StoragePageCache::StoragePageCache(size_t capacity) : _cache(new_lru_cache(capacity)) {
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(_cache.get(), lru_handle);
    return true;
}

void StoragePageCache::insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle,
                              bool in_memory) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        delete[] (uint8_t*)value;
    };

    CachePriority priority =  CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto lru_handle = _cache->insert(key.encode(), data.data, data.size, deleter, priority);
    *handle = PageCacheHandle(_cache.get(), lru_handle);
}

}
