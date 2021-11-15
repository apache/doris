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

StoragePageCache* StoragePageCache::_s_instance = nullptr;

void StoragePageCache::create_global_cache(size_t capacity, int32_t index_cache_percentage) {
    DCHECK(_s_instance == nullptr);
    static StoragePageCache instance(capacity, index_cache_percentage);
    _s_instance = &instance;
}

StoragePageCache::StoragePageCache(size_t capacity, int32_t index_cache_percentage)
        : _index_cache_percentage(index_cache_percentage),
          _mem_tracker(MemTracker::CreateTracker(capacity, "StoragePageCache", nullptr, true, true, MemTrackerLevel::OVERVIEW)) {
    if (index_cache_percentage == 0) {
        _data_page_cache = std::unique_ptr<Cache>(new_lru_cache("DataPageCache", capacity, _mem_tracker));
    } else if (index_cache_percentage == 100) {
        _index_page_cache = std::unique_ptr<Cache>(new_lru_cache("IndexPageCache", capacity, _mem_tracker));
    } else if (index_cache_percentage > 0 && index_cache_percentage < 100) {
        _data_page_cache = std::unique_ptr<Cache>(new_lru_cache("DataPageCache", capacity * (100 - index_cache_percentage) / 100, _mem_tracker));
        _index_page_cache = std::unique_ptr<Cache>(new_lru_cache("IndexPageCache", capacity * index_cache_percentage / 100, _mem_tracker));
    } else {
        CHECK(false) << "invalid index page cache percentage";
    }
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle, segment_v2::PageTypePB page_type) {
    auto cache = _get_page_cache(page_type);
    auto lru_handle = cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(cache, lru_handle);
    return true;
}

void StoragePageCache::insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type, bool in_memory) {
    auto deleter = [](const doris::CacheKey& key, void* value) { delete[](uint8_t*) value; };

    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto cache = _get_page_cache(page_type);
    auto lru_handle = cache->insert(key.encode(), data.data, data.size, deleter, priority);
    *handle = PageCacheHandle(cache, lru_handle);
}

} // namespace doris
