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

#include <glog/logging.h>

#include <ostream>

#include "runtime/exec_env.h"

namespace doris {
StoragePageCache* StoragePageCache::create_global_cache(size_t capacity,
                                                        int32_t index_cache_percentage,
                                                        int64_t pk_index_cache_capacity,
                                                        uint32_t num_shards) {
    StoragePageCache* res = new StoragePageCache(capacity, index_cache_percentage,
                                                 pk_index_cache_capacity, num_shards);
    return res;
}

StoragePageCache::StoragePageCache(size_t capacity, int32_t index_cache_percentage,
                                   int64_t pk_index_cache_capacity, uint32_t num_shards)
        : _index_cache_percentage(index_cache_percentage) {
    if (index_cache_percentage == 0) {
        _data_page_cache = std::make_unique<DataPageCache>(capacity, num_shards);
    } else if (index_cache_percentage == 100) {
        _index_page_cache = std::make_unique<IndexPageCache>(capacity, num_shards);
    } else if (index_cache_percentage > 0 && index_cache_percentage < 100) {
        _data_page_cache = std::make_unique<DataPageCache>(
                capacity * (100 - index_cache_percentage) / 100, num_shards);
        _index_page_cache = std::make_unique<IndexPageCache>(
                capacity * index_cache_percentage / 100, num_shards);
    } else {
        CHECK(false) << "invalid index page cache percentage";
    }

    _pk_index_page_cache = std::make_unique<PKIndexPageCache>(pk_index_cache_capacity, num_shards);
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type) {
    auto cache = _get_page_cache(page_type);
    auto lru_handle = cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(cache, lru_handle);
    handle->update_last_visit_time();
    return true;
}

void StoragePageCache::insert(const CacheKey& key, DataPage* data, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type, bool in_memory) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        DataPage* cache_value = (DataPage*)value;
        delete cache_value;
    };

    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto cache = _get_page_cache(page_type);
    auto lru_handle = cache->insert(key.encode(), data, data->capacity(), deleter, priority);
    *handle = PageCacheHandle(cache, lru_handle);
    handle->update_last_visit_time();
}

} // namespace doris
