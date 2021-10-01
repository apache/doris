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

#include "olap/segment_loader.h"

#include "olap/rowset/rowset.h"

namespace doris {

SegmentLoader* SegmentLoader::_s_instance = nullptr;

void SegmentLoader::create_global_instance(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static SegmentLoader instance(capacity);
    _s_instance = &instance;
}

SegmentLoader::SegmentLoader(size_t capacity)
    : _mem_tracker(MemTracker::CreateTracker(capacity, "SegmentLoader", nullptr, true, true, MemTrackerLevel::OVERVIEW)) {
        _cache = std::unique_ptr<Cache>(new_typed_lru_cache("SegmentCache", capacity, LRUCacheType::NUMBER, _mem_tracker));
}

bool SegmentLoader::_lookup(const SegmentLoader::CacheKey& key, SegmentCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
    return true;
}

void SegmentLoader::_insert(const SegmentLoader::CacheKey& key, SegmentLoader::CacheValue& value, SegmentCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        SegmentLoader::CacheValue* cache_value = (SegmentLoader::CacheValue*) value;
        cache_value->segments.clear();
        // LOG(INFO) << "delete segment cache for rowset: ";
        delete cache_value;
    };

    auto lru_handle = _cache->insert(key.encode(), &value, sizeof(SegmentLoader::CacheValue), deleter, CachePriority::NORMAL);
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
}

OLAPStatus SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                       SegmentCacheHandle* cache_handle) {
    SegmentCacheHandle handle;
    SegmentLoader::CacheKey cache_key(rowset->rowset_id());
    if (_lookup(cache_key, &handle)) {
        *cache_handle = std::move(handle);
        return OLAP_SUCCESS;
    }

    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_NOT_OK(rowset->load_segments(&segments));

    // memory of SegmentLoader::CacheValue will be handled by SegmentLoader
    SegmentLoader::CacheValue* cache_value = new SegmentLoader::CacheValue();
    cache_value->segments = std::move(segments);
    _insert(cache_key, *cache_value, &handle);
    // LOG(INFO) << "insert segment cache for rowset: " << cache_key.encode();
    *cache_handle = std::move(handle);
    
    return OLAP_SUCCESS;
}

} // namespace doris
