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
#include "util/stopwatch.hpp"

namespace doris {

SegmentLoader* SegmentLoader::_s_instance = nullptr;

void SegmentLoader::create_global_instance(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static SegmentLoader instance(capacity);
    _s_instance = &instance;
}

SegmentLoader::SegmentLoader(size_t capacity)
        : _mem_tracker(MemTracker::CreateTracker(capacity, "SegmentLoader", nullptr, true, true,
                                                 MemTrackerLevel::OVERVIEW)) {
    _cache = std::unique_ptr<Cache>(
            new_typed_lru_cache("SegmentCache", capacity, LRUCacheType::NUMBER, _mem_tracker));
}

bool SegmentLoader::_lookup(const SegmentLoader::CacheKey& key, SegmentCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
    return true;
}

void SegmentLoader::_insert(const SegmentLoader::CacheKey& key, SegmentLoader::CacheValue& value,
                            SegmentCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        SegmentLoader::CacheValue* cache_value = (SegmentLoader::CacheValue*)value;
        cache_value->segments.clear();
        delete cache_value;
    };

    auto lru_handle = _cache->insert(key.encode(), &value, sizeof(SegmentLoader::CacheValue),
                                     deleter, CachePriority::NORMAL);
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
}

OLAPStatus SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                        SegmentCacheHandle* cache_handle, bool use_cache) {
    SegmentLoader::CacheKey cache_key(rowset->rowset_id());
    if (_lookup(cache_key, cache_handle)) {
        cache_handle->owned = false;
        return OLAP_SUCCESS;
    }
    cache_handle->owned = !use_cache;

    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_NOT_OK(rowset->load_segments(&segments));

    if (use_cache) {
        // memory of SegmentLoader::CacheValue will be handled by SegmentLoader
        SegmentLoader::CacheValue* cache_value = new SegmentLoader::CacheValue();
        cache_value->segments = std::move(segments);
        _insert(cache_key, *cache_value, cache_handle);
    } else {
        cache_handle->segments = std::move(segments);
    }

    return OLAP_SUCCESS;
}

OLAPStatus SegmentLoader::prune() {
    const int64_t curtime = UnixMillis();
    auto pred = [curtime](const void* value) -> bool {
        SegmentLoader::CacheValue* cache_value = (SegmentLoader::CacheValue*)value;
        return (cache_value->last_visit_time + config::tablet_rowset_stale_sweep_time_sec * 1000) <
               curtime;
    };

    MonotonicStopWatch watch;
    watch.start();
    int64_t prune_num = _cache->prune_if(pred);
    LOG(INFO) << "prune " << prune_num
              << " entries in segment cache. cost(ms): " << watch.elapsed_time() / 1000 / 1000;
    return OLAP_SUCCESS;
}

} // namespace doris
