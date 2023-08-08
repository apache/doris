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

#include "common/config.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "util/stopwatch.hpp"

namespace doris {

SegmentLoader* SegmentLoader::_s_instance = nullptr;

void SegmentLoader::create_global_instance(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static SegmentLoader instance(capacity);
    _s_instance = &instance;
}

bool SegmentCache::lookup(const SegmentCache::CacheKey& key, SegmentCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
    return true;
}

void SegmentCache::insert(const SegmentCache::CacheKey& key, SegmentCache::CacheValue& value,
                          SegmentCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        SegmentCache::CacheValue* cache_value = (SegmentCache::CacheValue*)value;
        cache_value->segments.clear();
        delete cache_value;
    };

    int64_t meta_mem_usage = 0;
    for (auto segment : value.segments) {
        meta_mem_usage += segment->meta_mem_usage();
    }

    auto lru_handle = _cache->insert(key.encode(), &value, sizeof(SegmentCache::CacheValue),
                                     deleter, CachePriority::NORMAL, meta_mem_usage);
    *handle = SegmentCacheHandle(_cache.get(), lru_handle);
}

void SegmentCache::erase(const SegmentCache::CacheKey& key) {
    _cache->erase(key.encode());
}

Status SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                    SegmentCacheHandle* cache_handle, bool use_cache) {
    SegmentCache::CacheKey cache_key(rowset->rowset_id());
    if (_segment_cache->lookup(cache_key, cache_handle)) {
        cache_handle->owned = false;
        return Status::OK();
    }
    cache_handle->owned = !use_cache;

    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(rowset->load_segments(&segments));

    if (use_cache) {
        // memory of SegmentCache::CacheValue will be handled by SegmentCache
        SegmentCache::CacheValue* cache_value = new SegmentCache::CacheValue();
        cache_value->segments = std::move(segments);
        _segment_cache->insert(cache_key, *cache_value, cache_handle);
    } else {
        cache_handle->segments = std::move(segments);
    }

    return Status::OK();
}

void SegmentLoader::erase_segments(const SegmentCache::CacheKey& key) {
    _segment_cache->erase(key);
}

} // namespace doris
