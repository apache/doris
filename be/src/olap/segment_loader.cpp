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

SegmentLoader* SegmentLoader::instance() {
    return ExecEnv::GetInstance()->segment_loader();
}

bool SegmentCache::lookup(const SegmentCache::CacheKey& key, SegmentCacheHandle* handle) {
    auto lru_handle = cache()->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    handle->push_segment(cache(), lru_handle);
    return true;
}

void SegmentCache::insert(const SegmentCache::CacheKey& key, SegmentCache::CacheValue& value,
                          SegmentCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        SegmentCache::CacheValue* cache_value = (SegmentCache::CacheValue*)value;
        cache_value->segment.reset();
        delete cache_value;
    };

    auto lru_handle = cache()->insert(key.encode(), &value, 1, deleter, CachePriority::NORMAL,
                                      value.segment->meta_mem_usage());
    handle->push_segment(cache(), lru_handle);
}

void SegmentCache::erase(const SegmentCache::CacheKey& key) {
    cache()->erase(key.encode());
}

Status SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                    SegmentCacheHandle* cache_handle, bool use_cache) {
    if (cache_handle->is_inited()) {
        return Status::OK();
    }
    for (int64_t i = 0; i < rowset->num_segments(); i++) {
        SegmentCache::CacheKey cache_key(rowset->rowset_id(), i);
        if (_segment_cache->lookup(cache_key, cache_handle)) {
            continue;
        }
        segment_v2::SegmentSharedPtr segment;
        RETURN_IF_ERROR(rowset->load_segment(i, &segment));
        if (use_cache && !config::disable_segment_cache) {
            // memory of SegmentCache::CacheValue will be handled by SegmentCache
            SegmentCache::CacheValue* cache_value = new SegmentCache::CacheValue();
            cache_value->segment = std::move(segment);
            _segment_cache->insert(cache_key, *cache_value, cache_handle);
        } else {
            cache_handle->push_segment(std::move(segment));
        }
    }
    cache_handle->set_inited();
    return Status::OK();
}

void SegmentLoader::erase_segment(const SegmentCache::CacheKey& key) {
    _segment_cache->erase(key);
}

void SegmentLoader::erase_segments(const RowsetId& rowset_id, int64_t num_segments) {
    for (int64_t i = 0; i < num_segments; i++) {
        erase_segment(SegmentCache::CacheKey(rowset_id, i));
    }
}

} // namespace doris
