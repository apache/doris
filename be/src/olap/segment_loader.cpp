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
#include "common/status.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "util/stopwatch.hpp"

namespace doris {

SegmentLoader* SegmentLoader::instance() {
    return ExecEnv::GetInstance()->segment_loader();
}

bool SegmentCache::lookup(const SegmentCache::CacheKey& key, SegmentCacheHandle* handle) {
    auto* lru_handle = LRUCachePolicy::lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    handle->push_segment(this, lru_handle);
    return true;
}

void SegmentCache::insert(const SegmentCache::CacheKey& key, SegmentCache::CacheValue& value,
                          SegmentCacheHandle* handle) {
    auto* lru_handle = LRUCachePolicyTrackingManual::insert(
            key.encode(), &value, value.segment->meta_mem_usage(), value.segment->meta_mem_usage(),
            CachePriority::NORMAL);
    handle->push_segment(this, lru_handle);
}

void SegmentCache::erase(const SegmentCache::CacheKey& key) {
    LRUCachePolicy::erase(key.encode());
}

Status SegmentLoader::load_segments(const BetaRowsetSharedPtr& rowset,
                                    SegmentCacheHandle* cache_handle, bool use_cache,
                                    bool need_load_pk_index_and_bf) {
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
        if (need_load_pk_index_and_bf) {
            RETURN_IF_ERROR(segment->load_pk_index_and_bf());
        }
        if (use_cache && !config::disable_segment_cache) {
            // memory of SegmentCache::CacheValue will be handled by SegmentCache
            auto* cache_value = new SegmentCache::CacheValue();
            _cache_mem_usage += segment->meta_mem_usage();
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
