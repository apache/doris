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

#pragma once

#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h" // for rowset id
#include "olap/rowset/segment_v2/segment.h"
#include "runtime/memory/lru_cache_policy.h"
#include "util/time.h"

namespace doris {

class SegmentCacheHandle;
class BetaRowset;

// SegmentLoader is used to load the Segment of BetaRowset.
// An LRUCache is encapsulated inside it, which is used to cache the opened segments.
// The caller should use the following method to load and obtain
// the segments of a specified rowset:
//
//  SegmentCacheHandle cache_handle;
//  RETURN_IF_ERROR(SegmentCache::instance()->load_segments(_rowset, &cache_handle));
//  for (auto& seg_ptr : cache_handle.value()->segments) {
//      ... visit segment ...
//  }
//
// Make sure that cache_handle is valid during the segment usage period.
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;

class SegmentCache : public LRUCachePolicy {
public:
    // The cache key or segment lru cache
    struct CacheKey {
        CacheKey(RowsetId rowset_id_) : rowset_id(rowset_id_) {}
        RowsetId rowset_id;

        // Encode to a flat binary which can be used as LRUCache's key
        [[nodiscard]] std::string encode() const { return rowset_id.to_string(); }
    };

    // The cache value of segment lru cache.
    // Holding all opened segments of a rowset.
    struct CacheValue : public LRUCacheValueBase {
        std::vector<segment_v2::SegmentSharedPtr> segments;
    };

    SegmentCache(size_t capacity)
            : LRUCachePolicy("SegmentCache", capacity, LRUCacheType::NUMBER,
                             config::tablet_rowset_stale_sweep_time_sec) {}

    // Lookup the given rowset in the cache.
    // If the rowset is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool lookup(const SegmentCache::CacheKey& key, SegmentCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    void insert(const SegmentCache::CacheKey& key, CacheValue& value, SegmentCacheHandle* handle);

    void erase(const SegmentCache::CacheKey& key);
};

class SegmentLoader {
public:
    // Create global instance of this class.
    // "capacity" is the capacity of lru cache.
    // TODO: Currently we use the number of rowset as the cache capacity.
    // That is, the limit of cache is the number of rowset.
    // This is because currently we cannot accurately estimate the memory occupied by a segment.
    // After the estimation of segment memory usage is provided later, it is recommended
    // to use Memory as the capacity limit of the cache.
    static void create_global_instance(size_t capacity);

    // Return global instance.
    // Client should call create_global_cache before.
    static SegmentLoader* instance() { return _s_instance; }

    SegmentLoader(size_t capacity) { _segment_cache = std::make_unique<SegmentCache>(capacity); }

    // Load segments of "rowset", return the "cache_handle" which contains segments.
    // If use_cache is true, it will be loaded from _cache.
    Status load_segments(const BetaRowsetSharedPtr& rowset, SegmentCacheHandle* cache_handle,
                         bool use_cache = false, bool need_load_pk_index_and_bf = false);

    void erase_segments(const SegmentCache::CacheKey& key);

    // Just used for BE UT
    int64_t cache_mem_usage() const { return _cache_mem_usage; }

private:
    SegmentLoader();

    static SegmentLoader* _s_instance;
    std::unique_ptr<SegmentCache> _segment_cache;
    // Just used for BE UT
    int64_t _cache_mem_usage = 0;
};

// A handle for a single rowset from segment lru cache.
// The handle can ensure that the segment is valid
// and will not be closed while the holder of the handle is accessing the segment.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class SegmentCacheHandle {
public:
    SegmentCacheHandle() = default;

    ~SegmentCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            CHECK(_segments.empty()) << _segments.size();
            CHECK(!_owned);
            // last_visit_time is set when release.
            // because it only be needed when pruning.
            ((SegmentCache::CacheValue*)_cache->value(_handle))->last_visit_time = UnixMillis();
            _cache->release(_handle);
        }
    }

    [[nodiscard]] bool is_inited() const { return _init; }

    void init(std::vector<segment_v2::SegmentSharedPtr> segments) {
        DCHECK(!_init);
        _owned = true;
        _segments = std::move(segments);
        _init = true;
    }

    void init(Cache* cache, Cache::Handle* handle) {
        DCHECK(!_init);
        _owned = false;
        _cache = cache;
        _handle = handle;
        _init = true;
    }

    std::vector<segment_v2::SegmentSharedPtr>& get_segments() {
        if (_owned) {
            return _segments;
        } else {
            return ((SegmentCache::CacheValue*)_cache->value(_handle))->segments;
        }
    }

private:
    bool _init {false};
    bool _owned {false};
    std::vector<segment_v2::SegmentSharedPtr> _segments;
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(SegmentCacheHandle);
};

} // namespace doris
