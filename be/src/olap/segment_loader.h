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

#include <memory>
#include <string>
#include <utility>

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"
#include "olap/olap_common.h" // for rowset id
#include "olap/rowset/beta_rowset.h"
#include "util/time.h"

namespace doris {

class SegmentCacheHandle;

// SegmentLoader is used to load the Segment of BetaRowset.
// An LRUCache is encapsulated inside it, which is used to cache the opened segments.
// The caller should use the following method to load and obtain
// the segments of a specified rowset:
//
//  SegmentCacheHandle cache_handle;
//  RETURN_NOT_OK(SegmentCache::instance()->load_segments(_rowset, &cache_handle));
//  for (auto& seg_ptr : cache_handle.value()->segments) {
//      ... visit segment ...
//  }
//
// Make sure that cache_handle is valid during the segment usage period.
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
class SegmentLoader {
public:
    // The cache key or segment lru cache
    struct CacheKey {
        CacheKey(RowsetId rowset_id_) : rowset_id(rowset_id_) {}
        RowsetId rowset_id;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const { return rowset_id.to_string(); }
    };

    // The cache value of segment lru cache.
    // Holding all opened segments of a rowset.
    struct CacheValue {
        // Save the last visit time of this cache entry.
        // Use atomic because it may be modified by multi threads.
        std::atomic<int64_t> last_visit_time = 0;
        std::vector<segment_v2::SegmentSharedPtr> segments;
    };

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

    SegmentLoader(size_t capacity);

    // Load segments of "rowset", return the "cache_handle" which contains segments.
    // If use_cache is true, it will be loaded from _cache.
    Status load_segments(const BetaRowsetSharedPtr& rowset, SegmentCacheHandle* cache_handle,
                         bool use_cache = false);

    // Try to prune the segment cache if expired.
    Status prune();

private:
    SegmentLoader();

    // Lookup the given rowset in the cache.
    // If the rowset is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool _lookup(const SegmentLoader::CacheKey& key, SegmentCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    void _insert(const SegmentLoader::CacheKey& key, CacheValue& value, SegmentCacheHandle* handle);

private:
    static SegmentLoader* _s_instance;
    // A LRU cache to cache all opened segments
    std::unique_ptr<Cache> _cache = nullptr;
};

// A handle for a single rowset from segment lru cache.
// The handle can ensure that the segment is valid
// and will not be closed while the holder of the handle is accessing the segment.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class SegmentCacheHandle {
public:
    SegmentCacheHandle() {}
    SegmentCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}

    ~SegmentCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            CHECK(segments.empty()) << segments.size();
            CHECK(!owned);
            // last_visit_time is set when release.
            // because it only be needed when pruning.
            ((SegmentLoader::CacheValue*)_cache->value(_handle))->last_visit_time = UnixMillis();
            _cache->release(_handle);
        }
    }

    SegmentCacheHandle(SegmentCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        this->owned = other.owned;
        this->segments = std::move(other.segments);
    }

    SegmentCacheHandle& operator=(SegmentCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        this->owned = other.owned;
        this->segments = std::move(other.segments);
        return *this;
    }

    std::vector<segment_v2::SegmentSharedPtr>& get_segments() {
        if (owned) {
            return segments;
        } else {
            return ((SegmentLoader::CacheValue*)_cache->value(_handle))->segments;
        }
    }

public:
    // If set to true, the loaded segments will be saved in segments, not in lru cache;
    bool owned = false;
    std::vector<segment_v2::SegmentSharedPtr> segments;

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(SegmentCacheHandle);
};

} // namespace doris
