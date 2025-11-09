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
#include <roaring/roaring.hh>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/lru_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris::segment_v2 {

class ConditionCacheHandle;

class ConditionCache : public LRUCachePolicy {
public:
    using LRUCachePolicy::insert;

    // The cache key or segment lru cache
    struct CacheKey {
        CacheKey(RowsetId rowset_id_, int64_t segment_id_, uint64_t digest_)
                : rowset_id(rowset_id_), segment_id(segment_id_), digest(digest_) {}
        RowsetId rowset_id;
        int64_t segment_id;
        uint64_t digest;

        // Encode to a flat binary which can be used as LRUCache's key
        [[nodiscard]] std::string encode() const {
            char buf[16];
            memcpy(buf, &segment_id, 8);
            memcpy(buf + 8, &digest, 8);

            return rowset_id.to_string() + std::string(buf, 16);
        }
    };

    class CacheValue : public LRUCacheValueBase {
    public:
        std::shared_ptr<std::vector<bool>> filter_result;
    };

    // Create global instance of this class
    static ConditionCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        auto* res = new ConditionCache(capacity, num_shards);
        return res;
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static ConditionCache* instance() { return ExecEnv::GetInstance()->get_condition_cache(); }

    ConditionCache() = delete;

    ConditionCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::CONDITION_CACHE, capacity, LRUCacheType::SIZE,
                             config::inverted_index_cache_stale_sweep_time_sec, num_shards) {}

    bool lookup(const CacheKey& key, ConditionCacheHandle* handle);

    void insert(const CacheKey& key, std::shared_ptr<std::vector<bool>> filter_result);
};

class ConditionCacheHandle {
public:
    ConditionCacheHandle() = default;

    ConditionCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~ConditionCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    ConditionCacheHandle(ConditionCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    ConditionCacheHandle& operator=(ConditionCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    LRUCachePolicy* cache() const { return _cache; }

    std::shared_ptr<std::vector<bool>> get_filter_result() const {
        if (!_cache) {
            return nullptr;
        }
        return ((ConditionCache::CacheValue*)_cache->value(_handle))->filter_result;
    }

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(ConditionCacheHandle);
};

} // namespace doris::segment_v2
