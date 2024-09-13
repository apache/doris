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

namespace doris {

using CacheResult = std::vector<vectorized::BlockUPtr>;
// A handle for mid-result from query lru cache.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class QueryCacheHandle {
public:
    QueryCacheHandle() = default;
    QueryCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~QueryCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            {
                SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                        ExecEnv::GetInstance()->query_cache_mem_tracker());
                _cache->release(_handle);
            }
        }
    }

    QueryCacheHandle(QueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    QueryCacheHandle& operator=(QueryCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    std::vector<int>* get_cache_slot_orders();

    CacheResult* get_cache_result();

    int64_t get_cache_version();

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(QueryCacheHandle);
};

class QueryCache : public LRUCachePolicyTrackingManual {
public:
    using LRUCachePolicyTrackingManual::insert;

    struct CacheValue : public LRUCacheValueBase {
        int64_t version;
        CacheResult result;
        std::vector<int> slot_orders;

        CacheValue(int64_t v, CacheResult&& r, const std::vector<int>& so)
                : LRUCacheValueBase(), version(v), result(std::move(r)), slot_orders(so) {}
    };

    // Create global instance of this class
    static QueryCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        auto* res = new QueryCache(capacity, num_shards);
        return res;
    }

    static Status build_cache_key(const std::vector<TScanRangeParams>& scan_ranges,
                                  const TQueryCacheParam& cache_param, std::string* cache_key,
                                  int64_t* version) {
        if (scan_ranges.size() > 1) {
            return Status::InternalError(
                    "CacheSourceOperator only support one scan range, plan error");
        }
        auto& scan_range = scan_ranges[0];
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        auto tablet_id = scan_range.scan_range.palo_scan_range.tablet_id;

        std::from_chars(scan_range.scan_range.palo_scan_range.version.data(),
                        scan_range.scan_range.palo_scan_range.version.data() +
                                scan_range.scan_range.palo_scan_range.version.size(),
                        *version);

        auto find_tablet = cache_param.tablet_to_range.find(tablet_id);
        if (find_tablet == cache_param.tablet_to_range.end()) {
            return Status::InternalError("Not find tablet in partition_to_tablets, plan error");
        }

        *cache_key = cache_param.digest +
                     std::string(reinterpret_cast<char*>(&tablet_id), sizeof(tablet_id)) +
                     find_tablet->second;

        return Status::OK();
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static QueryCache* instance() { return ExecEnv::GetInstance()->get_query_cache(); }

    QueryCache() = delete;

    QueryCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicyTrackingManual(CachePolicy::CacheType::QUERY_CACHE, capacity,
                                           LRUCacheType::SIZE, 3600 * 24, num_shards) {}

    bool lookup(const CacheKey& key, int64_t version, QueryCacheHandle* handle);

    void insert(const CacheKey& key, int64_t version, CacheResult& result,
                const std::vector<int>& solt_orders, int64_t cache_size);
};
} // namespace doris
