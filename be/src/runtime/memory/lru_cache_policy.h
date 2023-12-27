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

#include "olap/lru_cache.h"
#include "runtime/memory/cache_policy.h"
#include "util/time.h"

namespace doris {

// Base of the lru cache value.
struct LRUCacheValueBase {
    // Save the last visit time of this cache entry.
    // Use atomic because it may be modified by multi threads.
    std::atomic<int64_t> last_visit_time = 0;
    size_t size = 0;
};

// Base of lru cache, allow prune stale entry and prune all entry.
class LRUCachePolicy : public CachePolicy {
public:
    LRUCachePolicy(CacheType type, uint32_t stale_sweep_time_s)
            : CachePolicy(type, stale_sweep_time_s) {};
    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards = -1)
            : CachePolicy(type, stale_sweep_time_s) {
        _cache = num_shards == -1
                         ? std::unique_ptr<Cache>(
                                   new ShardedLRUCache(type_string(type), capacity, lru_cache_type))
                         : std::unique_ptr<Cache>(new ShardedLRUCache(type_string(type), capacity,
                                                                      lru_cache_type, num_shards));
    }

    ~LRUCachePolicy() override = default;

    // Try to prune the cache if expired.
    void prune_stale() override {
        if (_cache->mem_consumption() > CACHE_MIN_FREE_SIZE) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            const int64_t curtime = UnixMillis();
            int64_t byte_size = 0L;
            auto pred = [this, curtime, &byte_size](const void* value) -> bool {
                LRUCacheValueBase* cache_value = (LRUCacheValueBase*)value;
                if ((cache_value->last_visit_time + _stale_sweep_time_s * 1000) < curtime) {
                    byte_size += cache_value->size;
                    return true;
                }
                return false;
            };

            // Prune cache in lazy mode to save cpu and minimize the time holding write lock
            COUNTER_SET(_freed_entrys_counter, _cache->prune_if(pred, true));
            COUNTER_SET(_freed_memory_counter, byte_size);
            COUNTER_UPDATE(_prune_stale_number_counter, 1);
            LOG(INFO) << fmt::format("{} prune stale {} entries, {} bytes, {} times prune",
                                     type_string(_type), _freed_entrys_counter->value(),
                                     _freed_memory_counter->value(),
                                     _prune_stale_number_counter->value());
        }
    }

    void prune_all(bool clear) override {
        if ((clear && _cache->mem_consumption() != 0) ||
            _cache->mem_consumption() > CACHE_MIN_FREE_SIZE) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            auto size = _cache->mem_consumption();
            COUNTER_SET(_freed_entrys_counter, _cache->prune());
            COUNTER_SET(_freed_memory_counter, size);
            COUNTER_UPDATE(_prune_all_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "{} prune all {} entries, {} bytes, {} times prune, is clear: {}",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _prune_stale_number_counter->value(), clear);
        }
    }

    Cache* get() { return _cache.get(); }

protected:
    std::unique_ptr<Cache> _cache;
};

} // namespace doris
