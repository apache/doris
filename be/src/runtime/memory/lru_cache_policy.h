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

#include <fmt/format.h>

#include <memory>

#include "olap/lru_cache.h"
#include "runtime/memory/cache_policy.h"
#include "runtime/memory/lru_cache_value_base.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/time.h"

namespace doris {

// Base of lru cache, allow prune stale entry and prune all entry.
class LRUCachePolicy : public CachePolicy {
public:
    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS,
                   uint32_t element_count_capacity = DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY,
                   bool enable_prune = true)
            : CachePolicy(type, stale_sweep_time_s, enable_prune), _lru_cache_type(lru_cache_type) {
        if (check_capacity(capacity, num_shards)) {
            _cache = std::shared_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type), capacity, lru_cache_type, num_shards,
                                        element_count_capacity));
        } else {
            CHECK(ExecEnv::GetInstance()->get_dummy_lru_cache());
            _cache = ExecEnv::GetInstance()->get_dummy_lru_cache();
        }
    }

    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards,
                   uint32_t element_count_capacity,
                   CacheValueTimeExtractor cache_value_time_extractor,
                   bool cache_value_check_timestamp, bool enable_prune = true)
            : CachePolicy(type, stale_sweep_time_s, enable_prune), _lru_cache_type(lru_cache_type) {
        if (check_capacity(capacity, num_shards)) {
            _cache = std::shared_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type), capacity, lru_cache_type, num_shards,
                                        cache_value_time_extractor, cache_value_check_timestamp,
                                        element_count_capacity));
        } else {
            CHECK(ExecEnv::GetInstance()->get_dummy_lru_cache());
            _cache = ExecEnv::GetInstance()->get_dummy_lru_cache();
        }
    }

    void reset_cache() { _cache.reset(); }

    bool check_capacity(size_t capacity, uint32_t num_shards) {
        if (capacity < num_shards) {
            LOG(INFO) << fmt::format(
                    "{} lru cache capacity({} B) less than num_shards({}), init failed, will be "
                    "disabled.",
                    type_string(type()), capacity, num_shards);
            _enable_prune = false;
            return false;
        }
        return true;
    }

    static std::string lru_cache_type_string(LRUCacheType type) {
        switch (type) {
        case LRUCacheType::SIZE:
            return "size";
        case LRUCacheType::NUMBER:
            return "number";
        default:
            LOG(FATAL) << "not match type of lru cache:" << static_cast<int>(type);
        }
    }

    virtual int64_t mem_consumption() = 0;

    virtual Cache::Handle* insert(const CacheKey& key, void* value, size_t charge,
                                  size_t tracking_bytes,
                                  CachePriority priority = CachePriority::NORMAL) = 0;

    Cache::Handle* lookup(const CacheKey& key) { return _cache->lookup(key); }

    void release(Cache::Handle* handle) { _cache->release(handle); }

    void* value(Cache::Handle* handle) { return _cache->value(handle); }

    void erase(const CacheKey& key) { _cache->erase(key); }

    int64_t get_usage() { return _cache->get_usage(); }

    size_t get_total_capacity() { return _cache->get_total_capacity(); }

    uint64_t new_id() { return _cache->new_id(); };

    // Subclass can override this method to determine whether to do the minor or full gc
    virtual bool exceed_prune_limit() {
        return _lru_cache_type == LRUCacheType::SIZE ? mem_consumption() > CACHE_MIN_FREE_SIZE
                                                     : get_usage() > CACHE_MIN_FREE_NUMBER;
    }

    // Try to prune the cache if expired.
    void prune_stale() override {
        COUNTER_SET(_freed_entrys_counter, (int64_t)0);
        COUNTER_SET(_freed_memory_counter, (int64_t)0);
        if (_stale_sweep_time_s <= 0 && _cache == ExecEnv::GetInstance()->get_dummy_lru_cache()) {
            return;
        }
        if (exceed_prune_limit()) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            const int64_t curtime = UnixMillis();
            auto pred = [this, curtime](const LRUHandle* handle) -> bool {
                return static_cast<bool>((handle->last_visit_time + _stale_sweep_time_s * 1000) <
                                         curtime);
            };

            LOG(INFO) << fmt::format("[MemoryGC] {} prune stale start, consumption {}, usage {}",
                                     type_string(_type), mem_consumption(), get_usage());
            // Prune cache in lazy mode to save cpu and minimize the time holding write lock
            PrunedInfo pruned_info = _cache->prune_if(pred, true);
            COUNTER_SET(_freed_entrys_counter, pruned_info.pruned_count);
            COUNTER_SET(_freed_memory_counter, pruned_info.pruned_size);
            COUNTER_UPDATE(_prune_stale_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] {} prune stale {} entries, {} bytes, {} times prune",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _prune_stale_number_counter->value());
        } else {
            if (_lru_cache_type == LRUCacheType::SIZE) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune stale, LRUCacheType::SIZE consumption {} "
                        "less "
                        "than CACHE_MIN_FREE_SIZE {}",
                        type_string(_type), mem_consumption(), CACHE_MIN_FREE_SIZE);
            } else if (_lru_cache_type == LRUCacheType::NUMBER) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune stale, LRUCacheType::NUMBER usage {} less "
                        "than "
                        "CACHE_MIN_FREE_NUMBER {}",
                        type_string(_type), get_usage(), CACHE_MIN_FREE_NUMBER);
            }
        }
    }

    void prune_all(bool force) override {
        COUNTER_SET(_freed_entrys_counter, (int64_t)0);
        COUNTER_SET(_freed_memory_counter, (int64_t)0);
        if (_cache == ExecEnv::GetInstance()->get_dummy_lru_cache()) {
            return;
        }
        if ((force && mem_consumption() != 0) || exceed_prune_limit()) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            LOG(INFO) << fmt::format("[MemoryGC] {} prune all start, consumption {}, usage {}",
                                     type_string(_type), mem_consumption(), get_usage());
            PrunedInfo pruned_info = _cache->prune();
            COUNTER_SET(_freed_entrys_counter, pruned_info.pruned_count);
            COUNTER_SET(_freed_memory_counter, pruned_info.pruned_size);
            COUNTER_UPDATE(_prune_all_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] {} prune all {} entries, {} bytes, {} times prune, is force: {}",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _prune_all_number_counter->value(), force);
        } else {
            if (_lru_cache_type == LRUCacheType::SIZE) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune all, force is {}, LRUCacheType::SIZE "
                        "consumption {}, "
                        "CACHE_MIN_FREE_SIZE {}",
                        type_string(_type), force, mem_consumption(), CACHE_MIN_FREE_SIZE);
            } else if (_lru_cache_type == LRUCacheType::NUMBER) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune all, force is {}, LRUCacheType::NUMBER "
                        "usage {}, CACHE_MIN_FREE_NUMBER {}",
                        type_string(_type), force, get_usage(), CACHE_MIN_FREE_NUMBER);
            }
        }
    }

protected:
    // if check_capacity failed, will return dummy lru cache,
    // compatible with ShardedLRUCache usage, but will not actually cache.
    std::shared_ptr<Cache> _cache;
    LRUCacheType _lru_cache_type;
};

class LRUCachePolicyTrackingAllocator : public LRUCachePolicy {
public:
    LRUCachePolicyTrackingAllocator(
            CacheType type, size_t capacity, LRUCacheType lru_cache_type,
            uint32_t stale_sweep_time_s, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS,
            uint32_t element_count_capacity = DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY,
            bool enable_prune = true)
            : LRUCachePolicy(type, capacity, lru_cache_type, stale_sweep_time_s, num_shards,
                             element_count_capacity, enable_prune) {
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
    }

    LRUCachePolicyTrackingAllocator(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                                    uint32_t stale_sweep_time_s, uint32_t num_shards,
                                    uint32_t element_count_capacity,
                                    CacheValueTimeExtractor cache_value_time_extractor,
                                    bool cache_value_check_timestamp, bool enable_prune = true)
            : LRUCachePolicy(type, capacity, lru_cache_type, stale_sweep_time_s, num_shards,
                             element_count_capacity, cache_value_time_extractor,
                             cache_value_check_timestamp, enable_prune) {
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
    }

    ~LRUCachePolicyTrackingAllocator() override { reset_cache(); }

    std::shared_ptr<MemTrackerLimiter> mem_tracker() const {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker;
    }

    int64_t mem_consumption() override {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker->consumption();
    }

    Cache::Handle* insert(const CacheKey& key, void* value, size_t charge, size_t tracking_bytes,
                          CachePriority priority = CachePriority::NORMAL) override {
        return _cache->insert(key, value, charge, priority);
    }

protected:
    void _init_mem_tracker(const std::string& type_name) {
        _mem_tracker = MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::GLOBAL,
                fmt::format("{}[{}](AllocByAllocator)", type_string(_type), type_name));
    }

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

class LRUCachePolicyTrackingManual : public LRUCachePolicy {
public:
    LRUCachePolicyTrackingManual(
            CacheType type, size_t capacity, LRUCacheType lru_cache_type,
            uint32_t stale_sweep_time_s, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS,
            uint32_t element_count_capacity = DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY,
            bool enable_prune = true)
            : LRUCachePolicy(type, capacity, lru_cache_type, stale_sweep_time_s, num_shards,
                             element_count_capacity, enable_prune) {
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
    }

    LRUCachePolicyTrackingManual(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                                 uint32_t stale_sweep_time_s, uint32_t num_shards,
                                 uint32_t element_count_capacity,
                                 CacheValueTimeExtractor cache_value_time_extractor,
                                 bool cache_value_check_timestamp, bool enable_prune = true)
            : LRUCachePolicy(type, capacity, lru_cache_type, stale_sweep_time_s, num_shards,
                             element_count_capacity, cache_value_time_extractor,
                             cache_value_check_timestamp, enable_prune) {
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
    }

    ~LRUCachePolicyTrackingManual() override { reset_cache(); }

    MemTracker* mem_tracker() {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker.get();
    }

    int64_t mem_consumption() override {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker->consumption();
    }

    // Insert and cache value destroy will be manually consume tracking_bytes to mem tracker.
    // If lru cache is LRUCacheType::SIZE, tracking_bytes usually equal to charge.
    Cache::Handle* insert(const CacheKey& key, void* value, size_t charge, size_t tracking_bytes,
                          CachePriority priority = CachePriority::NORMAL) override {
        size_t bytes_with_handle = _get_bytes_with_handle(key, charge, tracking_bytes);
        if (value != nullptr) { // if tracking_bytes = 0, only tracking handle size.
            mem_tracker()->consume(bytes_with_handle);
            ((LRUCacheValueBase*)value)->set_tracking_bytes(bytes_with_handle, mem_tracker());
        }
        return _cache->insert(key, value, charge, priority);
    }

private:
    void _init_mem_tracker(const std::string& type_name) {
        _mem_tracker =
                std::make_unique<MemTracker>(fmt::format("{}[{}]", type_string(_type), type_name),
                                             ExecEnv::GetInstance()->details_mem_tracker_set());
    }

    // LRUCacheType::SIZE equal to total_size.
    size_t _get_bytes_with_handle(const CacheKey& key, size_t charge, size_t bytes) {
        size_t handle_size = sizeof(LRUHandle) - 1 + key.size();
        DCHECK(_lru_cache_type == LRUCacheType::SIZE || bytes != -1)
                << " _type " << type_string(_type);
        // if LRUCacheType::NUMBER and bytes equals 0, such as some caches cannot accurately track memory size.
        // cache mem tracker value and _usage divided by handle_size(106) will get the number of cache entries.
        return _lru_cache_type == LRUCacheType::SIZE ? handle_size + charge : handle_size + bytes;
    }

    std::unique_ptr<MemTracker> _mem_tracker;
};

} // namespace doris
