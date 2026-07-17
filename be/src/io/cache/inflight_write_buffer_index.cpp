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

#include "io/cache/inflight_write_buffer_index.h"

#include <utility>

#include "common/logging.h"
#include "util/time.h"

namespace doris::io {

namespace {

/// Acquire one index shard while recording both contention and critical-section duration.
class TimedShardLock {
public:
    TimedShardLock(std::mutex& mutex, bvar::LatencyRecorder* wait_latency,
                   bvar::LatencyRecorder* hold_latency)
            : _lock(mutex, std::defer_lock),
              _wait_latency(wait_latency),
              _hold_latency(hold_latency) {
        DORIS_CHECK(wait_latency != nullptr);
        DORIS_CHECK(_hold_latency != nullptr);
        const int64_t wait_start_us = MonotonicMicros();
        _lock.lock();
        _acquired_at_us = MonotonicMicros();
        _wait_us = _acquired_at_us - wait_start_us;
    }

    ~TimedShardLock() {
        const int64_t hold_us = MonotonicMicros() - _acquired_at_us;
        _lock.unlock();
        *_wait_latency << _wait_us;
        *_hold_latency << hold_us;
    }

private:
    std::unique_lock<std::mutex> _lock;
    bvar::LatencyRecorder* _wait_latency;
    bvar::LatencyRecorder* _hold_latency;
    int64_t _acquired_at_us {0};
    int64_t _wait_us {0};
};

} // namespace

InflightWriteBufferIndex::InflightWriteBufferIndex(size_t shard_count, std::string metric_prefix) {
    DORIS_CHECK(shard_count > 0);
    _shards.reserve(shard_count);
    for (size_t index = 0; index < shard_count; ++index) {
        _shards.emplace_back(std::make_unique<Shard>());
    }

    const char* prefix = metric_prefix.c_str();
    _size_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "inflight_write_buffer_index_size",
            [](void* index) { return static_cast<InflightWriteBufferIndex*>(index)->size(); },
            this);
    _lookup_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_lookup_total");
    _hit_metric = std::make_shared<bvar::Adder<uint64_t>>(prefix,
                                                          "inflight_write_buffer_index_hit_total");
    _miss_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_miss_total");
    _insert_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_insert_total");
    _insert_existing_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_insert_existing_total");
    _remove_success_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_remove_if_success_total");
    _remove_failed_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_remove_if_failed_total");
    _stale_epoch_miss_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_stale_epoch_miss_total");
    _stale_epoch_replace_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_stale_epoch_replace_total");
    _rollback_on_backpressure_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "inflight_write_buffer_index_rollback_on_backpressure_total");
    _lock_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "inflight_write_buffer_index_lock_wait_latency_us");
    _lock_hold_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "inflight_write_buffer_index_lock_hold_latency_us");
}

std::shared_ptr<InflightWriteBufferEntry> InflightWriteBufferIndex::insert_if_absent(
        const UInt128Wrapper& cache_hash, size_t block_offset,
        std::shared_ptr<InflightWriteBufferEntry> entry) {
    DORIS_CHECK(entry != nullptr);
    Key key {.cache_hash = cache_hash, .block_offset = block_offset};
    auto& shard = *_shards[_shard_index(key)];
    bool inserted = false;
    {
        TimedShardLock lock(shard.mutex, _lock_wait_latency_metric.get(),
                            _lock_hold_latency_metric.get());
        auto iterator = shard.entries.find(key);
        if (iterator == shard.entries.end()) {
            shard.entries.emplace(key, std::move(entry));
            _size.fetch_add(1, std::memory_order_relaxed);
            inserted = true;
        } else if (iterator->second->write_epoch < entry->write_epoch) {
            iterator->second = std::move(entry);
            *_stale_epoch_replace_metric << 1;
            *_insert_metric << 1;
            return nullptr;
        } else {
            *_insert_existing_metric << 1;
            return iterator->second;
        }
    }

    DORIS_CHECK(inserted);
    *_insert_metric << 1;
    return nullptr;
}

std::shared_ptr<InflightWriteBufferEntry> InflightWriteBufferIndex::lookup(
        const UInt128Wrapper& cache_hash, size_t block_offset, uint64_t expected_epoch) {
    *_lookup_metric << 1;
    Key key {.cache_hash = cache_hash, .block_offset = block_offset};
    auto& shard = *_shards[_shard_index(key)];
    {
        TimedShardLock lock(shard.mutex, _lock_wait_latency_metric.get(),
                            _lock_hold_latency_metric.get());
        auto iterator = shard.entries.find(key);
        if (iterator == shard.entries.end()) {
            *_miss_metric << 1;
            return nullptr;
        }
        if (iterator->second->write_epoch == expected_epoch) {
            *_hit_metric << 1;
            return iterator->second;
        }
        *_stale_epoch_miss_metric << 1;
        if (iterator->second->write_epoch < expected_epoch) {
            shard.entries.erase(iterator);
            const size_t old_size = _size.fetch_sub(1, std::memory_order_relaxed);
            DORIS_CHECK(old_size > 0);
        }
    }
    *_miss_metric << 1;
    return nullptr;
}

std::vector<InflightWriteBufferIndex::LookupResult> InflightWriteBufferIndex::lookup_all(
        const UInt128Wrapper& cache_hash, const std::vector<size_t>& block_offsets,
        uint64_t expected_epoch) {
    std::vector<LookupResult> results;
    results.reserve(block_offsets.size());
    for (size_t block_offset : block_offsets) {
        results.emplace_back(LookupResult {
                .block_offset = block_offset,
                .entry = lookup(cache_hash, block_offset, expected_epoch),
        });
    }
    return results;
}

bool InflightWriteBufferIndex::remove_if(
        const UInt128Wrapper& cache_hash, size_t block_offset,
        const std::shared_ptr<InflightWriteBufferEntry>& expected) {
    DORIS_CHECK(expected != nullptr);
    Key key {.cache_hash = cache_hash, .block_offset = block_offset};
    auto& shard = *_shards[_shard_index(key)];
    bool removed = false;
    {
        TimedShardLock lock(shard.mutex, _lock_wait_latency_metric.get(),
                            _lock_hold_latency_metric.get());
        auto iterator = shard.entries.find(key);
        if (iterator != shard.entries.end() && iterator->second == expected) {
            shard.entries.erase(iterator);
            const size_t old_size = _size.fetch_sub(1, std::memory_order_relaxed);
            DORIS_CHECK(old_size > 0);
            removed = true;
        }
    }
    if (removed) {
        *_remove_success_metric << 1;
        return true;
    }
    *_remove_failed_metric << 1;
    return false;
}

} // namespace doris::io
