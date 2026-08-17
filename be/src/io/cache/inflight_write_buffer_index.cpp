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
    TimedShardLock(std::mutex& mutex, bvar::LatencyRecorder& wait_latency,
                   bvar::LatencyRecorder& hold_latency)
            : _lock(mutex, std::defer_lock),
              _wait_latency(wait_latency),
              _hold_latency(hold_latency) {
        const int64_t wait_start_us = MonotonicMicros();
        _lock.lock();
        _acquired_at_us = MonotonicMicros();
        _wait_us = _acquired_at_us - wait_start_us;
    }

    ~TimedShardLock() {
        const int64_t hold_us = MonotonicMicros() - _acquired_at_us;
        _lock.unlock();
        _wait_latency << _wait_us;
        _hold_latency << hold_us;
    }

private:
    std::unique_lock<std::mutex> _lock;
    bvar::LatencyRecorder& _wait_latency;
    bvar::LatencyRecorder& _hold_latency;
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
    // The retained entry count and bytes are the operational signals needed to diagnose inflight
    // memory. Keep operation breakdowns and lock timings unnamed so they do not add per-disk
    // Prometheus series.
    _count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "inflight_write_buffer_index_entry_count",
            [](void* index) { return static_cast<InflightWriteBufferIndex*>(index)->count(); },
            this);
    _buffer_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "inflight_write_buffer_index_buffer_bytes",
            [](void* index) {
                return static_cast<InflightWriteBufferIndex*>(index)->buffer_bytes();
            },
            this);
    _lookup_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _hit_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _miss_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _insert_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _insert_existing_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _remove_success_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _remove_failed_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _rollback_on_backpressure_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _lock_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _lock_hold_latency_metric = std::make_shared<bvar::LatencyRecorder>();
}

std::shared_ptr<InflightWriteBufferEntry> InflightWriteBufferIndex::insert_if_absent(
        const UInt128Wrapper& cache_hash, size_t block_offset,
        std::shared_ptr<InflightWriteBufferEntry> entry) {
    DORIS_CHECK(entry != nullptr);
    const size_t entry_buffer_bytes = entry->buffer->size();
    Key key {.cache_hash = cache_hash, .block_offset = block_offset};
    auto& shard = *_shards[_shard_index(key)];
    {
        TimedShardLock lock(shard.mutex, *_lock_wait_latency_metric, *_lock_hold_latency_metric);
        auto iterator = shard.entries.find(key);
        if (iterator == shard.entries.end()) {
            shard.entries.emplace(key, std::move(entry));
            _count.fetch_add(1, std::memory_order_relaxed);
            _buffer_bytes.fetch_add(entry_buffer_bytes, std::memory_order_relaxed);
        } else {
            *_insert_existing_metric << 1;
            return iterator->second;
        }
    }

    *_insert_metric << 1;
    return nullptr;
}

std::shared_ptr<InflightWriteBufferEntry> InflightWriteBufferIndex::lookup(
        const UInt128Wrapper& cache_hash, size_t block_offset) {
    *_lookup_metric << 1;
    Key key {.cache_hash = cache_hash, .block_offset = block_offset};
    auto& shard = *_shards[_shard_index(key)];
    {
        TimedShardLock lock(shard.mutex, *_lock_wait_latency_metric, *_lock_hold_latency_metric);
        auto iterator = shard.entries.find(key);
        if (iterator == shard.entries.end()) {
            *_miss_metric << 1;
            return nullptr;
        }
        *_hit_metric << 1;
        return iterator->second;
    }
}

std::vector<InflightWriteBufferIndex::LookupResult> InflightWriteBufferIndex::lookup_all(
        const UInt128Wrapper& cache_hash, const std::vector<size_t>& block_offsets) {
    std::vector<LookupResult> results;
    results.reserve(block_offsets.size());
    for (size_t block_offset : block_offsets) {
        results.emplace_back(LookupResult {
                .block_offset = block_offset,
                .entry = lookup(cache_hash, block_offset),
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
        TimedShardLock lock(shard.mutex, *_lock_wait_latency_metric, *_lock_hold_latency_metric);
        auto iterator = shard.entries.find(key);
        if (iterator != shard.entries.end() && iterator->second == expected) {
            const size_t entry_buffer_bytes = iterator->second->buffer->size();
            shard.entries.erase(iterator);
            const size_t old_count = _count.fetch_sub(1, std::memory_order_relaxed);
            DCHECK_GT(old_count, 0);
            _buffer_bytes.fetch_sub(entry_buffer_bytes, std::memory_order_relaxed);
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
