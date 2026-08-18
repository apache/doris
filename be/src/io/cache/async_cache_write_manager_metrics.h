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

#include <bvar/bvar.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include "io/cache/async_cache_write_manager.h"

namespace doris::io {

/// Private observability component for AsyncCacheWriteManager. Keeping bvar ownership and
/// multi-counter event updates here leaves the manager focused on queue and worker state changes.
class AsyncCacheWriteManager::Metrics {
public:
    /// Read-only values used by focused unit tests without exposing individual bvar objects.
    struct Snapshot {
        uint64_t submitted {0};
        uint64_t submitted_bytes {0};
        uint64_t finished {0};
        uint64_t finished_bytes {0};
        uint64_t worker_finished {0};
        uint64_t worker_finished_bytes {0};
        uint64_t evicted_oldest {0};
        uint64_t evicted_oldest_bytes {0};
        int64_t evicted_oldest_age_count {0};
        uint64_t rejected {0};
        uint64_t reject_backpressure {0};
        uint64_t buffer_alloc_fail {0};
        int64_t submit_latency_count {0};
        int64_t buffer_alloc_latency_count {0};
        int64_t queue_wait_latency_count {0};
        int64_t worker_task_latency_count {0};
        int64_t get_or_set_latency_count {0};
        int64_t append_latency_count {0};
        int64_t finalize_latency_count {0};
        uint64_t skip_downloaded {0};
        uint64_t skip_downloading {0};
        uint64_t skip_partial_overlap {0};
        uint64_t drop_stale_key_epoch {0};
        uint64_t cache_epoch_invalidate {0};
        uint64_t key_epoch_invalidate {0};
        uint64_t skip_deleting {0};
    };

    enum class RejectionReason : uint8_t {
        NOT_RUNNING,
        BACKPRESSURE,
    };

    enum class StaleEpochReason : uint8_t {
        CACHE,
        KEY,
    };

    enum class EpochInvalidationScope : uint8_t {
        CACHE,
        KEY,
    };

    enum class SkippedBlockReason : uint8_t {
        DOWNLOADED,
        DOWNLOADING,
        PARTIAL_OVERLAP,
        DELETING,
    };

    enum class BlockOperation : uint8_t {
        APPEND,
        FINALIZE,
    };

    Metrics(AsyncCacheWriteManager& manager, const char* prefix);

    void record_stale_epoch(StaleEpochReason reason);
    void record_epoch_invalidation(EpochInvalidationScope scope);
    void record_task_submitted(size_t bytes);
    void record_task_rejected(RejectionReason reason);
    void record_task_finalized(const AsyncCacheWriteTask& task, TaskFinalizationReason reason);
    void record_buffer_allocation_failure();
    void record_submit_latency(int64_t latency_us);
    void record_buffer_allocation_latency(int64_t latency_us);
    void record_queue_wait_latency(int64_t latency_us);
    void record_worker_task_latency(int64_t latency_us);
    void record_get_or_set_latency(int64_t latency_us);
    void record_block_operation_latency(BlockOperation operation, int64_t latency_us);
    void record_block_operation_failure(BlockOperation operation);
    void record_skipped_block(SkippedBlockReason reason);

    bvar::LatencyRecorder& queue_lock_wait_latency() { return *_queue_lock_wait_latency_metric; }
    bvar::LatencyRecorder& queue_lock_hold_latency() { return *_queue_lock_hold_latency_metric; }
    int64_t queue_lock_wait_p99_us() const;
    int64_t queue_lock_hold_p99_us() const;
    uint64_t evicted_oldest_count() const;
    Snapshot snapshot() const;

private:
    std::shared_ptr<bvar::PassiveStatus<size_t>> _pending_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _pending_bytes_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _running_worker_count_metric;
    std::shared_ptr<bvar::PassiveStatus<int64_t>> _buffer_memory_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _submitted_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _submitted_bytes_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _finished_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _finished_bytes_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _worker_finished_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _worker_finished_bytes_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _evicted_oldest_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _evicted_oldest_bytes_metric;
    std::shared_ptr<bvar::LatencyRecorder> _evicted_oldest_age_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _rejected_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _reject_not_running_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _reject_backpressure_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _buffer_alloc_fail_metric;
    std::shared_ptr<bvar::LatencyRecorder> _submit_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _buffer_alloc_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _queue_wait_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _queue_lock_wait_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _queue_lock_hold_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _worker_task_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _get_or_set_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _append_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _finalize_latency_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_downloaded_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_downloading_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_partial_overlap_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _drop_stale_epoch_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _drop_stale_cache_epoch_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _drop_stale_key_epoch_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _cache_epoch_invalidate_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _key_epoch_invalidate_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_deleting_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _append_fail_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _finalize_fail_metric;
};

} // namespace doris::io
