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

#include "io/cache/async_cache_write_manager_metrics.h"

#include "util/time.h"

namespace doris::io {

AsyncCacheWriteManager::Metrics::Metrics(AsyncCacheWriteManager& manager, const char* prefix) {
    // Keep the per-disk scrape surface limited to capacity, lifecycle, and outcome metrics. The
    // unnamed bvars below retain focused test and benchmark diagnostics without multiplying
    // Prometheus series for every file-cache instance.
    _running_worker_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_running_worker_count",
            [](void* manager) {
                return static_cast<AsyncCacheWriteManager*>(manager)->running_worker_count();
            },
            &manager);
    _buffer_memory_metric = std::make_shared<bvar::PassiveStatus<int64_t>>(
            prefix, "async_cache_write_buffer_memory_bytes",
            [](void* manager) {
                return static_cast<AsyncCacheWriteManager*>(manager)->buffer_memory_bytes();
            },
            &manager);
    _submitted_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_submitted_task_count");
    _submitted_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _finished_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _finished_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _worker_finished_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _worker_finished_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _evicted_oldest_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_evicted_oldest_task_count");
    _evicted_oldest_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _evicted_oldest_age_metric = std::make_shared<bvar::LatencyRecorder>();
    _rejected_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _reject_not_running_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _reject_backpressure_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _buffer_alloc_fail_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _submit_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _buffer_alloc_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _queue_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _queue_lock_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _queue_lock_hold_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _worker_task_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _get_or_set_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _append_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _finalize_latency_metric = std::make_shared<bvar::LatencyRecorder>();
    _skip_downloaded_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _skip_downloading_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _skip_partial_overlap_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _drop_stale_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _drop_stale_cache_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _drop_stale_key_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _cache_epoch_invalidate_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _key_epoch_invalidate_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _skip_deleting_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _append_fail_metric = std::make_shared<bvar::Adder<uint64_t>>();
    _finalize_fail_metric = std::make_shared<bvar::Adder<uint64_t>>();
}

void AsyncCacheWriteManager::Metrics::record_stale_epoch(StaleEpochReason reason) {
    *_drop_stale_epoch_metric << 1;
    if (reason == StaleEpochReason::CACHE) {
        *_drop_stale_cache_epoch_metric << 1;
    } else {
        *_drop_stale_key_epoch_metric << 1;
    }
}

void AsyncCacheWriteManager::Metrics::record_epoch_invalidation(EpochInvalidationScope scope) {
    if (scope == EpochInvalidationScope::CACHE) {
        *_cache_epoch_invalidate_metric << 1;
    } else {
        *_key_epoch_invalidate_metric << 1;
    }
}

void AsyncCacheWriteManager::Metrics::record_task_submitted(size_t bytes) {
    *_submitted_metric << 1;
    *_submitted_bytes_metric << bytes;
}

void AsyncCacheWriteManager::Metrics::record_task_rejected(RejectionReason reason) {
    *_rejected_metric << 1;
    if (reason == RejectionReason::NOT_RUNNING) {
        *_reject_not_running_metric << 1;
    } else {
        *_reject_backpressure_metric << 1;
    }
}

void AsyncCacheWriteManager::Metrics::record_task_finalized(const AsyncCacheWriteTask& task,
                                                            TaskFinalizationReason reason) {
    const size_t bytes = task.buffer_size();
    if (reason == TaskFinalizationReason::WORKER_FINISHED) {
        *_worker_finished_metric << 1;
        *_worker_finished_bytes_metric << bytes;
    } else {
        *_evicted_oldest_metric << 1;
        *_evicted_oldest_bytes_metric << bytes;
        *_evicted_oldest_age_metric << (MonotonicMicros() - task.submit_ts_us);
    }
    *_finished_metric << 1;
    *_finished_bytes_metric << bytes;
}

void AsyncCacheWriteManager::Metrics::record_buffer_allocation_failure() {
    *_buffer_alloc_fail_metric << 1;
}

void AsyncCacheWriteManager::Metrics::record_submit_latency(int64_t latency_us) {
    *_submit_latency_metric << latency_us;
}

void AsyncCacheWriteManager::Metrics::record_buffer_allocation_latency(int64_t latency_us) {
    *_buffer_alloc_latency_metric << latency_us;
}

void AsyncCacheWriteManager::Metrics::record_queue_wait_latency(int64_t latency_us) {
    *_queue_wait_latency_metric << latency_us;
}

void AsyncCacheWriteManager::Metrics::record_worker_task_latency(int64_t latency_us) {
    *_worker_task_latency_metric << latency_us;
}

void AsyncCacheWriteManager::Metrics::record_get_or_set_latency(int64_t latency_us) {
    *_get_or_set_latency_metric << latency_us;
}

void AsyncCacheWriteManager::Metrics::record_block_operation_latency(BlockOperation operation,
                                                                     int64_t latency_us) {
    if (operation == BlockOperation::APPEND) {
        *_append_latency_metric << latency_us;
    } else {
        *_finalize_latency_metric << latency_us;
    }
}

void AsyncCacheWriteManager::Metrics::record_block_operation_failure(BlockOperation operation) {
    if (operation == BlockOperation::APPEND) {
        *_append_fail_metric << 1;
    } else {
        *_finalize_fail_metric << 1;
    }
}

void AsyncCacheWriteManager::Metrics::record_skipped_block(SkippedBlockReason reason) {
    switch (reason) {
    case SkippedBlockReason::DOWNLOADED:
        *_skip_downloaded_metric << 1;
        break;
    case SkippedBlockReason::DOWNLOADING:
        *_skip_downloading_metric << 1;
        break;
    case SkippedBlockReason::PARTIAL_OVERLAP:
        *_skip_partial_overlap_metric << 1;
        break;
    case SkippedBlockReason::DELETING:
        *_skip_deleting_metric << 1;
        break;
    }
}

int64_t AsyncCacheWriteManager::Metrics::queue_lock_wait_p99_us() const {
    return _queue_lock_wait_latency_metric->latency_percentile(0.99);
}

int64_t AsyncCacheWriteManager::Metrics::queue_lock_hold_p99_us() const {
    return _queue_lock_hold_latency_metric->latency_percentile(0.99);
}

uint64_t AsyncCacheWriteManager::Metrics::evicted_oldest_count() const {
    return _evicted_oldest_metric->get_value();
}

AsyncCacheWriteManager::Metrics::Snapshot AsyncCacheWriteManager::Metrics::snapshot() const {
    return Snapshot {
            .submitted = _submitted_metric->get_value(),
            .submitted_bytes = _submitted_bytes_metric->get_value(),
            .finished = _finished_metric->get_value(),
            .finished_bytes = _finished_bytes_metric->get_value(),
            .worker_finished = _worker_finished_metric->get_value(),
            .worker_finished_bytes = _worker_finished_bytes_metric->get_value(),
            .evicted_oldest = _evicted_oldest_metric->get_value(),
            .evicted_oldest_bytes = _evicted_oldest_bytes_metric->get_value(),
            .evicted_oldest_age_count = _evicted_oldest_age_metric->count(),
            .rejected = _rejected_metric->get_value(),
            .reject_backpressure = _reject_backpressure_metric->get_value(),
            .buffer_alloc_fail = _buffer_alloc_fail_metric->get_value(),
            .submit_latency_count = _submit_latency_metric->count(),
            .buffer_alloc_latency_count = _buffer_alloc_latency_metric->count(),
            .queue_wait_latency_count = _queue_wait_latency_metric->count(),
            .worker_task_latency_count = _worker_task_latency_metric->count(),
            .get_or_set_latency_count = _get_or_set_latency_metric->count(),
            .append_latency_count = _append_latency_metric->count(),
            .finalize_latency_count = _finalize_latency_metric->count(),
            .skip_downloaded = _skip_downloaded_metric->get_value(),
            .skip_downloading = _skip_downloading_metric->get_value(),
            .skip_partial_overlap = _skip_partial_overlap_metric->get_value(),
            .drop_stale_key_epoch = _drop_stale_key_epoch_metric->get_value(),
            .cache_epoch_invalidate = _cache_epoch_invalidate_metric->get_value(),
            .key_epoch_invalidate = _key_epoch_invalidate_metric->get_value(),
            .skip_deleting = _skip_deleting_metric->get_value(),
    };
}

} // namespace doris::io
