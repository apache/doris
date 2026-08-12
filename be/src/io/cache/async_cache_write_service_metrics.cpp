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

#include "io/cache/async_cache_write_service_metrics.h"

#include "util/time.h"

namespace doris::io {

AsyncCacheWriteService::Metrics::Metrics(AsyncCacheWriteService& service, const char* prefix) {
    _pending_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_pending_count",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->pending_count();
            },
            &service);
    _pending_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_pending_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->pending_bytes();
            },
            &service);
    _queued_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_queue_size",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->queued_count();
            },
            &service);
    _queued_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_queued_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->queued_bytes();
            },
            &service);
    _active_task_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_tasks",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->active_task_count();
            },
            &service);
    _active_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->active_bytes();
            },
            &service);
    _running_worker_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_running_workers",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->running_worker_count();
            },
            &service);
    _configured_worker_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_configured_workers",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_configured_worker_count.load(
                        std::memory_order_relaxed);
            },
            &service);
    _max_pending_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_max_pending_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)
                        ->_options.load(std::memory_order_acquire)
                        ->max_pending_bytes;
            },
            &service);
    _active_get_or_set_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_get_or_set",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_get_or_set_count.load(
                        std::memory_order_relaxed);
            },
            &service);
    _active_append_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_append",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_append_count.load(
                        std::memory_order_relaxed);
            },
            &service);
    _active_finalize_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_finalize",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_finalize_count.load(
                        std::memory_order_relaxed);
            },
            &service);
    _active_write_epoch_key_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_key_epoch_count",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)
                        ->active_write_epoch_key_count();
            },
            &service);
    _buffer_memory_metric = std::make_shared<bvar::PassiveStatus<int64_t>>(
            prefix, "async_cache_write_buffer_memory_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->buffer_memory_bytes();
            },
            &service);
    _submitted_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_submitted_total");
    _submitted_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_submitted_bytes_total");
    _finished_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_finished_total");
    _finished_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_finished_bytes_total");
    _worker_finished_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_worker_finished_total");
    _worker_finished_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_worker_finished_bytes_total");
    _evicted_oldest_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_evicted_oldest_total");
    _evicted_oldest_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_evicted_oldest_bytes_total");
    _evicted_oldest_age_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_evicted_oldest_age_us");
    _rejected_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_rejected_total");
    _reject_not_running_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_reject_not_running_total");
    _reject_backpressure_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_reject_backpressure_total");
    _buffer_alloc_fail_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_buffer_alloc_fail_total");
    _submit_latency_metric =
            std::make_shared<bvar::LatencyRecorder>(prefix, "async_cache_write_submit_latency_us");
    _buffer_alloc_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_buffer_alloc_latency_us");
    _queue_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_queue_wait_latency_us");
    _queue_lock_wait_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_queue_lock_wait_latency_us");
    _queue_lock_hold_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_queue_lock_hold_latency_us");
    _worker_task_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_worker_task_latency_us");
    _get_or_set_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_get_or_set_latency_us");
    _append_latency_metric =
            std::make_shared<bvar::LatencyRecorder>(prefix, "async_cache_write_append_latency_us");
    _finalize_latency_metric = std::make_shared<bvar::LatencyRecorder>(
            prefix, "async_cache_write_finalize_latency_us");
    _skip_downloaded_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_skip_downloaded_total");
    _skip_downloading_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_skip_downloading_total");
    _skip_partial_overlap_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_skip_partial_overlap_total");
    _drop_stale_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_drop_stale_epoch_total");
    _drop_stale_cache_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_drop_stale_cache_epoch_total");
    _drop_stale_key_epoch_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_drop_stale_key_epoch_total");
    _cache_epoch_invalidate_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_cache_epoch_invalidate_total");
    _key_epoch_invalidate_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_key_epoch_invalidate_total");
    _skip_deleting_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_skip_deleting_total");
    _append_fail_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_append_fail_total");
    _finalize_fail_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_finalize_fail_total");
    _persisted_blocks_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_persisted_blocks_total");
    _persisted_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_persisted_bytes_total");
}

void AsyncCacheWriteService::Metrics::record_stale_epoch(StaleEpochReason reason) {
    *_drop_stale_epoch_metric << 1;
    if (reason == StaleEpochReason::CACHE) {
        *_drop_stale_cache_epoch_metric << 1;
    } else {
        *_drop_stale_key_epoch_metric << 1;
    }
}

void AsyncCacheWriteService::Metrics::record_epoch_invalidation(EpochInvalidationScope scope) {
    if (scope == EpochInvalidationScope::CACHE) {
        *_cache_epoch_invalidate_metric << 1;
    } else {
        *_key_epoch_invalidate_metric << 1;
    }
}

void AsyncCacheWriteService::Metrics::record_task_submitted(size_t bytes) {
    *_submitted_metric << 1;
    *_submitted_bytes_metric << bytes;
}

void AsyncCacheWriteService::Metrics::record_task_rejected(RejectionReason reason) {
    *_rejected_metric << 1;
    if (reason == RejectionReason::NOT_RUNNING) {
        *_reject_not_running_metric << 1;
    } else {
        *_reject_backpressure_metric << 1;
    }
}

void AsyncCacheWriteService::Metrics::record_task_finalized(const AsyncCacheWriteTask& task,
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

void AsyncCacheWriteService::Metrics::record_buffer_allocation_failure() {
    *_buffer_alloc_fail_metric << 1;
}

void AsyncCacheWriteService::Metrics::record_submit_latency(int64_t latency_us) {
    *_submit_latency_metric << latency_us;
}

void AsyncCacheWriteService::Metrics::record_buffer_allocation_latency(int64_t latency_us) {
    *_buffer_alloc_latency_metric << latency_us;
}

void AsyncCacheWriteService::Metrics::record_queue_wait_latency(int64_t latency_us) {
    *_queue_wait_latency_metric << latency_us;
}

void AsyncCacheWriteService::Metrics::record_worker_task_latency(int64_t latency_us) {
    *_worker_task_latency_metric << latency_us;
}

void AsyncCacheWriteService::Metrics::record_get_or_set_latency(int64_t latency_us) {
    *_get_or_set_latency_metric << latency_us;
}

void AsyncCacheWriteService::Metrics::record_block_operation_latency(BlockOperation operation,
                                                                     int64_t latency_us) {
    if (operation == BlockOperation::APPEND) {
        *_append_latency_metric << latency_us;
    } else {
        *_finalize_latency_metric << latency_us;
    }
}

void AsyncCacheWriteService::Metrics::record_block_operation_failure(BlockOperation operation) {
    if (operation == BlockOperation::APPEND) {
        *_append_fail_metric << 1;
    } else {
        *_finalize_fail_metric << 1;
    }
}

void AsyncCacheWriteService::Metrics::record_skipped_block(SkippedBlockReason reason) {
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

void AsyncCacheWriteService::Metrics::record_persisted_block(size_t bytes) {
    *_persisted_blocks_metric << 1;
    *_persisted_bytes_metric << bytes;
}

int64_t AsyncCacheWriteService::Metrics::queue_lock_wait_p99_us() const {
    return _queue_lock_wait_latency_metric->latency_percentile(0.99);
}

int64_t AsyncCacheWriteService::Metrics::queue_lock_hold_p99_us() const {
    return _queue_lock_hold_latency_metric->latency_percentile(0.99);
}

uint64_t AsyncCacheWriteService::Metrics::evicted_oldest_count() const {
    return _evicted_oldest_metric->get_value();
}

AsyncCacheWriteService::Metrics::Snapshot AsyncCacheWriteService::Metrics::snapshot() const {
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
            .persisted_blocks = _persisted_blocks_metric->get_value(),
            .persisted_bytes = _persisted_bytes_metric->get_value(),
    };
}

} // namespace doris::io
