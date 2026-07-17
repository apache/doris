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
#include <concurrentqueue.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/atomic_shared_ptr.h"
#include "common/status.h"
#include "io/cache/file_cache_common.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/threadpool.h"

namespace doris::io {

class BlockFileCache;

/// Cache admission attributes captured on the query thread and replayed by a write worker.
struct CacheAdmissionContext {
    /// Query identity used by per-query cache admission and accounting.
    TUniqueId query_id;
    FileCacheType cache_type {FileCacheType::NORMAL};
    int64_t expiration_time {0};
    int64_t tablet_id {0};
    bool is_warmup {false};
};

/// Reference-counted payload whose allocation is charged to the async-write memory tracker.
class AsyncCacheWriteBuffer {
public:
    ~AsyncCacheWriteBuffer();

    char* data() { return _data; }
    const char* data() const { return _data; }
    size_t size() const { return _size; }

private:
    friend class AsyncCacheWriteService;

    AsyncCacheWriteBuffer(size_t size, std::shared_ptr<MemTrackerLimiter> tracker);

    char* _data = nullptr;
    size_t _size = 0;
    std::shared_ptr<MemTrackerLimiter> _tracker;
};

using AsyncCacheWriteBufferPtr = std::shared_ptr<AsyncCacheWriteBuffer>;

/// One block-aligned cache write. `buffer` contains exactly `file_size` bytes starting at
/// `file_offset`; `write_epoch` prevents a worker from resurrecting data after cache invalidation.
struct AsyncCacheWriteTask {
    UInt128Wrapper cache_hash;
    size_t file_offset {0};
    size_t file_size {0};
    AsyncCacheWriteBufferPtr buffer;
    CacheAdmissionContext admission_ctx;
    int64_t submit_ts_us {0};
    uint64_t write_epoch {0};
    std::function<void(const AsyncCacheWriteTask&)> on_finalized;
};

/// Complete per-cache-disk worker, queue, batch, and watchdog settings. The service receives this
/// value explicitly at construction and through update_options(); it never reads global config.
struct AsyncCacheWriteServiceOptions {
    size_t worker_count {1};
    size_t max_pending_tasks {1};
    size_t batch_size {1};
    int64_t watchdog_warn_secs {30};
    int64_t watchdog_drop_secs {120};
};

/// Owns the bounded async-write queue and workers for one BlockFileCache (one cache disk).
///
/// The referenced cache must outlive this service. Shutdown stops new producers, waits registered
/// producers, and drains all accepted tasks before worker resources are released.
class AsyncCacheWriteService {
public:
    /// @param cache Non-owning target cache; it must outlive this service.
    /// @param options Initial worker, queue, batch, and watchdog limits.
    AsyncCacheWriteService(BlockFileCache* cache, AsyncCacheWriteServiceOptions options);
    ~AsyncCacheWriteService();

    /// Create the worker pool and schedule the configured long-running workers. Idempotent.
    Status start();

    /// Reserve one pending slot and enqueue `task` without blocking the query thread.
    /// @return true if ownership was transferred to the queue; false when workers have not been
    /// started, during shutdown, on backpressure, or on queue rejection. A rejected task's
    /// finalization callback is not invoked.
    bool try_submit(AsyncCacheWriteTask task);

    /// Allocate `size` payload bytes charged to the service tracker and return them in `buffer`.
    Status allocate_tracked_buffer(size_t size, AsyncCacheWriteBufferPtr* buffer);

    /// Return the epoch accepted by workers and inflight-buffer readers.
    uint64_t current_write_epoch() const { return _write_epoch.load(std::memory_order_acquire); }

    /// Test whether `epoch` still belongs to the current cache contents.
    bool is_current_write_epoch(uint64_t epoch) const { return epoch == current_write_epoch(); }

    /// Advance the epoch so queued/inflight writes captured before invalidation become stale.
    /// @return The newly active epoch.
    uint64_t invalidate_pending_writes() {
        return _write_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    /// Resize the number of active workers. A shrink waits only for retiring worker loops.
    /// @param worker_count Positive target worker count for this cache disk.
    Status resize_workers(size_t worker_count);

    /// Replace all mutable service settings with one coherent snapshot. Configuration adapters
    /// call this method explicitly; the service itself has no dependency on global config.
    /// @param options Complete validated settings, including the desired worker count.
    /// @return OK after the new snapshot is active; InvalidArgument for invalid limits, or a
    /// worker-resize error when the requested concurrency cannot be applied.
    Status update_options(const AsyncCacheWriteServiceOptions& options);

    /// Return the currently active settings as a value snapshot.
    AsyncCacheWriteServiceOptions options() const;

    /// Stop submissions, drain all accepted tasks, and join worker loops. Idempotent.
    void shutdown();

    /// Return accepted tasks that have not yet completed finalization.
    size_t pending_count() const { return _pending_count.load(std::memory_order_relaxed); }

    /// Return accepted tasks still waiting in the MPMC queue, excluding active workers.
    size_t queued_count() const { return _queued_count.load(std::memory_order_relaxed); }

    /// Return tasks currently owned by workers, including watchdog and write processing.
    size_t active_task_count() const { return _active_task_count.load(std::memory_order_relaxed); }

    /// Return worker loops that are currently alive.
    size_t running_worker_count() const {
        return _running_worker_count.load(std::memory_order_relaxed);
    }

    /// Return bytes currently held by tracked task buffers.
    int64_t buffer_memory_bytes() const { return _mem_tracker->consumption(); }

private:
    /// Mark `worker_id` active and submit its persistent loop to the thread pool.
    Status _schedule_worker(size_t worker_id);

    /// Drain batches until shutdown or until this worker id is retired by a resize.
    void _worker_loop(size_t worker_id);

    /// Revalidate epoch/cache state and persist the task's still-empty complete blocks.
    Status _write_one(const AsyncCacheWriteTask& task);

    /// Release one pending slot and invoke the inflight-index cleanup callback.
    void _finish_task(const AsyncCacheWriteTask& task);

    BlockFileCache* _cache;
    atomic_shared_ptr<const AsyncCacheWriteServiceOptions> _options;
    moodycamel::ConcurrentQueue<AsyncCacheWriteTask> _queue;
    std::atomic<size_t> _pending_count {0};
    std::atomic<size_t> _queued_count {0};
    std::atomic<size_t> _active_task_count {0};
    std::atomic<size_t> _running_worker_count {0};
    std::atomic<size_t> _active_get_or_set_count {0};
    std::atomic<size_t> _active_append_count {0};
    std::atomic<size_t> _active_finalize_count {0};
    std::condition_variable _cv;
    std::mutex _cv_mutex;
    std::atomic<bool> _accepting {true};
    std::atomic<size_t> _active_submitters {0};
    std::atomic<bool> _shutdown_requested {false};
    std::atomic<bool> _started {false};
    std::atomic<uint64_t> _write_epoch {1};

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    std::unique_ptr<ThreadPool> _worker_pool;
    std::atomic<size_t> _desired_worker_count {0};
    std::mutex _resize_mutex;
    std::mutex _worker_state_mutex;
    std::condition_variable _worker_state_cv;
    std::vector<bool> _worker_scheduled;

    std::shared_ptr<bvar::PassiveStatus<size_t>> _pending_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _queued_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_task_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _running_worker_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _configured_worker_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _max_pending_tasks_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_get_or_set_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_append_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_finalize_count_metric;
    std::shared_ptr<bvar::PassiveStatus<int64_t>> _buffer_memory_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _submitted_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _submitted_bytes_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _finished_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _rejected_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _reject_not_running_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _reject_backpressure_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _reject_enqueue_failure_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _buffer_alloc_fail_metric;
    std::shared_ptr<bvar::LatencyRecorder> _submit_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _buffer_alloc_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _queue_wait_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _worker_task_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _get_or_set_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _append_latency_metric;
    std::shared_ptr<bvar::LatencyRecorder> _finalize_latency_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_downloaded_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_downloading_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_partial_overlap_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _drop_stale_epoch_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _skip_deleting_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _append_fail_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _finalize_fail_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _persisted_blocks_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _persisted_bytes_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _watchdog_warn_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _watchdog_drop_metric;
};

} // namespace doris::io
