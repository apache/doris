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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common/atomic_shared_ptr.h"
#include "common/status.h"
#include "io/cache/file_cache_common.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/threadpool.h"

namespace doris::io {

class BlockFileCache;
class AsyncCacheWriteEpochRegistry;

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

/// One live per-file write generation. All async read plans and writes for the same cache key share
/// the current token. A key-scoped remove invalidates that token and lets later reads capture a new
/// generation without retaining an epoch entry for every historical cache key.
class AsyncCacheWriteEpochToken {
public:
    ~AsyncCacheWriteEpochToken();

    uint64_t generation() const { return _generation; }
    bool is_valid() const { return _valid.load(std::memory_order_acquire); }

private:
    friend class AsyncCacheWriteEpochRegistry;

    AsyncCacheWriteEpochToken(const UInt128Wrapper& cache_hash, uint64_t generation,
                              std::weak_ptr<AsyncCacheWriteEpochRegistry> registry);

    UInt128Wrapper _cache_hash;
    uint64_t _generation {0};
    std::weak_ptr<AsyncCacheWriteEpochRegistry> _registry;
    std::atomic<bool> _valid {true};
};

/// Composite persistence fence shared by an async read plan and its derived write tasks.
/// `cache_epoch` invalidates all older writes on one cache disk during a full-cache clear, while
/// `key_token` lets a single-file removal invalidate only older writes for that cache key. The
/// epoch does not govern inflight reads because a cache hash identifies immutable file content.
struct AsyncCacheWriteEpoch {
    uint64_t cache_epoch {0};
    std::shared_ptr<AsyncCacheWriteEpochToken> key_token;
};

/// One cache-block write. The sole production submitter allocates every `buffer` with exactly
/// `file_cache_each_block_size` bytes. `write_size` is the valid prefix starting at `file_offset`;
/// only the physical EOF block may use less than the full buffer. `write_epoch` prevents a worker
/// from resurrecting data after cache invalidation.
struct AsyncCacheWriteTask {
    UInt128Wrapper cache_hash;
    size_t file_offset {0};
    size_t write_size {0};
    AsyncCacheWriteBufferPtr buffer;
    CacheAdmissionContext admission_ctx;
    int64_t submit_ts_us {0};
    AsyncCacheWriteEpoch write_epoch;
    std::function<void(const AsyncCacheWriteTask&)> on_finalized;
};

/// Complete per-cache-disk worker and memory settings. The service receives this value
/// explicitly at construction and through update_options(); it never reads global config.
struct AsyncCacheWriteServiceOptions {
    size_t worker_count {1};
    // Accepted queued+active buffer capacity. With fixed block-size buffers, any remainder smaller
    // than one block is intentionally unusable.
    size_t max_pending_bytes {1};
};

/// Resolve the configured per-disk pending-byte limit. A positive value is used unchanged; -1
/// selects max(512 MiB, 1% of `be_mem_limit`). Config validation rejects every other value.
Status resolve_async_file_cache_write_max_pending_bytes_per_disk(int64_t configured_bytes,
                                                                 int64_t be_mem_limit,
                                                                 size_t* resolved_bytes);

/// Owns the bounded async-write queue and workers for one BlockFileCache (one cache disk).
///
/// The referenced cache must outlive this service. Shutdown stops new producers, waits registered
/// producers, and drains all accepted tasks before worker resources are released.
class AsyncCacheWriteService {
public:
    /// @param cache Non-owning target cache; it must outlive this service.
    /// @param options Initial worker and pending-memory limits.
    AsyncCacheWriteService(BlockFileCache* cache, AsyncCacheWriteServiceOptions options);
    ~AsyncCacheWriteService();

    /// Create the worker pool and schedule the configured long-running workers. Idempotent.
    Status start();

    /// Admit `task` into the memory-bounded FIFO without waiting for disk I/O. Because all tasks
    /// have one fixed cache-block buffer capacity, a full queue displaces exactly one oldest queued
    /// task. Active tasks are never displaced. After a runtime limit decrease, submissions continue
    /// to replace the oldest queued task without increasing pending bytes, even while existing
    /// pending bytes exceed the new limit. This call can briefly wait for the queue mutex and
    /// finalizes a displaced task before returning.
    /// @return true if ownership was transferred to the queue; false when workers have not been
    /// started, during shutdown, or on backpressure. A rejected task's finalization callback is
    /// not invoked.
    bool try_submit(AsyncCacheWriteTask task);

    /// Allocate `size` payload bytes charged to the service tracker and return them in `buffer`.
    Status allocate_tracked_buffer(size_t size, AsyncCacheWriteBufferPtr* buffer);

    /// Capture the disk-wide epoch and current live generation for `cache_hash`.
    AsyncCacheWriteEpoch current_write_epoch(const UInt128Wrapper& cache_hash);

    /// Return the current disk-wide epoch used by cache-clear operations.
    uint64_t current_cache_epoch() const { return _cache_epoch.load(std::memory_order_acquire); }

    /// Test whether both levels of `epoch` still accept writes.
    bool is_current_write_epoch(const AsyncCacheWriteEpoch& epoch) const;

    /// Test the epoch and record one stale-drop metric when it is no longer current.
    bool check_write_epoch(const AsyncCacheWriteEpoch& epoch);

    /// Invalidate only queued/inflight work captured for `cache_hash` before this call.
    void invalidate_pending_writes(const UInt128Wrapper& cache_hash);

    /// Advance the disk-wide epoch so every previously captured write becomes stale.
    /// @return The newly active disk-wide epoch.
    uint64_t invalidate_all_pending_writes();

    /// Return cache keys whose current valid generation is retained by at least one plan or task.
    /// Invalidated generations are excluded even while stale tasks finish releasing them.
    size_t active_write_epoch_key_count() const;

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

    /// Return buffer-capacity bytes owned by queued and active tasks.
    size_t pending_bytes() const { return _pending_bytes.load(std::memory_order_relaxed); }

    /// Return accepted tasks still waiting in the FIFO queue, excluding active workers.
    size_t queued_count() const;

    /// Return buffer-capacity bytes still waiting in the FIFO queue.
    size_t queued_bytes() const { return _queued_bytes.load(std::memory_order_relaxed); }

    /// Return tasks currently owned by workers.
    size_t active_task_count() const { return _active_task_count.load(std::memory_order_relaxed); }

    /// Return buffer-capacity bytes currently owned by workers.
    size_t active_bytes() const { return _active_bytes.load(std::memory_order_relaxed); }

    /// Return worker loops that are currently alive.
    size_t running_worker_count() const {
        return _running_worker_count.load(std::memory_order_relaxed);
    }

    /// Return bytes currently held by tracked task buffers.
    int64_t buffer_memory_bytes() const { return _mem_tracker->consumption(); }

    /// Return tasks displaced by full-queue admission.
    uint64_t evicted_oldest_count() const { return _evicted_oldest_metric->get_value(); }

    /// Return the current rolling P99 wait to acquire the FIFO mutex.
    int64_t queue_lock_wait_p99_us() const;

    /// Return the current rolling P99 FIFO mutex critical-section duration.
    int64_t queue_lock_hold_p99_us() const;

private:
    class Worker;

    enum class TaskFinalizationReason : uint8_t {
        WORKER_FINISHED,
        EVICTED_OLDEST,
    };

    /// Resize the owned worker set while `_lifecycle_mutex` is held and `_worker_pool` exists.
    Status _resize_workers_locked(size_t worker_count);

    /// Stop and join workers in `[keep_worker_count, _workers.size())` while the lifecycle mutex is
    /// held. Stop requests are published under `_queue_mutex` before waking the worker loops.
    void _stop_workers_locked(size_t keep_worker_count);

    /// Process one task already moved from queued to active ownership.
    void _process_task(AsyncCacheWriteTask task);

    /// Move the oldest queued task to active ownership.
    bool _try_take_task(AsyncCacheWriteTask* task);

    /// Revalidate epoch/cache state and persist the task's still-empty complete blocks.
    Status _write_one(const AsyncCacheWriteTask& task);

    /// Release one active pending slot, then finalize and destroy `task` outside the queue lock.
    void _finish_active_task(AsyncCacheWriteTask task);

    /// Record the terminal reason and invoke the task cleanup callback without the queue lock.
    void _finalize_task(AsyncCacheWriteTask task, TaskFinalizationReason reason);

    BlockFileCache* _cache;
    atomic_shared_ptr<const AsyncCacheWriteServiceOptions> _options;
    std::deque<AsyncCacheWriteTask> _queue;
    mutable std::mutex _queue_mutex;
    std::condition_variable _queue_cv;
    // Learned from the first submission and protected by `_queue_mutex`.
    size_t _task_buffer_size {0};
    // Pending covers all accepted tasks, while queued and active are its disjoint ownership states:
    //   pending_count = queue.size() + active_task_count
    //   pending_bytes = queued_bytes + active_bytes
    // Byte state is maintained directly and is authoritative for memory admission. Production
    // tasks currently have a fixed cache-block buffer capacity, including a partial EOF write, but
    // byte accounting deliberately does not depend on deriving bytes from task counts.
    std::atomic<size_t> _pending_count {0};
    std::atomic<size_t> _pending_bytes {0};
    std::atomic<size_t> _queued_bytes {0};
    std::atomic<size_t> _active_task_count {0};
    std::atomic<size_t> _active_bytes {0};
    std::atomic<size_t> _running_worker_count {0};
    std::atomic<size_t> _active_get_or_set_count {0};
    std::atomic<size_t> _active_append_count {0};
    std::atomic<size_t> _active_finalize_count {0};
    std::atomic<bool> _accepting {true};
    std::atomic<size_t> _active_submitters {0};
    std::atomic<bool> _started {false};
    std::atomic<uint64_t> _cache_epoch {1};
    std::shared_ptr<AsyncCacheWriteEpochRegistry> _write_epoch_registry;

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    std::unique_ptr<ThreadPool> _worker_pool;
    std::atomic<size_t> _configured_worker_count {0};
    // Serializes start, resize, and shutdown, including all changes to `_workers`.
    std::mutex _lifecycle_mutex;
    // Protected by `_lifecycle_mutex`. Worker stop state is owned by each Worker.
    std::vector<std::shared_ptr<Worker>> _workers;

    std::shared_ptr<bvar::PassiveStatus<size_t>> _pending_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _pending_bytes_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _queued_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _queued_bytes_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_task_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_bytes_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _running_worker_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _configured_worker_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _max_pending_bytes_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_get_or_set_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_append_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_finalize_count_metric;
    std::shared_ptr<bvar::PassiveStatus<size_t>> _active_write_epoch_key_count_metric;
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
    std::shared_ptr<bvar::Adder<uint64_t>> _persisted_blocks_metric;
    std::shared_ptr<bvar::Adder<uint64_t>> _persisted_bytes_metric;
};

} // namespace doris::io
