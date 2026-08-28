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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "io/cache/async_cache_write_manager.h"
#include "io/fs/file_range_coalescer.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/file_reader.h"
#include "util/slice.h"

namespace doris {

class ThreadPool;

} // namespace doris

namespace doris::io {

class InflightWriteBufferIndex;

struct PartialBlockWritebackOptions {
    /// Allocation and persistence unit; also bounds each pending task's owned buffer.
    size_t block_size {1};
    /// Number of long-running workers allowed to perform hole-fill source reads.
    size_t worker_count {1};
    /// BE-wide byte limit for queued and active full-block buffers.
    size_t max_pending_bytes {1};
    /// Limits used to combine uncovered intervals within one block into source reads.
    FileRangeCoalesceOptions hole_fill_coalesce;

    Status validate() const;
};

enum class PartialBlockSubmitResult : uint8_t {
    /// A new per-block task owns a copy of the fragment and is waiting for a worker.
    QUEUED,
    /// The fragment was copied into an existing queued task for the same block.
    MERGED,
    /// The block is already being filled; the fragment was not copied because that worker reads
    /// every byte not present when it took the task.
    ACTIVE_DEDUPLICATED,
    /// A completed buffer for this block is already pending or active in the cache writer.
    CACHE_WRITE_INFLIGHT,
    /// The cache invalidation fence changed before the fragment could be accepted.
    STALE_EPOCH,
    /// A tracked full-block buffer could not be allocated for a new task.
    BUFFER_ALLOCATION_FAILED,
    /// The manager is unavailable or all pending capacity is occupied by active tasks.
    REJECTED,
};

/// One foreground fragment of a partial File Cache block. `data` is copied before try_submit()
/// returns. The target write manager and inflight index must outlive every accepted task;
/// source_reader is retained by the task. `write_epoch` is captured before the foreground read.
struct PartialBlockWritebackRequest {
    AsyncCacheWriteManager* write_manager {nullptr};
    InflightWriteBufferIndex* inflight_index {nullptr};
    FileReaderSPtr source_reader;
    UInt128Wrapper cache_hash;
    size_t block_offset {0};
    size_t block_valid_size {0};
    size_t fragment_offset {0};
    Slice data;
    CacheAdmissionContext admission_ctx;
    AsyncCacheWriteEpoch write_epoch;
    FileRangeReadIOContext io_context;
};

/// BE-level bounded queue and dedicated remote-read workers for completing partial File Cache
/// blocks. Query threads only perform memory admission and one fragment copy.
class PartialBlockWritebackManager {
public:
    ~PartialBlockWritebackManager();

    PartialBlockWritebackManager(const PartialBlockWritebackManager&) = delete;
    PartialBlockWritebackManager& operator=(const PartialBlockWritebackManager&) = delete;

    /// Validate options, create the dedicated source-read pool, and start its worker loops.
    static Status create(const PartialBlockWritebackOptions& options,
                         std::unique_ptr<PartialBlockWritebackManager>* output_manager);

    /// Best-effort submission that performs no remote or cache-disk IO on the caller. QUEUED and
    /// MERGED retain a copy of `request.data`; the two deduplication results mean equivalent work
    /// is already progressing. Other results retain none of the fragment bytes.
    PartialBlockSubmitResult try_submit(PartialBlockWritebackRequest request);

    /// Resize the dedicated remote-read worker set. Shrinking waits for retiring workers to finish
    /// their active reads; queued tasks remain available to retained workers.
    Status resize_workers(size_t worker_count);
    /// Return the configured worker target; a resize may still be converging to this value.
    size_t worker_count() const { return _configured_worker_count.load(std::memory_order_acquire); }
    /// Return worker loops currently alive in the dedicated source-read pool.
    size_t running_worker_count() const {
        return _running_worker_count.load(std::memory_order_relaxed);
    }

    /// Stop admission, discard queued tasks, and wait for active source reads. Idempotent.
    void shutdown();
    /// Return whether a new submission may currently enter admission.
    bool accepting() const;
    /// Return unique block tasks owned by the manager, including queued and active tasks.
    size_t pending_count() const;
    /// Return full-block buffer capacity owned by all pending tasks.
    size_t pending_bytes() const;
    /// Return tasks still waiting for source-read and cache-writer capacity.
    size_t queued_count() const;
    /// Return tasks currently owned by source-read workers.
    size_t active_count() const;

private:
    class Worker;

    struct BlockKey {
        AsyncCacheWriteManager* write_manager {nullptr};
        UInt128Wrapper cache_hash;
        size_t block_offset {0};

        bool operator==(const BlockKey&) const = default;
    };

    struct BlockKeyHash {
        size_t operator()(const BlockKey& key) const;
    };

    struct Task;

    using TaskPtr = std::shared_ptr<Task>;
    using Queue = std::list<TaskPtr>;

    enum class EnqueueResult : uint8_t {
        /// The candidate became the queued task for its block key.
        QUEUED,
        /// A current task already owns the key and is returned to the caller.
        EXISTING,
        /// Admission stopped or no queued task could be displaced for the candidate.
        REJECTED,
    };

    explicit PartialBlockWritebackManager(PartialBlockWritebackOptions options);

    Status _start();
    /// Resize workers while `_lifecycle_mutex` is held.
    Status _resize_workers_locked(size_t worker_count);
    /// Stop and join workers in `[keep_worker_count, _workers.size())` under the lifecycle lock.
    void _stop_workers_locked(size_t keep_worker_count);
    void _validate_request(const PartialBlockWritebackRequest& request) const;
    /// Allocate one tracked block buffer and copy the first fragment into it.
    TaskPtr _create_task(PartialBlockWritebackRequest request, const BlockKey& key);
    /// Atomically install `candidate`, replace a stale queued task, or return a current same-key
    /// task. Destruction of a displaced task occurs after the manager lock is released.
    EnqueueResult _enqueue_or_get_existing(const TaskPtr& candidate, TaskPtr* existing);
    /// Wait for a runnable queued task. When cache-writer capacity is exhausted, the worker leaves
    /// tasks in the queue and waits briefly before rescanning rather than occupying an active slot.
    TaskPtr _take_task(const Worker& worker);
    /// Under `_mutex`, discard stale/deduplicated entries and activate one task whose target cache
    /// writer has point-in-time spare capacity.
    TaskPtr _take_runnable_task_locked(Queue* discarded_tasks);
    /// Remove one queued task under `_mutex` and defer its destruction to `discarded_tasks`.
    void _discard_queued_task_locked(Queue::iterator iterator, Queue* discarded_tasks);
    /// Plan holes, perform blocking source reads, and hand the completed owned buffer to the cache
    /// writer. Every failure drops this best-effort task.
    void _process_task(const TaskPtr& task);
    /// Remove an active task and release its per-cache-writer slot accounting.
    void _complete_task(const TaskPtr& task);

    const PartialBlockWritebackOptions _options;
    const size_t _max_pending_tasks;
    // `_tasks` owns every queued or active block; `_queue` orders only queued blocks.
    Queue _queue;
    std::unordered_map<BlockKey, TaskPtr, BlockKeyHash> _tasks;
    // Count active hole-fill tasks against each writer's point-in-time spare capacity. This does
    // not reserve capacity inside AsyncCacheWriteManager.
    std::unordered_map<AsyncCacheWriteManager*, size_t> _active_hole_fill_slots_by_writer;
    bool _accepting {false};
    // Lock order is `_lifecycle_mutex` then `_mutex`; the reverse order is never used. Cache writer
    // and inflight-index calls made under `_mutex` do not call back into this manager. A Task
    // fragment mutex is always acquired alone, and removed tasks are destroyed after `_mutex` is
    // released.
    mutable std::mutex _mutex;
    std::condition_variable _queue_cv;
    std::unique_ptr<ThreadPool> _read_pool;
    std::atomic<size_t> _configured_worker_count {0};
    std::atomic<size_t> _running_worker_count {0};
    // Serializes startup, worker resizing, and shutdown.
    std::mutex _lifecycle_mutex;
    std::vector<std::shared_ptr<Worker>> _workers;
};

} // namespace doris::io
