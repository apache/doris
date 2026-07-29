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

#include "io/cache/async_cache_write_service.h"

#include <algorithm>
#include <new>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>

#include "common/logging.h"
#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::io {

using AsyncCacheWriteAllocator = Allocator<false, false, false, DefaultMemoryAllocator, true>;

namespace {

static_assert(std::is_nothrow_move_constructible_v<AsyncCacheWriteTask>);
static_assert(std::is_nothrow_move_assignable_v<AsyncCacheWriteTask>);

enum class SubmitResult : uint8_t {
    ACCEPTED,
    REPLACED_OLDEST,
    REJECT_BACKPRESSURE,
    REJECT_NO_QUEUED_VICTIM,
    REJECT_ABOVE_CURRENT_LIMIT,
    REJECT_ENQUEUE_FAILURE,
};

/// Keep an in-progress phase gauge balanced across every return path.
class ScopedActiveCounter {
public:
    explicit ScopedActiveCounter(std::atomic<size_t>* counter) : _counter(counter) {
        DORIS_CHECK(_counter != nullptr);
        _counter->fetch_add(1, std::memory_order_relaxed);
    }

    ~ScopedActiveCounter() {
        const size_t old_count = _counter->fetch_sub(1, std::memory_order_relaxed);
        DORIS_CHECK(old_count > 0);
    }

private:
    std::atomic<size_t>* _counter;
};

/// Acquire the FIFO mutex while measuring only the actual lock wait and critical-section hold.
class TimedQueueLock {
public:
    TimedQueueLock(std::mutex& mutex, bvar::LatencyRecorder* wait_latency,
                   bvar::LatencyRecorder* hold_latency)
            : _lock(mutex, std::defer_lock),
              _wait_latency(wait_latency),
              _hold_latency(hold_latency) {
        DORIS_CHECK(_wait_latency != nullptr);
        DORIS_CHECK(_hold_latency != nullptr);
        const int64_t wait_start_us = MonotonicMicros();
        _lock.lock();
        _acquired_at_us = MonotonicMicros();
        _wait_us = _acquired_at_us - wait_start_us;
    }

    ~TimedQueueLock() {
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

AsyncCacheWriteBuffer::AsyncCacheWriteBuffer(size_t size,
                                             std::shared_ptr<MemTrackerLimiter> tracker)
        : _size(size), _tracker(std::move(tracker)) {
    AsyncCacheWriteAllocator allocator;
    _data = reinterpret_cast<char*>(allocator.alloc(_size));
}

AsyncCacheWriteBuffer::~AsyncCacheWriteBuffer() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_tracker);
    AsyncCacheWriteAllocator allocator;
    allocator.free(_data, _size);
}

AsyncCacheWriteService::AsyncCacheWriteService(BlockFileCache* cache,
                                               AsyncCacheWriteServiceOptions options)
        : _cache(cache),
          _options(std::make_shared<const AsyncCacheWriteServiceOptions>(options)),
          _desired_worker_count(options.worker_count) {
    DORIS_CHECK(_cache != nullptr);
    DORIS_CHECK(options.worker_count > 0);
    DORIS_CHECK(options.max_pending_tasks > 0);
    DORIS_CHECK(options.batch_size > 0);
    DORIS_CHECK(options.watchdog_warn_secs >= 0);
    DORIS_CHECK(options.watchdog_drop_secs > options.watchdog_warn_secs);

    const char* prefix = _cache->get_base_path().c_str();
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::CACHE,
            fmt::format("AsyncFileCacheWrite:{}", _cache->get_base_path()));
    _pending_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_pending_count",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->pending_count();
            },
            this);
    _queued_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_queue_size",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->queued_count();
            },
            this);
    _active_task_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_tasks",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->active_task_count();
            },
            this);
    _running_worker_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_running_workers",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->running_worker_count();
            },
            this);
    _configured_worker_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_configured_workers",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_desired_worker_count.load(
                        std::memory_order_relaxed);
            },
            this);
    _max_pending_tasks_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_max_pending_tasks",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)
                        ->_options.load(std::memory_order_acquire)
                        ->max_pending_tasks;
            },
            this);
    _active_get_or_set_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_get_or_set",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_get_or_set_count.load(
                        std::memory_order_relaxed);
            },
            this);
    _active_append_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_append",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_append_count.load(
                        std::memory_order_relaxed);
            },
            this);
    _active_finalize_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_finalize",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->_active_finalize_count.load(
                        std::memory_order_relaxed);
            },
            this);
    _buffer_memory_metric = std::make_shared<bvar::PassiveStatus<int64_t>>(
            prefix, "async_cache_write_buffer_memory_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->buffer_memory_bytes();
            },
            this);
    _submitted_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_submitted_total");
    _submitted_bytes_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_submitted_bytes_total");
    _finished_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_finished_total");
    _worker_finished_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_worker_finished_total");
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
    _reject_enqueue_failure_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_reject_enqueue_failure_total");
    _reject_no_queued_victim_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_reject_no_queued_victim_total");
    _reject_above_current_limit_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_reject_above_current_limit_total");
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
    _watchdog_warn_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_watchdog_warn_total");
    _watchdog_drop_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_watchdog_drop_total");
}

AsyncCacheWriteService::~AsyncCacheWriteService() {
    shutdown();
}

Status AsyncCacheWriteService::start() {
    std::lock_guard resize_lock(_resize_mutex);
    if (_shutdown_requested.load(std::memory_order_acquire) ||
        !_accepting.load(std::memory_order_acquire)) {
        return Status::InternalError("async file cache write service is shutting down");
    }
    if (_started.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    const size_t worker_count = _desired_worker_count.load(std::memory_order_acquire);
    if (_worker_pool == nullptr) {
        RETURN_IF_ERROR(
                ThreadPoolBuilder(fmt::format("AsyncFileCacheWrite-{}",
                                              std::hash<std::string> {}(_cache->get_base_path())))
                        .set_min_threads(0)
                        .set_max_threads(static_cast<int>(worker_count))
                        .set_max_queue_size(128)
                        .build(&_worker_pool));
    } else {
        // A previous start may have created only part of the requested workers before returning an
        // error. Reuse that pool and apply the latest desired size before filling the missing ids.
        RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
    }
    {
        std::lock_guard state_lock(_worker_state_mutex);
        if (_worker_scheduled.size() < worker_count) {
            _worker_scheduled.resize(worker_count, false);
        }
    }
    for (size_t worker_id = 0; worker_id < worker_count; ++worker_id) {
        bool scheduled = false;
        {
            std::lock_guard state_lock(_worker_state_mutex);
            scheduled = _worker_scheduled[worker_id];
        }
        if (!scheduled) {
            RETURN_IF_ERROR(_schedule_worker(worker_id));
        }
    }
    // Publish readiness only after every configured worker loop has been accepted by the pool.
    _started.store(true, std::memory_order_release);
    return Status::OK();
}

bool AsyncCacheWriteService::try_submit(AsyncCacheWriteTask task) {
    const int64_t submit_start_us = MonotonicMicros();
    Defer record_submit_latency {
            [&]() { *_submit_latency_metric << (MonotonicMicros() - submit_start_us); }};
    _active_submitters.fetch_add(1, std::memory_order_acq_rel);
    Defer submitter_done {[&]() { _active_submitters.fetch_sub(1, std::memory_order_acq_rel); }};
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::try_submit:after_register", &task);
    if (!_started.load(std::memory_order_acquire) || !_accepting.load(std::memory_order_acquire)) {
        *_rejected_metric << 1;
        *_reject_not_running_metric << 1;
        return false;
    }

    const size_t task_bytes = task.file_size;
    bool inject_enqueue_failure = false;
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::try_submit:inject_enqueue_failure",
                             &inject_enqueue_failure);
    SubmitResult result = SubmitResult::REJECT_BACKPRESSURE;
    std::optional<AsyncCacheWriteTask> victim;
    {
        TimedQueueLock lock(_queue_mutex, _queue_lock_wait_latency_metric.get(),
                            _queue_lock_hold_latency_metric.get());
        const auto options = _options.load(std::memory_order_acquire);
        const size_t max_pending = options->max_pending_tasks;
        const size_t pending = _pending_count.load(std::memory_order_relaxed);
        const auto push_task = [&]() {
            if (inject_enqueue_failure) {
                return false;
            }
            try {
                _queue.push_back(std::move(task));
                return true;
            } catch (const std::bad_alloc&) {
                return false;
            }
        };

        if (pending < max_pending) {
            if (push_task()) {
                _queued_count.fetch_add(1, std::memory_order_relaxed);
                _pending_count.fetch_add(1, std::memory_order_relaxed);
                result = SubmitResult::ACCEPTED;
            } else {
                result = SubmitResult::REJECT_ENQUEUE_FAILURE;
            }
        } else if (pending > max_pending) {
            result = SubmitResult::REJECT_ABOVE_CURRENT_LIMIT;
        } else if (_queue.empty()) {
            result = SubmitResult::REJECT_NO_QUEUED_VICTIM;
        } else if (push_task()) {
            victim.emplace(std::move(_queue.front()));
            _queue.pop_front();
            result = SubmitResult::REPLACED_OLDEST;
        } else {
            result = SubmitResult::REJECT_ENQUEUE_FAILURE;
        }
        _check_queue_invariants_locked();
    }

    if (result == SubmitResult::ACCEPTED || result == SubmitResult::REPLACED_OLDEST) {
        *_submitted_metric << 1;
        *_submitted_bytes_metric << task_bytes;
        _queue_cv.notify_one();
        if (victim) {
            _finalize_task(std::move(*victim), TaskFinalizationReason::EVICTED_OLDEST);
        }
        return true;
    }

    *_rejected_metric << 1;
    if (result == SubmitResult::REJECT_ENQUEUE_FAILURE) {
        *_reject_enqueue_failure_metric << 1;
        return false;
    }
    *_reject_backpressure_metric << 1;
    if (result == SubmitResult::REJECT_NO_QUEUED_VICTIM) {
        *_reject_no_queued_victim_metric << 1;
    } else if (result == SubmitResult::REJECT_ABOVE_CURRENT_LIMIT) {
        *_reject_above_current_limit_metric << 1;
    }
    return false;
}

Status AsyncCacheWriteService::allocate_tracked_buffer(size_t size,
                                                       AsyncCacheWriteBufferPtr* buffer) {
    DORIS_CHECK(buffer != nullptr);
    DORIS_CHECK(size > 0);
    const int64_t allocation_start_us = MonotonicMicros();
    Defer record_allocation_latency {
            [&]() { *_buffer_alloc_latency_metric << (MonotonicMicros() - allocation_start_us); }};
    Status injected_status;
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::allocate_tracked_buffer:inject_failure",
                             &injected_status);
    if (!injected_status.ok()) {
        *_buffer_alloc_fail_metric << 1;
        return injected_status;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    Status status = Status::OK();
    try {
        *buffer = AsyncCacheWriteBufferPtr(new AsyncCacheWriteBuffer(size, _mem_tracker));
    } catch (const std::exception& e) {
        status = Status::MemoryAllocFailed("allocate async file cache write buffer failed: {}",
                                           e.what());
    }
    if (!status.ok()) {
        *_buffer_alloc_fail_metric << 1;
    }
    return status;
}

Status AsyncCacheWriteService::_schedule_worker(size_t worker_id) {
    {
        std::lock_guard lock(_worker_state_mutex);
        if (_worker_scheduled.size() <= worker_id) {
            _worker_scheduled.resize(worker_id + 1, false);
        }
        DORIS_CHECK(!_worker_scheduled[worker_id]);
        _worker_scheduled[worker_id] = true;
    }
    Status status = _worker_pool->submit_func([this, worker_id]() { _worker_loop(worker_id); });
    if (!status.ok()) {
        std::lock_guard lock(_worker_state_mutex);
        _worker_scheduled[worker_id] = false;
        _worker_state_cv.notify_all();
    }
    return status;
}

void AsyncCacheWriteService::_worker_loop(size_t worker_id) {
    _running_worker_count.fetch_add(1, std::memory_order_relaxed);
    Defer mark_stopped {[&]() {
        const size_t old_running = _running_worker_count.fetch_sub(1, std::memory_order_relaxed);
        DORIS_CHECK(old_running > 0);
        std::lock_guard lock(_worker_state_mutex);
        _worker_scheduled[worker_id] = false;
        _worker_state_cv.notify_all();
    }};

    while (true) {
        if (!_shutdown_requested.load(std::memory_order_acquire) &&
            worker_id >= _desired_worker_count.load(std::memory_order_acquire)) {
            return;
        }

        size_t processed = 0;
        const auto options = _options.load(std::memory_order_acquire);
        AsyncCacheWriteTask task;
        while (processed < options->batch_size && _try_take_task(&task)) {
            ++processed;
            Defer finish {[&]() { _finish_active_task(std::move(task)); }};

            const int64_t age_us = MonotonicMicros() - task.submit_ts_us;
            *_queue_wait_latency_metric << age_us;
            const int64_t warn_us = options->watchdog_warn_secs * 1000 * 1000;
            const int64_t drop_us = options->watchdog_drop_secs * 1000 * 1000;
            if (age_us > warn_us) {
                *_watchdog_warn_metric << 1;
                LOG(WARNING) << "Async file cache write task waited " << age_us
                             << " us, cache=" << _cache->get_base_path()
                             << ", hash=" << task.cache_hash.to_string()
                             << ", offset=" << task.file_offset;
            }
            if (age_us > drop_us) {
                *_watchdog_drop_metric << 1;
                continue;
            }
            if (!is_current_write_epoch(task.write_epoch)) {
                *_drop_stale_epoch_metric << 1;
                continue;
            }

            const int64_t start_us = MonotonicMicros();
            Status status = _write_one(task);
            *_worker_task_latency_metric << (MonotonicMicros() - start_us);
            if (!status.ok()) {
                LOG(WARNING) << "Async file cache write failed, cache=" << _cache->get_base_path()
                             << ", hash=" << task.cache_hash.to_string()
                             << ", offset=" << task.file_offset << ", size=" << task.file_size
                             << ", status=" << status;
            }
        }

        if (_shutdown_requested.load(std::memory_order_acquire) &&
            _pending_count.load(std::memory_order_acquire) == 0) {
            return;
        }
        if (processed == 0) {
            std::unique_lock lock(_queue_mutex);
            _queue_cv.wait(lock, [&]() {
                const bool shutdown_requested = _shutdown_requested.load(std::memory_order_acquire);
                return !_queue.empty() ||
                       (!shutdown_requested &&
                        worker_id >= _desired_worker_count.load(std::memory_order_acquire)) ||
                       (shutdown_requested && _pending_count.load(std::memory_order_relaxed) == 0);
            });
        }
    }
}

bool AsyncCacheWriteService::_try_take_task(AsyncCacheWriteTask* task) {
    DORIS_CHECK(task != nullptr);
    TimedQueueLock lock(_queue_mutex, _queue_lock_wait_latency_metric.get(),
                        _queue_lock_hold_latency_metric.get());
    if (_queue.empty()) {
        _check_queue_invariants_locked();
        return false;
    }

    *task = std::move(_queue.front());
    _queue.pop_front();
    const size_t old_queued = _queued_count.fetch_sub(1, std::memory_order_relaxed);
    DORIS_CHECK(old_queued > 0);
    _active_task_count.fetch_add(1, std::memory_order_relaxed);
    _check_queue_invariants_locked();
    return true;
}

Status AsyncCacheWriteService::_write_one(const AsyncCacheWriteTask& task) {
    DORIS_CHECK(task.buffer != nullptr);
    DORIS_CHECK(task.file_size == task.buffer->size());
    if (!is_current_write_epoch(task.write_epoch)) {
        *_drop_stale_epoch_metric << 1;
        return Status::OK();
    }

    ReadStatistics dummy_stats;
    CacheContext context;
    context.query_id = task.admission_ctx.query_id;
    context.cache_type = task.admission_ctx.cache_type;
    context.expiration_time = task.admission_ctx.expiration_time;
    context.tablet_id = task.admission_ctx.tablet_id;
    context.is_warmup = task.admission_ctx.is_warmup;
    context.stats = &dummy_stats;
    auto holder = [&]() {
        ScopedActiveCounter active_get_or_set(&_active_get_or_set_count);
        const int64_t start_us = MonotonicMicros();
        Defer record_latency {
                [&]() { *_get_or_set_latency_metric << (MonotonicMicros() - start_us); }};
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:before_get_or_set", &task);
        auto result =
                _cache->get_or_set(task.cache_hash, task.file_offset, task.file_size, context);
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:after_get_or_set", &task);
        return result;
    }();

    if (!is_current_write_epoch(task.write_epoch)) {
        *_drop_stale_epoch_metric << 1;
        return Status::OK();
    }

    const size_t task_end = task.file_offset + task.file_size;
    for (const auto& block : holder.file_blocks) {
        if (block->range().left < task.file_offset || block->range().right >= task_end) {
            *_skip_partial_overlap_metric << 1;
            continue;
        }
        if (!is_current_write_epoch(task.write_epoch)) {
            *_drop_stale_epoch_metric << 1;
            return Status::OK();
        }
        if (_cache->is_block_deleting(block)) {
            *_skip_deleting_metric << 1;
            continue;
        }

        switch (block->state()) {
        case FileBlock::State::DOWNLOADED:
            *_skip_downloaded_metric << 1;
            continue;
        case FileBlock::State::DOWNLOADING:
            *_skip_downloading_metric << 1;
            continue;
        case FileBlock::State::SKIP_CACHE:
            continue;
        case FileBlock::State::EMPTY:
            break;
        }

        if (block->get_or_set_downloader() != FileBlock::get_caller_id()) {
            *_skip_downloading_metric << 1;
            continue;
        }
        const size_t buffer_offset = block->range().left - task.file_offset;
        Status status;
        {
            ScopedActiveCounter active_append(&_active_append_count);
            TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:before_append", &task);
            const int64_t start_us = MonotonicMicros();
            status = block->append(
                    Slice(task.buffer->data() + buffer_offset, block->range().size()));
            *_append_latency_metric << (MonotonicMicros() - start_us);
        }
        if (!status.ok()) {
            *_append_fail_metric << 1;
            LOG(WARNING) << "Append async file cache block failed, cache="
                         << _cache->get_base_path() << ", hash=" << task.cache_hash.to_string()
                         << ", offset=" << block->offset() << ", size=" << block->range().size()
                         << ", status=" << status;
            continue;
        }
        {
            ScopedActiveCounter active_finalize(&_active_finalize_count);
            const int64_t start_us = MonotonicMicros();
            status = block->finalize();
            *_finalize_latency_metric << (MonotonicMicros() - start_us);
        }
        if (!status.ok()) {
            *_finalize_fail_metric << 1;
            LOG(WARNING) << "Finalize async file cache block failed, cache="
                         << _cache->get_base_path() << ", hash=" << task.cache_hash.to_string()
                         << ", offset=" << block->offset() << ", size=" << block->range().size()
                         << ", status=" << status;
            continue;
        }
        *_persisted_blocks_metric << 1;
        *_persisted_bytes_metric << block->range().size();
    }
    return Status::OK();
}

void AsyncCacheWriteService::_finish_active_task(AsyncCacheWriteTask task) {
    bool became_empty = false;
    {
        TimedQueueLock lock(_queue_mutex, _queue_lock_wait_latency_metric.get(),
                            _queue_lock_hold_latency_metric.get());
        const size_t old_active = _active_task_count.fetch_sub(1, std::memory_order_relaxed);
        DORIS_CHECK(old_active > 0);
        const size_t old_pending = _pending_count.fetch_sub(1, std::memory_order_relaxed);
        DORIS_CHECK(old_pending > 0);
        became_empty = old_pending == 1;
        _check_queue_invariants_locked();
    }
    _finalize_task(std::move(task), TaskFinalizationReason::WORKER_FINISHED);
    if (became_empty) {
        _queue_cv.notify_all();
    }
}

void AsyncCacheWriteService::_finalize_task(AsyncCacheWriteTask task,
                                            TaskFinalizationReason reason) {
    if (reason == TaskFinalizationReason::WORKER_FINISHED) {
        *_worker_finished_metric << 1;
    } else {
        *_evicted_oldest_metric << 1;
        *_evicted_oldest_bytes_metric << task.file_size;
        *_evicted_oldest_age_metric << (MonotonicMicros() - task.submit_ts_us);
    }
    *_finished_metric << 1;
    if (task.on_finalized) {
        task.on_finalized(task);
    }
}

void AsyncCacheWriteService::_check_queue_invariants_locked() const {
    const size_t queued = _queued_count.load(std::memory_order_relaxed);
    const size_t active = _active_task_count.load(std::memory_order_relaxed);
    const size_t pending = _pending_count.load(std::memory_order_relaxed);
    DORIS_CHECK(queued == _queue.size());
    DORIS_CHECK(pending == queued + active);
}

Status AsyncCacheWriteService::resize_workers(size_t worker_count) {
    if (worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    std::lock_guard resize_lock(_resize_mutex);
    if (!_started.load(std::memory_order_acquire)) {
        _desired_worker_count.store(worker_count, std::memory_order_release);
        return Status::OK();
    }
    if (_shutdown_requested.load(std::memory_order_acquire)) {
        return Status::InternalError("async file cache write service is shutting down");
    }

    const size_t old_count =
            _desired_worker_count.exchange(worker_count, std::memory_order_acq_rel);
    if (old_count == worker_count) {
        return Status::OK();
    }
    _queue_cv.notify_all();

    if (worker_count < old_count) {
        std::unique_lock state_lock(_worker_state_mutex);
        _worker_state_cv.wait(state_lock, [&]() {
            for (size_t id = worker_count; id < old_count; ++id) {
                if (id < _worker_scheduled.size() && _worker_scheduled[id]) {
                    return false;
                }
            }
            return true;
        });
        state_lock.unlock();
        RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
        return Status::OK();
    }

    RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
    for (size_t worker_id = old_count; worker_id < worker_count; ++worker_id) {
        RETURN_IF_ERROR(_schedule_worker(worker_id));
    }
    return Status::OK();
}

Status AsyncCacheWriteService::update_options(const AsyncCacheWriteServiceOptions& options) {
    if (options.worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    if (options.max_pending_tasks == 0) {
        return Status::InvalidArgument(
                "async file cache write pending task limit must be positive");
    }
    if (options.batch_size == 0) {
        return Status::InvalidArgument("async file cache write batch size must be positive");
    }
    if (options.watchdog_warn_secs < 0 ||
        options.watchdog_drop_secs <= options.watchdog_warn_secs) {
        return Status::InvalidArgument(
                "async file cache write watchdog thresholds must satisfy 0 <= warn < drop");
    }
    auto next_options = std::make_shared<const AsyncCacheWriteServiceOptions>(options);
    RETURN_IF_ERROR(resize_workers(options.worker_count));
    {
        TimedQueueLock lock(_queue_mutex, _queue_lock_wait_latency_metric.get(),
                            _queue_lock_hold_latency_metric.get());
        _options.store(std::move(next_options), std::memory_order_release);
        _check_queue_invariants_locked();
    }
    return Status::OK();
}

AsyncCacheWriteServiceOptions AsyncCacheWriteService::options() const {
    AsyncCacheWriteServiceOptions result = *_options.load(std::memory_order_acquire);
    result.worker_count = _desired_worker_count.load(std::memory_order_acquire);
    return result;
}

int64_t AsyncCacheWriteService::queue_lock_wait_p99_us() const {
    return _queue_lock_wait_latency_metric->latency_percentile(0.99);
}

int64_t AsyncCacheWriteService::queue_lock_hold_p99_us() const {
    return _queue_lock_hold_latency_metric->latency_percentile(0.99);
}

void AsyncCacheWriteService::shutdown() {
    std::lock_guard resize_lock(_resize_mutex);
    if (!_accepting.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    TEST_SYNC_POINT("AsyncCacheWriteService::shutdown:after_stop_accepting");
    while (_active_submitters.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }
    _shutdown_requested.store(true, std::memory_order_release);
    _queue_cv.notify_all();

    if (_worker_pool) {
        {
            std::unique_lock lock(_worker_state_mutex);
            _worker_state_cv.wait(lock, [&]() {
                return std::none_of(_worker_scheduled.begin(), _worker_scheduled.end(),
                                    [](bool scheduled) { return scheduled; });
            });
        }
        {
            TimedQueueLock lock(_queue_mutex, _queue_lock_wait_latency_metric.get(),
                                _queue_lock_hold_latency_metric.get());
            _check_queue_invariants_locked();
            DORIS_CHECK(_queue.empty());
            DORIS_CHECK(_pending_count.load(std::memory_order_relaxed) == 0);
            DORIS_CHECK(_queued_count.load(std::memory_order_relaxed) == 0);
            DORIS_CHECK(_active_task_count.load(std::memory_order_relaxed) == 0);
        }
        _worker_pool->shutdown();
    }
}

} // namespace doris::io
