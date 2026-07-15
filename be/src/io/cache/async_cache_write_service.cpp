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
#include <chrono>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::io {

using AsyncCacheWriteAllocator = Allocator<false, false, false, DefaultMemoryAllocator, true>;

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

AsyncCacheWriteServiceOptions AsyncCacheWriteServiceOptions::from_config() {
    return AsyncCacheWriteServiceOptions {
            .worker_count = static_cast<size_t>(config::async_file_cache_write_workers_per_disk),
            .max_pending_tasks =
                    static_cast<size_t>(config::async_file_cache_write_max_pending_tasks_per_disk),
            .batch_size = static_cast<size_t>(config::async_file_cache_write_batch_size),
            .watchdog_warn_secs = config::async_file_cache_write_watchdog_warn_secs,
            .watchdog_drop_secs = config::async_file_cache_write_watchdog_drop_secs,
            .follow_global_config = true,
    };
}

AsyncCacheWriteService::AsyncCacheWriteService(BlockFileCache* cache,
                                               AsyncCacheWriteServiceOptions options)
        : _cache(cache), _options(std::move(options)) {
    DORIS_CHECK(_cache != nullptr);
    DORIS_CHECK(_options.worker_count > 0);
    DORIS_CHECK(_options.max_pending_tasks > 0);
    DORIS_CHECK(_options.batch_size > 0);
    DORIS_CHECK(_options.watchdog_drop_secs > _options.watchdog_warn_secs);

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
    _buffer_memory_metric = std::make_shared<bvar::PassiveStatus<int64_t>>(
            prefix, "async_cache_write_buffer_memory_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->buffer_memory_bytes();
            },
            this);
    _submitted_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_submitted_total");
    _rejected_metric =
            std::make_shared<bvar::Adder<uint64_t>>(prefix, "async_cache_write_rejected_total");
    _buffer_alloc_fail_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_buffer_alloc_fail_total");
    _latency_metric =
            std::make_shared<bvar::LatencyRecorder>(prefix, "async_cache_write_latency_us");
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
    _watchdog_timeout_metric = std::make_shared<bvar::Adder<uint64_t>>(
            prefix, "async_cache_write_watchdog_timeout_total");
}

AsyncCacheWriteService::~AsyncCacheWriteService() {
    shutdown();
}

Status AsyncCacheWriteService::start() {
    bool expected = false;
    if (!_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return Status::OK();
    }

    const size_t worker_count = _options.worker_count;
    RETURN_IF_ERROR(
            ThreadPoolBuilder(fmt::format("AsyncFileCacheWrite-{}",
                                          std::hash<std::string> {}(_cache->get_base_path())))
                    .set_min_threads(0)
                    .set_max_threads(static_cast<int>(worker_count))
                    .set_max_queue_size(128)
                    .build(&_worker_pool));
    _desired_worker_count.store(worker_count, std::memory_order_release);
    _worker_scheduled.resize(worker_count, false);
    for (size_t worker_id = 0; worker_id < worker_count; ++worker_id) {
        RETURN_IF_ERROR(_schedule_worker(worker_id));
    }
    return Status::OK();
}

bool AsyncCacheWriteService::try_submit(AsyncCacheWriteTask task) {
    _active_submitters.fetch_add(1, std::memory_order_acq_rel);
    Defer submitter_done {[&]() { _active_submitters.fetch_sub(1, std::memory_order_acq_rel); }};
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::try_submit:after_register", &task);
    if (!_accepting.load(std::memory_order_acquire)) {
        *_rejected_metric << 1;
        return false;
    }

    const size_t max_pending = _max_pending_tasks();
    size_t current = _pending_count.load(std::memory_order_relaxed);
    while (current < max_pending) {
        if (_pending_count.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
            if (_queue.enqueue(std::move(task))) {
                *_submitted_metric << 1;
                _cv.notify_one();
                return true;
            }
            _pending_count.fetch_sub(1, std::memory_order_acq_rel);
            *_rejected_metric << 1;
            return false;
        }
    }
    *_rejected_metric << 1;
    return false;
}

Status AsyncCacheWriteService::allocate_tracked_buffer(size_t size,
                                                       AsyncCacheWriteBufferPtr* buffer) {
    DORIS_CHECK(buffer != nullptr);
    DORIS_CHECK(size > 0);
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
    Defer mark_stopped {[&]() {
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
        const size_t batch_size = _batch_size();
        AsyncCacheWriteTask task;
        while (processed < batch_size && _queue.try_dequeue(task)) {
            ++processed;
            Defer finish {[&]() {
                _finish_task(task);
                task = AsyncCacheWriteTask {};
            }};

            const int64_t age_us = MonotonicMicros() - task.submit_ts_us;
            const int64_t warn_us = _watchdog_warn_secs() * 1000 * 1000;
            const int64_t drop_us = _watchdog_drop_secs() * 1000 * 1000;
            if (age_us > warn_us) {
                *_watchdog_timeout_metric << 1;
                LOG(WARNING) << "Async file cache write task waited " << age_us
                             << " us, cache=" << _cache->get_base_path()
                             << ", hash=" << task.cache_hash.to_string()
                             << ", offset=" << task.file_offset;
            }
            if (age_us > drop_us) {
                continue;
            }
            if (!is_current_write_epoch(task.write_epoch)) {
                *_drop_stale_epoch_metric << 1;
                continue;
            }

            const int64_t start_us = MonotonicMicros();
            Status status = _write_one(task);
            *_latency_metric << (MonotonicMicros() - start_us);
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
            std::unique_lock lock(_cv_mutex);
            _cv.wait_for(lock, std::chrono::milliseconds(1), [&]() {
                return _queue.size_approx() != 0 ||
                       _shutdown_requested.load(std::memory_order_acquire) ||
                       worker_id >= _desired_worker_count.load(std::memory_order_acquire);
            });
        }
    }
}

Status AsyncCacheWriteService::_write_one(const AsyncCacheWriteTask& task) {
    DORIS_CHECK(task.buffer != nullptr);
    DORIS_CHECK(task.file_size == task.buffer->size());
    if (!is_current_write_epoch(task.write_epoch)) {
        *_drop_stale_epoch_metric << 1;
        return Status::OK();
    }

    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:before_get_or_set", &task);
    ReadStatistics dummy_stats;
    CacheContext context;
    context.query_id = task.admission_ctx.query_id;
    context.cache_type = task.admission_ctx.cache_type;
    context.expiration_time = task.admission_ctx.expiration_time;
    context.tablet_id = task.admission_ctx.tablet_id;
    context.is_warmup = task.admission_ctx.is_warmup;
    context.stats = &dummy_stats;
    auto holder = _cache->get_or_set(task.cache_hash, task.file_offset, task.file_size, context);
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:after_get_or_set", &task);

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
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:before_append", &task);
        Status status =
                block->append(Slice(task.buffer->data() + buffer_offset, block->range().size()));
        if (!status.ok()) {
            *_append_fail_metric << 1;
            LOG(WARNING) << "Append async file cache block failed, cache="
                         << _cache->get_base_path() << ", hash=" << task.cache_hash.to_string()
                         << ", offset=" << block->offset() << ", size=" << block->range().size()
                         << ", status=" << status;
            continue;
        }
        status = block->finalize();
        if (!status.ok()) {
            *_append_fail_metric << 1;
            LOG(WARNING) << "Finalize async file cache block failed, cache="
                         << _cache->get_base_path() << ", hash=" << task.cache_hash.to_string()
                         << ", offset=" << block->offset() << ", size=" << block->range().size()
                         << ", status=" << status;
        }
    }
    return Status::OK();
}

void AsyncCacheWriteService::_finish_task(const AsyncCacheWriteTask& task) {
    const size_t old_pending = _pending_count.fetch_sub(1, std::memory_order_acq_rel);
    DORIS_CHECK(old_pending > 0);
    if (task.on_finalized) {
        task.on_finalized(task);
    }
}

Status AsyncCacheWriteService::resize_workers(size_t worker_count) {
    if (worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    std::lock_guard resize_lock(_resize_mutex);
    if (!_started.load(std::memory_order_acquire)) {
        _options.worker_count = worker_count;
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
    _cv.notify_all();

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
    _cv.notify_all();

    if (_worker_pool) {
        {
            std::unique_lock lock(_worker_state_mutex);
            _worker_state_cv.wait(lock, [&]() {
                return std::none_of(_worker_scheduled.begin(), _worker_scheduled.end(),
                                    [](bool scheduled) { return scheduled; });
            });
        }
        DORIS_CHECK(_pending_count.load(std::memory_order_acquire) == 0);
        _worker_pool->shutdown();
    }
}

AsyncCacheWriteServiceStats AsyncCacheWriteService::stats() const {
    return AsyncCacheWriteServiceStats {
            .pending_count = pending_count(),
            .submitted = _submitted_metric->get_value(),
            .rejected = _rejected_metric->get_value(),
            .buffer_alloc_fail = _buffer_alloc_fail_metric->get_value(),
            .skip_downloaded = _skip_downloaded_metric->get_value(),
            .skip_downloading = _skip_downloading_metric->get_value(),
            .skip_partial_overlap = _skip_partial_overlap_metric->get_value(),
            .drop_stale_epoch = _drop_stale_epoch_metric->get_value(),
            .skip_deleting = _skip_deleting_metric->get_value(),
            .append_fail = _append_fail_metric->get_value(),
            .watchdog_timeout = _watchdog_timeout_metric->get_value(),
    };
}

size_t AsyncCacheWriteService::_max_pending_tasks() const {
    if (_options.follow_global_config) {
        return static_cast<size_t>(config::async_file_cache_write_max_pending_tasks_per_disk);
    }
    return _options.max_pending_tasks;
}

size_t AsyncCacheWriteService::_batch_size() const {
    if (_options.follow_global_config) {
        return static_cast<size_t>(config::async_file_cache_write_batch_size);
    }
    return _options.batch_size;
}

int64_t AsyncCacheWriteService::_watchdog_warn_secs() const {
    return _options.follow_global_config ? config::async_file_cache_write_watchdog_warn_secs
                                         : _options.watchdog_warn_secs;
}

int64_t AsyncCacheWriteService::_watchdog_drop_secs() const {
    return _options.follow_global_config ? config::async_file_cache_write_watchdog_drop_secs
                                         : _options.watchdog_drop_secs;
}

} // namespace doris::io
