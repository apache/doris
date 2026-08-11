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
#include <array>
#include <exception>
#include <limits>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/logging.h"
#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "runtime/thread_context.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::io {

using AsyncCacheWriteAllocator = Allocator<false, false, false, DefaultMemoryAllocator, true>;

namespace {

static_assert(std::is_nothrow_move_constructible_v<AsyncCacheWriteTask>);
static_assert(std::is_nothrow_move_assignable_v<AsyncCacheWriteTask>);

/// Keep an in-progress phase gauge balanced across every return path.
class ScopedActiveCounter {
public:
    explicit ScopedActiveCounter(std::atomic<size_t>& counter) : _counter(counter) {
        _counter.fetch_add(1, std::memory_order_relaxed);
    }

    ~ScopedActiveCounter() { _counter.fetch_sub(1, std::memory_order_relaxed); }

private:
    std::atomic<size_t>& _counter;
};

/// Acquire the FIFO mutex while measuring only the actual lock wait and critical-section hold.
class TimedQueueLock {
public:
    TimedQueueLock(std::mutex& mutex, bvar::LatencyRecorder& wait_latency,
                   bvar::LatencyRecorder& hold_latency)
            : _lock(mutex, std::defer_lock),
              _wait_latency(wait_latency),
              _hold_latency(hold_latency) {
        const int64_t wait_start_us = MonotonicMicros();
        _lock.lock();
        _acquired_at_us = MonotonicMicros();
        _wait_us = _acquired_at_us - wait_start_us;
    }

    ~TimedQueueLock() {
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

class AsyncCacheWriteEpochRegistry
        : public std::enable_shared_from_this<AsyncCacheWriteEpochRegistry> {
public:
    std::shared_ptr<AsyncCacheWriteEpochToken> capture(const UInt128Wrapper& cache_hash) {
        auto& shard = _shards[_shard_index(cache_hash)];
        std::lock_guard lock(shard.mutex);
        auto iterator = shard.tokens.find(cache_hash);
        if (iterator != shard.tokens.end()) {
            auto token = iterator->second.token.lock();
            if (token != nullptr) {
                return token;
            }
            shard.tokens.erase(iterator);
            _active_key_count.fetch_sub(1, std::memory_order_relaxed);
        }

        const uint64_t generation = _next_generation.fetch_add(1, std::memory_order_relaxed);
        auto token = std::shared_ptr<AsyncCacheWriteEpochToken>(
                new AsyncCacheWriteEpochToken(cache_hash, generation, weak_from_this()));
        shard.tokens.emplace(cache_hash, Entry {.generation = generation, .token = token});
        _active_key_count.fetch_add(1, std::memory_order_relaxed);
        return token;
    }

    void invalidate(const UInt128Wrapper& cache_hash) {
        // The token destructor calls release(), so its last strong reference must outlive the shard
        // lock instead of re-entering the same mutex from inside this critical section.
        std::shared_ptr<AsyncCacheWriteEpochToken> token;
        {
            auto& shard = _shards[_shard_index(cache_hash)];
            std::lock_guard lock(shard.mutex);
            auto iterator = shard.tokens.find(cache_hash);
            if (iterator == shard.tokens.end()) {
                return;
            }
            token = iterator->second.token.lock();
            if (token != nullptr) {
                token->_valid.store(false, std::memory_order_release);
            }
            shard.tokens.erase(iterator);
            _active_key_count.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void release(const UInt128Wrapper& cache_hash, uint64_t generation) {
        auto& shard = _shards[_shard_index(cache_hash)];
        std::lock_guard lock(shard.mutex);
        auto iterator = shard.tokens.find(cache_hash);
        if (iterator == shard.tokens.end() || iterator->second.generation != generation) {
            return;
        }
        shard.tokens.erase(iterator);
        _active_key_count.fetch_sub(1, std::memory_order_relaxed);
    }

    size_t active_key_count() const { return _active_key_count.load(std::memory_order_relaxed); }

private:
    struct Entry {
        uint64_t generation {0};
        std::weak_ptr<AsyncCacheWriteEpochToken> token;
    };

    struct Shard {
        std::mutex mutex;
        std::unordered_map<UInt128Wrapper, Entry, KeyHash> tokens;
    };

    static constexpr size_t kShardCount = 64;

    static size_t _shard_index(const UInt128Wrapper& cache_hash) {
        return KeyHash()(cache_hash) % kShardCount;
    }

    std::array<Shard, kShardCount> _shards;
    std::atomic<uint64_t> _next_generation {1};
    std::atomic<size_t> _active_key_count {0};
};

AsyncCacheWriteEpochToken::AsyncCacheWriteEpochToken(
        const UInt128Wrapper& cache_hash, uint64_t generation,
        std::weak_ptr<AsyncCacheWriteEpochRegistry> registry)
        : _cache_hash(cache_hash), _generation(generation), _registry(std::move(registry)) {}

AsyncCacheWriteEpochToken::~AsyncCacheWriteEpochToken() {
    auto registry = _registry.lock();
    if (registry != nullptr) {
        registry->release(_cache_hash, _generation);
    }
}

Status resolve_async_file_cache_write_max_pending_bytes_per_disk(int64_t configured_bytes,
                                                                 int64_t be_mem_limit,
                                                                 size_t* resolved_bytes) {
    DORIS_CHECK(resolved_bytes != nullptr);
    if (configured_bytes > 0) {
        *resolved_bytes = static_cast<size_t>(configured_bytes);
        return Status::OK();
    }
    if (configured_bytes != -1) {
        return Status::InvalidArgument(
                "async file cache write pending byte limit must be positive or -1");
    }

    DORIS_CHECK(be_mem_limit > 0);
    constexpr int64_t kMinimumAutoPendingBytes = 512LL * 1024 * 1024;
    *resolved_bytes = static_cast<size_t>(std::max(kMinimumAutoPendingBytes, be_mem_limit / 100));
    return Status::OK();
}

class AsyncCacheWriteService::Worker : public std::enable_shared_from_this<Worker> {
public:
    explicit Worker(AsyncCacheWriteService& service) : _service(service) {}

    Status start() {
        auto self = shared_from_this();
        return _service._worker_pool->submit_func([self = std::move(self)]() { self->_run(); });
    }

    void request_stop() { _stop_requested.store(true, std::memory_order_release); }

    void wait_until_stopped() { _stopped.wait(); }

private:
    void _run() {
        _service._running_worker_count.fetch_add(1, std::memory_order_relaxed);
        Defer mark_finished {[this]() {
            const size_t old_running =
                    _service._running_worker_count.fetch_sub(1, std::memory_order_relaxed);
            DCHECK_GT(old_running, 0);
            _stopped.count_down();
        }};

        while (!_stop_requested.load(std::memory_order_acquire)) {
            AsyncCacheWriteTask task;
            if (_service._try_take_task(&task)) {
                _service._process_task(std::move(task));
                continue;
            }

            if (_service._shutdown_requested.load(std::memory_order_acquire) &&
                _service._pending_count.load(std::memory_order_acquire) == 0) {
                return;
            }
            std::unique_lock lock(_service._queue_mutex);
            _service._queue_cv.wait(lock, [this]() {
                const bool shutdown_requested =
                        _service._shutdown_requested.load(std::memory_order_acquire);
                return !_service._queue.empty() ||
                       _stop_requested.load(std::memory_order_acquire) ||
                       (shutdown_requested &&
                        _service._pending_count.load(std::memory_order_relaxed) == 0);
            });
        }
    }

    AsyncCacheWriteService& _service;
    std::atomic<bool> _stop_requested {false};
    CountDownLatch _stopped {1};
};

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
          _write_epoch_registry(std::make_shared<AsyncCacheWriteEpochRegistry>()),
          _configured_worker_count(options.worker_count) {
    DORIS_CHECK(_cache != nullptr);
    DORIS_CHECK(options.worker_count > 0);
    DORIS_CHECK(options.max_pending_bytes > 0);

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
    _pending_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_pending_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->pending_bytes();
            },
            this);
    _queued_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_queue_size",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->queued_count();
            },
            this);
    _queued_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_queued_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->queued_bytes();
            },
            this);
    _active_task_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_tasks",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->active_task_count();
            },
            this);
    _active_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)->active_bytes();
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
                return static_cast<AsyncCacheWriteService*>(service)->_configured_worker_count.load(
                        std::memory_order_relaxed);
            },
            this);
    _max_pending_bytes_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_max_pending_bytes",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)
                        ->_options.load(std::memory_order_acquire)
                        ->max_pending_bytes;
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
    _active_write_epoch_key_count_metric = std::make_shared<bvar::PassiveStatus<size_t>>(
            prefix, "async_cache_write_active_key_epoch_count",
            [](void* service) {
                return static_cast<AsyncCacheWriteService*>(service)
                        ->active_write_epoch_key_count();
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

AsyncCacheWriteService::~AsyncCacheWriteService() {
    shutdown();
}

AsyncCacheWriteEpoch AsyncCacheWriteService::current_write_epoch(const UInt128Wrapper& cache_hash) {
    return AsyncCacheWriteEpoch {
            .cache_epoch = current_cache_epoch(),
            .key_token = _write_epoch_registry->capture(cache_hash),
    };
}

bool AsyncCacheWriteService::is_current_write_epoch(const AsyncCacheWriteEpoch& epoch) const {
    DORIS_CHECK(epoch.key_token != nullptr);
    return epoch.cache_epoch == current_cache_epoch() && epoch.key_token->is_valid();
}

bool AsyncCacheWriteService::check_write_epoch(const AsyncCacheWriteEpoch& epoch) {
    DORIS_CHECK(epoch.key_token != nullptr);
    if (epoch.cache_epoch != current_cache_epoch()) {
        *_drop_stale_epoch_metric << 1;
        *_drop_stale_cache_epoch_metric << 1;
        return false;
    }
    if (!epoch.key_token->is_valid()) {
        *_drop_stale_epoch_metric << 1;
        *_drop_stale_key_epoch_metric << 1;
        return false;
    }
    return true;
}

void AsyncCacheWriteService::invalidate_pending_writes(const UInt128Wrapper& cache_hash) {
    *_key_epoch_invalidate_metric << 1;
    _write_epoch_registry->invalidate(cache_hash);
}

uint64_t AsyncCacheWriteService::invalidate_all_pending_writes() {
    *_cache_epoch_invalidate_metric << 1;
    return _cache_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
}

size_t AsyncCacheWriteService::active_write_epoch_key_count() const {
    return _write_epoch_registry->active_key_count();
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

    const size_t worker_count = _configured_worker_count.load(std::memory_order_acquire);
    if (_worker_pool == nullptr) {
        RETURN_IF_ERROR(
                ThreadPoolBuilder(fmt::format("AsyncFileCacheWrite-{}",
                                              std::hash<std::string> {}(_cache->get_base_path())))
                        .set_min_threads(0)
                        .set_max_threads(static_cast<int>(worker_count))
                        .set_max_queue_size(128)
                        .build(&_worker_pool));
    }
    // A failed earlier start may have left a partial worker set. Reconcile the owned workers with
    // the latest configured count before publishing readiness.
    RETURN_IF_ERROR(_resize_workers_locked(worker_count));
    // Publish readiness only after every configured worker loop has been accepted by the pool.
    _started.store(true, std::memory_order_release);
    return Status::OK();
}

bool AsyncCacheWriteService::try_submit(AsyncCacheWriteTask task) {
    DORIS_CHECK(task.buffer != nullptr);
    DORIS_CHECK(task.write_epoch.key_token != nullptr);
    DORIS_CHECK(task.write_size > 0);
    DORIS_CHECK(task.write_size <= task.buffer->size());
    DORIS_CHECK(task.write_size <= std::numeric_limits<size_t>::max() - task.file_offset);
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

    const size_t task_buffer_bytes = task.buffer->size();
    std::optional<AsyncCacheWriteTask> victim;
    {
        TimedQueueLock lock(_queue_mutex, *_queue_lock_wait_latency_metric,
                            *_queue_lock_hold_latency_metric);
        const auto options = _options.load(std::memory_order_acquire);
        const size_t max_pending_bytes = options->max_pending_bytes;
        const size_t pending_bytes = _pending_bytes.load(std::memory_order_relaxed);
        if (_task_buffer_size == 0) {
            _task_buffer_size = task_buffer_bytes;
        }
        DORIS_CHECK(task_buffer_bytes == _task_buffer_size);

        if (task_buffer_bytes > max_pending_bytes) {
            *_rejected_metric << 1;
            *_reject_backpressure_metric << 1;
            return false;
        }

        const bool has_capacity = pending_bytes <= max_pending_bytes - task_buffer_bytes;
        if (!has_capacity && _queue.empty()) {
            *_rejected_metric << 1;
            *_reject_backpressure_metric << 1;
            return false;
        }

        _queue.push_back(std::move(task));
        if (has_capacity) {
            _queued_bytes.fetch_add(task_buffer_bytes, std::memory_order_relaxed);
            _pending_count.fetch_add(1, std::memory_order_relaxed);
            _pending_bytes.fetch_add(task_buffer_bytes, std::memory_order_relaxed);
        } else {
            victim.emplace(std::move(_queue.front()));
            _queue.pop_front();
        }
    }

    *_submitted_metric << 1;
    *_submitted_bytes_metric << task_buffer_bytes;
    _queue_cv.notify_one();
    if (victim) {
        _finalize_task(std::move(*victim), TaskFinalizationReason::EVICTED_OLDEST);
    }
    return true;
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

void AsyncCacheWriteService::_process_task(AsyncCacheWriteTask task) {
    Defer finish {[&]() { _finish_active_task(std::move(task)); }};

    const int64_t age_us = MonotonicMicros() - task.submit_ts_us;
    *_queue_wait_latency_metric << age_us;
    if (!check_write_epoch(task.write_epoch)) {
        return;
    }

    const int64_t start_us = MonotonicMicros();
    Status status = _write_one(task);
    *_worker_task_latency_metric << (MonotonicMicros() - start_us);
    if (!status.ok()) {
        LOG(WARNING) << "Async file cache write failed, cache=" << _cache->get_base_path()
                     << ", hash=" << task.cache_hash.to_string() << ", offset=" << task.file_offset
                     << ", size=" << task.write_size << ", status=" << status;
    }
}

bool AsyncCacheWriteService::_try_take_task(AsyncCacheWriteTask* task) {
    TimedQueueLock lock(_queue_mutex, *_queue_lock_wait_latency_metric,
                        *_queue_lock_hold_latency_metric);
    if (_queue.empty()) {
        return false;
    }

    *task = std::move(_queue.front());
    _queue.pop_front();
    const size_t task_buffer_bytes = task->buffer->size();
    _queued_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
    _active_task_count.fetch_add(1, std::memory_order_relaxed);
    _active_bytes.fetch_add(task_buffer_bytes, std::memory_order_relaxed);
    return true;
}

Status AsyncCacheWriteService::_write_one(const AsyncCacheWriteTask& task) {
    if (!check_write_epoch(task.write_epoch)) {
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
        ScopedActiveCounter active_get_or_set(_active_get_or_set_count);
        const int64_t start_us = MonotonicMicros();
        Defer record_latency {
                [&]() { *_get_or_set_latency_metric << (MonotonicMicros() - start_us); }};
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:before_get_or_set", &task);
        auto result =
                _cache->get_or_set(task.cache_hash, task.file_offset, task.write_size, context);
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteService::_write_one:after_get_or_set", &task);
        return result;
    }();

    if (!check_write_epoch(task.write_epoch)) {
        return Status::OK();
    }

    const size_t task_end = task.file_offset + task.write_size;
    for (const auto& block : holder.file_blocks) {
        if (block->range().left < task.file_offset || block->range().right >= task_end) {
            *_skip_partial_overlap_metric << 1;
            continue;
        }
        if (!check_write_epoch(task.write_epoch)) {
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
        DORIS_CHECK(buffer_offset <= task.write_size);
        DORIS_CHECK(block->range().size() <= task.write_size - buffer_offset);
        Status status;
        {
            ScopedActiveCounter active_append(_active_append_count);
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
            ScopedActiveCounter active_finalize(_active_finalize_count);
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
    const size_t task_buffer_bytes = task.buffer->size();
    bool became_empty = false;
    {
        TimedQueueLock lock(_queue_mutex, *_queue_lock_wait_latency_metric,
                            *_queue_lock_hold_latency_metric);
        const size_t old_active = _active_task_count.fetch_sub(1, std::memory_order_relaxed);
        DCHECK_GT(old_active, 0);
        _active_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
        const size_t old_pending = _pending_count.fetch_sub(1, std::memory_order_relaxed);
        DCHECK_GT(old_pending, 0);
        _pending_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
        became_empty = old_pending == 1;
    }
    _finalize_task(std::move(task), TaskFinalizationReason::WORKER_FINISHED);
    if (became_empty) {
        _queue_cv.notify_all();
    }
}

void AsyncCacheWriteService::_finalize_task(AsyncCacheWriteTask task,
                                            TaskFinalizationReason reason) {
    const size_t task_buffer_bytes = task.buffer->size();
    if (reason == TaskFinalizationReason::WORKER_FINISHED) {
        *_worker_finished_metric << 1;
        *_worker_finished_bytes_metric << task_buffer_bytes;
    } else {
        *_evicted_oldest_metric << 1;
        *_evicted_oldest_bytes_metric << task_buffer_bytes;
        *_evicted_oldest_age_metric << (MonotonicMicros() - task.submit_ts_us);
    }
    *_finished_metric << 1;
    *_finished_bytes_metric << task_buffer_bytes;
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
        _configured_worker_count.store(worker_count, std::memory_order_release);
        return Status::OK();
    }
    if (_shutdown_requested.load(std::memory_order_acquire)) {
        return Status::InternalError("async file cache write service is shutting down");
    }
    _configured_worker_count.store(worker_count, std::memory_order_release);
    return _resize_workers_locked(worker_count);
}

Status AsyncCacheWriteService::_resize_workers_locked(size_t worker_count) {
    DORIS_CHECK(_worker_pool != nullptr);
    if (worker_count < _workers.size()) {
        for (size_t index = worker_count; index < _workers.size(); ++index) {
            _workers[index]->request_stop();
        }
        _queue_cv.notify_all();
        for (size_t index = worker_count; index < _workers.size(); ++index) {
            _workers[index]->wait_until_stopped();
        }
        _workers.resize(worker_count);
        RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
        return Status::OK();
    }

    RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
    while (_workers.size() < worker_count) {
        auto worker = std::make_shared<Worker>(*this);
        RETURN_IF_ERROR(worker->start());
        _workers.emplace_back(std::move(worker));
    }
    return Status::OK();
}

Status AsyncCacheWriteService::update_options(const AsyncCacheWriteServiceOptions& options) {
    if (options.worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    if (options.max_pending_bytes == 0) {
        return Status::InvalidArgument(
                "async file cache write pending byte limit must be positive");
    }
    auto next_options = std::make_shared<const AsyncCacheWriteServiceOptions>(options);
    RETURN_IF_ERROR(resize_workers(options.worker_count));
    {
        TimedQueueLock lock(_queue_mutex, *_queue_lock_wait_latency_metric,
                            *_queue_lock_hold_latency_metric);
        _options.store(std::move(next_options), std::memory_order_release);
    }
    return Status::OK();
}

AsyncCacheWriteServiceOptions AsyncCacheWriteService::options() const {
    AsyncCacheWriteServiceOptions result = *_options.load(std::memory_order_acquire);
    result.worker_count = _configured_worker_count.load(std::memory_order_acquire);
    return result;
}

size_t AsyncCacheWriteService::queued_count() const {
    std::lock_guard lock(_queue_mutex);
    return _queue.size();
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
        for (const auto& worker : _workers) {
            worker->wait_until_stopped();
        }
        _worker_pool->shutdown();
    }
}

} // namespace doris::io
