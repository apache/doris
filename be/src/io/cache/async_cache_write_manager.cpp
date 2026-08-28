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

#include "io/cache/async_cache_write_manager.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "io/cache/async_cache_write_manager_metrics.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/inflight_write_buffer_index.h"
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

CacheAdmissionContext CacheAdmissionContext::from_cache_context(const CacheContext& context,
                                                                int64_t tablet_id) {
    return CacheAdmissionContext {
            .query_id = context.query_id,
            .cache_type = context.cache_type,
            .expiration_time = context.expiration_time,
            .tablet_id = tablet_id,
            .is_warmup = context.is_warmup,
    };
}

CacheContext CacheAdmissionContext::to_cache_context(ReadStatistics* stats) const {
    DORIS_CHECK(stats != nullptr);
    CacheContext context;
    context.query_id = query_id;
    context.cache_type = cache_type;
    context.expiration_time = expiration_time;
    context.tablet_id = tablet_id;
    context.is_warmup = is_warmup;
    context.stats = stats;
    return context;
}

void AsyncCacheWriteTask::validate() const {
    DORIS_CHECK(buffer != nullptr);
    DORIS_CHECK(write_epoch.key_token != nullptr);
    DORIS_CHECK(write_size > 0);
    DORIS_CHECK(write_size <= buffer_size());
    DORIS_CHECK(write_size <= std::numeric_limits<size_t>::max() - file_offset);
}

size_t AsyncCacheWriteTask::buffer_size() const {
    return buffer->size();
}

void AsyncCacheWriteTask::finalize() const {
    if (on_finalized) {
        on_finalized(*this);
    }
}

class AsyncCacheWriteEpochRegistry
        : public std::enable_shared_from_this<AsyncCacheWriteEpochRegistry> {
public:
    std::shared_ptr<AsyncCacheWriteEpochToken> capture(const UInt128Wrapper& cache_hash) {
        auto& shard = _shards[_shard_index(cache_hash)];
        const auto find_live_token = [&]() -> std::shared_ptr<AsyncCacheWriteEpochToken> {
            auto iterator = shard.tokens.find(cache_hash);
            if (iterator == shard.tokens.end()) {
                return nullptr;
            }
            auto token = iterator->second.token.lock();
            if (token != nullptr) {
                return token;
            }
            shard.tokens.erase(iterator);
            _active_key_count.fetch_sub(1, std::memory_order_relaxed);
            return nullptr;
        };

        {
            std::lock_guard lock(shard.mutex);
            if (auto token = find_live_token(); token != nullptr) {
                return token;
            }
        }

        const uint64_t generation = _next_generation.fetch_add(1, std::memory_order_relaxed);
        auto candidate_object = std::unique_ptr<AsyncCacheWriteEpochToken>(
                new AsyncCacheWriteEpochToken(cache_hash, generation, weak_from_this()));
        TEST_SYNC_POINT("AsyncCacheWriteEpochRegistry::capture:after_candidate_object_created");
        auto candidate = std::shared_ptr<AsyncCacheWriteEpochToken>(std::move(candidate_object));
        {
            std::lock_guard lock(shard.mutex);
            if (auto token = find_live_token(); token != nullptr) {
                return token;
            }
            TEST_SYNC_POINT("AsyncCacheWriteEpochRegistry::capture:before_candidate_publish");
            shard.tokens.emplace(cache_hash, Entry {.generation = generation, .token = candidate});
            _active_key_count.fetch_add(1, std::memory_order_relaxed);
        }
        return candidate;
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

Status resolve_async_file_cache_write_max_pending_bytes(int64_t configured_bytes,
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
    constexpr int64_t kMinimumAutoPendingBytes = 1024LL * 1024 * 1024;
    *resolved_bytes = static_cast<size_t>(std::max(kMinimumAutoPendingBytes, be_mem_limit / 100));
    return Status::OK();
}

class AsyncCacheWriteManager::Worker : public std::enable_shared_from_this<Worker> {
public:
    explicit Worker(AsyncCacheWriteManager& manager) : _manager(manager) {}

    Status start() {
        auto self = shared_from_this();
        return _manager._worker_pool->submit_func([self = std::move(self)]() { self->_run(); });
    }

    // The caller must hold the manager queue mutex so changing the wait predicate cannot race
    // with a worker between evaluating it and blocking on the condition variable.
    void request_stop() { _stop_requested.store(true, std::memory_order_release); }

    void wait_until_stopped() { _stopped.wait(); }

private:
    void _run() {
        _manager._running_worker_count.fetch_add(1, std::memory_order_relaxed);
        Defer mark_finished {[this]() {
            const size_t old_running =
                    _manager._running_worker_count.fetch_sub(1, std::memory_order_relaxed);
            DCHECK_GT(old_running, 0);
            _stopped.count_down();
        }};

        while (!_stop_requested.load(std::memory_order_acquire)) {
            AsyncCacheWriteTask task;
            if (_manager._try_activate_task(&task)) {
                _process_task(std::move(task));
                continue;
            }

            std::unique_lock lock(_manager._queue_mutex);
            TEST_SYNC_POINT("AsyncCacheWriteManager::Worker::_run:before_wait");
            _manager._queue_cv.wait(lock, [this]() {
                return !_manager._queue.empty() || _stop_requested.load(std::memory_order_acquire);
            });
        }
    }

    // A task is the worker loop's exception boundary. Its manager-side completion guard releases
    // active accounting and owner state while this boundary keeps the long-lived worker alive.
    void _process_task(AsyncCacheWriteTask task) {
        const UInt128Wrapper cache_hash = task.cache_hash;
        const size_t file_offset = task.file_offset;
        const size_t write_size = task.write_size;
        try {
            _manager._process_task(std::move(task));
        } catch (const Exception& exception) {
            _record_task_exception(cache_hash, file_offset, write_size, exception.what());
        } catch (const std::exception& exception) {
            _record_task_exception(cache_hash, file_offset, write_size, exception.what());
        } catch (...) {
            _record_task_exception(cache_hash, file_offset, write_size, "unknown exception");
        }
    }

    void _record_task_exception(const UInt128Wrapper& cache_hash, size_t file_offset,
                                size_t write_size, const char* message) {
        LOG(WARNING) << "Async file cache write task threw an exception, cache="
                     << _manager._cache->get_base_path() << ", hash=" << cache_hash.to_string()
                     << ", offset=" << file_offset << ", size=" << write_size
                     << ", exception=" << message;
    }

    AsyncCacheWriteManager& _manager;
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

AsyncCacheWriteManager::AsyncCacheWriteManager(BlockFileCache* cache,
                                               AsyncCacheWriteManagerOptions options)
        : _cache(cache),
          _options(std::make_shared<const AsyncCacheWriteManagerOptions>(options)),
          _write_epoch_registry(std::make_shared<AsyncCacheWriteEpochRegistry>()),
          _configured_worker_count(options.worker_count) {
    DORIS_CHECK(_cache != nullptr);
    DORIS_CHECK(options.worker_count > 0);
    DORIS_CHECK(options.max_pending_bytes > 0);

    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::CACHE,
            fmt::format("AsyncFileCacheWrite:{}", _cache->get_base_path()));
    _metrics = std::make_unique<Metrics>(*this, _cache->get_base_path().c_str());
}

AsyncCacheWriteManager::~AsyncCacheWriteManager() {
    shutdown();
}

AsyncCacheWriteEpoch AsyncCacheWriteManager::current_write_epoch(const UInt128Wrapper& cache_hash) {
    return AsyncCacheWriteEpoch {
            .cache_epoch = current_cache_epoch(),
            .key_token = _write_epoch_registry->capture(cache_hash),
    };
}

bool AsyncCacheWriteManager::is_current_write_epoch(const AsyncCacheWriteEpoch& epoch) const {
    DORIS_CHECK(epoch.key_token != nullptr);
    return epoch.cache_epoch == current_cache_epoch() && epoch.key_token->is_valid();
}

bool AsyncCacheWriteManager::check_write_epoch(const AsyncCacheWriteEpoch& epoch) {
    DORIS_CHECK(epoch.key_token != nullptr);
    if (epoch.cache_epoch != current_cache_epoch()) {
        _metrics->record_stale_epoch(Metrics::StaleEpochReason::CACHE);
        return false;
    }
    if (!epoch.key_token->is_valid()) {
        _metrics->record_stale_epoch(Metrics::StaleEpochReason::KEY);
        return false;
    }
    return true;
}

void AsyncCacheWriteManager::invalidate_pending_writes(const UInt128Wrapper& cache_hash) {
    _metrics->record_epoch_invalidation(Metrics::EpochInvalidationScope::KEY);
    _write_epoch_registry->invalidate(cache_hash);
}

uint64_t AsyncCacheWriteManager::invalidate_all_pending_writes() {
    _metrics->record_epoch_invalidation(Metrics::EpochInvalidationScope::CACHE);
    return _cache_epoch.fetch_add(1, std::memory_order_acq_rel) + 1;
}

size_t AsyncCacheWriteManager::active_write_epoch_key_count() const {
    return _write_epoch_registry->active_key_count();
}

Status AsyncCacheWriteManager::start() {
    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    if (!_accepting.load(std::memory_order_acquire)) {
        return Status::InternalError("async file cache write manager is shutting down");
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

bool AsyncCacheWriteManager::try_submit(AsyncCacheWriteTask task) {
    task.validate();
    const int64_t submit_start_us = MonotonicMicros();
    Defer record_submit_latency {
            [&]() { _metrics->record_submit_latency(MonotonicMicros() - submit_start_us); }};
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteManager::try_submit:before_register", &task);
    const size_t task_buffer_bytes = task.buffer_size();
    bool registered = false;
    Defer submitter_done {[&]() {
        if (registered) {
            _active_submitters.fetch_sub(1, std::memory_order_acq_rel);
        }
    }};
    std::optional<AsyncCacheWriteTask> victim;
    {
        TimedQueueLock lock(_queue_mutex, _metrics->queue_lock_wait_latency(),
                            _metrics->queue_lock_hold_latency());
        if (!_started.load(std::memory_order_acquire) ||
            !_accepting.load(std::memory_order_acquire)) {
            _metrics->record_task_rejected(Metrics::RejectionReason::NOT_RUNNING);
            return false;
        }
        _active_submitters.fetch_add(1, std::memory_order_acq_rel);
        registered = true;

        const auto options = _options.load(std::memory_order_acquire);
        const size_t max_pending_bytes = options->max_pending_bytes;
        const size_t pending_bytes = _pending_bytes.load(std::memory_order_relaxed);
        if (_task_buffer_size == 0) {
            _task_buffer_size = task_buffer_bytes;
        }
        DORIS_CHECK(task_buffer_bytes == _task_buffer_size);

        if (task_buffer_bytes > max_pending_bytes) {
            _metrics->record_task_rejected(Metrics::RejectionReason::BACKPRESSURE);
            return false;
        }

        const bool has_capacity = pending_bytes <= max_pending_bytes - task_buffer_bytes;
        if (!has_capacity && _queue.empty()) {
            _metrics->record_task_rejected(Metrics::RejectionReason::BACKPRESSURE);
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

    _metrics->record_task_submitted(task_buffer_bytes);
    _queue_cv.notify_one();
    if (victim) {
        _complete_task(std::move(*victim), TaskFinalizationReason::EVICTED_OLDEST);
    }
    return true;
}

AsyncCacheWriteBlockSubmitResult AsyncCacheWriteManager::try_submit_block(
        AsyncCacheWriteBlockRequest request) {
    DORIS_CHECK(request.data.data != nullptr);
    DORIS_CHECK(request.data.size > 0);
    DORIS_CHECK(request.buffer_size > 0);
    DORIS_CHECK(request.data.size <= request.buffer_size);
    if (!check_write_epoch(request.write_epoch)) {
        return AsyncCacheWriteBlockSubmitResult::STALE_EPOCH;
    }

    AsyncCacheWriteBufferPtr buffer;
    if (!allocate_tracked_buffer(request.buffer_size, &buffer).ok()) {
        return AsyncCacheWriteBlockSubmitResult::BUFFER_ALLOCATION_FAILED;
    }
    std::memcpy(buffer->data(), request.data.data, request.data.size);

    return try_submit_owned_block(AsyncCacheWriteOwnedBlockRequest {
            .cache_hash = request.cache_hash,
            .file_offset = request.file_offset,
            .write_size = request.data.size,
            .buffer = std::move(buffer),
            .admission_ctx = std::move(request.admission_ctx),
            .write_epoch = std::move(request.write_epoch),
            .inflight_index = request.inflight_index,
    });
}

AsyncCacheWriteBlockSubmitResult AsyncCacheWriteManager::try_submit_owned_block(
        AsyncCacheWriteOwnedBlockRequest request) {
    DORIS_CHECK(request.buffer != nullptr);
    DORIS_CHECK(request.write_size > 0);
    DORIS_CHECK(request.write_size <= request.buffer->size());
    if (!check_write_epoch(request.write_epoch)) {
        return AsyncCacheWriteBlockSubmitResult::STALE_EPOCH;
    }

    const int64_t submit_ts_us = MonotonicMicros();
    AsyncCacheWriteTask task {
            .cache_hash = request.cache_hash,
            .file_offset = request.file_offset,
            .write_size = request.write_size,
            .buffer = request.buffer,
            .admission_ctx = std::move(request.admission_ctx),
            .submit_ts_us = submit_ts_us,
            .write_epoch = std::move(request.write_epoch),
            .on_finalized = nullptr,
    };
    std::shared_ptr<InflightWriteBufferEntry> entry;
    if (request.inflight_index != nullptr) {
        entry = std::make_shared<InflightWriteBufferEntry>(request.buffer, request.file_offset,
                                                           request.write_size, submit_ts_us);
        TEST_SYNC_POINT_CALLBACK(
                "CachedRemoteFileReader::_submit_async_write_tasks:before_inflight_insert", &task);
        auto existing = request.inflight_index->insert_if_absent(request.cache_hash,
                                                                 request.file_offset, entry);
        if (existing != nullptr) {
            return AsyncCacheWriteBlockSubmitResult::ALREADY_INFLIGHT;
        }
        task.on_finalized = [cache_hash = request.cache_hash, offset = request.file_offset,
                             inflight_index = request.inflight_index,
                             entry](const AsyncCacheWriteTask&) {
            inflight_index->remove_if(cache_hash, offset, entry);
        };
    }

    if (!try_submit(std::move(task))) {
        if (entry != nullptr) {
            request.inflight_index->remove_if(request.cache_hash, request.file_offset, entry);
            request.inflight_index->record_backpressure_rollback();
        }
        return AsyncCacheWriteBlockSubmitResult::REJECTED;
    }
    return AsyncCacheWriteBlockSubmitResult::SUBMITTED;
}

bool AsyncCacheWriteManager::can_accept_without_eviction(size_t buffer_size) const {
    DORIS_CHECK(buffer_size > 0);
    std::lock_guard lock(_queue_mutex);
    if (!_started.load(std::memory_order_acquire) || !_accepting.load(std::memory_order_acquire)) {
        return false;
    }
    DORIS_CHECK(_task_buffer_size == 0 || _task_buffer_size == buffer_size);
    const size_t max_pending_bytes = _options.load(std::memory_order_acquire)->max_pending_bytes;
    const size_t pending_bytes = _pending_bytes.load(std::memory_order_relaxed);
    return buffer_size <= max_pending_bytes && pending_bytes <= max_pending_bytes - buffer_size;
}

Status AsyncCacheWriteManager::allocate_tracked_buffer(size_t size,
                                                       AsyncCacheWriteBufferPtr* buffer) {
    DORIS_CHECK(buffer != nullptr);
    DORIS_CHECK(size > 0);
    const int64_t allocation_start_us = MonotonicMicros();
    Defer record_allocation_latency {[&]() {
        _metrics->record_buffer_allocation_latency(MonotonicMicros() - allocation_start_us);
    }};
    Status injected_status;
    TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteManager::allocate_tracked_buffer:inject_failure",
                             &injected_status);
    if (!injected_status.ok()) {
        _metrics->record_buffer_allocation_failure();
        return injected_status;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    Status status = Status::OK();
    try {
        ASSIGN_STATUS_IF_CATCH_EXCEPTION(*buffer = AsyncCacheWriteBufferPtr(
                                                 new AsyncCacheWriteBuffer(size, _mem_tracker)),
                                         status);
    } catch (const std::exception& e) {
        status = Status::MemoryAllocFailed("allocate async file cache write buffer failed: {}",
                                           e.what());
    }
    if (!status.ok()) {
        _metrics->record_buffer_allocation_failure();
    }
    return status;
}

void AsyncCacheWriteManager::_process_task(AsyncCacheWriteTask task) {
    Defer complete {[&]() { _complete_active_task(std::move(task)); }};

    const int64_t age_us = MonotonicMicros() - task.submit_ts_us;
    _metrics->record_queue_wait_latency(age_us);
    if (!check_write_epoch(task.write_epoch)) {
        return;
    }

    const int64_t start_us = MonotonicMicros();
    Status status = _persist_task(task);
    _metrics->record_worker_task_latency(MonotonicMicros() - start_us);
    if (!status.ok()) {
        LOG(WARNING) << "Async file cache write failed, cache=" << _cache->get_base_path()
                     << ", hash=" << task.cache_hash.to_string() << ", offset=" << task.file_offset
                     << ", size=" << task.write_size << ", status=" << status;
    }
}

bool AsyncCacheWriteManager::_try_activate_task(AsyncCacheWriteTask* task) {
    TimedQueueLock lock(_queue_mutex, _metrics->queue_lock_wait_latency(),
                        _metrics->queue_lock_hold_latency());
    if (_queue.empty()) {
        return false;
    }

    *task = std::move(_queue.front());
    _queue.pop_front();
    const size_t task_buffer_bytes = task->buffer_size();
    _queued_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
    _active_task_count.fetch_add(1, std::memory_order_relaxed);
    _active_bytes.fetch_add(task_buffer_bytes, std::memory_order_relaxed);
    return true;
}

Status AsyncCacheWriteManager::_persist_task(const AsyncCacheWriteTask& task) {
    if (!check_write_epoch(task.write_epoch)) {
        return Status::OK();
    }

    ReadStatistics dummy_stats;
    CacheContext context = task.admission_ctx.to_cache_context(&dummy_stats);
    auto holder = [&]() {
        ScopedActiveCounter active_get_or_set(_active_get_or_set_count);
        const int64_t start_us = MonotonicMicros();
        Defer record_latency {
                [&]() { _metrics->record_get_or_set_latency(MonotonicMicros() - start_us); }};
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteManager::_persist_task:before_get_or_set", &task);
        auto result =
                _cache->get_or_set(task.cache_hash, task.file_offset, task.write_size, context);
        TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteManager::_persist_task:after_get_or_set", &task);
        return result;
    }();

    if (!check_write_epoch(task.write_epoch)) {
        return Status::OK();
    }

    const size_t task_end = task.file_offset + task.write_size;
    for (const auto& block : holder.file_blocks) {
        const auto block_range = block->range();
        const bool contained_in_task =
                block_range.left >= task.file_offset && block_range.right < task_end;
        // A short task is the physical EOF block. Its EMPTY cell may have been preallocated with
        // full cache-block capacity, so write only the valid prefix and let finalize() shrink it.
        const bool is_preallocated_eof_container = task.write_size < task.buffer_size() &&
                                                   block_range.left == task.file_offset &&
                                                   block_range.right >= task_end;
        if (!contained_in_task && !is_preallocated_eof_container) {
            _metrics->record_skipped_block(Metrics::SkippedBlockReason::PARTIAL_OVERLAP);
            continue;
        }
        if (!check_write_epoch(task.write_epoch)) {
            return Status::OK();
        }
        if (_cache->is_block_deleting(block)) {
            _metrics->record_skipped_block(Metrics::SkippedBlockReason::DELETING);
            continue;
        }

        switch (block->state()) {
        case FileBlock::State::DOWNLOADED:
            _metrics->record_skipped_block(Metrics::SkippedBlockReason::DOWNLOADED);
            continue;
        case FileBlock::State::DOWNLOADING:
            _metrics->record_skipped_block(Metrics::SkippedBlockReason::DOWNLOADING);
            continue;
        case FileBlock::State::SKIP_CACHE:
            continue;
        case FileBlock::State::EMPTY:
            break;
        }

        if (block->get_or_set_downloader() != FileBlock::get_caller_id()) {
            _metrics->record_skipped_block(Metrics::SkippedBlockReason::DOWNLOADING);
            continue;
        }
        const size_t buffer_offset = block_range.left - task.file_offset;
        const size_t append_size =
                is_preallocated_eof_container ? task.write_size : block_range.size();
        DORIS_CHECK(buffer_offset <= task.write_size);
        DORIS_CHECK(append_size <= task.write_size - buffer_offset);
        Status status;
        {
            ScopedActiveCounter active_append(_active_append_count);
            TEST_SYNC_POINT_CALLBACK("AsyncCacheWriteManager::_persist_task:before_append", &task);
            const int64_t start_us = MonotonicMicros();
            status = block->append(Slice(task.buffer->data() + buffer_offset, append_size));
            _metrics->record_block_operation_latency(Metrics::BlockOperation::APPEND,
                                                     MonotonicMicros() - start_us);
        }
        if (!status.ok()) {
            _metrics->record_block_operation_failure(Metrics::BlockOperation::APPEND);
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
            _metrics->record_block_operation_latency(Metrics::BlockOperation::FINALIZE,
                                                     MonotonicMicros() - start_us);
        }
        if (!status.ok()) {
            _metrics->record_block_operation_failure(Metrics::BlockOperation::FINALIZE);
            LOG(WARNING) << "Finalize async file cache block failed, cache="
                         << _cache->get_base_path() << ", hash=" << task.cache_hash.to_string()
                         << ", offset=" << block->offset() << ", size=" << block->range().size()
                         << ", status=" << status;
            continue;
        }
    }
    return Status::OK();
}

void AsyncCacheWriteManager::_complete_active_task(AsyncCacheWriteTask task) {
    const size_t task_buffer_bytes = task.buffer_size();
    bool became_empty = false;
    {
        TimedQueueLock lock(_queue_mutex, _metrics->queue_lock_wait_latency(),
                            _metrics->queue_lock_hold_latency());
        const size_t old_active = _active_task_count.fetch_sub(1, std::memory_order_relaxed);
        DCHECK_GT(old_active, 0);
        _active_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
        const size_t old_pending = _pending_count.fetch_sub(1, std::memory_order_relaxed);
        DCHECK_GT(old_pending, 0);
        _pending_bytes.fetch_sub(task_buffer_bytes, std::memory_order_relaxed);
        became_empty = old_pending == 1;
    }
    _complete_task(std::move(task), TaskFinalizationReason::WORKER_FINISHED);
    if (became_empty) {
        _queue_cv.notify_all();
    }
}

void AsyncCacheWriteManager::_complete_task(AsyncCacheWriteTask task,
                                            TaskFinalizationReason reason) {
    _metrics->record_task_finalized(task, reason);
    task.finalize();
}

Status AsyncCacheWriteManager::resize_workers(size_t worker_count) {
    if (worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    if (!_accepting.load(std::memory_order_acquire)) {
        return Status::InternalError("async file cache write manager is shutting down");
    }
    if (!_started.load(std::memory_order_acquire)) {
        _configured_worker_count.store(worker_count, std::memory_order_release);
        return Status::OK();
    }
    _configured_worker_count.store(worker_count, std::memory_order_release);
    return _resize_workers_locked(worker_count);
}

Status AsyncCacheWriteManager::_resize_workers_locked(size_t worker_count) {
    DORIS_CHECK(_worker_pool != nullptr);
    if (worker_count < _workers.size()) {
        _stop_workers_locked(worker_count);
        RETURN_IF_ERROR(_worker_pool->set_min_threads(static_cast<int>(worker_count)));
        RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
        return Status::OK();
    }

    RETURN_IF_ERROR(_worker_pool->set_max_threads(static_cast<int>(worker_count)));
    // Worker tasks live until resize or shutdown, so reserve one actual pool thread for each task
    // before submitting any new Worker. Unlike submit_func(), set_min_threads() propagates an OS
    // thread creation failure even when another pool thread is already running; no accepted Worker
    // task can therefore remain queued forever without a backing thread.
    RETURN_IF_ERROR(_worker_pool->set_min_threads(static_cast<int>(worker_count)));
    while (_workers.size() < worker_count) {
        auto worker = std::make_shared<Worker>(*this);
        RETURN_IF_ERROR(worker->start());
        _workers.emplace_back(std::move(worker));
    }
    return Status::OK();
}

void AsyncCacheWriteManager::_stop_workers_locked(size_t keep_worker_count) {
    DORIS_CHECK(keep_worker_count <= _workers.size());
    if (keep_worker_count == _workers.size()) {
        return;
    }

    TEST_SYNC_POINT("AsyncCacheWriteManager::_stop_workers_locked:before_request_stop");
    {
        std::lock_guard queue_lock(_queue_mutex);
        for (size_t index = keep_worker_count; index < _workers.size(); ++index) {
            _workers[index]->request_stop();
        }
        TEST_SYNC_POINT("AsyncCacheWriteManager::_stop_workers_locked:after_request_stop");
    }
    _queue_cv.notify_all();
    for (size_t index = keep_worker_count; index < _workers.size(); ++index) {
        _workers[index]->wait_until_stopped();
    }
    _workers.resize(keep_worker_count);
}

Status AsyncCacheWriteManager::update_options(const AsyncCacheWriteManagerOptions& options) {
    if (options.worker_count == 0) {
        return Status::InvalidArgument("async file cache write worker count must be positive");
    }
    if (options.max_pending_bytes == 0) {
        return Status::InvalidArgument(
                "async file cache write pending byte limit must be positive");
    }
    auto next_options = std::make_shared<const AsyncCacheWriteManagerOptions>(options);
    RETURN_IF_ERROR(resize_workers(options.worker_count));
    {
        TimedQueueLock lock(_queue_mutex, _metrics->queue_lock_wait_latency(),
                            _metrics->queue_lock_hold_latency());
        _options.store(std::move(next_options), std::memory_order_release);
    }
    return Status::OK();
}

AsyncCacheWriteManagerOptions AsyncCacheWriteManager::options() const {
    AsyncCacheWriteManagerOptions result = *_options.load(std::memory_order_acquire);
    result.worker_count = _configured_worker_count.load(std::memory_order_acquire);
    return result;
}

size_t AsyncCacheWriteManager::queued_count() const {
    std::lock_guard lock(_queue_mutex);
    return _queue.size();
}

int64_t AsyncCacheWriteManager::queue_lock_wait_p99_us() const {
    return _metrics->queue_lock_wait_p99_us();
}

int64_t AsyncCacheWriteManager::queue_lock_hold_p99_us() const {
    return _metrics->queue_lock_hold_p99_us();
}

uint64_t AsyncCacheWriteManager::evicted_oldest_count() const {
    return _metrics->evicted_oldest_count();
}

void AsyncCacheWriteManager::shutdown() {
    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    {
        std::lock_guard queue_lock(_queue_mutex);
        if (!_accepting.exchange(false, std::memory_order_acq_rel)) {
            return;
        }
    }
    TEST_SYNC_POINT("AsyncCacheWriteManager::shutdown:after_stop_accepting");
    while (_active_submitters.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }

    if (_worker_pool) {
        {
            std::unique_lock queue_lock(_queue_mutex);
            _queue_cv.wait(queue_lock, [this]() {
                return _pending_count.load(std::memory_order_relaxed) == 0;
            });
        }
        _stop_workers_locked(0);
        _worker_pool->shutdown();
    }
}

} // namespace doris::io
