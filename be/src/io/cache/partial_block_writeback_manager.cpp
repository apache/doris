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

#include "io/cache/partial_block_writeback_manager.h"

#include <chrono>
#include <cstring>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/hole_fill_planner.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {
namespace {

using namespace std::chrono_literals;

constexpr auto kCapacityRetryInterval = 10ms;
constexpr size_t kMaxHoleFillWorkerCount = 128;

} // namespace

size_t PartialBlockWritebackManager::BlockKeyHash::operator()(const BlockKey& key) const {
    const size_t manager_hash = std::hash<AsyncCacheWriteManager*> {}(key.write_manager);
    const size_t file_hash = KeyHash {}(key.cache_hash);
    const size_t offset_hash = std::hash<size_t> {}(key.block_offset);
    return manager_hash ^ (file_hash << 1) ^ (offset_hash << 2);
}

struct PartialBlockWritebackManager::Task {
    // A queued task may merge while the manager lock is free. Once activate() wins, later
    // fragments are deduplicated and the worker obtains their bytes from the source read instead.
    std::optional<PartialBlockSubmitResult> try_merge(size_t fragment_offset, Slice data) {
        std::lock_guard lock(fragment_mutex);
        if (is_active()) {
            return PartialBlockSubmitResult::ACTIVE_DEDUPLICATED;
        }
        if (!key.write_manager->is_current_write_epoch(write_epoch)) {
            return std::nullopt;
        }

        TEST_SYNC_POINT("PartialBlockWritebackManager::try_submit:before_merge_copy");
        std::memcpy(buffer->data() + fragment_offset, data.data, data.size);
        covered_intervals.emplace_back(
                FileRange {.offset = key.block_offset + fragment_offset, .size = data.size});
        return PartialBlockSubmitResult::MERGED;
    }

    // Activation closes the merge window. Taking fragment_mutex here also includes a merge that
    // entered just before activation, so covered_intervals is stable for the hole complement.
    Status plan_hole_reads(const FileRangeCoalesceOptions& options,
                           std::vector<FileRange>* read_ranges) {
        DORIS_CHECK(is_active());
        std::lock_guard lock(fragment_mutex);
        const FileRange block {.offset = key.block_offset, .size = block_valid_size};
        return HoleFillPlanner::plan(block, covered_intervals, options, read_ranges);
    }

    void activate() {
        bool expected = false;
        DORIS_CHECK(active.compare_exchange_strong(expected, true, std::memory_order_acq_rel));
    }

    bool is_active() const { return active.load(std::memory_order_acquire); }

    // Avoid a remote GET when invalidation makes the write stale or the cache writer already owns
    // a completed buffer for this block.
    bool should_discard_before_read() const {
        if (!key.write_manager->accepting() ||
            !key.write_manager->is_current_write_epoch(write_epoch)) {
            return true;
        }
        return inflight_index != nullptr &&
               inflight_index->lookup(key.cache_hash, key.block_offset) != nullptr;
    }

    BlockKey key;
    InflightWriteBufferIndex* inflight_index {nullptr};
    FileReaderSPtr source_reader;
    size_t block_valid_size {0};
    AsyncCacheWriteBufferPtr buffer;
    std::vector<FileRange> covered_intervals;
    CacheAdmissionContext admission_ctx;
    AsyncCacheWriteEpoch write_epoch;
    FileRangeReadIOContext io_context;
    // Valid only while this task is in the manager queue; accessed under the manager mutex.
    Queue::iterator queue_position;
    // A queued task may absorb foreground fragments while unrelated queue operations proceed.
    std::mutex fragment_mutex;
    std::atomic<bool> active {false};
};

class PartialBlockWritebackManager::Worker : public std::enable_shared_from_this<Worker> {
public:
    explicit Worker(PartialBlockWritebackManager& manager) : _manager(manager) {}

    Status start() {
        auto self = shared_from_this();
        return _manager._read_pool->submit_func([self = std::move(self)]() { self->_run(); });
    }

    // The caller holds the manager mutex so the changed wait predicate and notification are
    // published atomically to an idle worker.
    void request_stop() { _stop_requested.store(true, std::memory_order_release); }

    bool stop_requested() const { return _stop_requested.load(std::memory_order_acquire); }

    void wait_until_stopped() { _stopped.wait(); }

private:
    void _run() {
        _manager._running_worker_count.fetch_add(1, std::memory_order_relaxed);
        Defer mark_stopped {[this]() {
            const size_t old_running =
                    _manager._running_worker_count.fetch_sub(1, std::memory_order_relaxed);
            DCHECK_GT(old_running, 0);
            _stopped.count_down();
        }};

        while (!stop_requested()) {
            auto task = _manager._take_task(*this);
            if (task == nullptr) {
                return;
            }
            _manager._process_task(task);
        }
    }

    PartialBlockWritebackManager& _manager;
    std::atomic<bool> _stop_requested {false};
    CountDownLatch _stopped {1};
};

Status PartialBlockWritebackOptions::validate() const {
    RETURN_IF_ERROR(hole_fill_coalesce.validate());
    if (block_size == 0) {
        return Status::InvalidArgument("partial block writeback block size must be positive");
    }
    if (worker_count == 0 || worker_count > kMaxHoleFillWorkerCount) {
        return Status::InvalidArgument("partial block writeback worker count {} is invalid",
                                       worker_count);
    }
    if (max_pending_bytes < block_size) {
        return Status::InvalidArgument(
                "partial block writeback pending bytes {} must hold at least one block of {} bytes",
                max_pending_bytes, block_size);
    }
    return Status::OK();
}

PartialBlockWritebackManager::PartialBlockWritebackManager(PartialBlockWritebackOptions options)
        : _options(std::move(options)),
          _max_pending_tasks(_options.max_pending_bytes / _options.block_size),
          _configured_worker_count(_options.worker_count) {
    DORIS_CHECK(_max_pending_tasks > 0);
}

PartialBlockWritebackManager::~PartialBlockWritebackManager() {
    shutdown();
}

Status PartialBlockWritebackManager::create(
        const PartialBlockWritebackOptions& options,
        std::unique_ptr<PartialBlockWritebackManager>* output_manager) {
    DORIS_CHECK(output_manager != nullptr);
    RETURN_IF_ERROR(options.validate());
    auto manager = std::unique_ptr<PartialBlockWritebackManager>(
            new PartialBlockWritebackManager(options));
    RETURN_IF_ERROR(manager->_start());
    *output_manager = std::move(manager);
    return Status::OK();
}

Status PartialBlockWritebackManager::_start() {
    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    DORIS_CHECK(_read_pool == nullptr);
    const size_t worker_count = _configured_worker_count.load(std::memory_order_acquire);
    RETURN_IF_ERROR(ThreadPoolBuilder("HoleFillReadPool")
                            .set_min_threads(0)
                            .set_max_threads(static_cast<int>(worker_count))
                            .set_max_queue_size(static_cast<int>(kMaxHoleFillWorkerCount))
                            .build(&_read_pool));
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(!_accepting);
        _accepting = true;
    }
    return _resize_workers_locked(worker_count);
}

PartialBlockSubmitResult PartialBlockWritebackManager::try_submit(
        PartialBlockWritebackRequest request) {
    _validate_request(request);

    if (!request.write_manager->accepting()) {
        return PartialBlockSubmitResult::REJECTED;
    }
    if (!request.write_manager->check_write_epoch(request.write_epoch)) {
        return PartialBlockSubmitResult::STALE_EPOCH;
    }
    if (request.inflight_index != nullptr &&
        request.inflight_index->lookup(request.cache_hash, request.block_offset) != nullptr) {
        return PartialBlockSubmitResult::CACHE_WRITE_INFLIGHT;
    }

    const BlockKey key {.write_manager = request.write_manager,
                        .cache_hash = request.cache_hash,
                        .block_offset = request.block_offset};
    const size_t fragment_offset = request.fragment_offset;
    const Slice fragment = request.data;

    TaskPtr existing;
    {
        std::lock_guard lock(_mutex);
        if (!_accepting) {
            return PartialBlockSubmitResult::REJECTED;
        }
        const auto entry = _tasks.find(key);
        if (entry != _tasks.end()) {
            existing = entry->second;
        } else if (_tasks.size() == _max_pending_tasks && _queue.empty()) {
            return PartialBlockSubmitResult::REJECTED;
        }
    }
    if (existing != nullptr) {
        DORIS_CHECK(existing->block_valid_size == request.block_valid_size);
        if (auto result = existing->try_merge(fragment_offset, fragment); result.has_value()) {
            return *result;
        }
    }

    auto candidate = _create_task(std::move(request), key);
    if (candidate == nullptr) {
        return PartialBlockSubmitResult::BUFFER_ALLOCATION_FAILED;
    }

    while (true) {
        existing.reset();
        switch (_enqueue_or_get_existing(candidate, &existing)) {
        case EnqueueResult::QUEUED:
            return PartialBlockSubmitResult::QUEUED;
        case EnqueueResult::REJECTED:
            return PartialBlockSubmitResult::REJECTED;
        case EnqueueResult::EXISTING:
            DORIS_CHECK(existing != nullptr);
            if (auto result = existing->try_merge(fragment_offset, fragment); result.has_value()) {
                return *result;
            }
            break;
        }
    }
}

void PartialBlockWritebackManager::_validate_request(
        const PartialBlockWritebackRequest& request) const {
    DORIS_CHECK(request.write_manager != nullptr);
    DORIS_CHECK(request.source_reader != nullptr);
    DORIS_CHECK(request.write_epoch.key_token != nullptr);
    DORIS_CHECK(request.block_offset % _options.block_size == 0);
    DORIS_CHECK(request.block_valid_size > 0);
    DORIS_CHECK(request.block_valid_size <= _options.block_size);
    DORIS_CHECK(request.block_offset <= request.source_reader->size());
    DORIS_CHECK(request.block_valid_size <= request.source_reader->size() - request.block_offset);
    DORIS_CHECK(request.data.data != nullptr);
    DORIS_CHECK(request.data.size > 0);
    DORIS_CHECK(request.fragment_offset < request.block_valid_size);
    DORIS_CHECK(request.data.size <= request.block_valid_size - request.fragment_offset);
}

PartialBlockWritebackManager::TaskPtr PartialBlockWritebackManager::_create_task(
        PartialBlockWritebackRequest request, const BlockKey& key) {
    AsyncCacheWriteBufferPtr buffer;
    if (!request.write_manager->allocate_tracked_buffer(_options.block_size, &buffer).ok()) {
        return nullptr;
    }
    std::memcpy(buffer->data() + request.fragment_offset, request.data.data, request.data.size);

    auto task = std::make_shared<Task>();
    task->key = key;
    task->inflight_index = request.inflight_index;
    task->source_reader = std::move(request.source_reader);
    task->block_valid_size = request.block_valid_size;
    task->buffer = std::move(buffer);
    task->covered_intervals = {FileRange {.offset = request.block_offset + request.fragment_offset,
                                          .size = request.data.size}};
    task->admission_ctx = std::move(request.admission_ctx);
    task->write_epoch = std::move(request.write_epoch);
    task->io_context = std::move(request.io_context);
    task->io_context.io_context.should_stop = false;
    task->io_context.io_context.bypass_peer_read = true;
    return task;
}

PartialBlockWritebackManager::EnqueueResult PartialBlockWritebackManager::_enqueue_or_get_existing(
        const TaskPtr& candidate, TaskPtr* existing) {
    DORIS_CHECK(candidate != nullptr);
    DORIS_CHECK(existing != nullptr);
    DORIS_CHECK(!candidate->is_active());
    existing->reset();

    // Keep a removed task alive until the manager mutex is released. Its tracked block buffer and
    // retained reader can have nontrivial destructors.
    TaskPtr discarded_task;
    {
        std::lock_guard lock(_mutex);
        if (!_accepting) {
            return EnqueueResult::REJECTED;
        }

        auto existing_entry = _tasks.find(candidate->key);
        if (existing_entry != _tasks.end()) {
            auto task = existing_entry->second;
            DORIS_CHECK(task->block_valid_size == candidate->block_valid_size);
            if (task->is_active() ||
                task->key.write_manager->is_current_write_epoch(task->write_epoch)) {
                *existing = std::move(task);
                return EnqueueResult::EXISTING;
            }

            discarded_task = std::move(task);
            const auto queue_position = discarded_task->queue_position;
            DORIS_CHECK(*queue_position == discarded_task);
            *queue_position = candidate;
            candidate->queue_position = queue_position;
            existing_entry->second = candidate;
        } else {
            DORIS_CHECK_LE(_tasks.size(), _max_pending_tasks);
            if (_tasks.size() == _max_pending_tasks) {
                // Queued tasks may all become active while the candidate buffer is allocated.
                if (_queue.empty()) {
                    return EnqueueResult::REJECTED;
                }
                discarded_task = _queue.front();
                const size_t erased = _tasks.erase(discarded_task->key);
                DORIS_CHECK(erased == 1);
                _queue.pop_front();
            }
            _queue.push_back(candidate);
            candidate->queue_position = std::prev(_queue.end());
            const auto [entry, inserted] = _tasks.emplace(candidate->key, candidate);
            DORIS_CHECK(inserted);
            static_cast<void>(entry);
        }
    }
    _queue_cv.notify_one();
    return EnqueueResult::QUEUED;
}

void PartialBlockWritebackManager::shutdown() {
    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    if (_read_pool == nullptr) {
        return;
    }

    Queue queued;
    {
        std::lock_guard lock(_mutex);
        _accepting = false;
        queued.splice(queued.end(), _queue);
        for (const auto& task : queued) {
            DORIS_CHECK(!task->is_active());
            const size_t erased = _tasks.erase(task->key);
            DORIS_CHECK(erased == 1);
        }
    }
    queued.clear();
    _queue_cv.notify_all();
    _stop_workers_locked(0);
    _read_pool->shutdown();
    _read_pool.reset();
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(_queue.empty());
        DORIS_CHECK(_tasks.empty());
        DORIS_CHECK(_active_hole_fill_slots_by_writer.empty());
    }
}

Status PartialBlockWritebackManager::resize_workers(size_t worker_count) {
    if (worker_count == 0 || worker_count > kMaxHoleFillWorkerCount) {
        return Status::InvalidArgument("partial block writeback worker count {} is invalid",
                                       worker_count);
    }

    std::lock_guard lifecycle_lock(_lifecycle_mutex);
    {
        std::lock_guard lock(_mutex);
        if (!_accepting) {
            return Status::InternalError("partial block writeback manager is shutting down");
        }
    }
    _configured_worker_count.store(worker_count, std::memory_order_release);
    return _resize_workers_locked(worker_count);
}

Status PartialBlockWritebackManager::_resize_workers_locked(size_t worker_count) {
    DORIS_CHECK(_read_pool != nullptr);
    if (worker_count < _workers.size()) {
        _stop_workers_locked(worker_count);
        RETURN_IF_ERROR(_read_pool->set_min_threads(static_cast<int>(worker_count)));
        RETURN_IF_ERROR(_read_pool->set_max_threads(static_cast<int>(worker_count)));
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_pool->set_max_threads(static_cast<int>(worker_count)));
    // Each Worker owns one long-lived pool task. Create the backing pool threads before submitting
    // more workers so accepted worker tasks cannot remain queued behind other worker loops.
    RETURN_IF_ERROR(_read_pool->set_min_threads(static_cast<int>(worker_count)));
    while (_workers.size() < worker_count) {
        auto worker = std::make_shared<Worker>(*this);
        RETURN_IF_ERROR(worker->start());
        _workers.emplace_back(std::move(worker));
    }
    return Status::OK();
}

void PartialBlockWritebackManager::_stop_workers_locked(size_t keep_worker_count) {
    DORIS_CHECK(keep_worker_count <= _workers.size());
    if (keep_worker_count == _workers.size()) {
        return;
    }

    {
        std::lock_guard lock(_mutex);
        for (size_t index = keep_worker_count; index < _workers.size(); ++index) {
            _workers[index]->request_stop();
        }
    }
    _queue_cv.notify_all();
    for (size_t index = keep_worker_count; index < _workers.size(); ++index) {
        _workers[index]->wait_until_stopped();
    }
    _workers.resize(keep_worker_count);
}

bool PartialBlockWritebackManager::accepting() const {
    std::lock_guard lock(_mutex);
    return _accepting;
}

size_t PartialBlockWritebackManager::pending_count() const {
    std::lock_guard lock(_mutex);
    return _tasks.size();
}

size_t PartialBlockWritebackManager::pending_bytes() const {
    std::lock_guard lock(_mutex);
    return _tasks.size() * _options.block_size;
}

size_t PartialBlockWritebackManager::queued_count() const {
    std::lock_guard lock(_mutex);
    return _queue.size();
}

size_t PartialBlockWritebackManager::active_count() const {
    std::lock_guard lock(_mutex);
    DCHECK_LE(_queue.size(), _tasks.size());
    return _tasks.size() - _queue.size();
}

PartialBlockWritebackManager::TaskPtr PartialBlockWritebackManager::_take_task(
        const Worker& worker) {
    while (true) {
        // Spliced tasks release their tracked buffers after the manager mutex leaves scope.
        Queue discarded_tasks;
        TaskPtr task;
        {
            std::unique_lock lock(_mutex);
            _queue_cv.wait(lock, [this, &worker]() {
                return !_accepting || worker.stop_requested() || !_queue.empty();
            });
            if (!_accepting || worker.stop_requested()) {
                return nullptr;
            }

            task = _take_runnable_task_locked(&discarded_tasks);
            if (task == nullptr && discarded_tasks.empty()) {
                _queue_cv.wait_for(lock, kCapacityRetryInterval);
            }
        }
        if (task != nullptr) {
            return task;
        }
    }
}

PartialBlockWritebackManager::TaskPtr PartialBlockWritebackManager::_take_runnable_task_locked(
        Queue* discarded_tasks) {
    DORIS_CHECK(discarded_tasks != nullptr);
    for (auto iterator = _queue.begin(); iterator != _queue.end();) {
        const auto& candidate = *iterator;
        DORIS_CHECK(!candidate->is_active());
        if (candidate->should_discard_before_read()) {
            const auto discarded = iterator++;
            _discard_queued_task_locked(discarded, discarded_tasks);
            continue;
        }

        const size_t available_slots =
                candidate->key.write_manager->available_slots_without_eviction(_options.block_size);
        const auto active_entry =
                _active_hole_fill_slots_by_writer.find(candidate->key.write_manager);
        const size_t active_hole_fill_slots =
                active_entry == _active_hole_fill_slots_by_writer.end() ? 0 : active_entry->second;
        if (active_hole_fill_slots < available_slots) {
            auto task = candidate;
            task->activate();
            _queue.erase(iterator);
            ++_active_hole_fill_slots_by_writer[task->key.write_manager];
            return task;
        }
        ++iterator;
    }
    return nullptr;
}

void PartialBlockWritebackManager::_discard_queued_task_locked(Queue::iterator iterator,
                                                               Queue* discarded_tasks) {
    const auto& task = *iterator;
    DORIS_CHECK(!task->is_active());
    const size_t erased = _tasks.erase(task->key);
    DORIS_CHECK(erased == 1);
    discarded_tasks->splice(discarded_tasks->end(), _queue, iterator);
}

void PartialBlockWritebackManager::_process_task(const TaskPtr& task) {
    Defer complete {[&]() { _complete_task(task); }};
    if (!task->key.write_manager->check_write_epoch(task->write_epoch)) {
        return;
    }

    std::vector<FileRange> read_ranges;
    Status status = task->plan_hole_reads(_options.hole_fill_coalesce, &read_ranges);
    if (!status.ok()) {
        LOG(WARNING) << "Plan partial block hole-fill reads failed, hash="
                     << task->key.cache_hash.to_string() << ", offset=" << task->key.block_offset
                     << ", status=" << status;
        return;
    }

    FileCacheStatistics file_cache_stats;
    FileReaderStats file_reader_stats;
    auto io_context = task->io_context;
    io_context.io_context.file_cache_stats = &file_cache_stats;
    io_context.io_context.file_reader_stats = &file_reader_stats;
    for (const auto& range : read_ranges) {
        size_t bytes_read = 0;
        status = task->source_reader->read_at(
                range.offset,
                Slice(task->buffer->data() + range.offset - task->key.block_offset, range.size),
                &bytes_read, &io_context.io_context);
        if (!status.ok() || bytes_read != range.size) {
            LOG(WARNING) << "Read partial block hole failed, hash="
                         << task->key.cache_hash.to_string()
                         << ", block_offset=" << task->key.block_offset
                         << ", read_offset=" << range.offset << ", read_size=" << range.size
                         << ", bytes_read=" << bytes_read << ", status=" << status;
            return;
        }
    }

    static_cast<void>(
            task->key.write_manager->try_submit_owned_block(AsyncCacheWriteOwnedBlockRequest {
                    .cache_hash = task->key.cache_hash,
                    .file_offset = task->key.block_offset,
                    .write_size = task->block_valid_size,
                    .buffer = std::move(task->buffer),
                    .admission_ctx = std::move(task->admission_ctx),
                    .write_epoch = std::move(task->write_epoch),
                    .admission_mode = AsyncCacheWriteAdmissionMode::REQUIRE_SPARE_CAPACITY,
                    .inflight_index = task->inflight_index,
            }));
}

void PartialBlockWritebackManager::_complete_task(const TaskPtr& task) {
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(task->is_active());
        const size_t erased = _tasks.erase(task->key);
        DORIS_CHECK(erased == 1);
        auto active_entry = _active_hole_fill_slots_by_writer.find(task->key.write_manager);
        DORIS_CHECK(active_entry != _active_hole_fill_slots_by_writer.end());
        DORIS_CHECK(active_entry->second > 0);
        if (--active_entry->second == 0) {
            _active_hole_fill_slots_by_writer.erase(active_entry);
        }
    }
    _queue_cv.notify_one();
}

} // namespace doris::io
