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

#include "io/fs/file_range_read_scheduler.h"

#include <exception>
#include <limits>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "core/allocator.h"
#include "cpp/sync_point.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {

using FileRangeReadAllocator = Allocator<false, false, false, DefaultMemoryAllocator, true>;

namespace {

bool add_overflows(size_t left, size_t right) {
    return right > std::numeric_limits<size_t>::max() - left;
}

bool is_terminal(FileRangeRead::State state) {
    return state == FileRangeRead::State::READY || state == FileRangeRead::State::FAILED ||
           state == FileRangeRead::State::CANCELLED;
}

FileRangeReadSubmitResult rejected(FileRangeReadRejectReason reason, Status status = Status::OK()) {
    return FileRangeReadSubmitResult {
            .reads = {},
            .reject_reason = reason,
            .status = std::move(status),
    };
}

} // namespace

FileRangeReadBudget::FileRangeReadBudget(size_t max_bytes) : _max_bytes(max_bytes) {
    DORIS_CHECK(_max_bytes > 0);
}

size_t FileRangeReadBudget::resident_bytes() const {
    std::lock_guard lock(_mutex);
    return _resident_bytes;
}

bool FileRangeReadBudget::_can_reserve_unlocked(size_t bytes) const {
    return bytes <= _max_bytes - _resident_bytes;
}

void FileRangeReadBudget::_reserve_unlocked(size_t bytes) {
    DORIS_CHECK(_can_reserve_unlocked(bytes));
    _resident_bytes += bytes;
}

void FileRangeReadBudget::_release(size_t bytes) {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(bytes <= _resident_bytes);
    _resident_bytes -= bytes;
}

FileRangeReadContext::FileRangeReadContext(size_t max_bytes) : _budget(max_bytes) {}

void FileRangeReadContext::cancel() {
    if (_cancelled.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    std::lock_guard lock(_reads_mutex);
    for (const auto& weak_read : _reads) {
        if (auto read = weak_read.lock(); read != nullptr) {
            read->request_cancel();
        }
    }
    _reads.clear();
}

void FileRangeReadContext::_register_reads(
        const std::vector<std::shared_ptr<FileRangeRead>>& reads) {
    bool cancel_reads = false;
    {
        std::lock_guard lock(_reads_mutex);
        cancel_reads = _cancelled.load(std::memory_order_acquire);
        if (!cancel_reads) {
            std::erase_if(_reads, [](const auto& read) { return read.expired(); });
            _reads.reserve(_reads.size() + reads.size());
            for (const auto& read : reads) {
                _reads.emplace_back(read);
            }
        }
    }
    if (cancel_reads) {
        for (const auto& read : reads) {
            read->request_cancel();
        }
    }
}

FileRangeReadReservation::FileRangeReadReservation(
        std::shared_ptr<FileRangeReadContext> context,
        std::shared_ptr<FileRangeReadBudget> global_budget, size_t bytes) noexcept
        : _context(std::move(context)), _global_budget(std::move(global_budget)), _bytes(bytes) {
    DORIS_CHECK(_context != nullptr);
    DORIS_CHECK(_global_budget != nullptr);
    DORIS_CHECK(_bytes > 0);
}

FileRangeReadReservation::FileRangeReadReservation(FileRangeReadReservation&& other) noexcept
        : _context(std::move(other._context)),
          _global_budget(std::move(other._global_budget)),
          _bytes(std::exchange(other._bytes, 0)) {}

FileRangeReadReservation& FileRangeReadReservation::operator=(
        FileRangeReadReservation&& other) noexcept {
    if (this != &other) {
        _reset();
        _context = std::move(other._context);
        _global_budget = std::move(other._global_budget);
        _bytes = std::exchange(other._bytes, 0);
    }
    return *this;
}

FileRangeReadReservation::~FileRangeReadReservation() {
    _reset();
}

void FileRangeReadReservation::_reset() {
    if (_context == nullptr) {
        return;
    }
    _context->_budget._release(_bytes);
    _global_budget->_release(_bytes);
    _context.reset();
    _global_budget.reset();
    _bytes = 0;
}

FileRangeRead::FileRangeRead(FileRange range, std::shared_ptr<MemTrackerLimiter> tracker,
                             FileRangeReadReservation reservation)
        : _range(range), _tracker(std::move(tracker)), _reservation(std::move(reservation)) {
    FileRangeReadAllocator allocator;
    _data = reinterpret_cast<char*>(allocator.alloc(_range.size));
}

FileRangeRead::~FileRangeRead() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_tracker);
    FileRangeReadAllocator allocator;
    allocator.free(_data, _range.size);
}

Status FileRangeRead::create(FileRange range, std::shared_ptr<MemTrackerLimiter> tracker,
                             FileRangeReadReservation reservation,
                             std::shared_ptr<FileRangeRead>* output) {
    DORIS_CHECK(range.size > 0);
    DORIS_CHECK(tracker != nullptr);
    DORIS_CHECK(output != nullptr);

    Status injected_status;
    TEST_SYNC_POINT_CALLBACK("FileRangeRead::create:inject_failure", &injected_status);
    if (!injected_status.ok()) {
        return injected_status;
    }

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
    try {
        output->reset(new FileRangeRead(range, std::move(tracker), std::move(reservation)));
    } catch (const Exception& exception) {
        return exception.to_status();
    } catch (const std::exception& exception) {
        return Status::MemoryAllocFailed("allocate file range read buffer failed: {}",
                                         exception.what());
    }
    return Status::OK();
}

Status FileRangeRead::wait() {
    std::unique_lock lock(_mutex);
    _cv.wait(lock, [this]() { return is_terminal(_state); });
    return _status;
}

void FileRangeRead::request_cancel() {
    bool publish_cancelled = false;
    {
        std::lock_guard lock(_mutex);
        if (is_terminal(_state)) {
            return;
        }
        _cancel_requested = true;
        publish_cancelled = _state == State::QUEUED;
        if (publish_cancelled) {
            _state = State::CANCELLED;
            _status = Status::Cancelled("asynchronous file range read cancelled");
        }
    }
    if (publish_cancelled) {
        _cv.notify_all();
    }
}

FileRangeRead::State FileRangeRead::state() const {
    std::lock_guard lock(_mutex);
    return _state;
}

Slice FileRangeRead::data() const {
    return slice(0, _range.size);
}

Slice FileRangeRead::slice(size_t buffer_offset, size_t size) const {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_state == State::READY);
    DORIS_CHECK(buffer_offset <= _range.size);
    DORIS_CHECK(size <= _range.size - buffer_offset);
    return {_data + buffer_offset, size};
}

FileRangeReadStats FileRangeRead::stats() const {
    std::lock_guard lock(_mutex);
    return _stats;
}

bool FileRangeRead::_mark_running() {
    std::lock_guard lock(_mutex);
    if (_state == State::CANCELLED) {
        return false;
    }
    DORIS_CHECK(_state == State::QUEUED);
    _state = State::RUNNING;
    return true;
}

bool FileRangeRead::_is_cancel_requested() const {
    std::lock_guard lock(_mutex);
    return _cancel_requested;
}

void FileRangeRead::_publish_ready(FileRangeReadStats stats) {
    _publish_from_running(State::READY, Status::OK(), std::move(stats));
}

void FileRangeRead::_publish_failed(Status status, FileRangeReadStats stats) {
    DORIS_CHECK(!status.ok());
    _publish_from_running(State::FAILED, std::move(status), std::move(stats));
}

void FileRangeRead::_publish_submit_failure(Status status) {
    DORIS_CHECK(!status.ok());
    {
        std::lock_guard lock(_mutex);
        if (_state == State::CANCELLED) {
            return;
        }
        DORIS_CHECK(_state == State::QUEUED);
        _state = State::FAILED;
        _status = std::move(status);
    }
    _cv.notify_all();
}

void FileRangeRead::_publish_cancelled(FileRangeReadStats stats) {
    _publish_from_running(State::CANCELLED,
                          Status::Cancelled("asynchronous file range read cancelled"),
                          std::move(stats));
}

void FileRangeRead::_publish_from_running(State state, Status status, FileRangeReadStats stats) {
    {
        std::lock_guard lock(_mutex);
        DORIS_CHECK(_state == State::RUNNING);
        DORIS_CHECK(is_terminal(state));
        if (_cancel_requested) {
            state = State::CANCELLED;
            status = Status::Cancelled("asynchronous file range read cancelled");
        }
        _state = state;
        _status = std::move(status);
        _stats = std::move(stats);
    }
    _cv.notify_all();
}

FileRangeReadIOContext FileRangeReadIOContext::from_caller(const IOContext& source) {
    FileRangeReadIOContext result;
    result.io_context = source;
    if (source.query_id != nullptr) {
        result.query_id = std::make_shared<TUniqueId>(*source.query_id);
    }
    result.io_context.file_cache_stats = nullptr;
    result.io_context.file_reader_stats = nullptr;
    result.io_context.remote_scan_cache_write_limiter = nullptr;
    result.io_context.condition_cache_filtered_rows = 0;
    result.io_context.predicate_filtered_rows = 0;
    result.io_context.cache_write_mode_override = CacheWriteMode::NO_WRITE;
    result.io_context.query_id = result.query_id.get();
    return result;
}

FileRangeReadScheduler::FileRangeReadScheduler(FileRangeReadSchedulerOptions options,
                                               ThreadPool* executor)
        : _options(options),
          _global_budget(std::make_shared<FileRangeReadBudget>(options.max_bytes_per_be)),
          _mem_tracker(MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                        "FileRangeReadScheduler")),
          _executor(executor) {
    DORIS_CHECK(_executor != nullptr);
}

FileRangeReadScheduler::~FileRangeReadScheduler() {
    shutdown();
}

Status FileRangeReadSchedulerOptions::validate() const {
    if (max_bytes_per_query == 0) {
        return Status::InvalidArgument("file range read query byte limit must be positive");
    }
    if (max_bytes_per_be == 0) {
        return Status::InvalidArgument("file range read BE byte limit must be positive");
    }
    return Status::OK();
}

Status FileRangeReadScheduler::create(const FileRangeReadSchedulerOptions& options,
                                      ThreadPool* executor,
                                      std::unique_ptr<FileRangeReadScheduler>* output) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(executor != nullptr);
    RETURN_IF_ERROR(options.validate());
    output->reset(new FileRangeReadScheduler(options, executor));
    return Status::OK();
}

std::shared_ptr<FileRangeReadContext> FileRangeReadScheduler::create_context() const {
    return std::make_shared<FileRangeReadContext>(_options.max_bytes_per_query);
}

FileRangeReadSubmitResult FileRangeReadScheduler::try_submit(
        const std::vector<FileRange>& ranges, FileReaderSPtr reader,
        FileRangeReadIOContext io_context, const std::shared_ptr<FileRangeReadContext>& context) {
    size_t total_bytes = 0;
    Status validation_status = _validate_request(ranges, reader, context, &total_bytes);
    if (!validation_status.ok()) {
        return rejected(FileRangeReadRejectReason::INVALID_REQUEST, std::move(validation_status));
    }
    if (!accepting()) {
        return rejected(FileRangeReadRejectReason::SHUTTING_DOWN,
                        Status::Cancelled("file range read scheduler is shutting down"));
    }
    if (context->cancelled()) {
        return rejected(FileRangeReadRejectReason::QUERY_CANCELLED,
                        Status::Cancelled("query cancelled before file range submission"));
    }

    std::vector<FileRangeReadReservation> reservations;
    reservations.reserve(ranges.size());
    const auto reserve_reason = _reserve_batch(context, ranges, total_bytes, &reservations);
    if (reserve_reason != FileRangeReadRejectReason::NONE) {
        return rejected(reserve_reason);
    }

    std::vector<std::shared_ptr<FileRangeRead>> reads;
    reads.reserve(ranges.size());
    std::vector<ReadTask> tasks;
    tasks.reserve(ranges.size());
    for (size_t index = 0; index < ranges.size(); ++index) {
        std::shared_ptr<FileRangeRead> read;
        Status status = FileRangeRead::create(ranges[index], _mem_tracker,
                                              std::move(reservations[index]), &read);
        if (!status.ok()) {
            return rejected(FileRangeReadRejectReason::ALLOCATION_FAILED, std::move(status));
        }
        tasks.push_back(ReadTask {
                .reader = reader, .read = read, .context = context, .io_context = io_context});
        reads.emplace_back(std::move(read));
    }
    context->_register_reads(reads);

    FileRangeReadRejectReason submit_reason = FileRangeReadRejectReason::NONE;
    Status submit_status = _submit_tasks(std::move(tasks), &submit_reason);
    if (!submit_status.ok()) {
        return rejected(submit_reason, std::move(submit_status));
    }
    return {.reads = std::move(reads),
            .reject_reason = FileRangeReadRejectReason::NONE,
            .status = Status::OK()};
}

Status FileRangeReadScheduler::_validate_request(
        const std::vector<FileRange>& ranges, const FileReaderSPtr& reader,
        const std::shared_ptr<FileRangeReadContext>& context, size_t* total_bytes) const {
    DORIS_CHECK(total_bytes != nullptr);
    if (reader == nullptr) {
        return Status::InvalidArgument("file range reader is null");
    }
    if (context == nullptr) {
        return Status::InvalidArgument("file range read context is null");
    }
    if (ranges.empty()) {
        return Status::InvalidArgument("file range read batch is empty");
    }

    *total_bytes = 0;
    for (const auto& range : ranges) {
        if (range.size == 0) {
            return Status::InvalidArgument("file range at offset {} is empty", range.offset);
        }
        if (add_overflows(range.offset, range.size)) {
            return Status::InvalidArgument("file range at offset {} with size {} overflows",
                                           range.offset, range.size);
        }
        if (range.end() > reader->size()) {
            return Status::InvalidArgument("file range [{}, {}) exceeds file size {}", range.offset,
                                           range.end(), reader->size());
        }
        if (add_overflows(*total_bytes, range.size)) {
            return Status::InvalidArgument("file range read batch byte size overflows");
        }
        *total_bytes += range.size;
    }
    return Status::OK();
}

FileRangeReadRejectReason FileRangeReadScheduler::_reserve_batch(
        const std::shared_ptr<FileRangeReadContext>& context, const std::vector<FileRange>& ranges,
        size_t total_bytes, std::vector<FileRangeReadReservation>* reservations) {
    DORIS_CHECK(reservations != nullptr);
    DORIS_CHECK(reservations->capacity() >= ranges.size());
    auto& query_budget = context->_budget;
    std::scoped_lock lock(query_budget._mutex, _global_budget->_mutex);

    if (total_bytes > query_budget._max_bytes - query_budget._resident_bytes) {
        return FileRangeReadRejectReason::QUERY_BYTE_LIMIT;
    }
    if (total_bytes > _global_budget->_max_bytes - _global_budget->_resident_bytes) {
        return FileRangeReadRejectReason::GLOBAL_BYTE_LIMIT;
    }

    query_budget._reserve_unlocked(total_bytes);
    _global_budget->_reserve_unlocked(total_bytes);
    for (const auto& range : ranges) {
        reservations->push_back(FileRangeReadReservation(context, _global_budget, range.size));
    }
    return FileRangeReadRejectReason::NONE;
}

Status FileRangeReadScheduler::_submit_tasks(std::vector<ReadTask> tasks,
                                             FileRangeReadRejectReason* reject_reason) {
    DORIS_CHECK(!tasks.empty());
    DORIS_CHECK(reject_reason != nullptr);
    std::lock_guard lock(_mutex);
    if (!_accepting) {
        *reject_reason = FileRangeReadRejectReason::SHUTTING_DOWN;
        return Status::Cancelled("file range read scheduler is shutting down");
    }

    size_t submitted_tasks = 0;
    Status first_failure;
    for (auto& task : tasks) {
        auto read = task.read;
        DORIS_CHECK(_inflight_reads.emplace(read).second);
        Status status = _executor->submit_func([this, task = std::move(task), read]() mutable {
            Defer task_finished {[this, read]() { _task_finished(read); }};
            _run_task(std::move(task));
        });
        if (status.ok()) {
            ++submitted_tasks;
            continue;
        }
        DORIS_CHECK(_inflight_reads.erase(read) == 1);
        if (first_failure.ok()) {
            first_failure = status;
        }
        read->_publish_submit_failure(std::move(status));
    }

    if (submitted_tasks == 0) {
        DORIS_CHECK(!first_failure.ok());
        *reject_reason = FileRangeReadRejectReason::EXECUTOR_REJECTED;
        return first_failure;
    }
    return Status::OK();
}

void FileRangeReadScheduler::_task_finished(const std::shared_ptr<FileRangeRead>& read) {
    std::lock_guard lock(_mutex);
    DORIS_CHECK(_inflight_reads.erase(read) == 1);
    if (_inflight_reads.empty()) {
        _tasks_cv.notify_all();
    }
}

void FileRangeReadScheduler::_run_task(ReadTask task) const {
    if (!task.read->_mark_running()) {
        return;
    }

    FileRangeReadStats stats;
    if (task.context->cancelled() || !accepting()) {
        task.read->_publish_cancelled(std::move(stats));
        return;
    }
    task.io_context.io_context.file_cache_stats = &stats.file_cache;
    task.io_context.io_context.file_reader_stats = &stats.file_reader;
    Status status;
    try {
        status = task.reader->read_at(task.read->range().offset,
                                      Slice(task.read->_data, task.read->range().size),
                                      &stats.bytes_read, &task.io_context.io_context);
    } catch (const Exception& exception) {
        status = exception.to_status();
    } catch (const std::exception& exception) {
        status = Status::IOError("file range read threw an exception: {}", exception.what());
    } catch (...) {
        status = Status::IOError("file range read threw an unknown exception");
    }

    if (task.context->cancelled() || task.read->_is_cancel_requested() || !accepting()) {
        task.read->_publish_cancelled(std::move(stats));
    } else if (!status.ok()) {
        task.read->_publish_failed(std::move(status), std::move(stats));
    } else if (stats.bytes_read != task.read->range().size) {
        const size_t bytes_read = stats.bytes_read;
        task.read->_publish_failed(Status::IOError("file range read returned {} of {} bytes",
                                                   bytes_read, task.read->range().size),
                                   std::move(stats));
    } else {
        TEST_SYNC_POINT("FileRangeReadScheduler::_run_task:before_publish_ready");
        task.read->_publish_ready(std::move(stats));
    }
}

void FileRangeReadScheduler::shutdown() {
    std::unique_lock lock(_mutex);
    _accepting = false;
    for (const auto& read : _inflight_reads) {
        read->request_cancel();
    }
    _tasks_cv.wait(lock, [this]() { return _inflight_reads.empty(); });
}

bool FileRangeReadScheduler::accepting() const {
    std::lock_guard lock(_mutex);
    return _accepting;
}

} // namespace doris::io
