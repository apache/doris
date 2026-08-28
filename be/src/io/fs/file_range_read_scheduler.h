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
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "io/fs/file_range_coalescer.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/slice.h"

namespace doris {

class MemTrackerLimiter;
class ThreadPool;

namespace io {

enum class FileRangeReadRejectReason : uint8_t {
    /// The complete batch was accepted. Individual handles may still fail during execution.
    NONE,
    /// A reader, context, range, or aggregate byte count failed request validation.
    INVALID_REQUEST,
    /// The query context had already been cancelled before admission.
    QUERY_CANCELLED,
    /// Reserving the complete batch would exceed its query-scoped byte budget.
    QUERY_BYTE_LIMIT,
    /// Reserving the complete batch would exceed the BE-wide byte budget.
    GLOBAL_BYTE_LIMIT,
    /// At least one range buffer could not be allocated; no read handles are returned.
    ALLOCATION_FAILED,
    /// The executor rejected every task in the batch.
    EXECUTOR_REJECTED,
    /// The scheduler stopped accepting work before the batch was submitted.
    SHUTTING_DOWN,
};

struct FileRangeReadSchedulerOptions {
    /// Maximum bytes retained by live range handles from one query.
    size_t max_bytes_per_query {1};
    /// Maximum bytes retained by all live range handles in this BE.
    size_t max_bytes_per_be {1};

    Status validate() const;
};

/// Byte reservation shared by range buffers. It limits retained memory, not executor concurrency.
class FileRangeReadBudget {
public:
    explicit FileRangeReadBudget(size_t max_bytes);

    size_t resident_bytes() const;

private:
    friend class FileRangeReadScheduler;
    friend class FileRangeReadReservation;

    bool _can_reserve_unlocked(size_t bytes) const;
    void _reserve_unlocked(size_t bytes);
    void _release(size_t bytes);

    mutable std::mutex _mutex;
    const size_t _max_bytes;
    size_t _resident_bytes {0};
};

class FileRangeRead;

/// Query-scoped cancellation and admission state shared by every segment read.
class FileRangeReadContext {
public:
    explicit FileRangeReadContext(size_t max_bytes);

    /// Cancel all registered reads. Running source IO finishes normally and publishes CANCELLED.
    void cancel();
    bool cancelled() const { return _cancelled.load(std::memory_order_acquire); }
    size_t resident_bytes() const { return _budget.resident_bytes(); }

private:
    friend class FileRangeReadScheduler;
    friend class FileRangeReadReservation;

    void _register_reads(const std::vector<std::shared_ptr<FileRangeRead>>& reads);

    FileRangeReadBudget _budget;
    std::atomic<bool> _cancelled {false};
    mutable std::mutex _reads_mutex;
    std::vector<std::weak_ptr<FileRangeRead>> _reads;
};

/// Move-only reservation that releases both query and BE byte budgets on destruction.
class FileRangeReadReservation {
public:
    FileRangeReadReservation(const FileRangeReadReservation&) = delete;
    FileRangeReadReservation& operator=(const FileRangeReadReservation&) = delete;
    FileRangeReadReservation(FileRangeReadReservation&& other) noexcept;
    FileRangeReadReservation& operator=(FileRangeReadReservation&& other) noexcept;
    ~FileRangeReadReservation();

private:
    friend class FileRangeReadScheduler;

    FileRangeReadReservation(std::shared_ptr<FileRangeReadContext> context,
                             std::shared_ptr<FileRangeReadBudget> global_budget,
                             size_t bytes) noexcept;
    void _reset();

    std::shared_ptr<FileRangeReadContext> _context;
    std::shared_ptr<FileRangeReadBudget> _global_budget;
    size_t _bytes {0};
};

struct FileRangeReadStats {
    size_t bytes_read {0};
    FileCacheStatistics file_cache;
    FileReaderStats file_reader;
};

/// One scheduled range and its owned buffer. The byte reservation follows this handle's lifetime.
class FileRangeRead {
public:
    enum class State : uint8_t {
        /// Admitted and allocated, but its executor task has not started.
        QUEUED,
        /// The executor task owns the handle and may be blocked in source IO.
        RUNNING,
        /// The entire requested range is available through data() or slice().
        READY,
        /// Submission, source IO, or exact-length validation failed.
        FAILED,
        /// Cancellation won before execution or was observed after source IO returned.
        CANCELLED,
    };

    ~FileRangeRead();

    /// Wait for a terminal state and return its status.
    Status wait();
    /// Request cancellation without interrupting an already running source read.
    void request_cancel();
    State state() const;
    const FileRange& range() const { return _range; }
    /// Return the full buffer. The handle must be READY and owns the returned memory.
    Slice data() const;
    /// Return a checked subrange of a READY buffer.
    Slice slice(size_t buffer_offset, size_t size) const;
    FileRangeReadStats stats() const;

private:
    friend class FileRangeReadScheduler;

    FileRangeRead(FileRange range, std::shared_ptr<MemTrackerLimiter> tracker,
                  FileRangeReadReservation reservation);
    static Status create(FileRange range, std::shared_ptr<MemTrackerLimiter> tracker,
                         FileRangeReadReservation reservation,
                         std::shared_ptr<FileRangeRead>* output);

    bool _mark_running();
    bool _is_cancel_requested() const;
    void _publish_ready(FileRangeReadStats stats);
    void _publish_failed(Status status, FileRangeReadStats stats);
    void _publish_submit_failure(Status status);
    void _publish_cancelled(FileRangeReadStats stats = {});
    void _publish_from_running(State state, Status status, FileRangeReadStats stats);
    const FileRange _range;
    char* _data {nullptr};
    const std::shared_ptr<MemTrackerLimiter> _tracker;
    FileRangeReadReservation _reservation;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    State _state {State::QUEUED};
    Status _status;
    FileRangeReadStats _stats;
    bool _cancel_requested {false};
};

/// Owns every pointer embedded in IOContext that an asynchronous read keeps after submission.
struct FileRangeReadIOContext {
    IOContext io_context;
    std::shared_ptr<TUniqueId> query_id;

    /// Copy fields safe for asynchronous use, own query_id, and detach caller-owned statistics.
    static FileRangeReadIOContext from_caller(const IOContext& source);
};

struct FileRangeReadSubmitResult {
    std::vector<std::shared_ptr<FileRangeRead>> reads;
    FileRangeReadRejectReason reject_reason {FileRangeReadRejectReason::NONE};
    Status status;

    bool accepted() const {
        return reject_reason == FileRangeReadRejectReason::NONE && status.ok();
    }
};

/// BE-global admission and execution scheduler for already-planned file ranges. It does not
/// understand pages, range coalescing, cache-block completion, or writeback policy.
class FileRangeReadScheduler {
public:
    ~FileRangeReadScheduler();

    FileRangeReadScheduler(const FileRangeReadScheduler&) = delete;
    FileRangeReadScheduler& operator=(const FileRangeReadScheduler&) = delete;

    /// `executor` must remain running and outlive the scheduler. Its thread count controls range
    /// read concurrency; the scheduler never shuts down the shared executor.
    static Status create(const FileRangeReadSchedulerOptions& options, ThreadPool* executor,
                         std::unique_ptr<FileRangeReadScheduler>* output);

    /// Create a query-scoped cancellation and byte-budget context.
    std::shared_ptr<FileRangeReadContext> create_context() const;

    /// Atomically admits and allocates the complete batch, then submits one executor task per
    /// range. A rejected batch returns no read handles and leaves both budgets unchanged. If the
    /// executor accepts only part of a batch, the returned handles expose each rejected range as a
    /// failed read.
    FileRangeReadSubmitResult try_submit(const std::vector<FileRange>& ranges,
                                         FileReaderSPtr reader, FileRangeReadIOContext io_context,
                                         const std::shared_ptr<FileRangeReadContext>& context);

    /// Stop admission, request cancellation, and wait for all accepted executor tasks. Idempotent.
    void shutdown();
    bool accepting() const;
    std::shared_ptr<FileRangeReadBudget> global_budget() const { return _global_budget; }

private:
    struct ReadTask {
        FileReaderSPtr reader;
        std::shared_ptr<FileRangeRead> read;
        std::shared_ptr<FileRangeReadContext> context;
        FileRangeReadIOContext io_context;
    };

    FileRangeReadScheduler(FileRangeReadSchedulerOptions options, ThreadPool* executor);

    /// Validate the complete request and compute its total buffer capacity.
    Status _validate_request(const std::vector<FileRange>& ranges, const FileReaderSPtr& reader,
                             const std::shared_ptr<FileRangeReadContext>& context,
                             size_t* total_bytes) const;
    /// Atomically reserve query and BE bytes for every range in the batch.
    FileRangeReadRejectReason _reserve_batch(const std::shared_ptr<FileRangeReadContext>& context,
                                             const std::vector<FileRange>& ranges,
                                             size_t total_bytes,
                                             std::vector<FileRangeReadReservation>* reservations);
    /// Submit one task per range while retaining accepted handles for shutdown coordination.
    Status _submit_tasks(std::vector<ReadTask> tasks, FileRangeReadRejectReason* reject_reason);
    void _task_finished(const std::shared_ptr<FileRangeRead>& read);
    /// Perform one exact source read and publish one terminal handle state.
    void _run_task(ReadTask task) const;

    const FileRangeReadSchedulerOptions _options;
    const std::shared_ptr<FileRangeReadBudget> _global_budget;
    const std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    ThreadPool* const _executor;

    mutable std::mutex _mutex;
    std::condition_variable _tasks_cv;
    // The shared executor owns task ordering. This set only keeps accepted reads alive and allows
    // shutdown to cancel them; its size is not a concurrency limit.
    std::unordered_set<std::shared_ptr<FileRangeRead>> _inflight_reads;
    bool _accepting {true};
};

} // namespace io
} // namespace doris
