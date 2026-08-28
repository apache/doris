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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {

namespace {

using namespace std::chrono_literals;

struct ObservedIOContext {
    bool has_query_id {false};
    int64_t query_id_hi {0};
    int64_t query_id_lo {0};
    uintptr_t file_cache_stats {0};
    uintptr_t file_reader_stats {0};
    bool has_remote_write_limiter {false};
    std::optional<CacheWriteMode> cache_write_mode;
    int64_t condition_cache_filtered_rows {0};
    int64_t predicate_filtered_rows {0};
};

class ControllableFileReader final : public FileReader {
public:
    explicit ControllableFileReader(std::string content, bool block_reads = false)
            : _content(std::move(content)), _block_reads(block_reads) {}

    Status close() override {
        std::lock_guard lock(_mutex);
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _content.size(); }

    bool closed() const override {
        std::lock_guard lock(_mutex);
        return _closed;
    }

    int64_t mtime() const override { return 0; }

    void fail_at(size_t offset) {
        std::lock_guard lock(_mutex);
        _failed_offsets.insert(offset);
    }

    void return_short_at(size_t offset) {
        std::lock_guard lock(_mutex);
        _short_offsets.insert(offset);
    }

    bool wait_for_entered(size_t count) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, 5s, [&]() { return _entered >= count; });
    }

    void release_reads() {
        {
            std::lock_guard lock(_mutex);
            _release_reads = true;
        }
        _cv.notify_all();
    }

    size_t read_calls() const {
        std::lock_guard lock(_mutex);
        return _read_calls;
    }

    size_t max_active_reads() const {
        std::lock_guard lock(_mutex);
        return _max_active_reads;
    }

    std::vector<ObservedIOContext> observed_contexts() const {
        std::lock_guard lock(_mutex);
        return _observed_contexts;
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_context) override {
        bool fail = false;
        bool return_short = false;
        {
            std::unique_lock lock(_mutex);
            ++_read_calls;
            ++_entered;
            ++_active_reads;
            _max_active_reads = std::max(_max_active_reads, _active_reads);
            fail = _failed_offsets.contains(offset);
            return_short = _short_offsets.contains(offset);
            ObservedIOContext observed;
            if (io_context != nullptr) {
                observed.has_query_id = io_context->query_id != nullptr;
                if (observed.has_query_id) {
                    observed.query_id_hi = io_context->query_id->hi;
                    observed.query_id_lo = io_context->query_id->lo;
                }
                observed.file_cache_stats =
                        reinterpret_cast<uintptr_t>(io_context->file_cache_stats);
                observed.file_reader_stats =
                        reinterpret_cast<uintptr_t>(io_context->file_reader_stats);
                observed.has_remote_write_limiter =
                        io_context->remote_scan_cache_write_limiter != nullptr;
                observed.cache_write_mode = io_context->cache_write_mode_override;
                observed.condition_cache_filtered_rows = io_context->condition_cache_filtered_rows;
                observed.predicate_filtered_rows = io_context->predicate_filtered_rows;
                if (io_context->file_cache_stats != nullptr) {
                    ++io_context->file_cache_stats->num_remote_io_total;
                }
                if (io_context->file_reader_stats != nullptr) {
                    ++io_context->file_reader_stats->read_calls;
                    io_context->file_reader_stats->read_bytes += result.size;
                }
            }
            _observed_contexts.emplace_back(observed);
            _cv.notify_all();
            if (_block_reads) {
                _cv.wait(lock, [&]() { return _release_reads; });
            }
            --_active_reads;
        }

        if (fail) {
            *bytes_read = 0;
            return Status::IOError("injected asynchronous read failure at {}", offset);
        }
        size_t read_size = result.size;
        if (return_short) {
            DORIS_CHECK(read_size > 1);
            --read_size;
        }
        std::memcpy(result.data, _content.data() + offset, read_size);
        *bytes_read = read_size;
        return Status::OK();
    }

private:
    const Path _path {"controllable_file"};
    const std::string _content;
    const bool _block_reads;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    bool _closed {false};
    bool _release_reads {false};
    size_t _read_calls {0};
    size_t _entered {0};
    size_t _active_reads {0};
    size_t _max_active_reads {0};
    std::set<size_t> _failed_offsets;
    std::set<size_t> _short_offsets;
    std::vector<ObservedIOContext> _observed_contexts;
};

FileRangeReadSchedulerOptions scheduler_options(size_t max_bytes_per_query = 4096,
                                                size_t max_bytes_per_be = 16384) {
    return FileRangeReadSchedulerOptions {
            .max_bytes_per_query = max_bytes_per_query,
            .max_bytes_per_be = max_bytes_per_be,
    };
}

std::unique_ptr<ThreadPool> create_test_executor(const std::string& name, int thread_count) {
    DORIS_CHECK(thread_count > 0);
    std::unique_ptr<ThreadPool> executor;
    DORIS_CHECK(ThreadPoolBuilder(name)
                        .set_min_threads(thread_count)
                        .set_max_threads(thread_count)
                        .build(&executor)
                        .ok());
    return executor;
}

ThreadPool* shared_test_executor() {
    static std::unique_ptr<ThreadPool> executor = []() {
        std::unique_ptr<ThreadPool> result;
        Status status = ThreadPoolBuilder("FileRangeReadTestPool")
                                .set_min_threads(4)
                                .set_max_threads(64)
                                .build(&result);
        DORIS_CHECK(status.ok());
        return result;
    }();
    return executor.get();
}

std::unique_ptr<FileRangeReadScheduler> create_scheduler(
        const FileRangeReadSchedulerOptions& options = scheduler_options(),
        ThreadPool* executor = shared_test_executor()) {
    std::unique_ptr<FileRangeReadScheduler> reader;
    EXPECT_TRUE(FileRangeReadScheduler::create(options, executor, &reader).ok());
    return reader;
}

FileRangeReadIOContext default_read_context() {
    IOContext context;
    return FileRangeReadIOContext::from_caller(context);
}

std::string alphabet(size_t size) {
    std::string result(size, '\0');
    for (size_t index = 0; index < size; ++index) {
        result[index] = static_cast<char>('a' + index % 26);
    }
    return result;
}

testing::AssertionResult has_expected_read_results(const FileRangeReadSubmitResult& result) {
    if (result.reads.size() != 2) {
        return testing::AssertionFailure() << "expected 2 reads, got " << result.reads.size();
    }
    if (!result.reads[0]->wait().ok() || !result.reads[1]->wait().ok()) {
        return testing::AssertionFailure() << "an exact range read did not become ready";
    }
    if (result.reads[0]->state() != FileRangeRead::State::READY) {
        return testing::AssertionFailure() << "first range is not ready";
    }
    if (result.reads[0]->data().to_string() != alphabet(128).substr(3, 5)) {
        return testing::AssertionFailure() << "first range contains unexpected bytes";
    }
    if (result.reads[1]->slice(2, 3).to_string() != alphabet(128).substr(22, 3)) {
        return testing::AssertionFailure() << "second range slice contains unexpected bytes";
    }
    const auto stats = result.reads[0]->stats();
    if (stats.bytes_read != 5 || stats.file_cache.num_remote_io_total != 1 ||
        stats.file_reader.read_calls != 1) {
        return testing::AssertionFailure() << "worker-owned read statistics are incomplete";
    }
    return testing::AssertionSuccess();
}

testing::AssertionResult has_worker_owned_contexts(const std::vector<ObservedIOContext>& contexts,
                                                   const TUniqueId& query_id,
                                                   const FileCacheStatistics* caller_cache_stats,
                                                   const FileReaderStats* caller_reader_stats) {
    if (contexts.size() != 2) {
        return testing::AssertionFailure()
               << "expected 2 observed contexts, got " << contexts.size();
    }
    for (const auto& context : contexts) {
        if (!context.has_query_id || context.query_id_hi != query_id.hi ||
            context.query_id_lo != query_id.lo) {
            return testing::AssertionFailure() << "query id was not copied into worker ownership";
        }
        if (context.file_cache_stats == reinterpret_cast<uintptr_t>(caller_cache_stats) ||
            context.file_reader_stats == reinterpret_cast<uintptr_t>(caller_reader_stats)) {
            return testing::AssertionFailure() << "worker reused caller-owned statistics";
        }
        if (context.file_cache_stats == 0 || context.file_reader_stats == 0 ||
            context.has_remote_write_limiter) {
            return testing::AssertionFailure() << "worker context contains unsafe pointers";
        }
        if (context.cache_write_mode != CacheWriteMode::NO_WRITE) {
            return testing::AssertionFailure() << "worker read did not disable cache writeback";
        }
        if (context.condition_cache_filtered_rows != 0 || context.predicate_filtered_rows != 0) {
            return testing::AssertionFailure() << "worker inherited caller-local row counters";
        }
    }
    return testing::AssertionSuccess();
}

} // namespace

TEST(FileRangeReadSchedulerTest, ReadsExactRangesWithWorkerOwnedIOContext) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();

    TUniqueId query_id;
    query_id.hi = 101;
    query_id.lo = 202;
    FileCacheStatistics caller_cache_stats;
    FileReaderStats caller_reader_stats;
    IOContext caller_context;
    caller_context.query_id = &query_id;
    caller_context.file_cache_stats = &caller_cache_stats;
    caller_context.file_reader_stats = &caller_reader_stats;
    caller_context.condition_cache_filtered_rows = 17;
    caller_context.predicate_filtered_rows = 19;

    auto result = scheduler->try_submit({{.offset = 3, .size = 5}, {.offset = 20, .size = 7}},
                                        file_reader,
                                        FileRangeReadIOContext::from_caller(caller_context), query);
    ASSERT_TRUE(result.accepted()) << result.status;
    EXPECT_TRUE(has_expected_read_results(result));
    EXPECT_TRUE(has_worker_owned_contexts(file_reader->observed_contexts(), query_id,
                                          &caller_cache_stats, &caller_reader_stats));
    EXPECT_EQ(caller_cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(caller_reader_stats.read_calls, 0);
}

TEST(FileRangeReadSchedulerTest, ExecutorThreadCountControlsConcurrency) {
    auto executor = create_test_executor("RangeConcurrencyExecutor", 2);
    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128), true);
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit(
            {{.offset = 0, .size = 8}, {.offset = 32, .size = 8}, {.offset = 64, .size = 8}},
            file_reader, default_read_context(), query);
    ASSERT_TRUE(result.accepted());

    const bool both_entered = file_reader->wait_for_entered(2);
    EXPECT_EQ(file_reader->read_calls(), 2);
    EXPECT_EQ(file_reader->max_active_reads(), 2);
    file_reader->release_reads();
    ASSERT_TRUE(both_entered);
    for (const auto& read : result.reads) {
        ASSERT_TRUE(read->wait().ok());
    }
    EXPECT_EQ(file_reader->max_active_reads(), 2);
    scheduler.reset();
    executor->shutdown();
}

TEST(FileRangeReadSchedulerTest, ExecutesReadsOnProvidedThreadPool) {
    std::unique_ptr<ThreadPool> executor;
    ASSERT_TRUE(ThreadPoolBuilder("BlockedRangeExecutor")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&executor)
                        .ok());

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    ASSERT_TRUE(executor->submit_func([&blocker_started, release_blocker_future]() {
                            blocker_started.set_value();
                            release_blocker_future.wait();
                        })
                        .ok());
    blocker_started_future.wait();

    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}}, file_reader,
                                        default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    EXPECT_EQ(file_reader->read_calls(), 0);

    release_blocker.set_value();
    ASSERT_TRUE(result.reads.front()->wait().ok());
    EXPECT_EQ(file_reader->read_calls(), 1);
    scheduler.reset();
    executor->shutdown();
}

TEST(FileRangeReadSchedulerTest, ExecutorRejectionRollsBackWholeBatch) {
    std::unique_ptr<ThreadPool> executor;
    ASSERT_TRUE(ThreadPoolBuilder("StoppedRangeExecutor")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .build(&executor)
                        .ok());
    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    executor->shutdown();

    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);

    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reject_reason, FileRangeReadRejectReason::EXECUTOR_REJECTED);
    EXPECT_TRUE(result.reads.empty());
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 0);
    EXPECT_EQ(file_reader->read_calls(), 0);
}

TEST(FileRangeReadSchedulerTest, ExecutorRejectionFailsOnlyUnsubmittedRanges) {
    std::unique_ptr<ThreadPool> executor;
    ASSERT_TRUE(ThreadPoolBuilder("PartiallyFullRangeExecutor")
                        .set_min_threads(1)
                        .set_max_threads(1)
                        .set_max_queue_size(1)
                        .build(&executor)
                        .ok());

    std::promise<void> blocker_started;
    auto blocker_started_future = blocker_started.get_future();
    std::promise<void> release_blocker;
    auto release_blocker_future = release_blocker.get_future().share();
    ASSERT_TRUE(executor->submit_func([&blocker_started, release_blocker_future]() {
                            blocker_started.set_value();
                            release_blocker_future.wait();
                        })
                        .ok());
    blocker_started_future.wait();

    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);

    ASSERT_TRUE(result.accepted());
    EXPECT_FALSE(result.reads[1]->wait().ok());
    EXPECT_EQ(result.reads[1]->state(), FileRangeRead::State::FAILED);
    EXPECT_EQ(file_reader->read_calls(), 0);

    release_blocker.set_value();
    EXPECT_TRUE(result.reads[0]->wait().ok());
    EXPECT_EQ(result.reads[0]->state(), FileRangeRead::State::READY);
    EXPECT_EQ(file_reader->read_calls(), 1);
    scheduler.reset();
    executor->shutdown();
}

TEST(FileRangeReadSchedulerTest, QueryByteAdmissionRejectsWholeBatch) {
    auto scheduler = create_scheduler(scheduler_options(15, 256));
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);

    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reject_reason, FileRangeReadRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_TRUE(result.reads.empty());
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 0);
    EXPECT_EQ(file_reader->read_calls(), 0);
}

TEST(FileRangeReadSchedulerTest, QueryByteAdmissionIncludesCompletedResidentBuffers) {
    auto scheduler = create_scheduler(scheduler_options(12, 64));
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto accepted = scheduler->try_submit({{.offset = 0, .size = 8}}, file_reader,
                                          default_read_context(), query);
    ASSERT_TRUE(accepted.accepted());
    ASSERT_TRUE(accepted.reads.front()->wait().ok());
    EXPECT_EQ(query->resident_bytes(), 8);

    auto rejected_result = scheduler->try_submit({{.offset = 32, .size = 8}}, file_reader,
                                                 default_read_context(), query);
    EXPECT_EQ(rejected_result.reject_reason, FileRangeReadRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_EQ(query->resident_bytes(), 8);
}

TEST(FileRangeReadSchedulerTest, GlobalBudgetIsSharedAcrossQueries) {
    auto scheduler = create_scheduler(scheduler_options(64, 10));
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128), true);
    auto query1 = scheduler->create_context();
    auto query2 = scheduler->create_context();
    auto accepted = scheduler->try_submit({{.offset = 0, .size = 6}}, file_reader,
                                          default_read_context(), query1);
    ASSERT_TRUE(accepted.accepted());
    ASSERT_TRUE(file_reader->wait_for_entered(1));

    auto rejected_result = scheduler->try_submit({{.offset = 16, .size = 5}}, file_reader,
                                                 default_read_context(), query2);
    EXPECT_EQ(rejected_result.reject_reason, FileRangeReadRejectReason::GLOBAL_BYTE_LIMIT);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 6);

    file_reader->release_reads();
    ASSERT_TRUE(accepted.reads.front()->wait().ok());
}

TEST(FileRangeReadSchedulerTest, ResidentBytesFollowReadLifetime) {
    auto executor = create_test_executor("RangeResidentBytesExecutor", 1);
    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 4, .size = 12}}, file_reader,
                                        default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    ASSERT_TRUE(result.reads.front()->wait().ok());
    executor->wait();
    EXPECT_EQ(query->resident_bytes(), 12);

    auto read = result.reads.front();
    result.reads.clear();
    EXPECT_EQ(query->resident_bytes(), 12);
    read.reset();
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 0);
    scheduler.reset();
    executor->shutdown();
}

TEST(FileRangeReadSchedulerTest, QueryCancellationSkipsQueuedIOAndCancelsRunningRead) {
    auto executor = create_test_executor("RangeCancellationExecutor", 1);
    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128), true);
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    ASSERT_TRUE(file_reader->wait_for_entered(1));

    query->cancel();
    EXPECT_FALSE(result.reads[1]->wait().ok());
    EXPECT_EQ(result.reads[1]->state(), FileRangeRead::State::CANCELLED);
    EXPECT_EQ(file_reader->read_calls(), 1);

    file_reader->release_reads();
    EXPECT_FALSE(result.reads[0]->wait().ok());
    EXPECT_EQ(result.reads[0]->state(), FileRangeRead::State::CANCELLED);
    EXPECT_EQ(file_reader->read_calls(), 1);
    scheduler.reset();
    executor->shutdown();
}

TEST(FileRangeReadSchedulerTest, CancellationWinsBeforeReadyIsPublished) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "FileRangeReadScheduler::_run_task:before_publish_ready",
            [&](auto&&) { query->cancel(); }, &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    auto result = scheduler->try_submit({{.offset = 0, .size = 8}}, file_reader,
                                        default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    EXPECT_FALSE(result.reads.front()->wait().ok());
    EXPECT_EQ(result.reads.front()->state(), FileRangeRead::State::CANCELLED);
    EXPECT_EQ(result.reads.front()->stats().bytes_read, 8);
}

TEST(FileRangeReadSchedulerTest, CancelledContextRejectsBatchBeforeAdmission) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    query->cancel();

    auto result = scheduler->try_submit({{.offset = 0, .size = 8}}, file_reader,
                                        default_read_context(), query);
    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reject_reason, FileRangeReadRejectReason::QUERY_CANCELLED);
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(file_reader->read_calls(), 0);
}

TEST(FileRangeReadSchedulerTest, ShutdownCancelsPendingReadAndWaitsForRunningRead) {
    auto executor = create_test_executor("RangeShutdownExecutor", 1);
    auto scheduler = create_scheduler(scheduler_options(), executor.get());
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128), true);
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    ASSERT_TRUE(file_reader->wait_for_entered(1));

    auto shutdown_future = std::async(std::launch::async, [&]() { scheduler->shutdown(); });
    const auto stop_accepting_deadline = std::chrono::steady_clock::now() + 5s;
    while (scheduler->accepting() && std::chrono::steady_clock::now() < stop_accepting_deadline) {
        std::this_thread::yield();
    }
    const bool stopped_accepting = !scheduler->accepting();
    if (!stopped_accepting) {
        file_reader->release_reads();
    }
    ASSERT_TRUE(stopped_accepting);
    EXPECT_EQ(shutdown_future.wait_for(20ms), std::future_status::timeout);
    EXPECT_FALSE(result.reads[1]->wait().ok());
    EXPECT_EQ(result.reads[1]->state(), FileRangeRead::State::CANCELLED);

    file_reader->release_reads();
    ASSERT_EQ(shutdown_future.wait_for(5s), std::future_status::ready);
    shutdown_future.get();
    EXPECT_FALSE(result.reads[0]->wait().ok());
    EXPECT_EQ(result.reads[0]->state(), FileRangeRead::State::CANCELLED);

    auto rejected_result = scheduler->try_submit({{.offset = 32, .size = 8}}, file_reader,
                                                 default_read_context(), query);
    EXPECT_EQ(rejected_result.reject_reason, FileRangeReadRejectReason::SHUTTING_DOWN);
}

TEST(FileRangeReadSchedulerTest, PublishesReadFailureAndShortRead) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    file_reader->fail_at(0);
    file_reader->return_short_at(32);
    auto query = scheduler->create_context();
    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 32, .size = 8}},
                                        file_reader, default_read_context(), query);
    ASSERT_TRUE(result.accepted());
    EXPECT_FALSE(result.reads[0]->wait().ok());
    EXPECT_FALSE(result.reads[1]->wait().ok());
    EXPECT_EQ(result.reads[0]->state(), FileRangeRead::State::FAILED);
    EXPECT_EQ(result.reads[1]->state(), FileRangeRead::State::FAILED);
    EXPECT_EQ(result.reads[0]->stats().bytes_read, 0);
    EXPECT_EQ(result.reads[1]->stats().bytes_read, 7);
}

TEST(FileRangeReadSchedulerTest, AllocationFailureRollsBackWholeBatch) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    size_t allocation_count = 0;
    sync_point->set_call_back(
            "FileRangeRead::create:inject_failure",
            [&](auto&& values) {
                ++allocation_count;
                if (allocation_count != 2) {
                    return;
                }
                auto* status = try_any_cast<Status*>(values.back());
                *status = Status::MemoryAllocFailed("injected asynchronous range allocation");
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    auto result = scheduler->try_submit({{.offset = 0, .size = 8}, {.offset = 16, .size = 8}},
                                        file_reader, default_read_context(), query);
    EXPECT_FALSE(result.accepted());
    EXPECT_EQ(result.reject_reason, FileRangeReadRejectReason::ALLOCATION_FAILED);
    EXPECT_TRUE(result.reads.empty());
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 0);
    EXPECT_EQ(file_reader->read_calls(), 0);
    EXPECT_EQ(allocation_count, 2);
}

TEST(FileRangeReadSchedulerTest, InvalidBatchDoesNotConsumeBudget) {
    auto scheduler = create_scheduler();
    auto file_reader = std::make_shared<ControllableFileReader>(alphabet(128));
    auto query = scheduler->create_context();

    const std::vector<std::vector<FileRange>> invalid_batches = {
            {},
            {{.offset = 0, .size = 0}},
            {{.offset = std::numeric_limits<size_t>::max() - 3, .size = 8}},
            {{.offset = 120, .size = 16}},
    };
    for (const auto& ranges : invalid_batches) {
        auto result = scheduler->try_submit(ranges, file_reader, default_read_context(), query);
        EXPECT_FALSE(result.accepted());
        EXPECT_EQ(result.reject_reason, FileRangeReadRejectReason::INVALID_REQUEST);
        EXPECT_TRUE(result.reads.empty());
    }
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(scheduler->global_budget()->resident_bytes(), 0);
}

TEST(FileRangeReadSchedulerTest, ValidatesReaderOptions) {
    auto options = scheduler_options();
    options.max_bytes_per_query = 0;
    EXPECT_FALSE(options.validate().ok());
    options = scheduler_options();
    options.max_bytes_per_be = 0;
    EXPECT_FALSE(options.validate().ok());
}

} // namespace doris::io
