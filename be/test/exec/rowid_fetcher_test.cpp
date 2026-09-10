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

#include "exec/rowid_fetcher.h"

#include <gtest/gtest.h>

#include <atomic>
#include <stdexcept>
#include <string>
#include <vector>

#include "bthread/countdown_event.h"
#include "common/exception.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"

namespace doris {

// source_column_key() decides which projected slots collapse onto a single scanned
// column. Two slots that name the same physical column must produce the same key, and
// any two slots that do not must produce different ones -- a false merge drops a column
// the result block still expects, which is the crash this keying was introduced to fix.
class RowIdStorageReaderTest : public testing::Test {
public:
    static std::string key_of(const SlotDescriptor& slot, uint32_t column_idx) {
        return RowIdStorageReader::source_column_key(slot, column_idx);
    }

protected:
    struct SlotSpec {
        std::string col_name = "c";
        int32_t col_unique_id = 1;
        std::vector<std::string> column_paths = {};
        TColumnAccessPaths access_paths = {};
    };

    static SlotDescriptor make_slot(const SlotSpec& spec) {
        TSlotDescriptor tdesc = TSlotDescriptorBuilder()
                                        .type(TYPE_INT)
                                        .nullable(true)
                                        .column_name(spec.col_name)
                                        .column_pos(0)
                                        .build();
        tdesc.__set_col_unique_id(spec.col_unique_id);
        tdesc.__set_column_paths(spec.column_paths);
        if (!spec.access_paths.empty()) {
            tdesc.__set_all_access_paths(spec.access_paths);
        }
        return SlotDescriptor(tdesc);
    }

    static TColumnAccessPath data_path(const std::vector<std::string>& path) {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        TDataAccessPath data;
        data.__set_path(path);
        access_path.__set_data_access_path(data);
        return access_path;
    }

    static TColumnAccessPath bare_path() {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        return access_path;
    }
};

TEST_F(RowIdStorageReaderTest, SameSourceColumnSharesKey) {
    // The bug case: one physical column projected twice must dedup onto one scan column.
    const SlotDescriptor first = make_slot({});
    const SlotDescriptor second = make_slot({});
    EXPECT_EQ(key_of(first, 3), key_of(second, 3));
}

TEST_F(RowIdStorageReaderTest, ColumnIndexSeparatesKeys) {
    const SlotDescriptor slot = make_slot({});
    EXPECT_NE(key_of(slot, 3), key_of(slot, 4));
}

TEST_F(RowIdStorageReaderTest, ColumnNameSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 0), key_of(make_slot({.col_name = "b"}), 0));
}

TEST_F(RowIdStorageReaderTest, UniqueIdSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_unique_id = 1}), 0),
              key_of(make_slot({.col_unique_id = 2}), 0));
}

TEST_F(RowIdStorageReaderTest, NameAndIndexBoundaryIsNotAmbiguous) {
    // Without length prefixes, "a" + idx 12 and "a1" + idx 2 both flatten to "a12".
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 12), key_of(make_slot({.col_name = "a1"}), 2));
}

TEST_F(RowIdStorageReaderTest, PathComponentBoundaryIsNotAmbiguous) {
    // The concatenation hazard the length prefix exists for: ["a", "b"] and ["a:b"] are
    // different nested columns but share the naive ':'-joined spelling.
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a:b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EmptyPathIsNotTheSameAsNoPath) {
    EXPECT_NE(key_of(make_slot({.column_paths = {}}), 0),
              key_of(make_slot({.column_paths = {""}}), 0));
}

TEST_F(RowIdStorageReaderTest, PathOrderMatters) {
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"b", "a"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EqualPathsShareKey) {
    EXPECT_EQ(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a", "b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"b"})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AbsentAccessPathIsNotAnEmptyOne) {
    // The presence bit: an unset data_access_path must not collide with one that is set
    // but carries no components.
    EXPECT_NE(key_of(make_slot({.access_paths = {bare_path()}}), 0),
              key_of(make_slot({.access_paths = {data_path({})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathCountSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"a"}), data_path({"b"})}}), 0));
}

// Runs every submitted task on the submitting thread. The point of these cases is which
// status reaches the caller, not the threading, and inline execution keeps them
// deterministic.
class InlineScanScheduler : public ScannerScheduler {
public:
    Status start(int, int, int, int) override { return Status::OK(); }
    void stop() override {}
    Status submit_scan_task(SimplifiedScanTask scan_task) override {
        scan_task.scan_func();
        return Status::OK();
    }
    Status submit_scan_task(SimplifiedScanTask scan_task, const std::string&) override {
        scan_task.scan_func();
        return Status::OK();
    }
    void reset_thread_num(int, int, int) override {}
    int get_queue_size() override { return 0; }
    int get_active_threads() override { return 0; }
    std::vector<int> thread_debug_info() override { return {}; }
    Status schedule_scan_task(std::shared_ptr<ScannerContext>, std::shared_ptr<ScanTask>,
                              std::unique_lock<std::mutex>&) override {
        return Status::OK();
    }
};

// submit_external_scan_tasks() signals completion from a Defer, so a worker that leaves
// without publishing its status would still wake the waiter and the caller would report
// success over a partially filled result block.
class SubmitExternalScanTasksTest : public RowIdStorageReaderTest {
protected:
    static constexpr size_t kTaskCount = 3;

    static Status run_tasks(const std::function<Status(size_t)>& run_task) {
        InlineScanScheduler scheduler;
        std::counting_semaphore<> semaphore {kTaskCount};
        return RowIdStorageReader::submit_external_scan_tasks(
                &scheduler, semaphore, kTaskCount,
                [](size_t idx) { return fmt::format("task-{}", idx); }, run_task);
    }
};

TEST_F(SubmitExternalScanTasksTest, AllTasksSucceedingReturnsOk) {
    size_t ran = 0;
    EXPECT_TRUE(run_tasks([&](size_t) -> Status {
                    ++ran;
                    return Status::OK();
                }).ok());
    EXPECT_EQ(ran, kTaskCount);
}

TEST_F(SubmitExternalScanTasksTest, ReturnedErrorReachesTheCaller) {
    const Status result = run_tasks([](size_t idx) -> Status {
        return idx == kTaskCount - 1 ? Status::InternalError("scanner returned an error")
                                     : Status::OK();
    });
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.to_string().find("scanner returned an error"), std::string::npos);
}

TEST_F(SubmitExternalScanTasksTest, ThrownExceptionReachesTheCaller) {
    // The last task is the interesting one: it is the completion that releases the
    // waiter, so a status lost here is a status the caller never sees.
    const Status result = run_tasks([](size_t idx) -> Status {
        if (idx == kTaskCount - 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "scanner threw");
        }
        return Status::OK();
    });
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.to_string().find("scanner threw"), std::string::npos);
}

class ParallelRowIdFetchTest : public RowIdStorageReaderTest {
protected:
    static Status read_groups(size_t count, int batch, int concurrency, bool row_store,
                              const std::function<Status(size_t, size_t)>& read) {
        return RowIdStorageReader::read_internal_segment_groups(count, batch, concurrency,
                                                                row_store, read);
    }
};

TEST_F(ParallelRowIdFetchTest, CoversEveryGroupIncludingPartialLastTask) {
    std::vector<int> visits(7, 0);
    std::atomic<int> tasks = 0;
    auto status = read_groups(7, 2, 3, false, [&](size_t begin, size_t end) {
        EXPECT_LE(end - begin, 2);
        for (size_t i = begin; i < end; ++i) {
            ++visits[i];
        }
        ++tasks;
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(tasks, 4);
    EXPECT_EQ(visits, std::vector<int>(7, 1));
}

TEST_F(ParallelRowIdFetchTest, SerialFallbacksReadOneRange) {
    auto check_serial = [&](int batch, int concurrency, bool row_store) {
        int calls = 0;
        auto status = read_groups(5, batch, concurrency, row_store, [&](size_t begin, size_t end) {
            EXPECT_EQ(begin, 0);
            EXPECT_EQ(end, 5);
            ++calls;
            return Status::OK();
        });
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ(calls, 1);
    };
    check_serial(0, 8, false);
    check_serial(-1, 8, false);
    check_serial(5, 8, false);
    check_serial(100, 8, false);
    check_serial(1, 1, false);
    check_serial(1, 0, false);
    check_serial(1, -1, false);
    check_serial(1, 8, true);
}

TEST_F(ParallelRowIdFetchTest, EmptyRequestDoesNotRead) {
    ASSERT_TRUE(read_groups(0, 1, 8, false, [](size_t, size_t) {
                    ADD_FAILURE() << "Empty request must not schedule a read";
                    return Status::OK();
                }).ok());
}

TEST_F(ParallelRowIdFetchTest, RunsConcurrentlyWithinLimit) {
    bthread::CountdownEvent first_pair(2);
    std::atomic<int> active = 0;
    std::atomic<int> peak = 0;
    auto status = read_groups(8, 1, 2, false, [&](size_t begin, size_t) {
        const int running = ++active;
        int previous = peak.load();
        while (previous < running && !peak.compare_exchange_weak(previous, running)) {
        }
        if (begin < 2) {
            first_pair.signal();
            first_pair.wait();
        }
        --active;
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(peak, 2);
    EXPECT_EQ(active, 0);
}

TEST_F(ParallelRowIdFetchTest, WaitsForStartedTasksOnError) {
    bthread::CountdownEvent both_started(2);
    std::atomic<bool> other_finished = false;
    auto status = read_groups(2, 1, 2, false, [&](size_t begin, size_t) {
        both_started.signal();
        both_started.wait();
        if (begin == 0) {
            return Status::InternalError("internal rowid read failed");
        }
        other_finished = true;
        return Status::OK();
    });
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("internal rowid read failed"), std::string::npos);
    EXPECT_TRUE(other_finished);
}

TEST_F(ParallelRowIdFetchTest, ExceptionsReachCallerWithoutHanging) {
    for (bool doris_exception : {false, true}) {
        auto status = read_groups(3, 1, 2, false, [&](size_t, size_t) -> Status {
            if (doris_exception) {
                throw Exception(ErrorCode::INTERNAL_ERROR, "parallel read exception");
            }
            throw std::runtime_error("parallel read exception");
        });
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("parallel read exception"), std::string::npos);
    }
}

} // namespace doris
