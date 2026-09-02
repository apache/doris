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

#include "runtime/result_buffer_mgr.h"

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "core/block/block.h"
#include "exec/sink/writer/varrow_flight_result_writer.h"
#include "exec/sink/writer/vmysql_result_writer.h"
#include "runtime/result_block_buffer.h"
#include "util/cpu_info.h"
#include "util/thread.h"

namespace doris {

class ResultBufferMgrTest : public testing::Test {
public:
    ResultBufferMgrTest() {}
    ~ResultBufferMgrTest() override {}

private:
    RuntimeState _state;
};

TEST_F(ResultBufferMgrTest, create_normal) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
    EXPECT_NE(control_block1, nullptr);
    control_block1.reset();

    EXPECT_FALSE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
}

TEST_F(ResultBufferMgrTest, create_arrow) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto schema = std::make_shared<arrow::Schema>(std::move(fields));
    EXPECT_TRUE(
            buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, true, schema).ok());
}

TEST_F(ResultBufferMgrTest, create_same_buffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());
    std::shared_ptr<ResultBlockBufferBase> control_block2;
    EXPECT_FALSE(buffer_mgr.create_sender(query_id, 1024, &control_block2, &_state, false).ok());
}

TEST_F(ResultBufferMgrTest, find_buffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto schema = std::make_shared<arrow::Schema>(std::move(fields));
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false, nullptr)
                        .ok());

    std::shared_ptr<MySQLResultBlockBuffer> buffer;
    EXPECT_TRUE(buffer_mgr.find_buffer(query_id, buffer).ok());
    EXPECT_TRUE(buffer != nullptr);
}

TEST_F(ResultBufferMgrTest, normal_cancel) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    std::shared_ptr<ResultBlockBufferBase> control_block1;
    EXPECT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block1, &_state, false).ok());

    EXPECT_TRUE(buffer_mgr.cancel(query_id, Status::InternalError("")));
}

TEST_F(ResultBufferMgrTest, cancel_no_block) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;

    EXPECT_FALSE(buffer_mgr.cancel(query_id, Status::InternalError("")));
}

TEST_F(ResultBufferMgrTest, RejectsNewSenderAfterStop) {
    ResultBufferMgr buffer_mgr;
    buffer_mgr.stop();

    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    std::shared_ptr<ResultBlockBufferBase> control_block;

    EXPECT_FALSE(buffer_mgr.create_sender(query_id, 1024, &control_block, &_state, false).ok());
    EXPECT_EQ(control_block, nullptr);
}

TEST_F(ResultBufferMgrTest, LateOutfileCleanupRetriesAfterCancellation) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 11;
    query_id.hi = 101;

    std::shared_ptr<ResultBlockBufferBase> control_block;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &control_block, &_state, false).ok());
    ASSERT_TRUE(buffer_mgr.cancel(query_id, Status::Cancelled("injected cancellation")));

    int cleanup_attempts = 0;
    EXPECT_TRUE(control_block
                        ->add_outfile_cleanup([&] {
                            ++cleanup_attempts;
                            return cleanup_attempts == 1
                                           ? Status::IOError("injected transient cleanup failure")
                                           : Status::OK();
                        })
                        .ok());
    EXPECT_EQ(cleanup_attempts, 2);
}

TEST_F(ResultBufferMgrTest, OutfileAbortCleansRegisteredAndLateFiles) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 20;
    query_id.hi = 200;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_EQ(cleanup_count, 1);
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());
    EXPECT_EQ(cleanup_count, 2);
}

TEST_F(ResultBufferMgrTest, OutfilePrepareKeepsRollbackOwnership) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 30;
    query_id.hi = 300;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::PREPARE).ok());
    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(ResultBufferMgrTest, OutfileCommitSurvivesDeferredBufferCleanup) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 35;
    query_id.hi = 350;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::PREPARE).ok());
    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::COMMIT).ok());
    EXPECT_TRUE(buffer_mgr.cancel(query_id, Status::Cancelled("deferred cleanup")));
    EXPECT_EQ(cleanup_count, 0);
}

TEST_F(ResultBufferMgrTest, OutfileCommitRequiresPrepare) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 40;
    query_id.hi = 400;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    EXPECT_FALSE(buffer_mgr.finish_outfile(query_id, OutfileOperation::COMMIT).ok());
    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(ResultBufferMgrTest, OutfilePrepareRejectsAbortedBuffer) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 50;
    query_id.hi = 500;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_FALSE(buffer_mgr.finish_outfile(query_id, OutfileOperation::PREPARE).ok());
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(ResultBufferMgrTest, OutfileAbortCanCompensatePartialCommit) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 60;
    query_id.hi = 600;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return Status::OK();
                      }).ok());

    ASSERT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::PREPARE).ok());
    ASSERT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::COMMIT).ok());
    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_EQ(cleanup_count, 1);
}

TEST_F(ResultBufferMgrTest, OutfileAbortRetainsFailedCleanupForRetry) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 70;
    query_id.hi = 700;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    int cleanup_count = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          ++cleanup_count;
                          return cleanup_count == 1 ? Status::IOError("injected cleanup failure")
                                                    : Status::OK();
                      }).ok());

    EXPECT_FALSE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_TRUE(buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT).ok());
    EXPECT_EQ(cleanup_count, 2);
}

TEST_F(ResultBufferMgrTest, ConcurrentCancellationWaitsForOutfileAbortDrain) {
    ResultBufferMgr buffer_mgr;
    TUniqueId query_id;
    query_id.lo = 80;
    query_id.hi = 800;

    std::shared_ptr<ResultBlockBufferBase> buffer;
    ASSERT_TRUE(buffer_mgr.create_sender(query_id, 1024, &buffer, &_state, false).ok());
    CountDownLatch cleanup_started(1);
    CountDownLatch release_cleanup(1);
    std::atomic<int> cleanup_attempts = 0;
    ASSERT_TRUE(buffer->add_outfile_cleanup([&] {
                          int attempt = ++cleanup_attempts;
                          if (attempt == 1) {
                              cleanup_started.count_down();
                              release_cleanup.wait();
                              return Status::IOError("injected cleanup failure");
                          }
                          return Status::OK();
                      }).ok());

    auto abort_future = std::async(std::launch::async, [&] {
        return buffer_mgr.finish_outfile(query_id, OutfileOperation::ABORT);
    });
    ASSERT_TRUE(cleanup_started.wait_for(std::chrono::seconds(5)));
    auto cancel_future = std::async(std::launch::async, [&] {
        return buffer_mgr.cancel(query_id, Status::Cancelled("injected cancellation"));
    });

    std::shared_ptr<MySQLResultBlockBuffer> observed_buffer;
    for (int attempt = 0; attempt < 5000 && buffer_mgr.find_buffer(query_id, observed_buffer).ok();
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    bool buffer_removed = !buffer_mgr.find_buffer(query_id, observed_buffer).ok();
    release_cleanup.count_down();

    ASSERT_TRUE(buffer_removed);
    EXPECT_FALSE(abort_future.get().ok());
    EXPECT_TRUE(cancel_future.get());
    EXPECT_EQ(cleanup_attempts.load(), 2);
}

} // namespace doris
