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

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <future>
#include <string>
#include <vector>

#include "common/config.h"
#include "load/routine_load/routine_load_task_executor.h"
#include "util/debug_points.h"

namespace doris {

class KinesisScanSchedulingTest : public testing::Test {
protected:
    void SetUp() override {
        old_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;
        executor = std::make_unique<RoutineLoadTaskExecutor>(nullptr);
        ASSERT_TRUE(ThreadPoolBuilder("kinesis_test_scan")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(8)
                            .build(&executor->_kinesis_scan_pool)
                            .ok());
    }

    void TearDown() override {
        executor->stop();
        executor.reset();
        DebugPoints::instance()->remove("RoutineLoadTaskExecutor.kinesis_scan_shard");
        DebugPoints::instance()->remove("RoutineLoadTaskExecutor.kinesis_scan_before_finish");
        config::enable_debug_points = old_debug_points;
    }

    PKinesisMetaProxyRequest request(std::initializer_list<std::string> shards) {
        PKinesisMetaProxyRequest result;
        result.mutable_kinesis_info()->set_region("test");
        result.mutable_kinesis_info()->set_stream("test");
        for (const auto& shard : shards) {
            result.add_shard_ids_for_latest_sequences(shard);
        }
        return result;
    }

    void scan_with(std::function<void(std::string, std::string*, Status*)> callback) {
        DebugPoints::instance()->add_with_callback("RoutineLoadTaskExecutor.kinesis_scan_shard",
                                                   callback);
    }

    std::unique_ptr<RoutineLoadTaskExecutor> executor;
    bool old_debug_points;
};

TEST_F(KinesisScanSchedulingTest, LargeJobYieldsBetweenShards) {
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::vector<std::string> order;
    scan_with([&](std::string shard, std::string* sequence, Status*) {
        order.push_back(shard);
        if (shard == "large-0") {
            entered.set_value();
            released.wait();
        }
        *sequence = "100";
    });
    std::promise<Status> large_done;
    std::promise<Status> small_done;
    int large_callbacks = 0;
    int small_callbacks = 0;
    executor->get_kinesis_latest_sequence_numbers(
            request({"large-0", "large-1"}), -1, [] { return false; },
            [&](const Status& st, const auto& positions) {
                ++large_callbacks;
                EXPECT_EQ(2, positions.size());
                large_done.set_value(st);
            });
    auto started = entered.get_future().wait_for(std::chrono::seconds(5));
    executor->get_kinesis_latest_sequence_numbers(
            request({"small"}), -1, [] { return false; },
            [&](const Status& st, const auto& positions) {
                ++small_callbacks;
                EXPECT_EQ(1, positions.size());
                small_done.set_value(st);
            });
    // The queued small job must get its turn before the second shard of the large job.
    release.set_value();
    ASSERT_EQ(std::future_status::ready, started);
    auto large = large_done.get_future();
    auto small = small_done.get_future();
    ASSERT_EQ(std::future_status::ready, large.wait_for(std::chrono::seconds(5)));
    ASSERT_EQ(std::future_status::ready, small.wait_for(std::chrono::seconds(5)));
    // Queue capacity must admit a successor as well as the already queued job.
    executor->_kinesis_scan_pool->wait();
    EXPECT_TRUE(large.get().ok());
    EXPECT_TRUE(small.get().ok());
    EXPECT_EQ((std::vector<std::string> {"large-0", "small", "large-1"}), order);
    EXPECT_EQ(1, large_callbacks);
    EXPECT_EQ(1, small_callbacks);
}

TEST_F(KinesisScanSchedulingTest, DeadlineAfterCompletedScanDoesNotDiscardResults) {
    scan_with([](std::string, std::string* sequence, Status*) { *sequence = "100"; });
    DebugPoints::instance()->add_with_callback(
            "RoutineLoadTaskExecutor.kinesis_scan_before_finish",
            std::function<void(int64_t*)>([](int64_t* deadline) { *deadline = 0; }));
    std::promise<Status> done;
    executor->get_kinesis_latest_sequence_numbers(
            request({"shard"}), -1, [] { return false; },
            [&](const Status& st, const auto& positions) {
                EXPECT_EQ(1, positions.size());
                done.set_value(st);
            });
    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(5)));
    EXPECT_TRUE(result.get().ok());
}

TEST_F(KinesisScanSchedulingTest, FailedShardDiscardsPreviouslyResolvedPositions) {
    std::vector<std::string> order;
    scan_with([&](std::string shard, std::string* sequence, Status* status) {
        order.push_back(shard);
        if (shard == "bad") {
            *status = Status::InternalError("injected scan failure");
        } else {
            *sequence = "100";
        }
    });
    std::promise<Status> done;
    executor->get_kinesis_latest_sequence_numbers(
            request({"good", "bad", "unvisited"}), -1, [] { return false; },
            [&](const Status& st, const auto& positions) {
                EXPECT_TRUE(positions.empty());
                done.set_value(st);
            });
    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(5)));
    EXPECT_FALSE(result.get().ok());
    EXPECT_EQ((std::vector<std::string> {"good", "bad"}), order);
}

TEST_F(KinesisScanSchedulingTest, CancellationAfterScanStillWins) {
    std::atomic<bool> cancelled {false};
    scan_with([&](std::string, std::string* sequence, Status*) {
        *sequence = "100";
        cancelled = true;
    });
    std::promise<Status> done;
    executor->get_kinesis_latest_sequence_numbers(
            request({"shard"}), -1, [&] { return cancelled.load(); },
            [&](const Status& st, const auto& positions) {
                EXPECT_TRUE(positions.empty());
                done.set_value(st);
            });
    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(5)));
    EXPECT_FALSE(result.get().ok());
}

TEST_F(KinesisScanSchedulingTest, RejectedContinuationDiscardsPartialResults) {
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    int scanned = 0;
    scan_with([&](std::string, std::string* sequence, Status*) {
        ++scanned;
        *sequence = "100";
        entered.set_value();
        released.wait();
    });
    int callbacks = 0;
    std::promise<Status> done;
    executor->get_kinesis_latest_sequence_numbers(
            request({"first", "second"}), -1, [] { return false; },
            [&](const Status& st, const auto& positions) {
                ++callbacks;
                EXPECT_TRUE(positions.empty());
                done.set_value(st);
            });
    auto started = entered.get_future().wait_for(std::chrono::seconds(5));
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(executor->_kinesis_scan_pool->submit_func([] {}).ok());
    }
    release.set_value();
    ASSERT_EQ(std::future_status::ready, started);
    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(5)));
    EXPECT_FALSE(result.get().ok());
    executor->_kinesis_scan_pool->wait();
    EXPECT_EQ(1, scanned);
    EXPECT_EQ(1, callbacks);
}

TEST_F(KinesisScanSchedulingTest, ExpiredBatchDoesNotStartScan) {
    scan_with([](std::string, std::string*, Status*) { ADD_FAILURE() << "Expired scan started"; });
    std::promise<Status> done;
    executor->get_kinesis_latest_sequence_numbers(
            request({"shard"}), 0, [] { return false; },
            [&](const Status& st, const auto& positions) {
                EXPECT_TRUE(positions.empty());
                done.set_value(st);
            });
    auto result = done.get_future();
    ASSERT_EQ(std::future_status::ready, result.wait_for(std::chrono::seconds(5)));
    EXPECT_FALSE(result.get().ok());
}

} // namespace doris
