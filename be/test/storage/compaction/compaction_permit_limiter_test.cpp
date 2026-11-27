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

#include "storage/compaction/compaction_permit_limiter.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "common/config.h"
#include "common/status.h"
#include "util/defer_op.h"

namespace doris {

class CompactionPermitLimiterTest : public testing::Test {};

TEST(CompactionPermitLimiterTest, UsageCorrectness) {
    CompactionPermitLimiter limiter;

    // Initial usage should be 0
    EXPECT_EQ(0, limiter.usage());

    // Test single request
    limiter.request(10);
    EXPECT_EQ(10, limiter.usage());

    // Test release
    limiter.release(5);
    EXPECT_EQ(5, limiter.usage());

    // Release all
    limiter.release(5);
    EXPECT_EQ(0, limiter.usage());

    // Test multiple concurrent requests
    const int num_threads = 10;
    const int permits_per_thread = 100;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&limiter]() { limiter.request(permits_per_thread); });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(num_threads * permits_per_thread, limiter.usage());

    // Test multiple concurrent releases
    threads.clear();
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&limiter]() { limiter.release(permits_per_thread); });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(0, limiter.usage());
}

TEST(CompactionPermitLimiterTest, BaseCannotConsumeCumulativeReserve) {
    auto old_total = config::total_permits_for_compaction_score;
    Defer restore([&] { config::total_permits_for_compaction_score = old_total; });
    config::total_permits_for_compaction_score = 12000;
    CompactionAdmission admission(3000);
    const auto base = CompactionType::BASE_COMPACTION;
    const auto cumulative = CompactionType::CUMULATIVE_COMPACTION;
    std::unique_ptr<CompactionAdmission::Lease> slow_base, rejected, cumu;

    ASSERT_TRUE(admission.acquire(nullptr, base, 4, 9000, false, slow_base).ok());
    EXPECT_TRUE(admission.acquire(nullptr, base, 4, 1, false, rejected)
                        .is<ErrorCode::TOO_MANY_TASKS>());
    EXPECT_TRUE(
            admission.acquire(nullptr, CompactionType::CHANGELOG_COMPACTION, 4, 1, false, rejected)
                    .is<ErrorCode::TOO_MANY_TASKS>());
    ASSERT_TRUE(admission.acquire(nullptr, cumulative, 4, 3000, false, cumu).ok());
    EXPECT_TRUE(admission.acquire(nullptr, cumulative, 4, 1, false, rejected)
                        .is<ErrorCode::TOO_MANY_TASKS>());
    cumu.reset();
    EXPECT_TRUE(admission.acquire(nullptr, cumulative, 4, 3000, false, cumu).ok());
    slow_base.reset();
    EXPECT_TRUE(admission.acquire(nullptr, base, 4, 9000, false, slow_base).ok());
}

TEST(CompactionPermitLimiterTest, QueuedAndForceTasksRespectReservedDiskSlot) {
    auto old_total = config::total_permits_for_compaction_score;
    Defer restore([&] { config::total_permits_for_compaction_score = old_total; });
    config::total_permits_for_compaction_score = 12000;
    CompactionAdmission admission(3000);
    const auto base = CompactionType::BASE_COMPACTION;
    const auto cumulative = CompactionType::CUMULATIVE_COMPACTION;
    std::unique_ptr<CompactionAdmission::Lease> full, cumu, rejected;

    ASSERT_TRUE(
            admission.acquire(nullptr, CompactionType::FULL_COMPACTION, 2, 1, false, full).ok());
    EXPECT_FALSE(admission.has_disk_slot(nullptr, base, 2));
    EXPECT_TRUE(admission.acquire(nullptr, base, 2, 0, false, rejected)
                        .is<ErrorCode::TOO_MANY_TASKS>());
    ASSERT_TRUE(admission.acquire(nullptr, cumulative, 2, 3000, false, cumu).ok());
    EXPECT_FALSE(admission.has_disk_slot(nullptr, cumulative, 2));
    full.reset();
    EXPECT_TRUE(admission.acquire(nullptr, base, 2, 1, false, rejected).ok());
}

TEST(CompactionPermitLimiterTest, OversizedTaskDrainsOnlyItsOwnLane) {
    auto old_total = config::total_permits_for_compaction_score;
    Defer restore([&] { config::total_permits_for_compaction_score = old_total; });
    config::total_permits_for_compaction_score = 12000;
    CompactionAdmission admission(3000);
    const auto base = CompactionType::BASE_COMPACTION;
    const auto cumulative = CompactionType::CUMULATIVE_COMPACTION;
    std::unique_ptr<CompactionAdmission::Lease> small, large, cumu;

    ASSERT_TRUE(admission.acquire(nullptr, base, 4, 1, false, small).ok());
    ASSERT_TRUE(admission.acquire(nullptr, cumulative, 4, 3000, false, cumu).ok());
    EXPECT_TRUE(admission.acquire(nullptr, base, 4, 15000, false, large)
                        .is<ErrorCode::TOO_MANY_TASKS>());
    small.reset();
    ASSERT_TRUE(admission.acquire(nullptr, base, 4, 15000, false, large).ok());
    EXPECT_TRUE(
            admission.acquire(nullptr, base, 4, 1, false, small).is<ErrorCode::TOO_MANY_TASKS>());
    large.reset();
    EXPECT_TRUE(admission.acquire(nullptr, base, 4, 9000, false, small).ok());
}

TEST(CompactionPermitLimiterTest, ClosingCancelsBlockingAdmission) {
    auto old_total = config::total_permits_for_compaction_score;
    Defer restore([&] { config::total_permits_for_compaction_score = old_total; });
    config::total_permits_for_compaction_score = 2;
    CompactionAdmission admission(1);
    const auto base = CompactionType::BASE_COMPACTION;
    std::unique_ptr<CompactionAdmission::Lease> running;
    ASSERT_TRUE(admission.acquire(nullptr, base, 4, 1, false, running).ok());

    auto waiter = std::async(std::launch::async, [&] {
        std::unique_ptr<CompactionAdmission::Lease> lease;
        return admission.acquire(nullptr, base, 4, 1, true, lease);
    });
    admission.close();
    EXPECT_TRUE(waiter.get().is<ErrorCode::CANCELLED>());
}

} // namespace doris