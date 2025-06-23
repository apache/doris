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

#include "olap/compaction_permit_limiter.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "common/config.h"

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

} // namespace doris