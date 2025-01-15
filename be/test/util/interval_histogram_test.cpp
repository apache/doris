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

#include "util/interval_histogram.h"

#include <gtest/gtest.h>

#include <thread>

namespace doris {

TEST(IntervalHistogramStat, SerialTest) {
    IntervalHistogramStat<int> stat(5);

    stat.add(10);
    stat.add(20);
    stat.add(30);
    stat.add(40);
    stat.add(50);

    EXPECT_EQ(stat.mean(), 30);
    EXPECT_EQ(stat.median(), 30);
    EXPECT_EQ(stat.max(), 50);
    EXPECT_EQ(stat.min(), 10);

    // Make window move forward
    stat.add(60);
    stat.add(70);

    // window now contains [30, 40, 50, 60, 70]
    EXPECT_EQ(stat.mean(), 50);
    EXPECT_EQ(stat.median(), 50);
    EXPECT_EQ(stat.max(), 70);
    EXPECT_EQ(stat.min(), 30);
}

TEST(IntervalHistogramStatTest, ParallelTest) {
    constexpr int thread_count = 10;
    constexpr int values_per_thread = 10;
    IntervalHistogramStat<int> stat(thread_count * values_per_thread);

    auto add_values = [&stat](int start_value, int count) {
        for (int i = 0; i < count; ++i) {
            stat.add(start_value + i);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back(add_values, i * values_per_thread, values_per_thread);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    int total_values = thread_count * values_per_thread;
    EXPECT_EQ(stat.mean(), (total_values - 1) / 2);
    EXPECT_EQ(stat.max(), total_values - 1);
    EXPECT_EQ(stat.min(), 0);
    EXPECT_EQ(stat.median(), (total_values - 1) / 2);
}

} // namespace doris
