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

#include "runtime_filter/runtime_filter_selectivity.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace doris {

class RuntimeFilterSelectivityTest : public testing::Test {};

TEST_F(RuntimeFilterSelectivityTest, basic) {
    RuntimeFilterSelectivity selectivity;
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, judge_selectivity_low_filter_rate) {
    bool always_true = false;
    RuntimeFilterSelectivity::judge_selectivity(0.1, 5, 100, always_true);
    EXPECT_TRUE(always_true);
}

TEST_F(RuntimeFilterSelectivityTest, judge_selectivity_high_filter_rate) {
    bool always_true = false;
    RuntimeFilterSelectivity::judge_selectivity(0.1, 50, 100, always_true);
    EXPECT_FALSE(always_true);
}

TEST_F(RuntimeFilterSelectivityTest, update_judge_selectivity_not_always_true) {
    RuntimeFilterSelectivity selectivity;
    selectivity.update_judge_selectivity(-1, 50, 100, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, update_judge_selectivity_always_true) {
    RuntimeFilterSelectivity selectivity;
    selectivity.update_judge_selectivity(-1, 5, 100, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, update_judge_selectivity_once_true_always_true) {
    RuntimeFilterSelectivity selectivity;
    selectivity.update_judge_selectivity(-1, 5, 100, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // After marked as always_true, subsequent updates should be ignored
    selectivity.update_judge_selectivity(-1, 90, 100, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, update_judge_counter_reset) {
    RuntimeFilterSelectivity selectivity;

    // Set sampling frequency to a small value for testing
    config::runtime_filter_sampling_frequency = 2;

    selectivity.update_judge_selectivity(-1, 5, 100, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // Trigger reset by calling update_judge_counter multiple times
    selectivity.update_judge_counter();
    selectivity.update_judge_counter();
    selectivity.update_judge_counter();

    // After reset, should be able to re-evaluate
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Now should be able to evaluate again
    selectivity.update_judge_selectivity(-1, 90, 100, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, accumulated_selectivity) {
    RuntimeFilterSelectivity selectivity;

    // First update with high filter rate
    selectivity.update_judge_selectivity(-1, 80, 100, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Second update, accumulated should still have high filter rate
    selectivity.update_judge_selectivity(-1, 70, 100, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Accumulated: 150 filtered out of 200 total (75% filter rate)
}

TEST_F(RuntimeFilterSelectivityTest, edge_case_zero_input_rows) {
    bool always_true = false;
    // This will cause division by zero, implementation should handle it
    RuntimeFilterSelectivity::judge_selectivity(0.1, 0, 0, always_true);
    // Depending on implementation, this might be true or false
}

TEST_F(RuntimeFilterSelectivityTest, edge_case_exact_threshold) {
    bool always_true = false;
    RuntimeFilterSelectivity::judge_selectivity(0.1, 10, 100, always_true);
    // 10/100 = 0.1, which is NOT less than 0.1, so should be false
    EXPECT_FALSE(always_true);

    RuntimeFilterSelectivity::judge_selectivity(0.1, 9, 100, always_true);
    // 9/100 = 0.09, which is less than 0.1, so should be true
    EXPECT_TRUE(always_true);
}

} // namespace doris
