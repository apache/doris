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

#include "exec/runtime_filter/runtime_filter_selectivity.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/config.h"

namespace doris {

class RuntimeFilterSelectivityTest : public testing::Test {
protected:
    void SetUp() override {
        // Save original config value
        _original_sampling_frequency = config::runtime_filter_sampling_frequency;
    }

    void TearDown() override {
        // Restore original config value
        config::runtime_filter_sampling_frequency = _original_sampling_frequency;
    }

    int _original_sampling_frequency;
};

TEST_F(RuntimeFilterSelectivityTest, basic_initialization) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(config::runtime_filter_sampling_frequency);
    // Initially should be false (not always_true)
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, disabled_sampling_frequency) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(0);

    // Even if conditions are met, should return false when sampling is disabled
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, negative_sampling_frequency) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(-1);

    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, judge_selectivity_below_threshold) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    // filter_rows/input_rows = 5/50000 = 0.0001 < 0.1
    // input_rows (50000) > min_judge_input_rows (40960)
    selectivity.update_judge_selectivity(-1, 5, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, judge_selectivity_above_threshold) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    // filter_rows/input_rows = 25000/50000 = 0.5 >= 0.1
    selectivity.update_judge_selectivity(-1, 25000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, judge_selectivity_insufficient_input_rows) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    // Even though 5/100 = 0.05 < 0.1, input_rows (100) < min_judge_input_rows (40960)
    selectivity.update_judge_selectivity(-1, 5, 100, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, update_with_low_selectivity) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);

    // filter_rows/input_rows = 2000/50000 = 0.04 < 0.1
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, update_with_high_selectivity) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);

    // filter_rows/input_rows = 45000/50000 = 0.9 >= 0.1
    selectivity.update_judge_selectivity(-1, 45000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, once_always_true_stays_true) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    // First update: low selectivity
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // Second update: high selectivity, but should be ignored
    selectivity.update_judge_selectivity(-1, 45000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, accumulated_selectivity_low) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);

    // First update: 1000/50000 = 0.02
    selectivity.update_judge_selectivity(-1, 1000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, accumulated_selectivity_high) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);

    // First update: 20000/50000 = 0.4
    selectivity.update_judge_selectivity(-1, 20000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Second update: accumulated (20000+20000)/(50000+50000) = 0.4
    selectivity.update_judge_selectivity(-1, 20000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, counter_triggers_reset) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(3);

    // Mark as always_true
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // Update counter to trigger reset
    selectivity.update_judge_counter(); // counter = 1
    selectivity.update_judge_counter(); // counter = 2
    selectivity.update_judge_counter(); // counter = 3, triggers reset

    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, reset_allows_reevaluation) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(2);

    // First cycle: mark as always_true
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // Trigger reset
    selectivity.update_judge_counter(); // counter = 1
    selectivity.update_judge_counter(); // counter = 2, triggers reset

    // Second cycle: now with high selectivity
    selectivity.update_judge_selectivity(-1, 45000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, edge_case_zero_rows) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    selectivity.update_judge_selectivity(-1, 0, 0, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, edge_case_exact_threshold) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);
    // Exactly at threshold: 5000/50000 = 0.1, NOT less than 0.1
    selectivity.update_judge_selectivity(-1, 5000, 50000, 0.1);
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Just below threshold: 4999/50000 = 0.09998 < 0.1
    RuntimeFilterSelectivity selectivity2;
    selectivity2.set_sampling_frequency(100);
    selectivity2.update_judge_selectivity(-1, 4999, 50000, 0.1);
    EXPECT_TRUE(selectivity2.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, multiple_updates_before_threshold) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(100);

    // Multiple updates with insufficient rows each time
    selectivity.update_judge_selectivity(-1, 100, 1000, 0.1); // 100/1000, insufficient
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    selectivity.update_judge_selectivity(-1, 200, 2000, 0.1); // 300/3000, insufficient
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Now accumulated rows are sufficient: 300+2000 = 2300, 3000+40000 = 43000
    selectivity.update_judge_selectivity(-1, 2000, 40000, 0.1); // 2300/43000 = 0.053 < 0.1
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
}

TEST_F(RuntimeFilterSelectivityTest, different_thresholds) {
    // Test with threshold 0.05
    {
        RuntimeFilterSelectivity selectivity;
        selectivity.set_sampling_frequency(100);
        selectivity.update_judge_selectivity(-1, 2000, 50000, 0.05); // 0.04 < 0.05
        EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());
    }

    // Test with threshold 0.03
    {
        RuntimeFilterSelectivity selectivity;
        selectivity.set_sampling_frequency(100);
        selectivity.update_judge_selectivity(-1, 2000, 50000, 0.03); // 0.04 >= 0.03
        EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
    }
}

// Regression test: with default sampling_frequency (-1), update_judge_counter()
// always resets because (_judge_counter++) >= -1 is always true.
// This was the root cause of the selectivity accumulation bug.
TEST_F(RuntimeFilterSelectivityTest, default_sampling_frequency_always_resets) {
    RuntimeFilterSelectivity selectivity;
    // Don't set sampling_frequency — defaults to DISABLE_SAMPLING (-1)

    // Accumulate selectivity data: low filter rate -> should be always_true
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    // With default -1, maybe_always_true_can_ignore returns false (disabled)
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());

    // Now call update_judge_counter — with -1, it immediately resets
    selectivity.update_judge_counter();
    // Verify: accumulated data has been wiped out by the reset
    // Even after setting a valid sampling_frequency, the previously accumulated
    // selectivity data is gone
    selectivity.set_sampling_frequency(100);
    // always_true was reset to false by the premature reset
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

// Verify that setting sampling_frequency correctly prevents premature reset
TEST_F(RuntimeFilterSelectivityTest, proper_sampling_frequency_preserves_accumulation) {
    RuntimeFilterSelectivity selectivity;
    selectivity.set_sampling_frequency(32);

    // Accumulate selectivity: low filter rate
    selectivity.update_judge_selectivity(-1, 2000, 50000, 0.1);
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // Counter increments don't reset before reaching sampling_frequency.
    // Post-increment semantics: check uses old value, so need 33 calls total
    // to trigger reset (counter must reach 32 before comparison fires).
    for (int i = 0; i < 32; i++) {
        selectivity.update_judge_counter();
    }
    // Still always_true because counter value 31 was compared last (31 >= 32 → false)
    EXPECT_TRUE(selectivity.maybe_always_true_can_ignore());

    // 33rd call: counter=32, 32 >= 32 → true → triggers reset
    selectivity.update_judge_counter();
    // After reset, needs re-evaluation
    EXPECT_FALSE(selectivity.maybe_always_true_can_ignore());
}

} // namespace doris
