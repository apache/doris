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

#include "vec/exec/executor/time_sharing/split_concurrency_controller.h"

#include <gtest/gtest.h>

#include <chrono>

namespace doris::vectorized {

using namespace std::chrono_literals;

class SplitConcurrencyControllerTest : public testing::Test {};

TEST_F(SplitConcurrencyControllerTest, test_ramp_up) {
    SplitConcurrencyController controller(1, std::chrono::seconds(1));

    for (int i = 0; i < 10; ++i) {
        controller.update(2'000'000'000, 0.0, i + 1);
        EXPECT_EQ(controller.target_concurrency(), i + 2) << "Rampup failed at iteration " << i;
    }
}

TEST_F(SplitConcurrencyControllerTest, test_ramp_down) {
    SplitConcurrencyController controller(10, std::chrono::seconds(1));

    for (int i = 0; i < 9; ++i) {
        controller.update(2'000'000'000, 1.0, 10 - i);
        controller.split_finished(30'000'000'000, 1.0, 10 - i);
        EXPECT_EQ(controller.target_concurrency(), 10 - i - 1)
                << "Rampdown failed at iteration " << i;
    }
}

TEST_F(SplitConcurrencyControllerTest, test_rapid_adjust_for_quick_splits) {
    SplitConcurrencyController controller(10, std::chrono::seconds(1));

    for (int i = 0; i < 9; ++i) {
        controller.update(200'000'000, 1.0, 10 - i);
        controller.split_finished(100'000'000, 1.0, 10 - i);
        EXPECT_EQ(controller.target_concurrency(), 10 - i - 1)
                << "Rapid decrease failed at iteration " << i;
    }

    controller.update(30'000'000'000, 0.0, 1);
    for (int i = 0; i < 10; ++i) {
        controller.update(200'000'000, 0.0, i + 1);
        controller.split_finished(100'000'000, 0.0, i + 1);
        EXPECT_EQ(controller.target_concurrency(), i + 2)
                << "Rapid increase failed at iteration " << i;
    }
}

} // namespace doris::vectorized
