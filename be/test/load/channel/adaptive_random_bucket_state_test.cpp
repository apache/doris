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

#include "load/channel/adaptive_random_bucket_state.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris {
namespace {

TEST(AdaptiveRandomBucketStateTest, TracksCurrentTabletPerSenderAndPartition) {
    AdaptiveRandomBucketState state(UniqueId(1, 2));

    state.init_partition(0, 10, std::vector<int64_t> {100, 101}, std::vector<int32_t> {0, 1}, 0);
    state.init_partition(1, 10, std::vector<int64_t> {200, 201}, std::vector<int32_t> {0, 1}, 1);

    EXPECT_EQ(state.current_tablet(0, 10), 100);
    EXPECT_EQ(state.current_tablet(1, 10), 201);
    EXPECT_EQ(state.current_tablet(2, 10), -1);

    state.rotate_by_tablet(0, 10, 100);
    EXPECT_EQ(state.current_tablet(0, 10), 101);
    EXPECT_EQ(state.current_tablet(1, 10), 201);

    state.rotate_by_tablet(1, 10, 100);
    EXPECT_EQ(state.current_tablet(1, 10), 201);

    state.rotate_by_tablet(1, 10, 201);
    EXPECT_EQ(state.current_tablet(0, 10), 101);
    EXPECT_EQ(state.current_tablet(1, 10), 200);
}

TEST(AdaptiveRandomBucketStateTest, IgnoresDuplicateInitForSameSenderPartition) {
    AdaptiveRandomBucketState state(UniqueId(1, 2));

    state.init_partition(0, 10, std::vector<int64_t> {100, 101}, std::vector<int32_t> {0, 1}, 0);
    state.init_partition(0, 10, std::vector<int64_t> {200, 201}, std::vector<int32_t> {0, 1}, 1);

    EXPECT_EQ(state.current_tablet(0, 10), 100);
}

} // namespace
} // namespace doris
