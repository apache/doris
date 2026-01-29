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

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace doris::pipeline {
namespace {
constexpr uint32_t kFanout = 8;
constexpr uint32_t kBitsPerLevel = 3;
constexpr uint32_t kMaxSplitDepth = 6;

uint32_t select_level_partition(uint32_t hash, uint32_t level) {
    return (hash >> (level * kBitsPerLevel)) & (kFanout - 1);
}

bool can_split(uint32_t current_depth) {
    return current_depth < kMaxSplitDepth;
}
} // namespace

// Scaffold tests for hierarchical spill partitioning; enable when the feature lands.
TEST(PartitionedHashJoinMultiLevelSpillTest, BitSlicingUsesThreeBitsPerLevel) {
    const uint32_t hash = 0xF2D4C3A1;
    EXPECT_EQ(select_level_partition(hash, 0), 1u);
    EXPECT_EQ(select_level_partition(hash, 1), 4u);
    EXPECT_EQ(select_level_partition(hash, 2), 6u);
    EXPECT_EQ(select_level_partition(hash, 3), 1u);
}

TEST(PartitionedHashJoinMultiLevelSpillTest, SameParentDifferentChild) {
    const uint32_t hash_a = (4u << kBitsPerLevel) | 2u;
    const uint32_t hash_b = (5u << kBitsPerLevel) | 2u;

    EXPECT_EQ(select_level_partition(hash_a, 0), 2u);
    EXPECT_EQ(select_level_partition(hash_b, 0), 2u);
    EXPECT_NE(select_level_partition(hash_a, 1), select_level_partition(hash_b, 1));
}

TEST(PartitionedHashJoinMultiLevelSpillTest, MaxSplitDepth) {
    EXPECT_TRUE(can_split(0));
    EXPECT_TRUE(can_split(kMaxSplitDepth - 1));
    EXPECT_FALSE(can_split(kMaxSplitDepth));
}
} // namespace doris::pipeline
