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

#include "io/cache/hole_fill_planner.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris::io {
namespace {

constexpr size_t KiB(size_t value) {
    return value * 1024;
}

const FileRangeCoalesceOptions kOptions {
        .max_gap_bytes = KiB(32),
        .max_range_bytes = KiB(1024),
        .max_read_amplification_ratio = 2.0,
};

TEST(HoleFillPlannerTest, MergesNearbyHolesWithinAmplificationLimit) {
    const FileRange block {.offset = 0, .size = KiB(1024)};
    const std::vector<FileRange> covered {{.offset = KiB(544), .size = KiB(480)},
                                          {.offset = 0, .size = KiB(256)},
                                          {.offset = KiB(384), .size = KiB(32)}};
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block, covered, kOptions, &ranges).ok());

    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = KiB(256), .size = KiB(288)}}));
}

TEST(HoleFillPlannerTest, KeepsHolesSeparateWhenGapExceedsLimit) {
    const FileRange block {.offset = 0, .size = KiB(1024)};
    const std::vector<FileRange> covered {{.offset = KiB(480), .size = KiB(64)}};
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block, covered, kOptions, &ranges).ok());

    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = 0, .size = KiB(480)},
                                               {.offset = KiB(544), .size = KiB(480)}}));
}

TEST(HoleFillPlannerTest, AppliesAmplificationLimitAfterGapCheck) {
    const FileRange block {.offset = 0, .size = KiB(1024)};
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block,
                                      {{.offset = KiB(8), .size = KiB(16)},
                                       {.offset = KiB(32), .size = KiB(992)}},
                                      kOptions, &ranges)
                        .ok());
    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = 0, .size = KiB(32)}}));

    ASSERT_TRUE(HoleFillPlanner::plan(block,
                                      {{.offset = KiB(4), .size = KiB(16)},
                                       {.offset = KiB(24), .size = KiB(1000)}},
                                      kOptions, &ranges)
                        .ok());
    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = 0, .size = KiB(4)},
                                               {.offset = KiB(20), .size = KiB(4)}}));
}

TEST(HoleFillPlannerTest, UsesOnlyTheCurrentPhysicalEofBlock) {
    const FileRange block {.offset = KiB(1024), .size = KiB(300)};
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block, {{.offset = KiB(1124), .size = KiB(100)}}, kOptions,
                                      &ranges)
                        .ok());

    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = KiB(1024), .size = KiB(100)},
                                               {.offset = KiB(1224), .size = KiB(100)}}));
    for (const auto& range : ranges) {
        EXPECT_GE(range.offset, block.offset);
        EXPECT_LE(range.end(), block.end());
    }
}

TEST(HoleFillPlannerTest, HandlesEmptyAndCompleteCoverage) {
    const FileRange block {.offset = 0, .size = KiB(1024)};
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block, {}, kOptions, &ranges).ok());
    EXPECT_EQ(ranges, (std::vector<FileRange> {block}));

    ASSERT_TRUE(HoleFillPlanner::plan(block, {block}, kOptions, &ranges).ok());
    EXPECT_TRUE(ranges.empty());
}

TEST(HoleFillPlannerTest, KeepsSingleHoleLargerThanCoalesceLimit) {
    const FileRange block {.offset = 0, .size = KiB(1024)};
    const FileRangeCoalesceOptions options {
            .max_gap_bytes = KiB(32),
            .max_range_bytes = KiB(128),
            .max_read_amplification_ratio = 2.0,
    };
    std::vector<FileRange> ranges;

    ASSERT_TRUE(HoleFillPlanner::plan(block, {}, options, &ranges).ok());
    EXPECT_EQ(ranges, (std::vector<FileRange> {block}));
}

TEST(HoleFillPlannerTest, RejectsCoverageOutsideBlockWithoutChangingOutput) {
    const FileRange block {.offset = KiB(1024), .size = KiB(1024)};
    std::vector<FileRange> ranges {{.offset = 7, .size = 11}};

    EXPECT_FALSE(HoleFillPlanner::plan(block, {{.offset = KiB(512), .size = KiB(1024)}}, kOptions,
                                       &ranges)
                         .ok());

    EXPECT_EQ(ranges, (std::vector<FileRange> {{.offset = 7, .size = 11}}));
}

} // namespace
} // namespace doris::io
