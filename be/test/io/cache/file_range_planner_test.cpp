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

#include "io/cache/file_range_planner.h"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

namespace doris::io {
namespace {

FileRange file_range(size_t offset, size_t size) {
    return {.offset = offset, .size = size};
}

FileRangePlanOptions options(size_t max_gap_bytes, size_t max_range_bytes,
                             double max_read_amplification_ratio, size_t cache_block_size,
                             double block_fill_min_coverage) {
    return {.coalesce_options = {.max_gap_bytes = max_gap_bytes,
                                 .max_range_bytes = max_range_bytes,
                                 .max_read_amplification_ratio = max_read_amplification_ratio},
            .cache_block_size = cache_block_size,
            .block_fill_min_coverage = block_fill_min_coverage};
}

FileRangePlan plan(const std::vector<FileRange>& ranges, size_t file_size,
                   const FileRangePlanOptions& plan_options) {
    FileRangePlan result;
    EXPECT_TRUE(FileRangePlanner::plan(ranges, file_size, plan_options, &result).ok());
    return result;
}

TEST(FileRangePlannerTest, EmptyInputClearsPlan) {
    FileRangePlan result {.ranges = {file_range(0, 1)}, .input_locations = {{.range_index = 0}}};
    ASSERT_TRUE(FileRangePlanner::plan({}, 0, options(0, 1, 1.0, 1, 1.0), &result).ok());
    EXPECT_EQ(result, FileRangePlan {});
}

TEST(FileRangePlannerTest, CoalescesAndMapsSortedInputs) {
    const auto result = plan({file_range(0, 100), file_range(120, 40), file_range(200, 100)}, 512,
                             options(20, 256, 2.0, 512, 1.0));

    ASSERT_EQ(result.ranges.size(), 2);
    EXPECT_EQ(result.ranges[0], file_range(0, 160));
    EXPECT_EQ(result.ranges[1], file_range(200, 100));
    EXPECT_EQ(result.input_locations,
              (std::vector<FileRangeLocation> {{.range_index = 0, .buffer_offset = 0},
                                               {.range_index = 0, .buffer_offset = 120},
                                               {.range_index = 1, .buffer_offset = 0}}));
}

TEST(FileRangePlannerTest, CompletesBlockAtCoverageBoundary) {
    const auto result = plan({file_range(0, 512)}, 1024, options(64, 2048, 2.0, 1024, 0.5));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(0, 1024));
}

TEST(FileRangePlannerTest, LeavesBlockPartialBelowCoverageBoundary) {
    const auto result = plan({file_range(0, 511)}, 1024, options(64, 2048, 2.0, 1024, 0.5));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(0, 511));
}

TEST(FileRangePlannerTest, CompletesBlockAfterCoalescingInputGap) {
    const auto result = plan({file_range(0, 300), file_range(350, 250)}, 1024,
                             options(64, 2048, 2.0, 1024, 0.5));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(0, 1024));
}

TEST(FileRangePlannerTest, CompletedBlockConnectsSeparateBaseRanges) {
    const auto result = plan({file_range(0, 400), file_range(500, 200)}, 1024,
                             options(64, 1024, 2.0, 1024, 0.5));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(0, 1024));
    EXPECT_EQ(result.input_locations[0].range_index, 0);
    EXPECT_EQ(result.input_locations[1].range_index, 0);
}

TEST(FileRangePlannerTest, UsesValidEofBlockSizeForCoverage) {
    const auto result = plan({file_range(1100, 140)}, 1280, options(64, 2048, 2.0, 1024, 0.5));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(1024, 256));
    EXPECT_EQ(result.input_locations[0],
              (FileRangeLocation {.range_index = 0, .buffer_offset = 76}));
}

TEST(FileRangePlannerTest, ChoosesSmallerBlockFillWhenBothEdgesDoNotFit) {
    const auto result = plan({file_range(70, 170)}, 300, options(0, 250, 1.0, 100, 0.3));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(70, 230));
}

TEST(FileRangePlannerTest, RejectsBlockFillThatWouldExceedFinalRangeLimit) {
    const auto result = plan({file_range(0, 700), file_range(900, 400)}, 2000,
                             options(64, 1200, 2.0, 1000, 0.5));

    ASSERT_EQ(result.ranges.size(), 2);
    EXPECT_EQ(result.ranges[0], file_range(0, 700));
    EXPECT_EQ(result.ranges[1], file_range(900, 400));
}

TEST(FileRangePlannerTest, KeepsInputLargerThanCoalesceLimit) {
    const auto result =
            plan({file_range(0, 300), file_range(320, 32)}, 512, options(64, 256, 2.0, 512, 1.0));

    ASSERT_EQ(result.ranges.size(), 2);
    EXPECT_EQ(result.ranges[0], file_range(0, 300));
    EXPECT_EQ(result.ranges[1], file_range(320, 32));
    EXPECT_EQ(result.input_locations[0],
              (FileRangeLocation {.range_index = 0, .buffer_offset = 0}));
    EXPECT_EQ(result.input_locations[1],
              (FileRangeLocation {.range_index = 1, .buffer_offset = 0}));
}

TEST(FileRangePlannerTest, OversizedInputDoesNotPreventOtherBlockFill) {
    const auto result =
            plan({file_range(0, 300), file_range(400, 80)}, 512, options(64, 256, 2.0, 100, 0.8));

    ASSERT_EQ(result.ranges.size(), 2);
    EXPECT_EQ(result.ranges[0], file_range(0, 300));
    EXPECT_EQ(result.ranges[1], file_range(400, 100));
}

TEST(FileRangePlannerTest, KeepsNaturallyCompleteBlock) {
    const auto result = plan({file_range(1024, 1024)}, 3072, options(64, 2048, 2.0, 1024, 1.0));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(1024, 1024));
}

TEST(FileRangePlannerTest, AlignsCompleteBlocksWithoutOverflow) {
    const size_t file_size = std::numeric_limits<size_t>::max();
    const auto result = plan({file_range(file_size - 1, 1)}, file_size, options(0, 1, 1.0, 4, 1.0));

    ASSERT_EQ(result.ranges.size(), 1);
    EXPECT_EQ(result.ranges[0], file_range(file_size - 1, 1));
}

TEST(FileRangePlannerTest, PreservesOutputOnInvalidOptionsOrInput) {
    const FileRangePlan original {.ranges = {file_range(7, 9)},
                                  .input_locations = {{.range_index = 0}}};
    auto result = original;

    EXPECT_FALSE(FileRangePlanner::plan({}, 0, options(0, 1, 1.0, 0, 0.5), &result).ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(FileRangePlanner::plan({}, 0, options(0, 1, 1.0, 1, 0.0), &result).ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(
            FileRangePlanner::plan(
                    {}, 0, options(0, 1, 1.0, 1, std::numeric_limits<double>::quiet_NaN()), &result)
                    .ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(FileRangePlanner::plan({}, 0, options(0, 0, 1.0, 1, 0.5), &result).ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(
            FileRangePlanner::plan({file_range(5, 0)}, 10, options(0, 10, 1.0, 10, 0.5), &result)
                    .ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(
            FileRangePlanner::plan({file_range(9, 2)}, 10, options(0, 10, 1.0, 10, 0.5), &result)
                    .ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(FileRangePlanner::plan({file_range(0, 80), file_range(40, 80)}, 120,
                                        options(0, 100, 1.0, 100, 1.0), &result)
                         .ok());
    EXPECT_EQ(result, original);

    EXPECT_FALSE(FileRangePlanner::plan({file_range(std::numeric_limits<size_t>::max() - 1, 2)},
                                        std::numeric_limits<size_t>::max(),
                                        options(0, 2, 1.0, 1, 1.0), &result)
                         .ok());
    EXPECT_EQ(result, original);
}

} // namespace
} // namespace doris::io
