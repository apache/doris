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

#include "storage/segment/file_cache_writeback_coordinator.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/config.h"

namespace doris::segment_v2 {
namespace {

constexpr size_t BLOCK_SIZE = 100;

PageCandidate page(uint32_t index, uint64_t offset, uint32_t size) {
    return PageCandidate {
            .page_index = index,
            .first_ordinal = index * 10,
            .last_ordinal = index * 10 + 9,
            .offset = offset,
            .size = size,
    };
}

PagePrefetchOptions options() {
    return PagePrefetchOptions {
            .window_pages = 4,
            .min_window_pages = 1,
            .max_window_pages = 8,
            .max_gap_bytes = 150,
            .max_range_bytes = 500,
            .max_pages_per_range = 8,
            .max_read_amplification_ratio = 5.0,
            .writeback_min_block_coverage = 0.5,
            .adaptive_window = false,
    };
}

class FileCacheWritebackCoordinatorTest : public testing::Test {
protected:
    void SetUp() override {
        _old_block_size = config::file_cache_each_block_size;
        config::file_cache_each_block_size = BLOCK_SIZE;
    }

    void TearDown() override { config::file_cache_each_block_size = _old_block_size; }

private:
    int64_t _old_block_size = 0;
};

TEST_F(FileCacheWritebackCoordinatorTest, DensePagesProduceOneCompleteBlockSlice) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    const std::vector<PageCandidate> pages {page(0, 10, 40), page(1, 60, 30)};

    ASSERT_TRUE(coordinator.plan_block_completion(pages, 200, options(), &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    const auto& range = plan.ranges[0];
    EXPECT_EQ(range.offset, 0);
    EXPECT_EQ(range.size, 100);
    EXPECT_EQ(range.requested_page_bytes, 70);
    EXPECT_EQ(range.block_fill_bytes, 30);
    EXPECT_EQ(range.coalesced_gap_bytes, 0);
    ASSERT_EQ(range.complete_blocks.size(), 1);
    EXPECT_EQ(range.complete_blocks[0].block_offset, 0);
    EXPECT_EQ(range.complete_blocks[0].valid_size, 100);
    EXPECT_EQ(range.complete_blocks[0].buffer_offset, 0);
    EXPECT_EQ(range.complete_blocks[0].source_page_indexes, (std::vector<uint32_t> {0, 1}));
    EXPECT_EQ(plan.requested_page_bytes, 70);
    EXPECT_EQ(plan.fetched_bytes, 100);
}

TEST_F(FileCacheWritebackCoordinatorTest, SparsePageKeepsExactPageOnlyPlan) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    auto planner_options = options();
    const std::vector<PageCandidate> pages {page(0, 10, 20)};

    ASSERT_TRUE(coordinator.plan_block_completion(pages, 200, planner_options, &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    EXPECT_EQ(plan.ranges[0].offset, 10);
    EXPECT_EQ(plan.ranges[0].size, 20);
    EXPECT_TRUE(plan.ranges[0].complete_blocks.empty());
    EXPECT_EQ(plan.ranges[0].block_fill_bytes, 0);
}

TEST_F(FileCacheWritebackCoordinatorTest, PageCrossingBlocksKeepsEachCompleteBlockIndivisible) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    auto planner_options = options();
    planner_options.writeback_min_block_coverage = 0.2;
    const std::vector<PageCandidate> pages {page(0, 80, 40)};

    ASSERT_TRUE(coordinator.plan_block_completion(pages, 250, planner_options, &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    EXPECT_EQ(plan.ranges[0].offset, 0);
    EXPECT_EQ(plan.ranges[0].size, 200);
    EXPECT_EQ(plan.ranges[0].requested_page_bytes, 40);
    EXPECT_EQ(plan.ranges[0].block_fill_bytes, 160);
    EXPECT_EQ(plan.ranges[0].coalesced_gap_bytes, 0);
    ASSERT_EQ(plan.ranges[0].complete_blocks.size(), 2);
    EXPECT_EQ(plan.ranges[0].complete_blocks[0].buffer_offset, 0);
    EXPECT_EQ(plan.ranges[0].complete_blocks[1].buffer_offset, 100);
}

TEST_F(FileCacheWritebackCoordinatorTest, ShortEofBlockUsesPhysicalValidSizeForCoverage) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    auto planner_options = options();
    planner_options.writeback_min_block_coverage = 0.4;
    const std::vector<PageCandidate> pages {page(0, 210, 20)};

    ASSERT_TRUE(coordinator.plan_block_completion(pages, 250, planner_options, &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    ASSERT_EQ(plan.ranges[0].complete_blocks.size(), 1);
    EXPECT_EQ(plan.ranges[0].offset, 200);
    EXPECT_EQ(plan.ranges[0].size, 50);
    EXPECT_EQ(plan.ranges[0].complete_blocks[0].block_offset, 200);
    EXPECT_EQ(plan.ranges[0].complete_blocks[0].valid_size, 50);
    EXPECT_EQ(plan.ranges[0].block_fill_bytes, 30);
}

TEST_F(FileCacheWritebackCoordinatorTest, AmplificationAndRangeLimitsRevokeOnlyBlockHoles) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    auto planner_options = options();
    planner_options.writeback_min_block_coverage = 0.4;
    planner_options.max_read_amplification_ratio = 2.0;
    const std::vector<PageCandidate> dense_page {page(0, 30, 40)};
    ASSERT_TRUE(coordinator.plan_block_completion(dense_page, 200, planner_options, &plan).ok());
    EXPECT_TRUE(plan.ranges[0].complete_blocks.empty());
    EXPECT_EQ(plan.ranges[0].offset, 30);
    EXPECT_EQ(plan.ranges[0].size, 40);

    planner_options = options();
    planner_options.writeback_min_block_coverage = 0.2;
    planner_options.max_range_bytes = 100;
    const std::vector<PageCandidate> crossing_page {page(0, 80, 40)};
    ASSERT_TRUE(coordinator.plan_block_completion(crossing_page, 250, planner_options, &plan).ok());
    EXPECT_TRUE(plan.ranges[0].complete_blocks.empty());
    EXPECT_EQ(plan.ranges[0].offset, 80);
    EXPECT_EQ(plan.ranges[0].size, 40);
}

TEST_F(FileCacheWritebackCoordinatorTest, SourceBreakdownUsesDisjointByteClasses) {
    FileCacheWritebackCoordinator coordinator;
    PageFetchPlan plan;
    auto planner_options = options();
    planner_options.writeback_min_block_coverage = 0.5;
    const std::vector<PageCandidate> pages {page(0, 10, 50), page(1, 210, 50)};

    ASSERT_TRUE(coordinator.plan_block_completion(pages, 400, planner_options, &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    const auto& range = plan.ranges[0];
    EXPECT_EQ(range.offset, 0);
    EXPECT_EQ(range.size, 300);
    EXPECT_EQ(range.requested_page_bytes, 100);
    EXPECT_EQ(range.block_fill_bytes, 100);
    EXPECT_EQ(range.coalesced_gap_bytes, 100);
    EXPECT_EQ(range.size,
              range.requested_page_bytes + range.block_fill_bytes + range.coalesced_gap_bytes);
    ASSERT_EQ(range.complete_blocks.size(), 2);
    EXPECT_EQ(range.complete_blocks[0].block_offset, 0);
    EXPECT_EQ(range.complete_blocks[1].block_offset, 200);
}

} // namespace
} // namespace doris::segment_v2
