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

#include "storage/segment/page_prefetcher.h"

#include <gtest/gtest.h>

#include <iterator>
#include <limits>

namespace doris::segment_v2 {
namespace {

PageCandidate page(uint32_t page_index, ordinal_t first_ordinal, ordinal_t last_ordinal,
                   uint64_t offset, uint32_t size = 16) {
    return PageCandidate {
            .page_index = page_index,
            .first_ordinal = first_ordinal,
            .last_ordinal = last_ordinal,
            .offset = offset,
            .size = size,
    };
}

PagePrefetchOptions options() {
    return PagePrefetchOptions {
            .window_pages = 4,
            .min_window_pages = 1,
            .max_window_pages = 8,
            .max_gap_bytes = 16,
            .max_range_bytes = 128,
            .max_pages_per_range = 4,
            .max_read_amplification_ratio = 2.0,
            .writeback_min_block_coverage = 0.5,
            .adaptive_window = false,
    };
}

std::vector<PageCandidate> six_pages() {
    return {
            page(0, 0, 9, 0),    page(1, 10, 19, 32),  page(2, 20, 29, 64),
            page(3, 30, 39, 96), page(4, 40, 49, 128), page(5, 50, 59, 160),
    };
}

TEST(PageReadPlannerTest, CoalescesPagesAndBuildsStableSliceMappings) {
    PageReadPlanner planner;
    PageFetchPlan plan;
    const std::vector<PageCandidate> pages {
            page(2, 20, 29, 100, 16),
            page(3, 30, 39, 120, 24),
            page(4, 40, 49, 152, 16),
    };

    ASSERT_TRUE(planner.plan(pages, 200, options(), &plan).ok());
    ASSERT_EQ(plan.ranges.size(), 1);
    const auto& range = plan.ranges[0];
    EXPECT_EQ(range.offset, 100);
    EXPECT_EQ(range.size, 68);
    EXPECT_EQ(range.requested_page_bytes, 56);
    EXPECT_EQ(range.coalesced_gap_bytes, 12);
    EXPECT_EQ(range.block_fill_bytes, 0);
    ASSERT_EQ(range.pages.size(), 3);
    EXPECT_EQ(range.pages[0].buffer_offset, 0);
    EXPECT_EQ(range.pages[1].buffer_offset, 20);
    EXPECT_EQ(range.pages[2].buffer_offset, 52);
    EXPECT_EQ(plan.page_to_range.at(2), (std::pair<size_t, size_t> {0, 0}));
    EXPECT_EQ(plan.page_to_range.at(3), (std::pair<size_t, size_t> {0, 1}));
    EXPECT_EQ(plan.page_to_range.at(4), (std::pair<size_t, size_t> {0, 2}));
    EXPECT_EQ(plan.candidate_pages, 3);
    EXPECT_EQ(plan.requested_page_bytes, 56);
    EXPECT_EQ(plan.fetched_bytes, 68);
}

TEST(PageReadPlannerTest, EveryCoalesceHardLimitSplitsRanges) {
    PageReadPlanner planner;
    PageFetchPlan plan;
    const std::vector<PageCandidate> pages {
            page(0, 0, 9, 0, 8),
            page(1, 10, 19, 16, 8),
            page(2, 20, 29, 32, 8),
    };

    auto planner_options = options();
    planner_options.max_gap_bytes = 7;
    ASSERT_TRUE(planner.plan(pages, 64, planner_options, &plan).ok());
    EXPECT_EQ(plan.ranges.size(), 3);

    planner_options = options();
    planner_options.max_range_bytes = 24;
    ASSERT_TRUE(planner.plan(pages, 64, planner_options, &plan).ok());
    EXPECT_EQ(plan.ranges.size(), 2);

    planner_options = options();
    planner_options.max_pages_per_range = 2;
    ASSERT_TRUE(planner.plan(pages, 64, planner_options, &plan).ok());
    EXPECT_EQ(plan.ranges.size(), 2);

    planner_options = options();
    planner_options.max_read_amplification_ratio = 1.4;
    ASSERT_TRUE(planner.plan(pages, 64, planner_options, &plan).ok());
    EXPECT_EQ(plan.ranges.size(), 3);
}

TEST(PageReadPlannerTest, RejectsCorruptCandidateMetadata) {
    PageReadPlanner planner;
    PageFetchPlan plan;
    auto planner_options = options();

    EXPECT_TRUE(planner.plan({page(0, 0, 9, 0, 7)}, 64, planner_options, &plan)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(planner.plan({page(0, 0, 9, 60, 8)}, 64, planner_options, &plan)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(planner.plan({page(0, 0, 9, std::numeric_limits<uint64_t>::max() - 3, 8)},
                             std::numeric_limits<uint64_t>::max(), planner_options, &plan)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(planner.plan({page(1, 10, 19, 32), page(0, 0, 9, 0)}, 64, planner_options, &plan)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(planner.plan({page(0, 0, 9, 0, 24), page(1, 10, 19, 16, 16)}, 64, planner_options,
                             &plan)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(planner.plan({page(0, 0, 9, 0), page(1, 9, 19, 16)}, 64, planner_options, &plan)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(PageReadPlannerTest, OversizedPageFallsBackWithoutViolatingRangeLimit) {
    PageReadPlanner planner;
    PageFetchPlan plan;
    auto planner_options = options();
    planner_options.max_range_bytes = 15;
    EXPECT_TRUE(planner.plan({page(0, 0, 9, 0, 16)}, 32, planner_options, &plan)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(FixedPagePrefetchWindowTest, ForwardWindowIncludesRequiredPagesAndFiltersTrackedPages) {
    FixedPagePrefetchWindow window;
    std::vector<PageCandidate> selected;
    const std::unordered_set<uint32_t> tracked {2};

    ASSERT_TRUE(window.select_ordinal_range(six_pages(), 192, 12, 25, true, 4, tracked, &selected)
                        .ok());
    ASSERT_EQ(selected.size(), 3);
    EXPECT_EQ(selected[0].page_index, 1);
    EXPECT_EQ(selected[1].page_index, 3);
    EXPECT_EQ(selected[2].page_index, 4);
}

TEST(FixedPagePrefetchWindowTest, ReverseWindowReturnsCandidatesInFileOrder) {
    FixedPagePrefetchWindow window;
    std::vector<PageCandidate> selected;

    ASSERT_TRUE(
            window.select_ordinal_range(six_pages(), 192, 45, 18, false, 4, {}, &selected).ok());
    ASSERT_EQ(selected.size(), 4);
    EXPECT_EQ(selected[0].page_index, 1);
    EXPECT_EQ(selected[1].page_index, 2);
    EXPECT_EQ(selected[2].page_index, 3);
    EXPECT_EQ(selected[3].page_index, 4);
}

TEST(FixedPagePrefetchWindowTest, SparseRowidsMapExactlyOncePerNewPage) {
    FixedPagePrefetchWindow window;
    std::vector<PageCandidate> selected;
    const rowid_t rowids[] {1, 2, 25, 27, 41, 55};
    const std::unordered_set<uint32_t> tracked {2};

    ASSERT_TRUE(
            window.select_rowids(six_pages(), 192, rowids, std::size(rowids), tracked, &selected)
                    .ok());
    ASSERT_EQ(selected.size(), 3);
    EXPECT_EQ(selected[0].page_index, 0);
    EXPECT_EQ(selected[1].page_index, 4);
    EXPECT_EQ(selected[2].page_index, 5);
}

TEST(FixedPagePrefetchWindowTest, RefillUsesHalfWindowLowWatermark) {
    EXPECT_FALSE(FixedPagePrefetchWindow::needs_refill(3, 4));
    EXPECT_TRUE(FixedPagePrefetchWindow::needs_refill(2, 4));
    EXPECT_FALSE(FixedPagePrefetchWindow::needs_refill(2, 3));
    EXPECT_TRUE(FixedPagePrefetchWindow::needs_refill(1, 3));
    EXPECT_TRUE(FixedPagePrefetchWindow::needs_refill(1, 1));
}

TEST(FixedPagePrefetchWindowTest, RejectsOutOfBoundsOrdinalRequests) {
    FixedPagePrefetchWindow window;
    std::vector<PageCandidate> selected;
    EXPECT_TRUE(window.select_ordinal_range(six_pages(), 192, 55, 6, true, 4, {}, &selected)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(window.select_ordinal_range(six_pages(), 192, 2, 4, false, 4, {}, &selected)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    const rowid_t rowids[] {60};
    EXPECT_TRUE(window.select_rowids(six_pages(), 192, rowids, 1, {}, &selected)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::segment_v2
