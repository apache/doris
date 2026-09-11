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

#include "storage/segment/column_read_ahead.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/cast_set.h"

namespace doris::segment_v2 {
namespace {

std::vector<ColumnReadAheadPage> make_pages(const std::vector<size_t>& sizes,
                                            rowid_t rows_per_page = 100) {
    std::vector<ColumnReadAheadPage> pages;
    size_t offset = 0;
    for (size_t index = 0; index < sizes.size(); ++index) {
        pages.push_back({.page_index = cast_set<int32_t>(index),
                         .first_ordinal = cast_set<ordinal_t>(index * rows_per_page),
                         .last_ordinal = cast_set<ordinal_t>((index + 1) * rows_per_page - 1),
                         .range = {.offset = offset, .size = sizes[index]}});
        offset += sizes[index];
    }
    return pages;
}

roaring::Roaring all_rows(rowid_t count) {
    roaring::Roaring rows;
    rows.addRange(0, count);
    return rows;
}

std::unique_ptr<ColumnReadAhead> create_window(const std::vector<size_t>& sizes,
                                               ColumnReadAheadOptions options, bool reverse = false,
                                               rowid_t rows_per_page = 100) {
    std::unique_ptr<ColumnReadAhead> window;
    EXPECT_TRUE(ColumnReadAhead::create(make_pages(sizes, rows_per_page), options, reverse, &window)
                        .ok());
    return window;
}

std::vector<int32_t> page_indexes(const std::vector<ColumnReadAheadPage>& pages) {
    std::vector<int32_t> result;
    for (const auto& page : pages) {
        result.push_back(page.page_index);
    }
    return result;
}

TEST(ColumnReadAheadTest, ValidateWindowBytes) {
    EXPECT_FALSE((ColumnReadAheadOptions {.window_bytes = 0}.validate().ok()));
    EXPECT_TRUE((ColumnReadAheadOptions {.window_bytes = 1}.validate().ok()));
    EXPECT_TRUE((ColumnReadAheadOptions {.window_bytes = 100}.validate().ok()));
}

TEST(ColumnReadAheadTest, FirstPlanFillsByCompressedBytes) {
    auto window = create_window({30, 40, 50, 60}, {.window_bytes = 100});
    const auto rows = all_rows(400);
    const rowid_t current[] = {0};
    ColumnReadAheadPlan plan;

    window->plan(current, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(window->pending_bytes(), 120);
}

TEST(ColumnReadAheadTest, ReachingSecondPageAppendsOneWindow) {
    auto window = create_window(std::vector<size_t>(12, 40), {.window_bytes = 120});
    const auto rows = all_rows(1200);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));

    const rowid_t second_batch[] = {100};
    window->plan(second_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4, 5}));
    EXPECT_EQ(window->pending_bytes(), 200);

    const rowid_t third_batch[] = {200};
    window->plan(third_batch, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());

    const rowid_t fourth_batch[] = {400};
    window->plan(fourth_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {6, 7, 8}));
    EXPECT_EQ(window->pending_bytes(), 200);
}

TEST(ColumnReadAheadTest, CurrentBatchCanExceedWindowBytes) {
    auto window = create_window({70, 70, 70}, {.window_bytes = 100});
    const auto rows = all_rows(300);
    const rowid_t current[] = {0, 100, 200};
    ColumnReadAheadPlan plan;

    window->plan(current, 3, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(window->pending_bytes(), 210);
}

TEST(ColumnReadAheadTest, SparsePredictionUsesOnlySelectedRows) {
    auto window = create_window({25, 25, 25, 25, 25}, {.window_bytes = 75});
    roaring::Roaring rows;
    rows.add(0);
    rows.add(200);
    rows.add(400);
    const rowid_t current[] = {0};
    ColumnReadAheadPlan plan;

    window->plan(current, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 2, 4}));
    EXPECT_EQ(window->pending_bytes(), 75);
}

TEST(ColumnReadAheadTest, ConsumedPageIsNotPlannedAgainWithinSamePage) {
    auto window = create_window({30, 30, 30}, {.window_bytes = 30});
    const auto rows = all_rows(300);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1}));
    window->complete(0);

    const rowid_t second_batch[] = {50};
    window->plan(second_batch, 1, rows, &plan);

    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(window->pending_bytes(), 30);
    EXPECT_FALSE(window->pending(0));
}

TEST(ColumnReadAheadTest, DiscardsSkippedPredictionsBehindScan) {
    auto window = create_window({30, 30, 30, 30}, {.window_bytes = 90});
    const auto rows = all_rows(400);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    const rowid_t second_batch[] = {300};
    window->plan(second_batch, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3}));
    EXPECT_EQ(window->pending_bytes(), 30);
}

TEST(ColumnReadAheadTest, FallbackLeavesPageRetiredUntilScanPassesIt) {
    auto window = create_window({30, 30, 30}, {.window_bytes = 60});
    const auto rows = all_rows(300);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    window->complete(0);
    EXPECT_EQ(window->pending_bytes(), 30);

    const rowid_t same_page[] = {50};
    window->plan(same_page, 1, rows, &plan);

    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_FALSE(window->pending(0));

    const rowid_t next_page[] = {100};
    window->plan(next_page, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {2}));
}

TEST(ColumnReadAheadTest, ReverseScanExtendsAndDiscardsInReverseOrder) {
    auto window = create_window({30, 30, 30, 30}, {.window_bytes = 90}, true);
    const auto rows = all_rows(400);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {350};
    window->plan(first_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 2, 1}));

    const rowid_t second_batch[] = {50};
    window->plan(second_batch, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {1, 2, 3}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0}));
    EXPECT_EQ(window->pending_bytes(), 30);
}

TEST(ColumnReadAheadTest, BatchReachesTriggerAndStartsTwoWindows) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90});
    const auto rows = all_rows(1200);
    const rowid_t current[] = {0, 100};
    ColumnReadAheadPlan plan;

    window->plan(current, 2, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2, 3, 4, 5}));
    EXPECT_EQ(window->pending_bytes(), 180);
}

TEST(ColumnReadAheadTest, CompletingFuturePagesDoesNotAdvanceScanPosition) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90});
    const auto rows = all_rows(1200);
    const rowid_t first_batch[] = {0};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    // Page Cache hits and rejected submissions complete predictions before they are accessed.
    for (const auto& page : plan.new_pages) {
        window->complete(page.page_index);
    }
    EXPECT_EQ(window->pending_bytes(), 0);

    const rowid_t same_page[] = {50};
    window->plan(same_page, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());

    const rowid_t trigger_page[] = {100};
    window->plan(trigger_page, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4, 5}));
    EXPECT_EQ(window->pending_bytes(), 90);
}

TEST(ColumnReadAheadTest, SkippingTriggerWithinWindowStillAppends) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90});
    const auto rows = all_rows(1200);
    const rowid_t first_batch[] = {0};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);

    const rowid_t beyond_trigger[] = {200};
    window->plan(beyond_trigger, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {0, 1}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4, 5}));
}

TEST(ColumnReadAheadTest, LargeJumpRestartsAtCurrentPage) {
    auto window = create_window(std::vector<size_t>(24, 30), {.window_bytes = 90});
    const auto rows = all_rows(2400);
    const rowid_t first_batch[] = {0};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);

    const rowid_t distant_batch[] = {1000};
    window->plan(distant_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {10, 11, 12}));

    const rowid_t next_batch[] = {1100};
    window->plan(next_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {13, 14, 15}));
}

TEST(ColumnReadAheadTest, LargeJumpBatchRebuildsTwoWindows) {
    auto window = create_window(std::vector<size_t>(24, 30), {.window_bytes = 90});
    const auto rows = all_rows(2400);
    const rowid_t first_batch[] = {0};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);

    const rowid_t distant_batch[] = {1000, 1100};
    window->plan(distant_batch, 2, rows, &plan);

    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {10, 11, 12, 13, 14, 15}));
}

TEST(ColumnReadAheadTest, SparseWindowTriggersOnSecondSelectedPage) {
    auto window = create_window(std::vector<size_t>(20, 25), {.window_bytes = 75});
    const std::vector<rowid_t> selected {0, 200, 600, 900, 1200, 1500};
    roaring::Roaring rows;
    rows.addMany(selected.size(), selected.data());
    ColumnReadAheadPlan plan;
    window->plan(selected.data(), 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 2, 6}));

    window->plan(selected.data() + 1, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {9, 12, 15}));
    EXPECT_EQ(window->pending_bytes(), 125);
}

TEST(ColumnReadAheadTest, OversizedPageUsesItselfAsTrigger) {
    auto window = create_window({150, 40, 70, 60}, {.window_bytes = 100});
    const auto rows = all_rows(400);
    const rowid_t current[] = {0};
    ColumnReadAheadPlan plan;

    window->plan(current, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(window->pending_bytes(), 260);
}

TEST(ColumnReadAheadTest, SinglePageWindowsKeepOnePageAhead) {
    auto window = create_window({30, 30, 30, 30}, {.window_bytes = 1});
    const auto rows = all_rows(400);
    const rowid_t first_batch[] = {0};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1}));

    const rowid_t second_batch[] = {100};
    window->plan(second_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {2}));

    const rowid_t final_batch[] = {200, 300};
    window->plan(final_batch, 2, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3}));

    const rowid_t same_page[] = {350};
    window->plan(same_page, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
}

TEST(ColumnReadAheadTest, LargeBatchPlansDemandAndOnlyOneAdditionalWindow) {
    auto window = create_window(std::vector<size_t>(30, 30), {.window_bytes = 90});
    const auto rows = all_rows(3000);
    const rowid_t current[] = {0, 100, 200, 300, 400, 500, 600, 700};
    ColumnReadAheadPlan plan;

    window->plan(current, std::size(current), rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages),
              (std::vector<int32_t> {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));
    EXPECT_EQ(window->pending_bytes(), 360);
}

TEST(ColumnReadAheadTest, TailWindowStopsAtLastSelectedPage) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90});
    const auto rows = all_rows(450);
    const rowid_t current[] = {300};
    ColumnReadAheadPlan plan;
    window->plan(current, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4}));

    const rowid_t last_batch[] = {400};
    window->plan(last_batch, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());

    window->complete(4);
    window->plan(last_batch, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(window->pending_bytes(), 0);
}

TEST(ColumnReadAheadTest, ReverseScanTriggersWithinWindow) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90}, true);
    const auto rows = all_rows(1200);
    const rowid_t first_batch[] = {1150};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {11, 10, 9}));

    const rowid_t trigger_page[] = {1050};
    window->plan(trigger_page, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {8, 7, 6}));
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {11}));
    EXPECT_EQ(window->pending_bytes(), 150);
}

TEST(ColumnReadAheadTest, ReverseBatchReachesTriggerInScanOrder) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90}, true);
    const auto rows = all_rows(1200);
    const rowid_t current[] = {1050, 1150};
    ColumnReadAheadPlan plan;

    window->plan(current, 2, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {11, 10, 9, 8, 7, 6}));
}

TEST(ColumnReadAheadTest, ReverseLargeJumpRebuildsTwoWindows) {
    auto window = create_window(std::vector<size_t>(24, 30), {.window_bytes = 90}, true);
    const auto rows = all_rows(2400);
    const rowid_t first_batch[] = {2350};
    ColumnReadAheadPlan plan;
    window->plan(first_batch, 1, rows, &plan);

    const rowid_t distant_batch[] = {950, 1050};
    window->plan(distant_batch, 2, rows, &plan);

    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {21, 22, 23}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {10, 9, 8, 7, 6, 5}));
}

TEST(ColumnReadAheadTest, ReverseSparseWindowTriggersOnSecondSelectedPage) {
    auto window = create_window(std::vector<size_t>(20, 25), {.window_bytes = 75}, true);
    const std::vector<rowid_t> selected {0, 200, 600, 900, 1200, 1500};
    roaring::Roaring rows;
    rows.addMany(selected.size(), selected.data());
    ColumnReadAheadPlan plan;
    window->plan(selected.data() + 5, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {15, 12, 9}));

    window->plan(selected.data() + 4, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {6, 2, 0}));
}

TEST(ColumnReadAheadTest, FutureCursorUsesPagePositionAcrossRowIdRequests) {
    auto window = create_window(std::vector<size_t>(10, 30), {.window_bytes = 60});
    const std::vector<rowid_t> first_rows {0, 200, 400};
    roaring::Roaring first_scan;
    first_scan.addMany(first_rows.size(), first_rows.data());
    ColumnReadAheadPlan plan;
    window->plan(first_rows.data(), 1, first_scan, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 2}));

    const std::vector<rowid_t> remaining_rows {200, 400, 600};
    roaring::Roaring remaining_scan;
    remaining_scan.addMany(remaining_rows.size(), remaining_rows.data());
    window->plan(remaining_rows.data(), 1, remaining_scan, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {4, 6}));
}

} // namespace
} // namespace doris::segment_v2
