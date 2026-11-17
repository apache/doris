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

#include <limits>
#include <memory>
#include <numeric>
#include <vector>

#include "common/cast_set.h"
#include "io/fs/read_ahead_metrics.h"

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

TEST(ColumnReadAheadTest, PlanResetPreservesCapacityAndChangesOwner) {
    auto first = create_window({30, 40, 50}, {.window_bytes = 100});
    auto second = create_window({60, 70}, {.window_bytes = 100});
    ColumnReadAheadPlan plan;
    plan.column = first.get();
    plan.new_pages = make_pages({30, 40, 50});
    plan.released_pages = make_pages({60, 70});
    plan.window_discard_ns = 11;
    plan.current_batch_plan_ns = 23;
    plan.window_extend_ns = 7;
    const size_t new_capacity = plan.new_pages.capacity();
    const size_t released_capacity = plan.released_pages.capacity();

    plan.reset(second.get());

    EXPECT_EQ(plan.column, second.get());
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(plan.new_pages.capacity(), new_capacity);
    EXPECT_EQ(plan.released_pages.capacity(), released_capacity);
    EXPECT_EQ(plan.window_discard_ns, 0);
    EXPECT_EQ(plan.current_batch_plan_ns, 0);
    EXPECT_EQ(plan.window_extend_ns, 0);
}

TEST(ColumnReadAheadTest, PlanAccumulatesTimingsEvenWhenEmpty) {
    ColumnReadAheadPlan plan;
    plan.window_discard_ns = 11;
    plan.current_batch_plan_ns = 23;
    plan.window_extend_ns = 7;
    ASSERT_TRUE(plan.empty());
    io::ReadAheadStatistics statistics;

    plan.update_statistics(nullptr);
    plan.update_statistics(&statistics);
    EXPECT_EQ(statistics.window_discard_time.value(), 11);
    EXPECT_EQ(statistics.current_batch_plan_time.value(), 23);
    EXPECT_EQ(statistics.window_extend_time.value(), 7);

    auto window = create_window({30}, {.window_bytes = 100});
    plan.reset(window.get());
    plan.update_statistics(&statistics);
    EXPECT_EQ(statistics.window_discard_time.value(), 11);
    EXPECT_EQ(statistics.current_batch_plan_time.value(), 23);
    EXPECT_EQ(statistics.window_extend_time.value(), 7);

    plan.window_discard_ns = 3;
    plan.current_batch_plan_ns = 5;
    plan.window_extend_ns = 2;
    plan.update_statistics(&statistics);
    EXPECT_EQ(statistics.window_discard_time.value(), 14);
    EXPECT_EQ(statistics.current_batch_plan_time.value(), 28);
    EXPECT_EQ(statistics.window_extend_time.value(), 9);
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
    EXPECT_GT(plan.window_extend_ns, 0);
    EXPECT_GE(plan.current_batch_plan_ns, plan.window_extend_ns);
    window->complete(0);

    const rowid_t second_batch[] = {50};
    window->plan(second_batch, 1, rows, &plan);

    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(window->pending_bytes(), 30);
    EXPECT_FALSE(window->pending(0));
    EXPECT_GT(plan.current_batch_plan_ns, 0);
    EXPECT_GT(plan.window_discard_ns, 0);
    // The reused output records this call only, including calls that produce no new pages.
    EXPECT_EQ(plan.window_extend_ns, 0);
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

TEST(ColumnReadAheadTest, PageDrivenScanKeepsPrunedSelectionWithoutRowIds) {
    auto window = create_window(std::vector<size_t>(20, 25), {.window_bytes = 75});
    ColumnReadAheadPlan plan;
    {
        const rowid_t selected[] = {100, 200, 600, 900, 1200, 1500};
        roaring::Roaring rows;
        rows.addMany(std::size(selected), selected);
        window->start(rows, &plan);
    }
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {1, 2, 6}));
    window->complete(1);
    window->advance(1, &plan);
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(plan.current_batch_plan_ns, 0);

    window->advance(2, &plan);
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {1}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {9, 12, 15}));
    EXPECT_EQ(window->pending_bytes(), 125);
    window->advance(15, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(window->pending_bytes(), 25);
    window->complete(15);
    window->advance(15, &plan);
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(window->pending_bytes(), 0);
}

TEST(ColumnReadAheadTest, PageDrivenJumpRestartsAndReachingTriggerExtends) {
    auto window = create_window(std::vector<size_t>(24, 30), {.window_bytes = 90});
    ColumnReadAheadPlan plan;
    window->start(all_rows(2400), &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));

    window->advance(10, &plan);
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {10, 11, 12}));
    window->advance(12, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {13, 14, 15}));
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {10, 11}));
}

TEST(ColumnReadAheadTest, DenseScanPlansEachPageOnceWithBoundedWindows) {
    // 64 KiB uncompressed INT pages contain 16K rows; model 30 KiB compressed pages.
    constexpr size_t page_count = 512;
    constexpr size_t page_bytes = 30 * 1024;
    constexpr size_t window_bytes = 4 * 1024 * 1024;
    constexpr rowid_t rows_per_page = 16 * 1024;
    auto window = create_window(std::vector<size_t>(page_count, page_bytes),
                                {.window_bytes = window_bytes}, false, rows_per_page);
    ColumnReadAheadPlan plan;
    window->start(all_rows(page_count * rows_per_page), &plan);
    size_t planned = plan.new_pages.size();
    EXPECT_EQ(planned, (window_bytes + page_bytes - 1) / page_bytes);
    for (int32_t page = 0; page < page_count; ++page) {
        window->advance(page, &plan);
        planned += plan.new_pages.size();
        EXPECT_TRUE(window->pending(page));
        EXPECT_LE(window->pending_bytes(), 2 * (window_bytes + page_bytes));
        window->complete(page);
    }
    EXPECT_EQ(planned, page_count);
    EXPECT_EQ(window->pending_bytes(), 0);
}

TEST(ColumnReadAheadTest, PageDrivenCacheHitsStillAdvanceFromReadPosition) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90});
    ColumnReadAheadPlan plan;
    window->start(all_rows(1200), &plan);
    for (const auto& page : plan.new_pages) {
        window->complete(page.page_index);
    }
    EXPECT_EQ(window->pending_bytes(), 0);
    window->advance(0, &plan);
    EXPECT_TRUE(plan.empty());
    window->advance(1, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4, 5}));
}

TEST(ColumnReadAheadTest, PageDrivenReverseKeepsAscendingPagesWithinBatch) {
    auto window = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90}, true);
    ColumnReadAheadPlan plan;
    window->start(all_rows(1200), &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {11, 10, 9}));
    // Reverse scan batch [900..1199] is physically read as page 9, 10, 11.
    window->advance(9, &plan);
    EXPECT_TRUE(plan.released_pages.empty());
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {8, 7, 6}));
    window->complete(9);
    window->advance(10, &plan);
    EXPECT_TRUE(plan.empty());
    window->complete(10);
    window->advance(11, &plan);
    EXPECT_TRUE(plan.empty());
    window->complete(11);

    // The next batch may skip an arbitrarily large range.
    window->advance(2, &plan);
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {10, 11}));
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {2, 1, 0}));
    window->advance(0, &plan);
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {6, 7, 8, 9}));
}

TEST(ColumnReadAheadTest, PageDrivenReverseUsesSelectedPagesAndOversizedWindows) {
    auto window = create_window({150, 40, 70, 60, 160}, {.window_bytes = 100}, true);
    const rowid_t selected[] = {0, 200, 400};
    roaring::Roaring rows;
    rows.addMany(std::size(selected), selected);
    ColumnReadAheadPlan plan;
    window->start(rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {4, 2, 0}));
    window->advance(2, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
    window->advance(0, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(page_indexes(plan.released_pages), (std::vector<int32_t> {4}));
}

TEST(ColumnReadAheadTest, CandidateSelectionHandlesMaximumRowId) {
    const rowid_t last_row = std::numeric_limits<rowid_t>::max();
    std::unique_ptr<ColumnReadAhead> window;
    ASSERT_TRUE(ColumnReadAhead::create({{.page_index = 0,
                                          .first_ordinal = 0,
                                          .last_ordinal = last_row,
                                          .range = {.offset = 0, .size = 30}}},
                                        {.window_bytes = 90}, false, &window)
                        .ok());
    roaring::Roaring rows;
    rows.add(last_row);
    ColumnReadAheadPlan plan;
    window->start(rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0}));
    window->advance(0, &plan);
    EXPECT_TRUE(plan.empty());
}

TEST(ColumnReadAheadTest, DenseRowIdBatchProducesTheSamePlanAsDistinctPages) {
    for (bool reverse : {false, true}) {
        auto dense = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90}, reverse);
        auto sparse = create_window(std::vector<size_t>(12, 30), {.window_bytes = 90}, reverse);
        std::vector<rowid_t> rowids(1000);
        std::iota(rowids.begin(), rowids.end(), 0);
        const rowid_t distinct[] = {0, 100, 200, 300, 400, 500, 600, 700, 800, 900};
        ColumnReadAheadPlan dense_plan;
        ColumnReadAheadPlan sparse_plan;
        dense->plan(rowids.data(), rowids.size(), all_rows(1200), &dense_plan);
        sparse->plan(distinct, std::size(distinct), all_rows(1200), &sparse_plan);
        EXPECT_EQ(dense_plan.new_pages, sparse_plan.new_pages);
        EXPECT_EQ(dense->pending_bytes(), sparse->pending_bytes());
    }
}

} // namespace
} // namespace doris::segment_v2
