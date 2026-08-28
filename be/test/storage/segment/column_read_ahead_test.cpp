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

TEST(ColumnReadAheadTest, ValidateWatermarks) {
    EXPECT_FALSE((ColumnReadAheadOptions {.high_watermark_bytes = 0, .low_watermark_bytes = 0}
                          .validate()
                          .ok()));
    EXPECT_FALSE((ColumnReadAheadOptions {.high_watermark_bytes = 100, .low_watermark_bytes = 100}
                          .validate()
                          .ok()));
    EXPECT_FALSE((ColumnReadAheadOptions {.high_watermark_bytes = 100, .low_watermark_bytes = 101}
                          .validate()
                          .ok()));
    EXPECT_TRUE((ColumnReadAheadOptions {.high_watermark_bytes = 100, .low_watermark_bytes = 50}
                         .validate()
                         .ok()));
}

TEST(ColumnReadAheadTest, FirstPlanFillsByCompressedBytes) {
    auto window = create_window({30, 40, 50, 60},
                                {.high_watermark_bytes = 100, .low_watermark_bytes = 50});
    const auto rows = all_rows(400);
    const rowid_t current[] = {0};
    ColumnReadAheadPlan plan;

    window->plan(current, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(window->pending_bytes(), 120);
}

TEST(ColumnReadAheadTest, ReplenishesOnlyAfterLowWatermark) {
    auto window = create_window({40, 40, 40, 40, 40},
                                {.high_watermark_bytes = 120, .low_watermark_bytes = 40});
    const auto rows = all_rows(500);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));

    window->complete(0);
    const rowid_t second_batch[] = {100};
    window->plan(second_batch, 1, rows, &plan);
    EXPECT_TRUE(plan.new_pages.empty());
    EXPECT_EQ(window->pending_bytes(), 80);

    window->complete(1);
    const rowid_t third_batch[] = {200};
    window->plan(third_batch, 1, rows, &plan);
    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {3, 4}));
    EXPECT_EQ(window->pending_bytes(), 120);
}

TEST(ColumnReadAheadTest, CurrentBatchCanExceedHighWatermark) {
    auto window =
            create_window({70, 70, 70}, {.high_watermark_bytes = 100, .low_watermark_bytes = 50});
    const auto rows = all_rows(300);
    const rowid_t current[] = {0, 100, 200};
    ColumnReadAheadPlan plan;

    window->plan(current, 3, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0, 1, 2}));
    EXPECT_EQ(window->pending_bytes(), 210);
}

TEST(ColumnReadAheadTest, SparsePredictionUsesOnlySelectedRows) {
    auto window = create_window({25, 25, 25, 25, 25},
                                {.high_watermark_bytes = 75, .low_watermark_bytes = 25});
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
    auto window =
            create_window({30, 30, 30}, {.high_watermark_bytes = 30, .low_watermark_bytes = 10});
    const auto rows = all_rows(300);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    ASSERT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {0}));
    window->complete(0);

    const rowid_t second_batch[] = {50};
    window->plan(second_batch, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {1}));
    EXPECT_EQ(window->pending_bytes(), 30);
    EXPECT_FALSE(window->pending(0));
}

TEST(ColumnReadAheadTest, DiscardsSkippedPredictionsBehindScan) {
    auto window = create_window({30, 30, 30, 30},
                                {.high_watermark_bytes = 90, .low_watermark_bytes = 30});
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
    auto window = create_window({30, 30}, {.high_watermark_bytes = 30, .low_watermark_bytes = 10});
    const auto rows = all_rows(200);
    ColumnReadAheadPlan plan;
    const rowid_t first_batch[] = {0};
    window->plan(first_batch, 1, rows, &plan);
    window->complete(0);
    EXPECT_EQ(window->pending_bytes(), 0);

    const rowid_t same_page[] = {50};
    window->plan(same_page, 1, rows, &plan);

    EXPECT_EQ(page_indexes(plan.new_pages), (std::vector<int32_t> {1}));
    EXPECT_FALSE(window->pending(0));
}

TEST(ColumnReadAheadTest, ReverseScanExtendsAndDiscardsInReverseOrder) {
    auto window = create_window({30, 30, 30, 30},
                                {.high_watermark_bytes = 90, .low_watermark_bytes = 30}, true);
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

} // namespace
} // namespace doris::segment_v2
