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

#include <vector>

#include "format_v2/parquet/parquet_file_context.h"

namespace doris::format::parquet {
namespace {

void expect_plan_entry(const ParquetPageCacheReadPlanEntry& entry,
                       const ParquetPageCacheRange& cached_range, int64_t copy_offset_in_cache,
                       int64_t output_offset, int64_t copy_size) {
    EXPECT_EQ(entry.cached_range.offset, cached_range.offset);
    EXPECT_EQ(entry.cached_range.size, cached_range.size);
    EXPECT_EQ(entry.copy_offset_in_cache, copy_offset_in_cache);
    EXPECT_EQ(entry.output_offset, output_offset);
    EXPECT_EQ(entry.copy_size, copy_size);
}

TEST(ParquetPageCacheRangeTest, SubsetRequestHitsSingleCachedRange) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {100, 100},
    };

    // Request [120, 150) is fully inside cached [100, 200). The reader should lookup
    // the exact cached key [100, 200), then copy from cached offset 20 into output offset 0.
    auto plan = detail::plan_page_cache_range_read(120, 30, cached_ranges);

    ASSERT_EQ(plan.size(), 1);
    expect_plan_entry(plan[0], {100, 100}, 20, 0, 30);
}

TEST(ParquetPageCacheRangeTest, SupersetRequestHitsMultipleAdjacentCachedRanges) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {180, 80},
            {100, 80},
    };

    // Request [100, 260) is larger than either cached entry, but the two cached ranges
    // exactly cover it. The copy plan stitches the two exact cache entries together.
    auto plan = detail::plan_page_cache_range_read(100, 160, cached_ranges);

    ASSERT_EQ(plan.size(), 2);
    expect_plan_entry(plan[0], {100, 80}, 0, 0, 80);
    expect_plan_entry(plan[1], {180, 80}, 0, 80, 80);
}

TEST(ParquetPageCacheRangeTest, SupersetRequestCanUseOverlappingCachedRanges) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {150, 110},
            {100, 100},
    };

    // Request [100, 260) is covered by overlapping cached ranges. The first copy uses
    // [100, 200); the second resumes at cursor 200 and copies the tail from [150, 260).
    auto plan = detail::plan_page_cache_range_read(100, 160, cached_ranges);

    ASSERT_EQ(plan.size(), 2);
    expect_plan_entry(plan[0], {100, 100}, 0, 0, 100);
    expect_plan_entry(plan[1], {150, 110}, 50, 100, 60);
}

TEST(ParquetPageCacheRangeTest, PartialOverlapWithoutFullCoverageMisses) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {100, 80},
            {200, 60},
    };

    // Cached ranges cover [100, 180) and [200, 260), but [180, 200) is missing.
    // The caller must read the whole request from the file instead of returning
    // a partially cached result.
    auto plan = detail::plan_page_cache_range_read(100, 160, cached_ranges);

    EXPECT_TRUE(plan.empty());
}

TEST(ParquetPageCacheRangeTest, NonCoveringAndInvalidRangesAreIgnored) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {50, 20}, {100, 0}, {100, -1}, {180, 20}, {120, 30},
    };

    // Only [120, 150) intersects the request, but it does not cover the request start
    // [100, 120), so this is still a miss.
    auto plan = detail::plan_page_cache_range_read(100, 50, cached_ranges);

    EXPECT_TRUE(plan.empty());
}

TEST(ParquetPageCacheRangeTest, InvalidRequestMisses) {
    const std::vector<ParquetPageCacheRange> cached_ranges = {
            {100, 100},
    };

    EXPECT_TRUE(detail::plan_page_cache_range_read(-1, 10, cached_ranges).empty());
    EXPECT_TRUE(detail::plan_page_cache_range_read(100, 0, cached_ranges).empty());
    EXPECT_TRUE(detail::plan_page_cache_range_read(100, -1, cached_ranges).empty());
}

} // namespace
} // namespace doris::format::parquet
