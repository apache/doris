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

#include <limits>
#include <vector>

#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_scan.h"
#include "io/fs/buffered_reader.h"

namespace doris::format::parquet {
namespace {

TEST(ParquetPageCacheRangeTest, ValidPrefetchRangesSkipInvalidAndOverflowRanges) {
    const std::vector<ParquetPageCacheRange> ranges = {
            {100, 50},
            {-1, 50},
            {200, 0},
            {300, -1},
            {std::numeric_limits<int64_t>::max() - 10, 20},
            {400, 60},
    };

    const auto valid_ranges = detail::valid_prefetch_ranges(ranges);

    ASSERT_EQ(valid_ranges.size(), 2);
    EXPECT_EQ(valid_ranges[0].offset, 100);
    EXPECT_EQ(valid_ranges[0].size, 50);
    EXPECT_EQ(valid_ranges[1].offset, 400);
    EXPECT_EQ(valid_ranges[1].size, 60);
}

TEST(ParquetPageCacheRangeTest, SerializedIndexesAreBoundedIndividuallyAndWhenCoalesced) {
    constexpr size_t file_size = 1ULL << 30;
    const auto budget = detail::MAX_SERIALIZED_PARQUET_INDEX_BYTES;

    EXPECT_TRUE(detail::is_serialized_index_range_safe(file_size, 0, budget));
    EXPECT_FALSE(detail::is_serialized_index_range_safe(file_size, 0, budget + 1));
    EXPECT_FALSE(detail::is_serialized_index_range_safe(file_size, -1, 1));

    EXPECT_TRUE(detail::is_serialized_index_span_safe(100, 100 + budget));
    // Individually small adjacent indexes must not combine into one unbounded allocation.
    EXPECT_FALSE(detail::is_serialized_index_span_safe(100, 101 + budget));
}

TEST(ParquetPageCacheRangeTest, MergeRangeReaderPlansAllProjectedRanges) {
    const std::vector<ParquetPageCacheRange> ranges = {
            {0, 512 * 1024},
            {512 * 1024, 1024 * 1024},
    };

    // Two sub-2MB column chunks are the intended merge-reader case: Arrow may issue many page-level
    // ReadAt calls inside these chunks, so v2 should route them through MergeRangeFileReader.
    EXPECT_TRUE(detail::should_use_merge_range_reader(ranges, false));

    // Large column chunks still contain page-level reads. Keep the row-group range plan installed
    // so those reads can share bounded buffers instead of falling back to independent cache IO.
    const std::vector<ParquetPageCacheRange> large_ranges = {
            {0, 8 * 1024 * 1024},
            {8 * 1024 * 1024, 8 * 1024 * 1024},
    };
    EXPECT_TRUE(detail::should_use_merge_range_reader(large_ranges, false));
}

TEST(ParquetPageCacheRangeTest, MergeRangeReaderDecisionRejectsEmptyInvalidAndInMemoryInputs) {
    const std::vector<ParquetPageCacheRange> invalid_ranges = {
            {-1, 128},
            {0, 0},
    };
    const std::vector<ParquetPageCacheRange> valid_ranges = {
            {0, 128 * 1024},
    };

    EXPECT_FALSE(detail::should_use_merge_range_reader({}, false));
    EXPECT_FALSE(detail::should_use_merge_range_reader(invalid_ranges, false));
    EXPECT_FALSE(detail::should_use_merge_range_reader(valid_ranges, true));
}

TEST(ParquetPageCacheRangeTest, DeferredMergeRangesRetainPredicateAndLazyColumns) {
    format::FileScanRequest request;
    ASSERT_TRUE(format::FileScanRequestBuilder(&request)
                        .add_predicate_column(format::LocalColumnId(1))
                        .ok());
    ASSERT_TRUE(format::FileScanRequestBuilder(&request)
                        .add_non_predicate_column(format::LocalColumnId(2))
                        .ok());
    ASSERT_TRUE(format::FileScanRequestBuilder(&request)
                        .add_non_predicate_column(format::LocalColumnId(3))
                        .ok());
    request.count_star_placeholder_columns.push_back(format::LocalColumnId(3));

    const auto columns = detail::deferred_merge_range_columns(request);

    ASSERT_EQ(columns.size(), 2);
    EXPECT_EQ(columns[0].column_id(), format::LocalColumnId(1));
    EXPECT_EQ(columns[1].column_id(), format::LocalColumnId(2));
}

TEST(ParquetPageCacheRangeTest, RevisitedPredicateRootNeedsIndependentMergeRangeReaders) {
    format::FileScanRequest request;
    format::FileScanRequestBuilder builder(&request);
    ASSERT_TRUE(builder.add_predicate_column(format::LocalColumnId(1)).ok());
    ASSERT_TRUE(builder.add_deferred_non_predicate_column(
                               format::LocalColumnIndex::top_level(format::LocalColumnId(1)))
                        .ok());

    EXPECT_TRUE(detail::needs_independent_merge_range_readers(request));

    format::FileScanRequest disjoint_request;
    format::FileScanRequestBuilder disjoint_builder(&disjoint_request);
    ASSERT_TRUE(disjoint_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    ASSERT_TRUE(disjoint_builder.add_non_predicate_column(format::LocalColumnId(2)).ok());
    EXPECT_FALSE(detail::needs_independent_merge_range_readers(disjoint_request));
}

} // namespace
} // namespace doris::format::parquet
