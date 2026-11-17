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

#include <string>
#include <vector>

#include "storage/index/index_disk_usage.h"

namespace doris::segment_v2 {

namespace {

IndexDiskUsageRow make_row(const std::string& rowset_id, int32_t segment_id, int64_t index_id,
                           IndexDiskUsageStructure structure, InvertedIndexStorageFormatPB format,
                           int64_t total_bytes, int64_t position_bytes) {
    IndexDiskUsageRow row;
    row.rowset_id = rowset_id;
    row.segment_id = segment_id;
    row.segment_count = 1;
    row.row_count = 100;
    row.format = format;
    row.record.index_id = index_id;
    row.record.structure = structure;
    row.record.total_bytes = total_bytes;
    row.record.dict_bytes = total_bytes / 2;
    row.record.posting_bytes = total_bytes / 2;
    row.record.position_bytes = position_bytes;
    return row;
}

// Two rowsets with two segments each, all holding index 7 in V2.
std::vector<IndexDiskUsageRow> four_segments() {
    return {make_row("r1", 0, 7, IndexDiskUsageStructure::kTerm, InvertedIndexStorageFormatPB::V2,
                     10, 1),
            make_row("r1", 1, 7, IndexDiskUsageStructure::kTerm, InvertedIndexStorageFormatPB::V2,
                     20, 2),
            make_row("r2", 0, 7, IndexDiskUsageStructure::kTerm, InvertedIndexStorageFormatPB::V2,
                     30, 3),
            make_row("r2", 1, 7, IndexDiskUsageStructure::kTerm, InvertedIndexStorageFormatPB::V2,
                     40, 4)};
}

const IndexDiskUsageRow* find_rowset(const std::vector<IndexDiskUsageRow>& rows,
                                     const std::string& rowset_id) {
    for (const auto& row : rows) {
        if (row.rowset_id == rowset_id) {
            return &row;
        }
    }
    return nullptr;
}

} // namespace

TEST(IndexDiskUsageAggregateTest, SegmentLevelKeepsEveryRow) {
    auto rows = aggregate_index_disk_usage(four_segments(), IndexDiskUsageLevel::kSegment);
    ASSERT_EQ(4U, rows.size());
    for (const auto& row : rows) {
        EXPECT_EQ(1, row.segment_count);
        EXPECT_GE(row.segment_id, 0);
    }
}

TEST(IndexDiskUsageAggregateTest, RowsetLevelMergesSegments) {
    auto rows = aggregate_index_disk_usage(four_segments(), IndexDiskUsageLevel::kRowset);
    ASSERT_EQ(2U, rows.size());
    const IndexDiskUsageRow* r1 = find_rowset(rows, "r1");
    ASSERT_NE(r1, nullptr);
    EXPECT_EQ(-1, r1->segment_id);
    EXPECT_EQ(2, r1->segment_count);
    EXPECT_EQ(200, r1->row_count);
    EXPECT_EQ(30, r1->record.total_bytes);
    EXPECT_EQ(3, r1->record.position_bytes);
}

TEST(IndexDiskUsageAggregateTest, TabletLevelMergesRowsets) {
    auto rows = aggregate_index_disk_usage(four_segments(), IndexDiskUsageLevel::kTablet);
    ASSERT_EQ(1U, rows.size());
    EXPECT_EQ("", rows[0].rowset_id);
    EXPECT_EQ(-1, rows[0].segment_id);
    EXPECT_EQ(4, rows[0].segment_count);
    EXPECT_EQ(400, rows[0].row_count);
    EXPECT_EQ(100, rows[0].record.total_bytes);
    EXPECT_EQ(50, rows[0].record.dict_bytes);
    EXPECT_EQ(10, rows[0].record.position_bytes);
}

TEST(IndexDiskUsageAggregateTest, UnknownComponentStaysUnknown) {
    auto input = four_segments();
    input[2].record.position_bytes = -1;
    auto rows = aggregate_index_disk_usage(std::move(input), IndexDiskUsageLevel::kTablet);
    ASSERT_EQ(1U, rows.size());
    EXPECT_EQ(-1, rows[0].record.position_bytes);
    EXPECT_EQ(100, rows[0].record.total_bytes);
}

TEST(IndexDiskUsageAggregateTest, ContainerRowsStaySeparate) {
    auto input = four_segments();
    input.push_back(make_row("r1", 0, -1, IndexDiskUsageStructure::kContainer,
                             InvertedIndexStorageFormatPB::V2, 5, 0));
    auto rows = aggregate_index_disk_usage(std::move(input), IndexDiskUsageLevel::kTablet);
    ASSERT_EQ(2U, rows.size());
    int64_t container_bytes = -1;
    for (const auto& row : rows) {
        if (row.record.structure == IndexDiskUsageStructure::kContainer) {
            container_bytes = row.record.total_bytes;
        }
    }
    EXPECT_EQ(5, container_bytes);
}

TEST(IndexDiskUsageAggregateTest, StorageFormatsStaySeparate) {
    auto input = four_segments();
    input[3].format = InvertedIndexStorageFormatPB::SNII;
    auto rows = aggregate_index_disk_usage(std::move(input), IndexDiskUsageLevel::kTablet);
    ASSERT_EQ(2U, rows.size());
}

} // namespace doris::segment_v2
