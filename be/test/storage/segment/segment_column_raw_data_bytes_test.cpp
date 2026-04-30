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

#include "storage/segment/mock/mock_segment.h"

namespace doris::segment_v2 {

class SegmentColumnRawDataBytesTest : public testing::Test {};

TEST_F(SegmentColumnRawDataBytesTest, ReturnsZeroForUnknownColumn) {
    MockSegment seg;
    EXPECT_EQ(seg.column_raw_data_bytes(999), 0);
}

TEST_F(SegmentColumnRawDataBytesTest, ReturnsSetValue) {
    MockSegment seg;
    seg.set_column_raw_data_bytes(1, 1024);
    seg.set_column_raw_data_bytes(2, 2048);

    EXPECT_EQ(seg.column_raw_data_bytes(1), 1024);
    EXPECT_EQ(seg.column_raw_data_bytes(2), 2048);
}

TEST_F(SegmentColumnRawDataBytesTest, OverwritesPreviousValue) {
    MockSegment seg;
    seg.set_column_raw_data_bytes(1, 100);
    EXPECT_EQ(seg.column_raw_data_bytes(1), 100);

    seg.set_column_raw_data_bytes(1, 200);
    EXPECT_EQ(seg.column_raw_data_bytes(1), 200);
}

TEST_F(SegmentColumnRawDataBytesTest, HandlesMultipleColumns) {
    MockSegment seg;
    for (int32_t uid = 0; uid < 50; uid++) {
        seg.set_column_raw_data_bytes(uid, uid * 1000);
    }
    for (int32_t uid = 0; uid < 50; uid++) {
        EXPECT_EQ(seg.column_raw_data_bytes(uid), uid * 1000);
    }
}

TEST_F(SegmentColumnRawDataBytesTest, HandlesLargeByteValues) {
    MockSegment seg;
    uint64_t large_value = 1ULL << 40; // 1 TB
    seg.set_column_raw_data_bytes(1, large_value);
    EXPECT_EQ(seg.column_raw_data_bytes(1), large_value);
}

} // namespace doris::segment_v2
