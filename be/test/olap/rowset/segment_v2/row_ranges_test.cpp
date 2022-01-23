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

#include "olap/rowset/segment_v2/row_ranges.h"

#include <gtest/gtest.h>

#include <memory>

namespace doris {
namespace segment_v2 {

class RowRangesTest : public testing::Test {
public:
    virtual ~RowRangesTest() {}
};

// Test for int
TEST_F(RowRangesTest, TestRange) {
    RowRange range1(10, 20);
    RowRange range2(15, 25);
    RowRange range3(30, 40);
    ASSERT_TRUE(range1.is_valid());
    ASSERT_EQ(10, range1.from());
    ASSERT_EQ(20, range1.to());
    ASSERT_EQ(10, range1.count());
    ASSERT_TRUE(range1.is_before(range3));
    ASSERT_FALSE(range1.is_after(range2));
    ASSERT_TRUE(range3.is_after(range1));
    RowRange tmp;
    RowRange::range_intersection(range1, range2, &tmp);
    ASSERT_TRUE(tmp.is_valid());
    ASSERT_EQ(5, tmp.count());
    ASSERT_TRUE(tmp.is_valid());
    RowRange tmp2;
    RowRange::range_intersection(range1, range3, &tmp2);
    ASSERT_FALSE(tmp2.is_valid());
    RowRange tmp3;
    RowRange::range_union(range1, range3, &tmp3);
    ASSERT_FALSE(tmp3.is_valid());
    RowRange range4(0, 0);
    ASSERT_FALSE(range4.is_valid());
    RowRange range5(20, 25);
    RowRange tmp4;
    ASSERT_FALSE(RowRange::range_intersection(range1, range5, &tmp4));
    ASSERT_TRUE(RowRange::range_union(range1, range5, &tmp4));
    ASSERT_EQ(15, tmp4.count());
    ASSERT_EQ(10, tmp4.from());
    ASSERT_EQ(25, tmp4.to());
}

TEST_F(RowRangesTest, TestRowRanges) {
    RowRanges row_ranges;
    RowRanges row_ranges1 = RowRanges::create_single(10, 20);
    RowRanges row_ranges2 = RowRanges::create_single(20, 30);
    RowRanges row_ranges3 = RowRanges::create_single(15, 30);
    RowRanges row_ranges4 = RowRanges::create_single(40, 50);

    RowRanges row_ranges_merge;
    RowRanges::ranges_intersection(row_ranges1, row_ranges2, &row_ranges_merge);
    ASSERT_EQ(0, row_ranges_merge.count());
    ASSERT_TRUE(row_ranges_merge.is_empty());

    RowRanges row_ranges_merge2;
    RowRanges::ranges_intersection(row_ranges1, row_ranges3, &row_ranges_merge2);
    ASSERT_EQ(5, row_ranges_merge2.count());
    ASSERT_FALSE(row_ranges_merge2.is_empty());
    ASSERT_TRUE(row_ranges_merge2.contain(16, 19));
    ASSERT_EQ(15, row_ranges_merge2.from());
    ASSERT_EQ(20, row_ranges_merge2.to());
    ASSERT_EQ(15, row_ranges_merge2.get_range_from(0));
    ASSERT_EQ(20, row_ranges_merge2.get_range_to(0));
    ASSERT_EQ(5, row_ranges_merge2.get_range_count(0));

    RowRanges row_ranges_merge3;
    RowRanges::ranges_intersection(row_ranges1, row_ranges4, &row_ranges_merge3);
    ASSERT_EQ(0, row_ranges_merge3.count());
    ASSERT_TRUE(row_ranges_merge3.is_empty());

    RowRanges row_ranges_union;
    RowRanges::ranges_union(row_ranges1, row_ranges2, &row_ranges_union);
    ASSERT_EQ(20, row_ranges_union.count());
    RowRanges::ranges_union(row_ranges_union, row_ranges4, &row_ranges_union);
    ASSERT_EQ(30, row_ranges_union.count());
    ASSERT_FALSE(row_ranges_union.is_empty());
    ASSERT_TRUE(row_ranges_union.contain(16, 19));
    ASSERT_EQ(10, row_ranges_union.from());
    ASSERT_EQ(50, row_ranges_union.to());
    ASSERT_EQ(10, row_ranges_union.get_range_from(0));
    ASSERT_EQ(30, row_ranges_union.get_range_to(0));
    ASSERT_EQ(20, row_ranges_union.get_range_count(0));
}

TEST_F(RowRangesTest, TestRangesToRoaring) {
    RowRanges row_ranges;
    RowRanges row_ranges1 = RowRanges::create_single(10, 20);
    RowRanges row_ranges2 = RowRanges::create_single(20, 30);
    RowRanges row_ranges3 = RowRanges::create_single(15, 30);
    RowRanges row_ranges4 = RowRanges::create_single(40, 50);

    roaring::Roaring row_bitmap = RowRanges::ranges_to_roaring(row_ranges1);
    ASSERT_EQ(row_ranges1.count(), row_bitmap.cardinality());

    row_bitmap = RowRanges::ranges_to_roaring(row_ranges3);
    ASSERT_EQ(row_ranges3.count(), row_bitmap.cardinality());

    RowRanges row_ranges_merge;
    RowRanges::ranges_intersection(row_ranges1, row_ranges2, &row_ranges_merge);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_merge);
    ASSERT_EQ(row_ranges_merge.count(), row_bitmap.cardinality());

    RowRanges row_ranges_merge2;
    RowRanges::ranges_intersection(row_ranges1, row_ranges3, &row_ranges_merge2);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_merge2);
    ASSERT_EQ(row_ranges_merge2.count(), row_bitmap.cardinality());

    RowRanges row_ranges_union;
    RowRanges::ranges_union(row_ranges1, row_ranges2, &row_ranges_union);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_union);
    ASSERT_EQ(row_ranges_union.count(), row_bitmap.cardinality());
}

} // namespace segment_v2
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
