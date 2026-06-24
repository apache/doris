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

#include "storage/segment/row_ranges.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

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
    EXPECT_TRUE(range1.is_valid());
    EXPECT_EQ(10, range1.from());
    EXPECT_EQ(20, range1.to());
    EXPECT_EQ(10, range1.count());
    EXPECT_TRUE(range1.is_before(range3));
    EXPECT_FALSE(range1.is_after(range2));
    EXPECT_TRUE(range3.is_after(range1));
    RowRange tmp;
    RowRange::range_intersection(range1, range2, &tmp);
    EXPECT_TRUE(tmp.is_valid());
    EXPECT_EQ(5, tmp.count());
    EXPECT_TRUE(tmp.is_valid());
    RowRange tmp2;
    RowRange::range_intersection(range1, range3, &tmp2);
    EXPECT_FALSE(tmp2.is_valid());
    RowRange tmp3;
    RowRange::range_union(range1, range3, &tmp3);
    EXPECT_FALSE(tmp3.is_valid());
    RowRange range4(0, 0);
    EXPECT_FALSE(range4.is_valid());
    RowRange range5(20, 25);
    RowRange tmp4;
    EXPECT_FALSE(RowRange::range_intersection(range1, range5, &tmp4));
    EXPECT_TRUE(RowRange::range_union(range1, range5, &tmp4));
    EXPECT_EQ(15, tmp4.count());
    EXPECT_EQ(10, tmp4.from());
    EXPECT_EQ(25, tmp4.to());
}

TEST_F(RowRangesTest, TestRowRanges) {
    RowRanges row_ranges;
    RowRanges row_ranges1 = RowRanges::create_single(10, 20);
    RowRanges row_ranges2 = RowRanges::create_single(20, 30);
    RowRanges row_ranges3 = RowRanges::create_single(15, 30);
    RowRanges row_ranges4 = RowRanges::create_single(40, 50);

    RowRanges row_ranges_merge;
    RowRanges::ranges_intersection(row_ranges1, row_ranges2, &row_ranges_merge);
    EXPECT_EQ(0, row_ranges_merge.count());
    EXPECT_TRUE(row_ranges_merge.is_empty());

    RowRanges row_ranges_merge2;
    RowRanges::ranges_intersection(row_ranges1, row_ranges3, &row_ranges_merge2);
    EXPECT_EQ(5, row_ranges_merge2.count());
    EXPECT_FALSE(row_ranges_merge2.is_empty());
    EXPECT_TRUE(row_ranges_merge2.contain(16, 19));
    EXPECT_EQ(15, row_ranges_merge2.from());
    EXPECT_EQ(20, row_ranges_merge2.to());
    EXPECT_EQ(15, row_ranges_merge2.get_range_from(0));
    EXPECT_EQ(20, row_ranges_merge2.get_range_to(0));
    EXPECT_EQ(5, row_ranges_merge2.get_range_count(0));

    RowRanges row_ranges_merge3;
    RowRanges::ranges_intersection(row_ranges1, row_ranges4, &row_ranges_merge3);
    EXPECT_EQ(0, row_ranges_merge3.count());
    EXPECT_TRUE(row_ranges_merge3.is_empty());

    RowRanges row_ranges_union;
    RowRanges::ranges_union(row_ranges1, row_ranges2, &row_ranges_union);
    EXPECT_EQ(20, row_ranges_union.count());
    RowRanges::ranges_union(row_ranges_union, row_ranges4, &row_ranges_union);
    EXPECT_EQ(30, row_ranges_union.count());
    EXPECT_FALSE(row_ranges_union.is_empty());
    EXPECT_TRUE(row_ranges_union.contain(16, 19));
    EXPECT_EQ(10, row_ranges_union.from());
    EXPECT_EQ(50, row_ranges_union.to());
    EXPECT_EQ(10, row_ranges_union.get_range_from(0));
    EXPECT_EQ(30, row_ranges_union.get_range_to(0));
    EXPECT_EQ(20, row_ranges_union.get_range_count(0));
}

TEST_F(RowRangesTest, TestRangesToRoaring) {
    RowRanges row_ranges;
    RowRanges row_ranges1 = RowRanges::create_single(10, 20);
    RowRanges row_ranges2 = RowRanges::create_single(20, 30);
    RowRanges row_ranges3 = RowRanges::create_single(15, 30);
    RowRanges row_ranges4 = RowRanges::create_single(40, 50);

    roaring::Roaring row_bitmap = RowRanges::ranges_to_roaring(row_ranges1);
    EXPECT_EQ(row_ranges1.count(), row_bitmap.cardinality());

    row_bitmap = RowRanges::ranges_to_roaring(row_ranges3);
    EXPECT_EQ(row_ranges3.count(), row_bitmap.cardinality());

    RowRanges row_ranges_merge;
    RowRanges::ranges_intersection(row_ranges1, row_ranges2, &row_ranges_merge);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_merge);
    EXPECT_EQ(row_ranges_merge.count(), row_bitmap.cardinality());

    RowRanges row_ranges_merge2;
    RowRanges::ranges_intersection(row_ranges1, row_ranges3, &row_ranges_merge2);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_merge2);
    EXPECT_EQ(row_ranges_merge2.count(), row_bitmap.cardinality());

    RowRanges row_ranges_union;
    RowRanges::ranges_union(row_ranges1, row_ranges2, &row_ranges_union);
    row_bitmap = RowRanges::ranges_to_roaring(row_ranges_union);
    EXPECT_EQ(row_ranges_union.count(), row_bitmap.cardinality());
}

TEST_F(RowRangesTest, TestRangesException) {
    // Case 1: Right subtracts a hole from the middle of left
    // [100, 300) \ [150, 200) = [100, 150), [200, 300)
    {
        RowRanges left = RowRanges::create_single(100, 300);
        RowRanges right = RowRanges::create_single(150, 200);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(150, result.count()); // 50 + 100
        EXPECT_EQ(2, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(150, result.get_range_to(0));
        EXPECT_EQ(200, result.get_range_from(1));
        EXPECT_EQ(300, result.get_range_to(1));
    }

    // Case 2: Right trims the left side
    // [100, 300) \ [0, 150) = [150, 300)
    {
        RowRanges left = RowRanges::create_single(100, 300);
        RowRanges right = RowRanges::create_single(0, 150);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(150, result.count());
        EXPECT_EQ(1, result.range_size());
        EXPECT_EQ(150, result.get_range_from(0));
        EXPECT_EQ(300, result.get_range_to(0));
    }

    // Case 3: Right trims the right side
    // [100, 300) \ [250, 400) = [100, 250)
    {
        RowRanges left = RowRanges::create_single(100, 300);
        RowRanges right = RowRanges::create_single(250, 400);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(150, result.count());
        EXPECT_EQ(1, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(250, result.get_range_to(0));
    }

    // Case 4: No overlap (right is after left) — left unchanged
    // [100, 200) \ [200, 300) = [100, 200)
    {
        RowRanges left = RowRanges::create_single(100, 200);
        RowRanges right = RowRanges::create_single(200, 300);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(100, result.count());
        EXPECT_EQ(1, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(200, result.get_range_to(0));
    }

    // Case 5: Right fully covers left — result is empty
    // [100, 300) \ [0, 400) = <EMPTY>
    {
        RowRanges left = RowRanges::create_single(100, 300);
        RowRanges right = RowRanges::create_single(0, 400);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(0, result.count());
        EXPECT_TRUE(result.is_empty());
    }

    // Case 6: Multiple left ranges, single right range cutting through both
    // [100, 200), [300, 400) \ [150, 350) = [100, 150), [350, 400)
    {
        RowRanges left;
        left.add(RowRange(100, 200));
        left.add(RowRange(300, 400));
        RowRanges right = RowRanges::create_single(150, 350);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(100, result.count()); // 50 + 50
        EXPECT_EQ(2, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(150, result.get_range_to(0));
        EXPECT_EQ(350, result.get_range_from(1));
        EXPECT_EQ(400, result.get_range_to(1));
    }

    // Case 7: No overlap (right is before left) — left unchanged
    // [100, 200) \ [0, 50) = [100, 200)
    {
        RowRanges left = RowRanges::create_single(100, 200);
        RowRanges right = RowRanges::create_single(0, 50);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(100, result.count());
        EXPECT_EQ(1, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(200, result.get_range_to(0));
    }

    // Case 8: Empty right — left unchanged
    // [100, 200) \ <EMPTY> = [100, 200)
    {
        RowRanges left = RowRanges::create_single(100, 200);
        RowRanges right;
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(100, result.count());
        EXPECT_EQ(1, result.range_size());
        EXPECT_EQ(100, result.get_range_from(0));
        EXPECT_EQ(200, result.get_range_to(0));
    }

    // Case 9: Empty left — result is empty
    // <EMPTY> \ [100, 200) = <EMPTY>
    {
        RowRanges left;
        RowRanges right = RowRanges::create_single(100, 200);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(0, result.count());
        EXPECT_TRUE(result.is_empty());
    }

    // Case 10: Left equals right — result is empty
    // [100, 200) \ [100, 200) = <EMPTY>
    {
        RowRanges left = RowRanges::create_single(100, 200);
        RowRanges right = RowRanges::create_single(100, 200);
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(0, result.count());
        EXPECT_TRUE(result.is_empty());
    }

    // Case 11: Multiple right ranges punching multiple holes
    // [0, 100) \ [10, 20), [30, 40), [60, 70) = [0, 10), [20, 30), [40, 60), [70, 100)
    {
        RowRanges left = RowRanges::create_single(0, 100);
        RowRanges right;
        right.add(RowRange(10, 20));
        right.add(RowRange(30, 40));
        right.add(RowRange(60, 70));
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(70, result.count()); // 10 + 10 + 20 + 30
        EXPECT_EQ(4, result.range_size());
        EXPECT_EQ(0, result.get_range_from(0));
        EXPECT_EQ(10, result.get_range_to(0));
        EXPECT_EQ(20, result.get_range_from(1));
        EXPECT_EQ(30, result.get_range_to(1));
        EXPECT_EQ(40, result.get_range_from(2));
        EXPECT_EQ(60, result.get_range_to(2));
        EXPECT_EQ(70, result.get_range_from(3));
        EXPECT_EQ(100, result.get_range_to(3));
    }

    // Case 12: Multiple left ranges, multiple right ranges
    // [0, 50), [100, 150), [200, 250) \ [25, 125), [225, 300)
    // = [0, 25), [125, 150), [200, 225)
    {
        RowRanges left;
        left.add(RowRange(0, 50));
        left.add(RowRange(100, 150));
        left.add(RowRange(200, 250));
        RowRanges right;
        right.add(RowRange(25, 125));
        right.add(RowRange(225, 300));
        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);
        EXPECT_EQ(75, result.count()); // 25 + 25 + 25
        EXPECT_EQ(3, result.range_size());
        EXPECT_EQ(0, result.get_range_from(0));
        EXPECT_EQ(25, result.get_range_to(0));
        EXPECT_EQ(125, result.get_range_from(1));
        EXPECT_EQ(150, result.get_range_to(1));
        EXPECT_EQ(200, result.get_range_from(2));
        EXPECT_EQ(225, result.get_range_to(2));
    }

    // Case 13: Verify consistency with roaring bitmap approach
    {
        RowRanges left;
        left.add(RowRange(10, 50));
        left.add(RowRange(80, 120));
        RowRanges right;
        right.add(RowRange(30, 90));

        RowRanges result;
        RowRanges::ranges_exception(left, right, &result);

        // Verify using roaring bitmaps
        roaring::Roaring left_bitmap = RowRanges::ranges_to_roaring(left);
        roaring::Roaring right_bitmap = RowRanges::ranges_to_roaring(right);
        roaring::Roaring expected = left_bitmap - right_bitmap;

        roaring::Roaring result_bitmap = RowRanges::ranges_to_roaring(result);
        EXPECT_EQ(expected.cardinality(), result_bitmap.cardinality());
        EXPECT_TRUE(expected == result_bitmap);
    }
}

} // namespace segment_v2
} // namespace doris
