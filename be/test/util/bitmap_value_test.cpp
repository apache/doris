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

#include "util/bitmap_value.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "util/coding.h"

namespace doris {
using roaring::Roaring;

TEST(BitmapValueTest, Roaring64Map_operator_eq) {
    detail::Roaring64Map roaring64_map1, roaring64_map2;
    EXPECT_TRUE(roaring64_map1 == roaring64_map2);

    roaring64_map2.add(uint32_t(100));
    EXPECT_FALSE(roaring64_map1 == roaring64_map2);

    roaring64_map2.remove(uint32_t(100));
    EXPECT_TRUE(roaring64_map1 == roaring64_map2);

    roaring64_map1.add(uint32_t(100));
    EXPECT_FALSE(roaring64_map1 == roaring64_map2);

    roaring64_map2.add(uint32_t(100));
    EXPECT_TRUE(roaring64_map1 == roaring64_map2);

    roaring64_map2.remove(uint32_t(100));
    EXPECT_FALSE(roaring64_map1 == roaring64_map2);

    roaring64_map1.remove(uint32_t(100));
    EXPECT_TRUE(roaring64_map1 == roaring64_map2);
}

TEST(BitmapValueTest, copy) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    BitmapValue copied = bitmap;
    EXPECT_TRUE(copied.contains(1024));
    EXPECT_TRUE(copied.contains(1025));
    EXPECT_TRUE(copied.contains(1026));
    copied &= single;

    // value of copied changed.
    EXPECT_TRUE(copied.contains(1024));
    EXPECT_FALSE(copied.contains(1025));
    EXPECT_FALSE(copied.contains(1026));

    // value of bitmap not changed.
    EXPECT_TRUE(bitmap.contains(1024));
    EXPECT_TRUE(bitmap.contains(1025));
    EXPECT_TRUE(bitmap.contains(1026));
}

TEST(BitmapValueTest, bitmap_union) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    EXPECT_EQ(0, empty.cardinality());
    EXPECT_EQ(1, single.cardinality());
    EXPECT_EQ(3, bitmap.cardinality());

    BitmapValue empty2;
    empty2 |= empty;
    EXPECT_EQ(0, empty2.cardinality());
    empty2 |= single;
    EXPECT_EQ(1, empty2.cardinality());
    BitmapValue empty3;
    empty3 |= bitmap;
    EXPECT_EQ(3, empty3.cardinality());

    BitmapValue empty4;
    empty4.fastunion({&empty});
    EXPECT_EQ(0, empty4.cardinality());
    empty4.fastunion({&single});
    EXPECT_EQ(1, empty4.cardinality());
    BitmapValue empty5;
    empty5.fastunion({&bitmap});
    EXPECT_EQ(3, empty3.cardinality());
    BitmapValue empty6;
    empty6.fastunion({&empty, &single, &bitmap});
    EXPECT_EQ(3, empty3.cardinality());

    BitmapValue single2(1025);
    single2 |= empty;
    EXPECT_EQ(1, single2.cardinality());
    single2 |= single;
    EXPECT_EQ(2, single2.cardinality());
    BitmapValue single3(1027);
    single3 |= bitmap;
    EXPECT_EQ(4, single3.cardinality());

    BitmapValue single4(1025);
    single4.fastunion({&empty});
    EXPECT_EQ(1, single4.cardinality());
    single4.fastunion({&single});
    EXPECT_EQ(2, single4.cardinality());
    BitmapValue single5(1027);
    single5.fastunion({&bitmap});
    EXPECT_EQ(4, single5.cardinality());
    BitmapValue single6(1027);
    single6.fastunion({&empty, &single, &bitmap});
    EXPECT_EQ(4, single6.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2.add(4096);
    bitmap2 |= empty;
    EXPECT_EQ(3, bitmap2.cardinality());
    bitmap2 |= single;
    EXPECT_EQ(3, bitmap2.cardinality());
    bitmap2 |= bitmap;
    EXPECT_EQ(5, bitmap2.cardinality());

    BitmapValue bitmap3;
    bitmap3.add(1024);
    bitmap3.add(2048);
    bitmap3.add(4096);
    bitmap3.fastunion({&empty});
    EXPECT_EQ(3, bitmap3.cardinality());
    bitmap3.fastunion({&single});
    EXPECT_EQ(3, bitmap3.cardinality());
    bitmap3.fastunion({&bitmap});
    EXPECT_EQ(5, bitmap3.cardinality());
}

TEST(BitmapValueTest, bitmap_intersect) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    BitmapValue empty2;
    empty2 &= empty;
    EXPECT_EQ(0, empty2.cardinality());
    empty2 &= single;
    EXPECT_EQ(0, empty2.cardinality());
    empty2 &= bitmap;
    EXPECT_EQ(0, empty2.cardinality());

    BitmapValue single2(1025);
    single2 &= empty;
    EXPECT_EQ(0, single2.cardinality());

    BitmapValue single4(1025);
    single4 &= single;
    EXPECT_EQ(0, single4.cardinality());

    BitmapValue single3(1024);
    single3 &= single;
    EXPECT_EQ(1, single3.cardinality());

    single3 &= bitmap;
    EXPECT_EQ(1, single3.cardinality());

    BitmapValue single5(2048);
    single5 &= bitmap;
    EXPECT_EQ(0, single5.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2 &= empty;
    EXPECT_EQ(0, bitmap2.cardinality());

    BitmapValue bitmap3;
    bitmap3.add(1024);
    bitmap3.add(2048);
    bitmap3 &= single;
    EXPECT_EQ(1, bitmap3.cardinality());

    BitmapValue bitmap4;
    bitmap4.add(2049);
    bitmap4.add(2048);
    bitmap4 &= single;
    EXPECT_EQ(0, bitmap4.cardinality());

    BitmapValue bitmap5;
    bitmap5.add(2049);
    bitmap5.add(2048);
    bitmap5 &= bitmap;
    EXPECT_EQ(0, bitmap5.cardinality());

    BitmapValue bitmap6;
    bitmap6.add(1024);
    bitmap6.add(1025);
    bitmap6 &= bitmap;
    EXPECT_EQ(2, bitmap6.cardinality());
}

std::string convert_bitmap_to_string(BitmapValue& bitmap) {
    std::string buf;
    buf.resize(bitmap.getSizeInBytes());
    bitmap.write_to((char*)buf.c_str());
    return buf;
}

TEST(BitmapValueTest, bitmap_serde) {
    { // EMPTY
        BitmapValue empty;
        std::string buffer = convert_bitmap_to_string(empty);
        std::string expect_buffer(1, BitmapTypeCode::EMPTY);
        EXPECT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        EXPECT_EQ(0, out.cardinality());
    }
    { // SINGLE32
        uint32_t i = UINT32_MAX;
        BitmapValue single32(i);
        std::string buffer = convert_bitmap_to_string(single32);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE32);
        put_fixed32_le(&expect_buffer, i);
        EXPECT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        EXPECT_EQ(1, out.cardinality());
        EXPECT_TRUE(out.contains(i));
    }
    { // BITMAP32
        BitmapValue bitmap32({0, UINT32_MAX});
        std::string buffer = convert_bitmap_to_string(bitmap32);

        BitmapValue out(buffer.data());
        EXPECT_EQ(2, out.cardinality());
        EXPECT_TRUE(out.contains(0));
        EXPECT_TRUE(out.contains(UINT32_MAX));
    }
    { // SINGLE64
        uint64_t i = static_cast<uint64_t>(UINT32_MAX) + 1;
        BitmapValue single64(i);
        std::string buffer = convert_bitmap_to_string(single64);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE64);
        put_fixed64_le(&expect_buffer, i);
        EXPECT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        EXPECT_EQ(1, out.cardinality());
        EXPECT_TRUE(out.contains(i));
    }
    { // BITMAP64
        BitmapValue bitmap64({0, static_cast<uint64_t>(UINT32_MAX) + 1});
        std::string buffer = convert_bitmap_to_string(bitmap64);

        BitmapValue out(buffer.data());
        EXPECT_EQ(2, out.cardinality());
        EXPECT_TRUE(out.contains(0));
        EXPECT_TRUE(out.contains(static_cast<uint64_t>(UINT32_MAX) + 1));
    }
}

// Forked from CRoaring's UT of Roaring64Map
TEST(BitmapValueTest, Roaring64Map) {
    using doris::detail::Roaring64Map;
    // create a new empty bitmap
    Roaring64Map r1;
    uint64_t r1_sum = 0;
    // then we can add values
    for (uint64_t i = 100; i < 1000; i++) {
        r1.add(i);
        r1_sum += i;
    }
    for (uint64_t i = 14000000000000000100ull; i < 14000000000000001000ull; i++) {
        r1.add(i);
        r1_sum += i;
    }
    EXPECT_TRUE(r1.contains((uint64_t)14000000000000000500ull));
    EXPECT_EQ(1800, r1.cardinality());
    size_t size_before = r1.getSizeInBytes(1);
    r1.runOptimize();
    size_t size_after = r1.getSizeInBytes(1);
    EXPECT_LT(size_after, size_before);

    Roaring64Map r2 = Roaring64Map::bitmapOf(5, 1ull, 2ull, 234294967296ull, 195839473298ull,
                                             14000000000000000100ull);
    EXPECT_EQ(1ull, r2.minimum());
    EXPECT_EQ(14000000000000000100ull, r2.maximum());
    EXPECT_EQ(4ull, r2.rank(234294967296ull));

    // we can also create a bitmap from a pointer to 32-bit integers
    const uint32_t values[] = {2, 3, 4};
    Roaring64Map r3(3, values);
    EXPECT_EQ(3, r3.cardinality());

    // we can also go in reverse and go from arrays to bitmaps
    uint64_t card1 = r1.cardinality();
    uint64_t* arr1 = new uint64_t[card1];
    EXPECT_TRUE(arr1 != nullptr);
    r1.toUint64Array(arr1);
    Roaring64Map r1f(card1, arr1);
    delete[] arr1;
    // bitmaps shall be equal
    EXPECT_TRUE(r1 == r1f);

    // we can copy and compare bitmaps
    Roaring64Map z(r3);
    EXPECT_TRUE(r3 == z);

    // we can compute union two-by-two
    Roaring64Map r1_2_3 = r1 | r2;
    r1_2_3 |= r3;

    // we can compute a big union
    const Roaring64Map* allmybitmaps[] = {&r1, &r2, &r3};
    Roaring64Map bigunion = Roaring64Map::fastunion(3, allmybitmaps);
    EXPECT_TRUE(r1_2_3 == bigunion);
    EXPECT_EQ(1806, r1_2_3.cardinality());

    // we can compute intersection two-by-two
    Roaring64Map i1_2 = r1 & r2;
    EXPECT_EQ(1, i1_2.cardinality());

    // we can write a bitmap to a pointer and recover it later
    uint32_t expectedsize = r1.getSizeInBytes(1);
    char* serializedbytes = new char[expectedsize];
    r1.write(serializedbytes, 1);
    Roaring64Map t = Roaring64Map::read(serializedbytes);
    EXPECT_TRUE(r1 == t);
    delete[] serializedbytes;

    // we can iterate over all values using custom functions
    uint64_t sum = 0;
    auto func = [](uint64_t value, void* param) {
        *(uint64_t*)param += value;
        return true; // we always process all values
    };
    r1.iterate(func, &sum);
    EXPECT_EQ(r1_sum, sum);

    // we can also iterate the C++ way
    sum = 0;
    for (Roaring64Map::const_iterator i = t.begin(); i != t.end(); i++) {
        sum += *i;
    }
    EXPECT_EQ(r1_sum, sum);
}

TEST(BitmapValueTest, bitmap_to_string) {
    BitmapValue empty;
    EXPECT_STREQ("", empty.to_string().c_str());
    empty.add(1);
    EXPECT_STREQ("1", empty.to_string().c_str());
    empty.add(2);
    EXPECT_STREQ("1,2", empty.to_string().c_str());
}

TEST(BitmapValueTest, sub_limit) {
    BitmapValue bitmap({1, 2, 3, 10, 11, 5, 6, 7, 8, 9});
    BitmapValue ret_bitmap1;
    EXPECT_EQ(5, bitmap.sub_limit(0, 5, &ret_bitmap1));
    EXPECT_STREQ("1,2,3,5,6", ret_bitmap1.to_string().c_str());

    BitmapValue ret_bitmap2;
    EXPECT_EQ(6, bitmap.sub_limit(6, 10, &ret_bitmap2));
    EXPECT_STREQ("6,7,8,9,10,11", ret_bitmap2.to_string().c_str());

    BitmapValue ret_bitmap3;
    EXPECT_EQ(3, bitmap.sub_limit(5, 3, &ret_bitmap3));
    EXPECT_STREQ("5,6,7", ret_bitmap3.to_string().c_str());

    BitmapValue ret_bitmap4;
    EXPECT_EQ(5, bitmap.sub_limit(2, 5, &ret_bitmap4));
    EXPECT_STREQ("2,3,5,6,7", ret_bitmap4.to_string().c_str());
}

TEST(BitmapValueTest, bitmap_single_convert) {
    BitmapValue bitmap;
    EXPECT_STREQ("", bitmap.to_string().c_str());
    bitmap.add(1);
    EXPECT_STREQ("1", bitmap.to_string().c_str());
    bitmap.add(1);
    EXPECT_STREQ("1", bitmap.to_string().c_str());
    EXPECT_EQ(BitmapValue::SINGLE, bitmap._type);

    BitmapValue bitmap_u;
    bitmap_u.add(1);
    bitmap |= bitmap_u;
    EXPECT_EQ(BitmapValue::SINGLE, bitmap._type);

    bitmap_u.add(2);
    EXPECT_EQ(BitmapValue::BITMAP, bitmap_u._type);

    bitmap |= bitmap_u;
    EXPECT_EQ(BitmapValue::BITMAP, bitmap._type);
}

TEST(BitmapValueTest, bitmap_value_iterator_test) {
    BitmapValue empty;
    for (auto iter = empty.begin(); iter != empty.end(); ++iter) {
        // should not goes here
        EXPECT_TRUE(false);
    }

    BitmapValue single(1024);
    auto single_iter = single.begin();
    EXPECT_EQ(1024, *single_iter);
    EXPECT_TRUE(single_iter == BitmapValue {1024}.begin());
    EXPECT_TRUE(single_iter != single.end());

    ++single_iter;
    EXPECT_TRUE(single_iter == single.end());

    int i = 0;
    BitmapValue bitmap({0, 1025, 1026, UINT32_MAX, UINT64_MAX});
    for (auto iter = bitmap.begin(); iter != bitmap.end(); ++iter, ++i) {
        switch (i) {
        case 0:
            EXPECT_EQ(0, *iter);
            break;
        case 1:
            EXPECT_EQ(1025, *iter);
            break;
        case 2:
            EXPECT_EQ(1026, *iter);
            break;
        case 3:
            EXPECT_EQ(UINT32_MAX, *iter);
            break;
        case 4:
            EXPECT_EQ(UINT64_MAX, *iter);
            break;
        default:
            EXPECT_TRUE(false);
        }
    }
}
} // namespace doris
