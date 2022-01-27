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

#include <cstdint>
#include <string>

#include "util/coding.h"
#define private public
#include "util/bitmap_value.h"

namespace doris {
using roaring::Roaring;

TEST(BitmapValueTest, bitmap_union) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    ASSERT_EQ(0, empty.cardinality());
    ASSERT_EQ(1, single.cardinality());
    ASSERT_EQ(3, bitmap.cardinality());

    BitmapValue empty2;
    empty2 |= empty;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 |= single;
    ASSERT_EQ(1, empty2.cardinality());
    BitmapValue empty3;
    empty3 |= bitmap;
    ASSERT_EQ(3, empty3.cardinality());

    BitmapValue single2(1025);
    single2 |= empty;
    ASSERT_EQ(1, single2.cardinality());
    single2 |= single;
    ASSERT_EQ(2, single2.cardinality());
    BitmapValue single3(1027);
    single3 |= bitmap;
    ASSERT_EQ(4, single3.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2.add(4096);
    bitmap2 |= empty;
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2 |= single;
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2 |= bitmap;
    ASSERT_EQ(5, bitmap2.cardinality());
}

TEST(BitmapValueTest, bitmap_intersect) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    BitmapValue empty2;
    empty2 &= empty;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 &= single;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 &= bitmap;
    ASSERT_EQ(0, empty2.cardinality());

    BitmapValue single2(1025);
    single2 &= empty;
    ASSERT_EQ(0, single2.cardinality());

    BitmapValue single4(1025);
    single4 &= single;
    ASSERT_EQ(0, single4.cardinality());

    BitmapValue single3(1024);
    single3 &= single;
    ASSERT_EQ(1, single3.cardinality());

    single3 &= bitmap;
    ASSERT_EQ(1, single3.cardinality());

    BitmapValue single5(2048);
    single5 &= bitmap;
    ASSERT_EQ(0, single5.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2 &= empty;
    ASSERT_EQ(0, bitmap2.cardinality());

    BitmapValue bitmap3;
    bitmap3.add(1024);
    bitmap3.add(2048);
    bitmap3 &= single;
    ASSERT_EQ(1, bitmap3.cardinality());

    BitmapValue bitmap4;
    bitmap4.add(2049);
    bitmap4.add(2048);
    bitmap4 &= single;
    ASSERT_EQ(0, bitmap4.cardinality());

    BitmapValue bitmap5;
    bitmap5.add(2049);
    bitmap5.add(2048);
    bitmap5 &= bitmap;
    ASSERT_EQ(0, bitmap5.cardinality());

    BitmapValue bitmap6;
    bitmap6.add(1024);
    bitmap6.add(1025);
    bitmap6 &= bitmap;
    ASSERT_EQ(2, bitmap6.cardinality());
}

std::string convert_bitmap_to_string(BitmapValue& bitmap) {
    std::string buf;
    buf.resize(bitmap.getSizeInBytes());
    bitmap.write((char*)buf.c_str());
    return buf;
}

TEST(BitmapValueTest, bitmap_serde) {
    { // EMPTY
        BitmapValue empty;
        std::string buffer = convert_bitmap_to_string(empty);
        std::string expect_buffer(1, BitmapTypeCode::EMPTY);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(0, out.cardinality());
    }
    { // SINGLE32
        uint32_t i = UINT32_MAX;
        BitmapValue single32(i);
        std::string buffer = convert_bitmap_to_string(single32);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE32);
        put_fixed32_le(&expect_buffer, i);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(1, out.cardinality());
        ASSERT_TRUE(out.contains(i));
    }
    { // BITMAP32
        BitmapValue bitmap32({0, UINT32_MAX});
        std::string buffer = convert_bitmap_to_string(bitmap32);

        Roaring roaring;
        roaring.add(0);
        roaring.add(UINT32_MAX);
        std::string expect_buffer(1, BitmapTypeCode::BITMAP32);
        expect_buffer.resize(1 + roaring.getSizeInBytes());
        roaring.write(&expect_buffer[1]);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(2, out.cardinality());
        ASSERT_TRUE(out.contains(0));
        ASSERT_TRUE(out.contains(UINT32_MAX));
    }
    { // SINGLE64
        uint64_t i = static_cast<uint64_t>(UINT32_MAX) + 1;
        BitmapValue single64(i);
        std::string buffer = convert_bitmap_to_string(single64);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE64);
        put_fixed64_le(&expect_buffer, i);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(1, out.cardinality());
        ASSERT_TRUE(out.contains(i));
    }
    { // BITMAP64
        BitmapValue bitmap64({0, static_cast<uint64_t>(UINT32_MAX) + 1});
        std::string buffer = convert_bitmap_to_string(bitmap64);

        Roaring roaring;
        roaring.add(0);
        std::string expect_buffer(1, BitmapTypeCode::BITMAP64);
        put_varint64(&expect_buffer, 2); // map size
        for (uint32_t i = 0; i < 2; ++i) {
            std::string map_entry;
            put_fixed32_le(&map_entry, i); // map key
            map_entry.resize(sizeof(uint32_t) + roaring.getSizeInBytes());
            roaring.write(&map_entry[4]); // map value

            expect_buffer.append(map_entry);
        }
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(2, out.cardinality());
        ASSERT_TRUE(out.contains(0));
        ASSERT_TRUE(out.contains(static_cast<uint64_t>(UINT32_MAX) + 1));
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
    ASSERT_TRUE(r1.contains((uint64_t)14000000000000000500ull));
    ASSERT_EQ(1800, r1.cardinality());
    size_t size_before = r1.getSizeInBytes();
    r1.runOptimize();
    size_t size_after = r1.getSizeInBytes();
    ASSERT_LT(size_after, size_before);

    Roaring64Map r2 = Roaring64Map::bitmapOf(5, 1ull, 2ull, 234294967296ull, 195839473298ull,
                                             14000000000000000100ull);
    ASSERT_EQ(1ull, r2.minimum());
    ASSERT_EQ(14000000000000000100ull, r2.maximum());
    ASSERT_EQ(4ull, r2.rank(234294967296ull));

    // we can also create a bitmap from a pointer to 32-bit integers
    const uint32_t values[] = {2, 3, 4};
    Roaring64Map r3(3, values);
    ASSERT_EQ(3, r3.cardinality());

    // we can also go in reverse and go from arrays to bitmaps
    uint64_t card1 = r1.cardinality();
    uint64_t* arr1 = new uint64_t[card1];
    ASSERT_TRUE(arr1 != nullptr);
    r1.toUint64Array(arr1);
    Roaring64Map r1f(card1, arr1);
    delete[] arr1;
    // bitmaps shall be equal
    ASSERT_TRUE(r1 == r1f);

    // we can copy and compare bitmaps
    Roaring64Map z(r3);
    ASSERT_TRUE(r3 == z);

    // we can compute union two-by-two
    Roaring64Map r1_2_3 = r1 | r2;
    r1_2_3 |= r3;

    // we can compute a big union
    const Roaring64Map* allmybitmaps[] = {&r1, &r2, &r3};
    Roaring64Map bigunion = Roaring64Map::fastunion(3, allmybitmaps);
    ASSERT_TRUE(r1_2_3 == bigunion);
    ASSERT_EQ(1806, r1_2_3.cardinality());

    // we can compute intersection two-by-two
    Roaring64Map i1_2 = r1 & r2;
    ASSERT_EQ(1, i1_2.cardinality());

    // we can write a bitmap to a pointer and recover it later
    uint32_t expectedsize = r1.getSizeInBytes();
    char* serializedbytes = new char[expectedsize];
    r1.write(serializedbytes);
    Roaring64Map t = Roaring64Map::read(serializedbytes);
    ASSERT_TRUE(r1 == t);
    delete[] serializedbytes;

    // we can iterate over all values using custom functions
    uint64_t sum = 0;
    auto func = [](uint64_t value, void* param) {
        *(uint64_t*)param += value;
        return true; // we always process all values
    };
    r1.iterate(func, &sum);
    ASSERT_EQ(r1_sum, sum);

    // we can also iterate the C++ way
    sum = 0;
    for (Roaring64Map::const_iterator i = t.begin(); i != t.end(); i++) {
        sum += *i;
    }
    ASSERT_EQ(r1_sum, sum);
}

TEST(BitmapValueTest, bitmap_to_string) {
    BitmapValue empty;
    ASSERT_STREQ("", empty.to_string().c_str());
    empty.add(1);
    ASSERT_STREQ("1", empty.to_string().c_str());
    empty.add(2);
    ASSERT_STREQ("1,2", empty.to_string().c_str());
}

TEST(BitmapValueTest, sub_limit) {
    BitmapValue bitmap({1,2,3,10,11,5,6,7,8,9});
    BitmapValue ret_bitmap1;
    ASSERT_EQ(5, bitmap.sub_limit(0, 5, &ret_bitmap1));
    ASSERT_STREQ("1,2,3,5,6", ret_bitmap1.to_string().c_str());

    BitmapValue ret_bitmap2;
    ASSERT_EQ(6, bitmap.sub_limit(6, 10, &ret_bitmap2));
    ASSERT_STREQ("6,7,8,9,10,11", ret_bitmap2.to_string().c_str());

    BitmapValue ret_bitmap3;
    ASSERT_EQ(3, bitmap.sub_limit(5, 3, &ret_bitmap3));
    ASSERT_STREQ("5,6,7", ret_bitmap3.to_string().c_str());

    BitmapValue ret_bitmap4;
    ASSERT_EQ(5, bitmap.sub_limit(2, 5, &ret_bitmap4));
    ASSERT_STREQ("2,3,5,6,7", ret_bitmap4.to_string().c_str());
}

TEST(BitmapValueTest, bitmap_single_convert) {
    BitmapValue bitmap;
    ASSERT_STREQ("", bitmap.to_string().c_str());
    bitmap.add(1);
    ASSERT_STREQ("1", bitmap.to_string().c_str());
    bitmap.add(1);
    ASSERT_STREQ("1", bitmap.to_string().c_str());
    ASSERT_EQ(BitmapValue::SINGLE, bitmap._type);

    BitmapValue bitmap_u;
    bitmap_u.add(1);
    bitmap |= bitmap_u;
    ASSERT_EQ(BitmapValue::SINGLE, bitmap._type);

    bitmap_u.add(2);
    ASSERT_EQ(BitmapValue::BITMAP, bitmap_u._type);

    bitmap |= bitmap_u;
    ASSERT_EQ(BitmapValue::BITMAP, bitmap._type);
}

TEST(BitmapValueTest, bitmap_value_iterator_test) {
    BitmapValue empty;
    for (auto iter = empty.begin(); iter != empty.end(); ++iter) {
        // should not goes here
        ASSERT_TRUE(false);
    }

    BitmapValue single(1024);
    for (auto iter = single.begin(); iter != single.end(); ++iter) {
        ASSERT_EQ(1024, *iter);
    }

    int i = 0;
    BitmapValue bitmap({0, 1025, 1026, UINT32_MAX, UINT64_MAX});
    for (auto iter = bitmap.begin(); iter != bitmap.end(); ++iter, ++i) {
        switch (i) {
            case 0:
                ASSERT_EQ(0, *iter);
                break;
            case 1:
                ASSERT_EQ(1025, *iter);
                break;
            case 2:
                ASSERT_EQ(1026, *iter);
                break;
            case 3:
                ASSERT_EQ(UINT32_MAX, *iter);
                break;
            case 4:
                ASSERT_EQ(UINT64_MAX, *iter);
                break;
            default:
                ASSERT_TRUE(false); 
        }
    }
}
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
