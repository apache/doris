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

#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "util/coding.h"

namespace doris {

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
    {   // EMPTY
        BitmapValue empty;
        std::string buffer = convert_bitmap_to_string(empty);
        std::string expect_buffer(1, BitmapTypeCode::EMPTY);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(0, out.cardinality());
    }
    {   // SINGLE32
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
    {   // BITMAP32
        BitmapValue bitmap32({ 0, UINT32_MAX });
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
    {   // SINGLE64
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
    {   // BITMAP64
        BitmapValue bitmap64({ 0, static_cast<uint64_t>(UINT32_MAX) + 1 });
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

TEST(BitmapValueTest, bitmap_to_string) {
    BitmapValue empty;
    ASSERT_STREQ("", empty.to_string().c_str());
    empty.add(1);
    ASSERT_STREQ("1", empty.to_string().c_str());
    empty.add(2);
    ASSERT_STREQ("1,2", empty.to_string().c_str());
}
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}