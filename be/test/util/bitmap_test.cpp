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

#include "util/bitmap.h"

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"

namespace doris {

class BitMapTest : public testing::Test {
public:
    BitMapTest() { }
    virtual ~BitMapTest() {
    }
};

TEST_F(BitMapTest, normal) {
    // bitmap size
    ASSERT_EQ(0, BitmapSize(0));
    ASSERT_EQ(1, BitmapSize(1));
    ASSERT_EQ(1, BitmapSize(8));
    ASSERT_EQ(2, BitmapSize(9));

    // set, test, clear
    uint8_t bitmap[1024];
    memset(bitmap, 0, 1024);
    ASSERT_FALSE(BitmapTest(bitmap, 123));
    BitmapSet(bitmap, 123);
    ASSERT_TRUE(BitmapTest(bitmap, 123));
    BitmapClear(bitmap, 123);
    ASSERT_FALSE(BitmapTest(bitmap, 123));
    BitmapChange(bitmap, 112, true);
    ASSERT_TRUE(BitmapTest(bitmap, 112));
    BitmapChange(bitmap, 112, false);
    ASSERT_FALSE(BitmapTest(bitmap, 112));

    // change bits
    BitmapChangeBits(bitmap, 100, 200, true);
    for (int i = 0; i < 200; i++) {
        ASSERT_TRUE(BitmapTest(bitmap, 100 + i));
    }

    // Find fist
    bool found = false;
    size_t idx;
    found = BitmapFindFirstSet(bitmap, 0, 1024 * 8, &idx);
    ASSERT_TRUE(found);
    ASSERT_EQ(100, idx);

    found = BitmapFindFirstZero(bitmap, 200, 1024 * 8, &idx);
    ASSERT_TRUE(found);
    ASSERT_EQ(300, idx);

    found = BitmapFindFirstSet(bitmap, 300, 1024 * 8, &idx);
    ASSERT_FALSE(found);

    found = BitmapFindFirstZero(bitmap, 300, 1024 * 8, &idx);
    ASSERT_TRUE(found);
    ASSERT_EQ(300, idx);

}

TEST_F(BitMapTest, iterator) {
    uint8_t bitmap[1024];
    memset(bitmap, 0, 1024);

    for (int i = 100; i < 2000; ++i) {
        BitmapSet(bitmap, i);
    }

    for (int i = 2100; i < 3000; ++i) {
        BitmapSet(bitmap, i);
    }

    BitmapIterator iter(bitmap, 1024 * 8);
    ASSERT_FALSE(iter.done());

    bool value;
    // 0,100 --- false
    auto run = iter.Next(&value);
    ASSERT_FALSE(value);
    ASSERT_EQ(100, run);
    // 100,2000 -- true
    run = iter.Next(&value);
    ASSERT_TRUE(value);
    ASSERT_EQ(2000 - 100, run);
    // 2000,2100 -- false
    run = iter.Next(&value);
    ASSERT_FALSE(value);
    ASSERT_EQ(2100 - 2000, run);
    // 2100,3000 -- true
    run = iter.Next(&value);
    ASSERT_TRUE(value);
    ASSERT_EQ(3000 - 2100, run);
    // 3000,8*1024 -- false
    run = iter.Next(&value);
    ASSERT_FALSE(value);
    ASSERT_EQ(8*1024 - 3000, run);
    ASSERT_TRUE(iter.done());
    // seek to 8000
    iter.SeekTo(8000);
    run = iter.Next(&value);
    ASSERT_FALSE(value);
    ASSERT_EQ(8*1024 - 8000, run);
    ASSERT_TRUE(iter.done());

    // with max_run
    iter.SeekTo(200);
    run = iter.Next(&value, 500);
    ASSERT_TRUE(value);
    ASSERT_EQ(500, run);
    run = iter.Next(&value);
    ASSERT_TRUE(value);
    ASSERT_EQ(2000 - 500 - 200, run);
}

TEST_F(BitMapTest, roaring_bitmap_union) {
    RoaringBitmap empty;
    RoaringBitmap single(1024);
    RoaringBitmap bitmap;
    bitmap.update(1024);
    bitmap.update(1025);
    bitmap.update(1026);

    ASSERT_EQ(0, empty.cardinality());
    ASSERT_EQ(1, single.cardinality());
    ASSERT_EQ(3, bitmap.cardinality());

    RoaringBitmap empty2;
    empty2.merge(empty);
    ASSERT_EQ(0, empty2.cardinality());
    empty2.merge(single);
    ASSERT_EQ(1, empty2.cardinality());
    RoaringBitmap empty3;
    empty3.merge(bitmap);
    ASSERT_EQ(3, empty3.cardinality());

    RoaringBitmap single2(1025);
    single2.merge(empty);
    ASSERT_EQ(1, single2.cardinality());
    single2.merge(single);
    ASSERT_EQ(2, single2.cardinality());
    RoaringBitmap single3(1027);
    single3.merge(bitmap);
    ASSERT_EQ(4, single3.cardinality());

    RoaringBitmap bitmap2;
    bitmap2.update(1024);
    bitmap2.update(2048);
    bitmap2.update(4096);
    bitmap2.merge(empty);
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2.merge(single);
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2.merge(bitmap);
    ASSERT_EQ(5, bitmap2.cardinality());
}

TEST_F(BitMapTest, roaring_bitmap_intersect) {
    RoaringBitmap empty;
    RoaringBitmap single(1024);
    RoaringBitmap bitmap;
    bitmap.update(1024);
    bitmap.update(1025);
    bitmap.update(1026);

    RoaringBitmap empty2;
    empty2.intersect(empty);
    ASSERT_EQ(0, empty2.cardinality());
    empty2.intersect(single);
    ASSERT_EQ(0, empty2.cardinality());
    empty2.intersect(bitmap);
    ASSERT_EQ(0, empty2.cardinality());

    RoaringBitmap single2(1025);
    single2.intersect(empty);
    ASSERT_EQ(0, single2.cardinality());

    RoaringBitmap single4(1025);
    single4.intersect(single);
    ASSERT_EQ(0, single4.cardinality());

    RoaringBitmap single3(1024);
    single3.intersect(single);
    ASSERT_EQ(1, single3.cardinality());

    single3.intersect(bitmap);
    ASSERT_EQ(1, single3.cardinality());

    RoaringBitmap single5(2048);
    single5.intersect(bitmap);
    ASSERT_EQ(0, single5.cardinality());

    RoaringBitmap bitmap2;
    bitmap2.update(1024);
    bitmap2.update(2048);
    bitmap2.intersect(empty);
    ASSERT_EQ(0, bitmap2.cardinality());

    RoaringBitmap bitmap3;
    bitmap3.update(1024);
    bitmap3.update(2048);
    bitmap3.intersect(single);
    ASSERT_EQ(1, bitmap3.cardinality());

    RoaringBitmap bitmap4;
    bitmap4.update(2049);
    bitmap4.update(2048);
    bitmap4.intersect(single);
    ASSERT_EQ(0, bitmap4.cardinality());

    RoaringBitmap bitmap5;
    bitmap5.update(2049);
    bitmap5.update(2048);
    bitmap5.intersect(bitmap);
    ASSERT_EQ(0, bitmap5.cardinality());

    RoaringBitmap bitmap6;
    bitmap6.update(1024);
    bitmap6.update(1025);
    bitmap6.intersect(bitmap);
    ASSERT_EQ(2, bitmap6.cardinality());
}

std::string convert_bitmap_to_string(RoaringBitmap& bitmap) {
    std::string buf;
    buf.resize(bitmap.size());
    bitmap.serialize((char*)buf.c_str());
    return buf;
}

TEST_F(BitMapTest, roaring_bitmap_serde) {
    RoaringBitmap empty;
    RoaringBitmap single(1024);
    RoaringBitmap bitmap;
    bitmap.update(1024);
    bitmap.update(1025);
    bitmap.update(1026);

    std::string buffer = convert_bitmap_to_string(empty);
    RoaringBitmap empty_serde((char*)buffer.c_str());
    ASSERT_EQ(0, empty_serde.cardinality());

    buffer = convert_bitmap_to_string(single);
    RoaringBitmap single_serde((char*)buffer.c_str());
    ASSERT_EQ(1, single_serde.cardinality());

    buffer = convert_bitmap_to_string(bitmap);
    RoaringBitmap bitmap_serde((char*)buffer.c_str());
    ASSERT_EQ(3, bitmap_serde.cardinality());
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

