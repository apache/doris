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
#include <roaring/roaring.h>

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"
#include "util/coding.h"

namespace doris {
using roaring::Roaring;

TEST(BitmapValueTest, BitmapTypeCode_validate) {
    EXPECT_EQ(BitmapTypeCode::validate(BitmapTypeCode::TYPE_MAX + 1).code(), ErrorCode::CORRUPTION);
}

TEST(BitmapValueTest, Roaring64Map_ctors) {
    const std::vector<uint32_t> values({1, 3, 5, 7, 9, 2, 4, 6, 8, 1, 8, 9});
    detail::Roaring64Map roaring64_map(values.size(), values.data());
    EXPECT_EQ(roaring64_map.cardinality(), 9);
    EXPECT_TRUE(roaring64_map.contains(uint32_t(1)));
    EXPECT_TRUE(roaring64_map.contains(uint32_t(2)));
    EXPECT_TRUE(roaring64_map.contains(uint32_t(9)));
    EXPECT_FALSE(roaring64_map.contains(uint32_t(0)));

    const std::vector<uint64_t> values_long(
            {1, 3, 5, 7, 9, 2, 4, 6, 8, 1, 8, std::numeric_limits<uint64_t>::max()});
    detail::Roaring64Map roaring64_map2(values_long.size(), values_long.data());
    EXPECT_EQ(roaring64_map.cardinality(), 9);
    EXPECT_TRUE(roaring64_map2.contains(uint64_t(1)));
    EXPECT_TRUE(roaring64_map2.contains(uint64_t(2)));
    EXPECT_TRUE(roaring64_map2.contains(uint64_t(9)));
    EXPECT_TRUE(roaring64_map2.contains(std::numeric_limits<uint64_t>::max()));
    EXPECT_FALSE(roaring64_map2.contains(uint32_t(0)));

    auto roaring64_map_and = roaring64_map2 & roaring64_map;
    EXPECT_EQ(roaring64_map_and.cardinality(), 9);
    EXPECT_EQ(roaring64_map_and.cardinality(), roaring64_map2.andCardinality(roaring64_map));

    auto roaring64_map_xor = roaring64_map2 ^ roaring64_map;
    EXPECT_EQ(roaring64_map_xor.cardinality(), 1);
    EXPECT_TRUE(roaring64_map_xor.contains(std::numeric_limits<uint64_t>::max()));

    roaring64_map_and.swap(roaring64_map_xor);
    EXPECT_EQ(roaring64_map_and.cardinality(), 1);
    EXPECT_EQ(roaring64_map_xor.cardinality(), 9);

    roaring::Roaring roaring32(values.size(), values.data());
    detail::Roaring64Map roaring64_map3(roaring32);
    EXPECT_EQ(roaring64_map3.cardinality(), 9);
    EXPECT_TRUE(roaring64_map3.contains(uint32_t(1)));
    EXPECT_TRUE(roaring64_map3.contains(uint32_t(2)));
    EXPECT_TRUE(roaring64_map3.contains(uint32_t(9)));
    EXPECT_FALSE(roaring64_map3.contains(uint32_t(0)));

    auto roaring_t = roaring_bitmap_copy(&roaring32.roaring);
    detail::Roaring64Map roaring64_map4(roaring_t);
    EXPECT_EQ(roaring64_map3.cardinality(), 9);
    EXPECT_TRUE(roaring64_map4.contains(uint32_t(1)));
    EXPECT_TRUE(roaring64_map4.contains(uint32_t(2)));
    EXPECT_TRUE(roaring64_map4.contains(uint32_t(9)));
    EXPECT_FALSE(roaring64_map4.contains(uint32_t(0)));

    auto roaring64_map5 = detail::Roaring64Map::bitmapOf(4, 0, 1, 4, 6);
    EXPECT_EQ(roaring64_map5.cardinality(), 4);
    EXPECT_TRUE(roaring64_map5.contains(uint32_t(0)));
    EXPECT_TRUE(roaring64_map5.contains(uint32_t(1)));
    EXPECT_TRUE(roaring64_map5.contains(uint32_t(4)));
    EXPECT_TRUE(roaring64_map5.contains(uint32_t(6)));
}

TEST(BitmapValueTest, Roaring64Map_add_remove) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({1, 3, 5, 7, 9, 2, 4, 6, 8, 1, 8, 9});
    roaring64_map.addMany(values.size(), values.data());
    EXPECT_EQ(roaring64_map.cardinality(), 9);

    const std::vector<uint16_t> values_short({1, 3, 5, 7, 8, 9, 100});
    roaring64_map.addMany(values_short.size(), values_short.data());
    EXPECT_EQ(roaring64_map.cardinality(), 10);

    const std::vector<uint64_t> values_long(
            {1, 3, 5, 7, 9, 2, 4, 6, 8, 1, 8, std::numeric_limits<uint64_t>::max()});
    roaring64_map.addMany(values_long.size(), values_long.data());
    EXPECT_EQ(roaring64_map.cardinality(), 11);

    roaring64_map.add(uint64_t(1000));
    EXPECT_TRUE(roaring64_map.contains(uint64_t(1000)));
    EXPECT_EQ(roaring64_map.cardinality(), 12);

    roaring64_map.addChecked(uint64_t(1001));
    EXPECT_TRUE(roaring64_map.contains(uint64_t(1001)));
    EXPECT_EQ(roaring64_map.cardinality(), 13);

    roaring64_map.addChecked(uint32_t(1002));
    EXPECT_TRUE(roaring64_map.contains(uint32_t(1002)));
    EXPECT_EQ(roaring64_map.cardinality(), 14);

    roaring64_map.remove(uint32_t(8));
    EXPECT_FALSE(roaring64_map.contains(uint32_t(8)));
    EXPECT_FALSE(roaring64_map.contains(uint64_t(8)));

    roaring64_map.remove(uint64_t(8));
    EXPECT_FALSE(roaring64_map.contains(uint64_t(8)));

    roaring64_map.removeChecked(uint32_t(9));
    EXPECT_FALSE(roaring64_map.contains(uint32_t(9)));
    EXPECT_FALSE(roaring64_map.contains(uint64_t(9)));

    roaring64_map.removeChecked(uint64_t(9));
    EXPECT_FALSE(roaring64_map.contains(uint64_t(9)));
}

TEST(BitmapValueTest, Roaring64Map_cardinality) {
    detail::Roaring64Map roaring64_map1, roaring64_map2;

    EXPECT_TRUE(roaring64_map1.isEmpty());
    EXPECT_FALSE(roaring64_map1.contains(std::numeric_limits<uint32_t>::max()));
    EXPECT_FALSE(roaring64_map2.contains(std::numeric_limits<uint64_t>::max()));
    EXPECT_EQ(roaring64_map1.minimum(), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(roaring64_map1.maximum(), std::numeric_limits<uint64_t>::min());

    const std::vector<uint32_t> values1({0, 1, 2, 3, 4, 5, 6});
    const std::vector<uint32_t> values2({3, 4, 5, 6, 7, 8, 9});

    roaring64_map1.addMany(values1.size(), values1.data());
    roaring64_map2.addMany(values2.size(), values2.data());

    EXPECT_EQ(roaring64_map1.xorCardinality(roaring64_map2), 6);
    EXPECT_EQ(roaring64_map1.orCardinality(roaring64_map2), 10);
    EXPECT_EQ(roaring64_map1.andCardinality(roaring64_map2), 4);
}

TEST(BitmapValueTest, Roaring64Map_subset) {
    detail::Roaring64Map roaring64_map1, roaring64_map2;
    EXPECT_TRUE(roaring64_map1.isEmpty());

    const std::vector<uint32_t> values1({0, 1, 2, 3, 4, 5, 6});
    const std::vector<uint32_t> values2({3, 4, 5, 6, 7, 8, 9});

    roaring64_map1.addMany(values1.size(), values1.data());
    roaring64_map2.addMany(values2.size(), values2.data());

    EXPECT_FALSE(roaring64_map2.isSubset(roaring64_map1));
    EXPECT_FALSE(roaring64_map1.isSubset(roaring64_map2));

    auto roaring64_map_and = roaring64_map1 & roaring64_map2;
    EXPECT_TRUE(roaring64_map_and.isSubset(roaring64_map1));
    EXPECT_TRUE(roaring64_map_and.isSubset(roaring64_map2));

    EXPECT_TRUE(roaring64_map1.isSubset(roaring64_map1));
    EXPECT_TRUE(!roaring64_map1.isStrictSubset(roaring64_map1));
    EXPECT_TRUE(roaring64_map_and.isStrictSubset(roaring64_map1));
    EXPECT_TRUE(roaring64_map_and.isStrictSubset(roaring64_map2));
}

TEST(BitmapValueTest, Roaring64Map_toUint64Array) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 2, 3, 4, 5, 6});
    roaring64_map.addMany(values.size(), values.data());

    uint64_t* ans = new uint64_t[values.size()];
    roaring64_map.toUint64Array(ans);
    auto* end = ans + values.size();
    EXPECT_NE(std::find(ans, ans + values.size(), 0), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 1), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 2), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 3), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 4), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 5), end);
    EXPECT_NE(std::find(ans, ans + values.size(), 6), end);

    delete[] ans;
}

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

TEST(BitmapValueTest, Roaring64Map_flip) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    roaring64_map.flip(0, 10);
    EXPECT_EQ(roaring64_map.cardinality(), 3);

    roaring64_map.flip(0, 10);
    EXPECT_EQ(roaring64_map.cardinality(), 7);

    roaring64_map.flip(0, uint64_t(std::numeric_limits<uint32_t>::max()) + 10);
    EXPECT_EQ(roaring64_map.cardinality(), 4294967297);
}

TEST(BitmapValueTest, Roaring64Map_removeRunCompression) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    EXPECT_FALSE(roaring64_map.removeRunCompression());
}

TEST(BitmapValueTest, Roaring64Map_shrinkToFit) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    EXPECT_EQ(roaring64_map.shrinkToFit(), 12);

    roaring64_map.add(std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(roaring64_map.shrinkToFit(), 11);

    roaring64_map.remove(std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(roaring64_map.shrinkToFit(), 88);
}

TEST(BitmapValueTest, Roaring64Map_iterate) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    std::vector<uint64_t> values2;
    roaring64_map.iterate(
            [](uint64_t low_bits, void* inner_iter_data) -> bool {
                if (low_bits == 7) {
                    return false;
                }
                reinterpret_cast<std::vector<uint64_t>*>(inner_iter_data)->emplace_back(low_bits);
                return true;
            },
            &values2);

    EXPECT_EQ(std::find(values2.begin(), values2.end(), 7), values2.cend());
}

TEST(BitmapValueTest, Roaring64Map_select) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    uint64_t value;
    roaring64_map.select(3, &value);
    EXPECT_EQ(value, 4);

    roaring64_map.add(std::numeric_limits<uint64_t>::max());
    roaring64_map.select(7, &value);
    EXPECT_EQ(value, std::numeric_limits<uint64_t>::max());

    EXPECT_FALSE(roaring64_map.select(8, &value));
}

TEST(BitmapValueTest, Roaring64Map_rank) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint64_t> values({0, 1, 3, 4, 5, 7, 8, 4294967295, 4294967296, 4294967297,
                                        4294967295 * 2, 4294967295 * 3, 4294967295 * 4,
                                        4294967295 * 5, 4294967295 * 5 + 1});
    roaring64_map.addMany(values.size(), values.data());

    EXPECT_EQ(roaring64_map.rank(0), 1);
    EXPECT_EQ(roaring64_map.rank(1), 2);
    EXPECT_EQ(roaring64_map.rank(3), 3);
    EXPECT_EQ(roaring64_map.rank(4), 4);
    EXPECT_EQ(roaring64_map.rank(5), 5);
    EXPECT_EQ(roaring64_map.rank(7), 6);
    EXPECT_EQ(roaring64_map.rank(8), 7);
    EXPECT_EQ(roaring64_map.rank(4294967296), 9);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 3 - 1), 11);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 3), 12);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 3 + 1), 12);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 5), 14);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 5 + 1), 15);
    EXPECT_EQ(roaring64_map.rank(4294967295 * 5 + 2), 15);
}

TEST(BitmapValueTest, Roaring64Map_write_read) {
    detail::Roaring64Map roaring64_map;

    // empty bitmap
    auto bytes = roaring64_map.getSizeInBytes(1);
    std::unique_ptr<char[]> buffer(new char[bytes]);

    roaring64_map.write(buffer.get(), 1);

    detail::Roaring64Map bitmap_read = detail::Roaring64Map::read(buffer.get());

    EXPECT_EQ(bitmap_read, roaring64_map);

    bytes = roaring64_map.getSizeInBytes(2);
    buffer.reset(new char[bytes]);

    roaring64_map.write(buffer.get(), 2);
    bitmap_read = detail::Roaring64Map::read(buffer.get());

    EXPECT_EQ(bitmap_read, roaring64_map);

    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    bytes = roaring64_map.getSizeInBytes(1);
    buffer.reset(new char[bytes]);

    roaring64_map.write(buffer.get(), 1);

    bitmap_read = detail::Roaring64Map::read(buffer.get());

    EXPECT_EQ(bitmap_read, roaring64_map);

    bytes = roaring64_map.getSizeInBytes(2);
    buffer.reset(new char[bytes]);

    roaring64_map.write(buffer.get(), 2);
    bitmap_read = detail::Roaring64Map::read(buffer.get());

    EXPECT_EQ(bitmap_read, roaring64_map);
}

TEST(BitmapValueTest, Roaring64Map_set_get_copy_on_write) {
    detail::Roaring64Map roaring64_map;
    const std::vector<uint32_t> values({0, 1, 3, 4, 5, 7, 8});
    roaring64_map.addMany(values.size(), values.data());

    roaring64_map.setCopyOnWrite(true);
    EXPECT_TRUE(roaring64_map.getCopyOnWrite());

    roaring64_map.setCopyOnWrite(true);
    EXPECT_TRUE(roaring64_map.getCopyOnWrite());

    roaring64_map.setCopyOnWrite(false);
    EXPECT_FALSE(roaring64_map.getCopyOnWrite());
}

TEST(BitmapValueTest, Roaring64Map_iterators) {
    detail::Roaring64Map roaring64_map;

    auto begin = roaring64_map.begin();
    auto end = roaring64_map.end();

    EXPECT_TRUE(begin == end);
    EXPECT_FALSE(begin < end);
    EXPECT_TRUE(begin <= end);
    EXPECT_TRUE(begin >= end);
    EXPECT_FALSE(begin != end);

    const std::vector<uint64_t> values({0, 1, 2, 3, 4, 5, 6, 4294967297, 4294967298});
    roaring64_map.addMany(values.size(), values.data());

    begin = roaring64_map.begin();
    end = roaring64_map.end();

    EXPECT_FALSE(begin == end);
    EXPECT_TRUE(begin < end);
    EXPECT_TRUE(begin <= end);
    EXPECT_FALSE(begin >= end);
    EXPECT_TRUE(begin != end);

    auto iter = roaring64_map.begin();
    while (iter != end) {
        EXPECT_TRUE(iter < end);
        ++iter;
    }

    iter = roaring64_map.begin();
    while (iter != end) {
        EXPECT_TRUE(iter < end);
        iter++;
    }

    iter = roaring64_map.begin();
    EXPECT_TRUE(iter.move(2));
    EXPECT_TRUE(iter.move(4294967296));
    EXPECT_FALSE(iter.move(4294967299));

    iter = roaring64_map.begin();
    auto iter2 = roaring64_map.begin();
    iter.move(3);
    iter2.move(3);
    EXPECT_TRUE(iter == iter2);
}

TEST(BitmapValueTest, set) {
    config::enable_set_in_bitmap_value = false;
    BitmapValue bitmap_value;

    bitmap_value.add(4294967297);
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SINGLE64);
    bitmap_value.reset();

    bitmap_value.add(10);
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SINGLE32);

    config::enable_set_in_bitmap_value = true;
    bitmap_value.add(11);

    BitmapValue bitmap_value2(bitmap_value);
    BitmapValue bitmap_value3(std::move(bitmap_value));

    EXPECT_EQ(bitmap_value2.get_type_code(), BitmapTypeCode::SET);
    EXPECT_EQ(bitmap_value3.get_type_code(), BitmapTypeCode::SET);
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::EMPTY);

    bitmap_value2 &= bitmap_value3;
    EXPECT_EQ(bitmap_value2.cardinality(), bitmap_value3.cardinality());
    EXPECT_TRUE(bitmap_value.empty());

    auto bitmap_value4 = std::move(bitmap_value3);
    EXPECT_EQ(bitmap_value2.cardinality(), bitmap_value4.cardinality());

    std::vector<uint64_t> values(32);
    std::iota(values.begin(), values.end(), 0);

    BitmapValue bitmap_value5(values);
    EXPECT_EQ(bitmap_value5.get_type_code(), BitmapTypeCode::SET);

    values.clear();
    values.resize(64);
    std::iota(values.begin(), values.end(), 0);
    BitmapValue bitmap_value6(values);
    EXPECT_EQ(bitmap_value6.get_type_code(), BitmapTypeCode::BITMAP32);

    bitmap_value6.add(4294967297);
    EXPECT_EQ(bitmap_value6.get_type_code(), BitmapTypeCode::BITMAP64);

    config::enable_set_in_bitmap_value = false;
}

TEST(BitmapValueTest, add) {
    config::enable_set_in_bitmap_value = true;

    std::vector<uint64_t> values(1);
    std::iota(values.begin(), values.end(), 0);
    BitmapValue bitmap_value(values);

    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SINGLE32);

    bitmap_value.add(2);
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SET);

    bitmap_value.add(3);
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SET);
    EXPECT_EQ(bitmap_value.cardinality(), 3);

    bitmap_value.remove(3);
    EXPECT_EQ(bitmap_value.cardinality(), 2);
    EXPECT_FALSE(bitmap_value.contains(3));

    values.clear();
    values.resize(32);
    std::iota(values.begin(), values.end(), 1);
    bitmap_value.add_many(values.data(), values.size());
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::BITMAP32);

    values.clear();
    values.resize(32);
    std::iota(values.begin(), values.end(), 32);
    bitmap_value.add_many(values.data(), values.size());
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::BITMAP32);

    bitmap_value.reset();
    values.clear();
    values.resize(31);
    std::iota(values.begin(), values.end(), 0);

    bitmap_value.add_many(values.data(), values.size());
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::SET);

    values.clear();
    values.resize(32);
    std::iota(values.begin(), values.end(), 32);
    bitmap_value.add_many(values.data(), values.size());
    EXPECT_EQ(bitmap_value.get_type_code(), BitmapTypeCode::BITMAP32);
    config::enable_set_in_bitmap_value = false;
}

void check_bitmap_value_operator(const BitmapValue& left, const BitmapValue& right) {
    auto left_cardinality = left.cardinality();
    auto right_cardinality = right.cardinality();
    auto and_cardinality = left.and_cardinality(right);
    auto and_not_cardinality = left.andnot_cardinality(right);

    std::cout << "left_cardinality: " << left_cardinality
              << ", right_cardinality: " << right_cardinality
              << ", and_cardinality: " << and_cardinality
              << ", and_not_cardinality: " << and_not_cardinality << std::endl;

    EXPECT_EQ(left_cardinality, and_cardinality + and_not_cardinality);

    EXPECT_LE(and_cardinality, left_cardinality);
    EXPECT_LE(and_cardinality, right_cardinality);
    EXPECT_EQ(and_not_cardinality, left_cardinality - and_cardinality);

    auto copy = left;
    copy -= right;
    EXPECT_EQ(copy.cardinality(), and_not_cardinality);

    copy = left;
    copy |= right;
    std::cout << "copy.cardinality(): " << copy.cardinality() << std::endl;
    EXPECT_EQ(copy.cardinality(), left_cardinality + right_cardinality - and_cardinality);

    copy = left;
    copy &= right;
    EXPECT_EQ(copy.cardinality(), and_cardinality);

    copy = left;
    copy ^= right;
    EXPECT_EQ(copy.cardinality(), left_cardinality + right_cardinality - and_cardinality * 2);
}

// '='
TEST(BitmapValueTest, copy_operator) {
    BitmapValue test_bitmap;

    std::vector<uint64_t> values1(31);
    BitmapValue bitmap;
    values1.resize(128);
    std::iota(values1.begin(), values1.begin() + 16, 0);
    std::iota(values1.begin() + 16, values1.begin() + 32, 4294967297);
    std::iota(values1.begin() + 32, values1.begin() + 64, 8589934594);
    std::iota(values1.begin() + 64, values1.end(), 42949672970);
    bitmap.add_many(values1.data(), values1.size());

    test_bitmap = bitmap; //should be bitmap
    EXPECT_EQ(test_bitmap.cardinality(), bitmap.cardinality());
    EXPECT_EQ(test_bitmap.to_string(), bitmap.to_string());

    BitmapValue single(1);
    test_bitmap = single; //should be single
    EXPECT_EQ(test_bitmap.cardinality(), 1);
    EXPECT_EQ(test_bitmap.cardinality(), single.cardinality());
    EXPECT_EQ(test_bitmap.to_string(), single.to_string());

    BitmapValue empty;
    test_bitmap = empty; // should be empty
    EXPECT_TRUE(test_bitmap.empty());

    BitmapValue bitmap2(bitmap);
    EXPECT_EQ(bitmap2.to_string(), bitmap.to_string());
    bitmap2 = bitmap;
    EXPECT_EQ(bitmap2.to_string(), bitmap.to_string());
}

// '-=', '|=', '&=', '^='
TEST(BitmapValueTest, operators) {
    config::enable_set_in_bitmap_value = true;
    BitmapValue left_empty;
    BitmapValue right_emtpy;

    BitmapValue left_single(1);
    BitmapValue right_single(2);

    BitmapValue left_set;
    BitmapValue right_set;

    std::vector<uint64_t> values1(31);
    std::vector<uint64_t> values2(31);

    std::iota(values1.begin(), values1.end(), 0);
    std::iota(values2.begin(), values2.end(), 17);

    left_set.add_many(values1.data(), values1.size());
    right_set.add_many(values2.data(), values2.size());

    BitmapValue left_bitmap;
    BitmapValue right_bitmap;

    values1.resize(128);
    values2.resize(128);

    std::iota(values1.begin(), values1.begin() + 16, 0);
    std::iota(values2.begin(), values2.begin() + 16, 8);

    std::iota(values1.begin() + 16, values1.begin() + 32, 4294967297);
    std::iota(values2.begin() + 16, values2.begin() + 32, 4294967305);

    std::iota(values1.begin() + 32, values1.begin() + 64, 8589934594);
    std::iota(values2.begin() + 32, values2.begin() + 64, 8589934626);

    std::iota(values1.begin() + 64, values1.end(), 42949672970);
    std::iota(values2.begin() + 64, values2.end(), 42949673002);

    left_bitmap.add_many(values1.data(), values1.size());
    right_bitmap.add_many(values2.data(), values2.size());

    check_bitmap_value_operator(left_empty, right_emtpy);
    check_bitmap_value_operator(left_empty, right_single);
    check_bitmap_value_operator(left_empty, right_set);
    check_bitmap_value_operator(left_empty, right_bitmap);

    check_bitmap_value_operator(left_single, right_emtpy);
    check_bitmap_value_operator(left_single, right_single);
    check_bitmap_value_operator(left_single, right_set);
    check_bitmap_value_operator(left_single, right_bitmap);

    check_bitmap_value_operator(left_set, right_emtpy);
    check_bitmap_value_operator(left_set, right_single);
    check_bitmap_value_operator(left_set, right_set);
    check_bitmap_value_operator(left_set, right_bitmap);

    check_bitmap_value_operator(left_bitmap, right_emtpy);
    check_bitmap_value_operator(left_bitmap, right_single);
    check_bitmap_value_operator(left_bitmap, right_set);
    check_bitmap_value_operator(left_bitmap, right_bitmap);

    config::enable_set_in_bitmap_value = false;
}

void check_bitmap_equal(const BitmapValue& left, const BitmapValue& right) {
    EXPECT_EQ(left.cardinality(), right.cardinality());
    EXPECT_EQ(left.minimum(), right.minimum());
    EXPECT_EQ(left.maximum(), right.maximum());

    for (auto v : left) {
        EXPECT_TRUE(right.contains(v));
    }

    for (auto v : right) {
        EXPECT_TRUE(left.contains(v));
    }
}

TEST(BitmapValueTest, write_read) {
    config::enable_set_in_bitmap_value = true;
    BitmapValue bitmap_empty;
    BitmapValue bitmap_single(1);
    BitmapValue bitmap_set;
    BitmapValue bitmap;

    std::vector<uint64_t> values1(31);

    std::iota(values1.begin(), values1.end(), 0);

    bitmap_set.add_many(values1.data(), values1.size());

    values1.resize(128);

    std::iota(values1.begin(), values1.begin() + 16, 0);
    std::iota(values1.begin() + 16, values1.begin() + 32, 4294967297);
    std::iota(values1.begin() + 32, values1.begin() + 64, 8589934594);
    std::iota(values1.begin() + 64, values1.end(), 42949672970);

    bitmap.add_many(values1.data(), values1.size());

    auto size = bitmap_empty.getSizeInBytes();
    std::unique_ptr<char[]> buffer(new char[size]);

    bitmap_empty.write_to(buffer.get());
    BitmapValue deserialized(buffer.get());

    check_bitmap_equal(deserialized, bitmap_empty);

    size = bitmap_single.getSizeInBytes();
    buffer.reset(new char[size]);

    bitmap_single.write_to(buffer.get());
    deserialized.reset();
    deserialized.deserialize(buffer.get());

    check_bitmap_equal(deserialized, bitmap_single);

    size = bitmap_set.getSizeInBytes();
    buffer.reset(new char[size]);

    bitmap_set.write_to(buffer.get());
    deserialized.reset();
    deserialized.deserialize(buffer.get());

    check_bitmap_equal(deserialized, bitmap_set);

    size = bitmap.getSizeInBytes();
    buffer.reset(new char[size]);

    bitmap.write_to(buffer.get());
    deserialized.reset();
    deserialized.deserialize(buffer.get());

    check_bitmap_equal(deserialized, bitmap);

    config::enable_set_in_bitmap_value = false;
}

TEST(BitmapValueTest, set_to_string) {
    config::enable_set_in_bitmap_value = true;
    BitmapValue bitmap;
    bitmap.add(1);
    bitmap.add(2);
    bitmap.add(3);

    EXPECT_EQ(bitmap.get_type_code(), BitmapTypeCode::SET);

    EXPECT_EQ(bitmap.to_string(), "1,2,3");
    config::enable_set_in_bitmap_value = false;
}

TEST(BitmapValueTest, min_max) {
    config::enable_set_in_bitmap_value = true;
    BitmapValue bitmap;

    bool empty {false};
    auto min = bitmap.min(&empty);
    EXPECT_TRUE(empty);
    auto max = bitmap.max(&empty);
    EXPECT_TRUE(empty);
    EXPECT_EQ(min, 0);
    EXPECT_EQ(max, 0);

    bitmap.add(1);
    min = bitmap.min(&empty);
    EXPECT_FALSE(empty);
    max = bitmap.max(&empty);
    EXPECT_FALSE(empty);
    EXPECT_EQ(min, 1);
    EXPECT_EQ(max, 1);

    bitmap.add(2);
    bitmap.add(3);
    min = bitmap.min(&empty);
    EXPECT_FALSE(empty);
    max = bitmap.max(&empty);
    EXPECT_FALSE(empty);
    EXPECT_EQ(min, 1);
    EXPECT_EQ(max, 3);

    std::vector<uint64_t> values(128);

    std::iota(values.begin(), values.begin() + 16, 0);
    std::iota(values.begin() + 16, values.begin() + 32, 4294967297);
    std::iota(values.begin() + 32, values.begin() + 64, 8589934594);
    std::iota(values.begin() + 64, values.end(), 42949672970);

    bitmap.add_many(values.data(), values.size());
    min = bitmap.min(&empty);
    EXPECT_FALSE(empty);
    max = bitmap.max(&empty);
    EXPECT_FALSE(empty);
    EXPECT_EQ(min, 0);
    EXPECT_EQ(max, bitmap.maximum());
    EXPECT_EQ(max, 42949672970 + 63);

    config::enable_set_in_bitmap_value = false;
}

TEST(BitmapValueTest, sub_range_limit) {
    config::enable_set_in_bitmap_value = true;
    BitmapValue bitmap;

    BitmapValue out;
    auto ret = bitmap.sub_range(0, 100, &out);
    EXPECT_EQ(ret, 0);

    EXPECT_TRUE(out.empty());

    ret = bitmap.sub_limit(0, 1, &out);
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(out.empty());

    ret = bitmap.offset_limit(0, 1, &out);
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(out.empty());

    BitmapValue bitmap_single(1);
    ret = bitmap_single.sub_range(0, 100, &out);
    EXPECT_EQ(ret, 1);

    ret = bitmap_single.sub_range(0, 1, &out);
    EXPECT_EQ(ret, 0);

    ret = bitmap_single.sub_limit(0, 100, &out);
    EXPECT_EQ(ret, 1);

    ret = bitmap_single.sub_limit(0, 1, &out);
    EXPECT_EQ(ret, 1);

    ret = bitmap_single.offset_limit(0, 100, &out);
    EXPECT_EQ(ret, 1);

    ret = bitmap_single.offset_limit(1, 1, &out);
    EXPECT_EQ(ret, 0);

    bitmap.add(1);
    bitmap.add(2);
    ret = bitmap.sub_range(0, 100, &out);
    EXPECT_EQ(ret, 2);

    ret = bitmap.sub_range(0, 2, &out);
    EXPECT_EQ(ret, 1);

    ret = bitmap.sub_limit(0, 100, &out);
    EXPECT_EQ(ret, 2);

    ret = bitmap.sub_limit(0, 2, &out);
    EXPECT_EQ(ret, 2);

    ret = bitmap.offset_limit(0, 100, &out);
    EXPECT_EQ(ret, 2);

    ret = bitmap.offset_limit(1, 2, &out);
    EXPECT_EQ(ret, 1);

    std::vector<uint64_t> values(128);

    std::iota(values.begin(), values.begin() + 16, 0);
    std::iota(values.begin() + 16, values.begin() + 32, 4294967297);
    std::iota(values.begin() + 32, values.begin() + 64, 8589934594);
    std::iota(values.begin() + 64, values.end(), 42949672970);

    bitmap.add_many(values.data(), values.size());
    ret = bitmap.sub_range(0, 42949672970 + 64, &out);
    EXPECT_EQ(ret, bitmap.cardinality());

    ret = bitmap.sub_range(0, 100, &out);
    EXPECT_EQ(ret, 16);

    ret = bitmap.offset_limit(0, 10, &out);
    EXPECT_EQ(ret, 10);
    ret = bitmap.offset_limit(20, 10, &out);
    EXPECT_EQ(ret, 10);
    ret = bitmap.offset_limit(100, 10, &out);
    EXPECT_EQ(ret, 10);
    config::enable_set_in_bitmap_value = false;
}

void bitmap_checker_for_all_type(const std::function<void(const BitmapValue&)>& checker) {
    BitmapValue bitmap_empty;
    BitmapValue bitmap_single(1);
    BitmapValue bitmap_set;
    BitmapValue bitmap;

    std::vector<uint64_t> values1(31);

    std::iota(values1.begin(), values1.end(), 0);

    bitmap_set.add_many(values1.data(), values1.size());

    values1.resize(128);

    std::iota(values1.begin(), values1.begin() + 16, 0);
    std::iota(values1.begin() + 16, values1.begin() + 32, 4294967297);
    std::iota(values1.begin() + 32, values1.begin() + 64, 8589934594);
    std::iota(values1.begin() + 64, values1.end(), 42949672970);

    bitmap.add_many(values1.data(), values1.size());

    checker(bitmap_empty);
    checker(bitmap_single);
    checker(bitmap_set);
    checker(bitmap);
}

TEST(BitmapValueTest, to_array) {
    config::enable_set_in_bitmap_value = true;
    auto checker = [](const BitmapValue& bitmap) {
        auto size = bitmap.cardinality();
        if (size == 0) {
            EXPECT_TRUE(bitmap.empty());
            return;
        }

        vectorized::PaddedPODArray<int64_t> data;
        bitmap.to_array(data);

        for (size_t i = 0; i != size; ++i) {
            EXPECT_TRUE(bitmap.contains(data[i]));
        }
    };

    bitmap_checker_for_all_type(checker);
    config::enable_set_in_bitmap_value = false;
}

TEST(BitmapValueTest, BitmapValueIterator) {
    config::enable_set_in_bitmap_value = true;
    auto checker = [](const BitmapValue& bitmap) {
        auto begin = bitmap.begin();
        const auto end = bitmap.end();

        auto iter = begin;
        while (iter != end) {
            EXPECT_TRUE(bitmap.contains(*iter));
            ++iter;
        }
        EXPECT_EQ(end, iter);

        auto iter2 = begin;
        while (iter2 != end) {
            EXPECT_TRUE(bitmap.contains(*iter2));
            iter2++;
        }
        EXPECT_EQ(end, iter2);
    };

    bitmap_checker_for_all_type(checker);
    config::enable_set_in_bitmap_value = false;
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
