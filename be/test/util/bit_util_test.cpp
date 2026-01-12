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

#include "util/bit_util.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <bit>
#include <boost/utility/binary.hpp>
#include <random>

#include "gtest/gtest_pred_impl.h"
#include "runtime/primitive_type.h"
#include "util/simd/bits.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/endian.h"

namespace doris {

TEST(BitUtil, Ceil) {
    EXPECT_EQ(BitUtil::ceil(0, 1), 0);
    EXPECT_EQ(BitUtil::ceil(1, 1), 1);
    EXPECT_EQ(BitUtil::ceil(1, 2), 1);
    EXPECT_EQ(BitUtil::ceil(1, 8), 1);
    EXPECT_EQ(BitUtil::ceil(7, 8), 1);
    EXPECT_EQ(BitUtil::ceil(8, 8), 1);
    EXPECT_EQ(BitUtil::ceil(9, 8), 2);
}

TEST(BitUtil, BigEndianToHost) {
    uint16_t v16 = 0x1234;
    uint32_t v32 = 0x12345678;
    uint64_t v64 = 0x123456789abcdef0;
    unsigned __int128 v128 = ((__int128)0x123456789abcdef0LL << 64) | 0x123456789abcdef0LL;
    wide::UInt256 v256 =
            wide::UInt256(0x123456789abcdef0) << 192 | wide::UInt256(0x123456789abcdef0) << 128 |
            wide::UInt256(0x123456789abcdef0) << 64 | wide::UInt256(0x123456789abcdef0);
    EXPECT_EQ(to_endian<std::endian::big>(v16), 0x3412);
    EXPECT_EQ(to_endian<std::endian::big>(v32), 0x78563412);
    EXPECT_EQ(to_endian<std::endian::big>(v64), 0xf0debc9a78563412);
    EXPECT_EQ(to_endian<std::endian::big>(v128),
              ((__int128)0xf0debc9a78563412LL << 64) | 0xf0debc9a78563412LL);
    EXPECT_EQ(to_endian<std::endian::big>(v256),
              wide::UInt256(0xf0debc9a78563412) << 192 | wide::UInt256(0xf0debc9a78563412) << 128 |
                      wide::UInt256(0xf0debc9a78563412) << 64 | wide::UInt256(0xf0debc9a78563412));
}

void insert_true(vectorized::ColumnNullable* column, size_t num = 1) {
    for (int i = 0; i < num; i++) {
        assert_cast<vectorized::ColumnUInt8*>(column->get_nested_column_ptr().get())
                ->insert_value(1);
        column->push_false_to_nullmap(1);
    }
}

void insert_false(vectorized::ColumnNullable* column, size_t num = 1) {
    for (int i = 0; i < num; i++) {
        assert_cast<vectorized::ColumnUInt8*>(column->get_nested_column_ptr().get())
                ->insert_value(0);
        column->push_false_to_nullmap(1);
    }
}

void insert_null(vectorized::ColumnNullable* column, size_t num = 1) {
    for (int i = 0; i < num; i++) {
        column->insert_default();
    }
}

size_t brute_force_count_zero_num(const uint8_t* __restrict data,
                                  const uint8_t* __restrict null_map, size_t size) {
    size_t num = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == 0 || null_map[i]) {
            num++;
        }
    }
    return num;
}

TEST(BitUtil, CountZero) {
    {
        auto column = vectorized::ColumnNullable::create(vectorized::ColumnUInt8::create(),
                                                         vectorized::ColumnUInt8::create());
        insert_false(column.get(), 5);
        insert_null(column.get(), 1);
        insert_false(column.get(), 8);
        insert_null(column.get(), 1);
        insert_false(column.get(), 54);
        insert_true(column.get(), 1);
        insert_false(column.get(), 14);
        ASSERT_EQ(
                brute_force_count_zero_num(assert_cast<const vectorized::ColumnUInt8*>(
                                                   column->get_nested_column_ptr().get())
                                                   ->get_data()
                                                   .data(),
                                           column->get_null_map_data().data(), column->size()),
                simd::count_zero_num((int8_t*)assert_cast<const vectorized::ColumnUInt8*>(
                                             column->get_nested_column_ptr().get())
                                             ->get_data()
                                             .data(),
                                     column->get_null_map_data().data(), (uint32_t)column->size()));
    }

    {
        auto column = vectorized::ColumnNullable::create(vectorized::ColumnUInt8::create(),
                                                         vectorized::ColumnUInt8::create());
        std::mt19937 rng(12345);
        std::uniform_int_distribution<int> val_dist(0, 1);
        std::uniform_int_distribution<int> null_dist(0, 5);
        for (int i = 0; i < 10000; ++i) {
            if (null_dist(rng) == 0) {
                insert_null(column.get(), 1);
            } else {
                if (val_dist(rng) == 0) {
                    insert_false(column.get(), 1);
                } else {
                    insert_true(column.get(), 1);
                }
            }
        }
        ASSERT_EQ(
                brute_force_count_zero_num(assert_cast<const vectorized::ColumnUInt8*>(
                                                   column->get_nested_column_ptr().get())
                                                   ->get_data()
                                                   .data(),
                                           column->get_null_map_data().data(), column->size()),
                simd::count_zero_num((int8_t*)assert_cast<const vectorized::ColumnUInt8*>(
                                             column->get_nested_column_ptr().get())
                                             ->get_data()
                                             .data(),
                                     column->get_null_map_data().data(), (uint32_t)column->size()));
    }
}

} // namespace doris
