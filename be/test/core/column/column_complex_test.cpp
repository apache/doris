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

#include "core/column/column_complex.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "core/string_ref.h"

namespace doris {

static std::vector<char> make_garbage_buffer(size_t n) {
    std::vector<char> buf(n);
    for (size_t i = 0; i < n; ++i) {
        buf[i] = static_cast<char>(0xAB);
    }
    return buf;
}

TEST(ColumnComplexTest, InsertBinaryDataZeroLengthBitmap) {
    auto col = ColumnBitmap::create();
    auto garbage = make_garbage_buffer(64);

    col->insert_binary_data(garbage.data(), 0);

    ASSERT_EQ(col->size(), 1);
    EXPECT_EQ(col->get_element(0).cardinality(), 0);
}

TEST(ColumnComplexTest, InsertBinaryDataZeroLengthHLL) {
    auto col = ColumnHLL::create();
    auto garbage = make_garbage_buffer(64);

    col->insert_binary_data(garbage.data(), 0);

    ASSERT_EQ(col->size(), 1);
    EXPECT_EQ(col->get_element(0).estimate_cardinality(), 0);
}

TEST(ColumnComplexTest, InsertBinaryDataZeroLengthQuantileState) {
    auto col = ColumnQuantileState::create();
    auto garbage = make_garbage_buffer(64);

    col->insert_binary_data(garbage.data(), 0);

    ASSERT_EQ(col->size(), 1);
}

TEST(ColumnComplexTest, InsertBinaryDataNonEmptyBitmap) {
    BitmapValue bv;
    bv.add(1);
    bv.add(2);
    bv.add(3);
    size_t serialize_size = bv.getSizeInBytes();
    std::vector<char> buf(serialize_size);
    bv.write_to(buf.data());

    auto col = ColumnBitmap::create();
    col->insert_binary_data(buf.data(), serialize_size);

    ASSERT_EQ(col->size(), 1);
    EXPECT_EQ(col->get_element(0).cardinality(), 3);
    EXPECT_TRUE(col->get_element(0).contains(1));
    EXPECT_TRUE(col->get_element(0).contains(2));
    EXPECT_TRUE(col->get_element(0).contains(3));
}

TEST(ColumnComplexTest, InsertManyContinuousBinaryDataWithZeroLengthCells) {
    BitmapValue bv;
    bv.add(42);
    size_t bv_size = bv.getSizeInBytes();

    std::vector<char> payload(bv_size * 2);
    bv.write_to(payload.data());
    bv.write_to(payload.data() + bv_size);

    std::vector<uint32_t> offsets = {
            0,
            0,
            static_cast<uint32_t>(bv_size),
            static_cast<uint32_t>(bv_size),
            static_cast<uint32_t>(bv_size * 2),
    };

    auto col = ColumnBitmap::create();
    col->insert_many_continuous_binary_data(payload.data(), offsets.data(), 4);

    ASSERT_EQ(col->size(), 4);
    EXPECT_EQ(col->get_element(0).cardinality(), 0);
    EXPECT_EQ(col->get_element(1).cardinality(), 1);
    EXPECT_TRUE(col->get_element(1).contains(42));
    EXPECT_EQ(col->get_element(2).cardinality(), 0);
    EXPECT_EQ(col->get_element(3).cardinality(), 1);
    EXPECT_TRUE(col->get_element(3).contains(42));
}

TEST(ColumnComplexTest, InsertManyStringsWithZeroLength) {
    BitmapValue bv;
    bv.add(7);
    bv.add(8);
    size_t bv_size = bv.getSizeInBytes();
    std::vector<char> buf(bv_size);
    bv.write_to(buf.data());

    auto garbage = make_garbage_buffer(64);

    std::vector<StringRef> refs = {
            StringRef(garbage.data(), 0),
            StringRef(buf.data(), bv_size),
            StringRef(garbage.data(), 0),
    };

    auto col = ColumnBitmap::create();
    col->insert_many_strings(refs.data(), refs.size());

    ASSERT_EQ(col->size(), 3);
    EXPECT_EQ(col->get_element(0).cardinality(), 0);
    EXPECT_EQ(col->get_element(1).cardinality(), 2);
    EXPECT_TRUE(col->get_element(1).contains(7));
    EXPECT_TRUE(col->get_element(1).contains(8));
    EXPECT_EQ(col->get_element(2).cardinality(), 0);
}

} // namespace doris
