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

#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/dictionary.h"
#include "exprs/function/flat_dictionary.h"

namespace doris {

template <typename DataType>
static ColumnPtr flat_create_column_with_data(std::vector<typename DataType::FieldType> datas) {
    auto column = DataType::ColumnType::create();
    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        for (auto data : datas) {
            column->insert_data(data.data(), data.size());
        }
    } else {
        for (auto data : datas) {
            column->insert_value(data);
        }
    }
    return std::move(column);
}

template <typename DataType>
static ColumnWithTypeAndName flat_create_column(std::vector<typename DataType::FieldType> datas,
                                                std::string name) {
    return ColumnWithTypeAndName(flat_create_column_with_data<DataType>(datas),
                                 std::make_shared<DataType>(), name);
}

// Build a FLAT dictionary from an integer key column and value columns.
static DictionaryPtr build_flat_dict(const ColumnWithTypeAndName& key_data,
                                     const ColumnsWithTypeAndName& values_data) {
    return create_flat_dict_from_column("flat_dict", key_data, values_data);
}

// Happy path: keys 0,1 plus a sparse key; look up present and missing keys.
TEST(FlatDictTest, HappyPathAndMissingKey) {
    auto key = flat_create_column<DataTypeInt32>({0, 1, 100}, "key");
    auto dict = build_flat_dict(
            key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({10, 11, 12}, "v")});

    // query keys: 0 (hit), 1 (hit), 100 (hit sparse), 5 (miss)
    auto query = flat_create_column<DataTypeInt32>({0, 1, 100, 5}, "q");
    auto result =
            dict->get_column("v", std::make_shared<DataTypeInt64>(), query.column, query.type);

    ASSERT_EQ(result->size(), 4);
    const auto* nullable = assert_cast<const ColumnNullable*>(result.get());
    const auto* data = assert_cast<const ColumnInt64*>(nullable->get_nested_column_ptr().get());

    EXPECT_FALSE(nullable->is_null_at(0));
    EXPECT_EQ(data->get_element(0), 10);
    EXPECT_FALSE(nullable->is_null_at(1));
    EXPECT_EQ(data->get_element(1), 11);
    EXPECT_FALSE(nullable->is_null_at(2));
    EXPECT_EQ(data->get_element(2), 12);
    // missing key -> null
    EXPECT_TRUE(nullable->is_null_at(3));
}

// Key exactly at the max boundary (MAX_ARRAY_SIZE - 1) is accepted.
TEST(FlatDictTest, KeyAtMaxBoundaryAccepted) {
    int64_t boundary = FlatDictionary::MAX_ARRAY_SIZE - 1;
    auto key = flat_create_column<DataTypeInt64>({boundary}, "key");
    auto dict = build_flat_dict(
            key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({777}, "v")});

    auto query = flat_create_column<DataTypeInt64>({boundary}, "q");
    auto result =
            dict->get_column("v", std::make_shared<DataTypeInt64>(), query.column, query.type);
    const auto* nullable = assert_cast<const ColumnNullable*>(result.get());
    const auto* data = assert_cast<const ColumnInt64*>(nullable->get_nested_column_ptr().get());
    EXPECT_FALSE(nullable->is_null_at(0));
    EXPECT_EQ(data->get_element(0), 777);
}

// Key >= MAX_ARRAY_SIZE is rejected at load time (before allocation).
TEST(FlatDictTest, KeyAboveMaxRejected) {
    int64_t over = FlatDictionary::MAX_ARRAY_SIZE;
    auto key = flat_create_column<DataTypeInt64>({over}, "key");
    EXPECT_THROW(build_flat_dict(
                         key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({1}, "v")}),
                 doris::Exception);
}

// Negative key is rejected at load time.
TEST(FlatDictTest, NegativeKeyRejected) {
    auto key = flat_create_column<DataTypeInt32>({-1}, "key");
    EXPECT_THROW(build_flat_dict(
                         key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({1}, "v")}),
                 doris::Exception);
}

// Duplicate key is rejected at load time.
TEST(FlatDictTest, DuplicateKeyRejected) {
    auto key = flat_create_column<DataTypeInt32>({3, 3}, "key");
    EXPECT_THROW(build_flat_dict(key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>(
                                              {1, 2}, "v")}),
                 doris::Exception);
}

// Nullable value column: a present key whose value is null returns null.
TEST(FlatDictTest, NullableValue) {
    auto key = flat_create_column<DataTypeInt32>({0, 1}, "key");
    // value column is nullable; row 1 is null
    auto nested = DataTypeInt64::ColumnType::create();
    nested->insert_value(100);
    nested->insert_value(0);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    auto nullable_value = ColumnNullable::create(std::move(nested), std::move(null_map));
    ColumnWithTypeAndName value_data(
            std::move(nullable_value),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "v");

    auto dict = build_flat_dict(key, ColumnsWithTypeAndName {value_data});
    auto query = flat_create_column<DataTypeInt32>({0, 1}, "q");
    auto result =
            dict->get_column("v", std::make_shared<DataTypeInt64>(), query.column, query.type);
    const auto* nullable = assert_cast<const ColumnNullable*>(result.get());
    const auto* data = assert_cast<const ColumnInt64*>(nullable->get_nested_column_ptr().get());
    EXPECT_FALSE(nullable->is_null_at(0));
    EXPECT_EQ(data->get_element(0), 100);
    // present key but null value -> null
    EXPECT_TRUE(nullable->is_null_at(1));
}

// allocated_bytes should be non-zero and include the key structures.
TEST(FlatDictTest, AllocatedBytes) {
    auto key = flat_create_column<DataTypeInt32>({0, 1, 2}, "key");
    auto dict = build_flat_dict(
            key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({1, 2, 3}, "v")});
    EXPECT_GT(dict->allocated_bytes(), 0);
}

// A LARGEINT key that exceeds MAX_ARRAY_SIZE but whose low 64 bits are small
// (e.g. 2^64, whose low bits are 0) must still be rejected at load time. The
// range check must be done in 128-bit arithmetic, not after narrowing.
TEST(FlatDictTest, LargeIntKeyOverMaxLowBitsZeroRejected) {
    Int128 over = (Int128 {1} << 64); // 18446744073709551616; low 64 bits = 0
    auto key = flat_create_column<DataTypeInt128>({over}, "key");
    EXPECT_THROW(build_flat_dict(
                         key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({1}, "v")}),
                 doris::Exception);
}

// Looking up a LARGEINT value whose low 64 bits collide with a present key
// (2^64 low bits = 0) must return not-found, NOT the entry for key 0.
TEST(FlatDictTest, LargeIntLookupOverMaxNotFound) {
    // dictionary has key 0 present
    auto key = flat_create_column<DataTypeInt128>({Int128 {0}}, "key");
    auto dict = build_flat_dict(
            key, ColumnsWithTypeAndName {flat_create_column<DataTypeInt64>({42}, "v")});

    // query key 2^64 (low 64 bits = 0) must NOT alias to key 0
    Int128 alias = (Int128 {1} << 64);
    auto query = flat_create_column<DataTypeInt128>({Int128 {0}, alias}, "q");
    auto result =
            dict->get_column("v", std::make_shared<DataTypeInt64>(), query.column, query.type);
    const auto* nullable = assert_cast<const ColumnNullable*>(result.get());
    const auto* data = assert_cast<const ColumnInt64*>(nullable->get_nested_column_ptr().get());
    // key 0 -> hit
    EXPECT_FALSE(nullable->is_null_at(0));
    EXPECT_EQ(data->get_element(0), 42);
    // key 2^64 -> must be not found (null), not key 0's value
    EXPECT_TRUE(nullable->is_null_at(1));
}

} // namespace doris
