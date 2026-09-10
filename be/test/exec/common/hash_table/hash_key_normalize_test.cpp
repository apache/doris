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

#include "exec/common/hash_table/hash_key_normalize.h"

#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "testutil/column_helper.h"

namespace doris {

namespace {

const double kQuietNaN = std::numeric_limits<double>::quiet_NaN();
const double kPayloadNaN = std::bit_cast<double>(std::bit_cast<uint64_t>(kQuietNaN) | 0x1234ULL);

bool is_positive_zero(double value) {
    return std::bit_cast<uint64_t>(value) == std::bit_cast<uint64_t>(0.0);
}

bool is_quiet_nan(double value) {
    return std::bit_cast<uint64_t>(value) == std::bit_cast<uint64_t>(kQuietNaN);
}

} // namespace

TEST(HashKeyNormalizeTest, ContainsFloatOrDouble) {
    const auto int32 = std::make_shared<DataTypeInt32>();
    const auto float32 = std::make_shared<DataTypeFloat32>();
    const auto float64 = std::make_shared<DataTypeFloat64>();
    const auto string = std::make_shared<DataTypeString>();

    EXPECT_FALSE(contains_float_or_double(int32));
    EXPECT_FALSE(contains_float_or_double(string));
    EXPECT_FALSE(contains_float_or_double(make_nullable(int32)));
    EXPECT_TRUE(contains_float_or_double(float32));
    EXPECT_TRUE(contains_float_or_double(float64));
    EXPECT_TRUE(contains_float_or_double(make_nullable(float64)));

    EXPECT_FALSE(contains_float_or_double(std::make_shared<DataTypeArray>(make_nullable(int32))));
    EXPECT_TRUE(contains_float_or_double(
            make_nullable(std::make_shared<DataTypeArray>(make_nullable(float64)))));
    EXPECT_FALSE(contains_float_or_double(std::make_shared<DataTypeMap>(string, int32)));
    EXPECT_TRUE(contains_float_or_double(std::make_shared<DataTypeMap>(string, float32)));
    EXPECT_TRUE(contains_float_or_double(std::make_shared<DataTypeMap>(float64, string)));
    EXPECT_FALSE(
            contains_float_or_double(std::make_shared<DataTypeStruct>(DataTypes {int32, string})));
    EXPECT_TRUE(contains_float_or_double(std::make_shared<DataTypeStruct>(
            DataTypes {int32, std::make_shared<DataTypeArray>(float32)})));
}

TEST(HashKeyNormalizeTest, NormalizeDoubleKeyInPlaceWhenUniquelyOwned) {
    ColumnPtr column = ColumnHelper::create_column<DataTypeFloat64>({-0.0, 0.0, kPayloadNaN, 1.5});
    const auto* origin = column.get();

    normalize_float_hash_key(column, std::make_shared<DataTypeFloat64>());

    EXPECT_EQ(origin, column.get());
    const auto& data = assert_cast<const ColumnFloat64&>(*column).get_data();
    EXPECT_TRUE(is_positive_zero(data[0]));
    EXPECT_TRUE(is_positive_zero(data[1]));
    EXPECT_TRUE(is_quiet_nan(data[2]));
    EXPECT_EQ(data[3], 1.5);
}

TEST(HashKeyNormalizeTest, SharedFloatKeyIsCopiedBeforeNormalization) {
    ColumnPtr shared = ColumnHelper::create_column<DataTypeFloat32>({-0.0F, 2.5F});
    ColumnPtr column = shared;

    normalize_float_hash_key(column, std::make_shared<DataTypeFloat32>());

    EXPECT_NE(shared.get(), column.get());
    EXPECT_TRUE(std::signbit(assert_cast<const ColumnFloat32&>(*shared).get_data()[0]));
    EXPECT_FALSE(std::signbit(assert_cast<const ColumnFloat32&>(*column).get_data()[0]));
}

TEST(HashKeyNormalizeTest, NonFloatKeyIsUntouched) {
    ColumnPtr shared = ColumnHelper::create_column<DataTypeInt64>({1, 2, 3});
    ColumnPtr column = shared;

    normalize_float_hash_key(column, std::make_shared<DataTypeInt64>());

    // No float leaf: the shared column must not be cloned.
    EXPECT_EQ(shared.get(), column.get());
}

TEST(HashKeyNormalizeTest, NormalizeNullableAndArrayLeaves) {
    auto nullable_type = make_nullable(std::make_shared<DataTypeFloat64>());
    ColumnPtr nullable =
            ColumnHelper::create_nullable_column<DataTypeFloat64>({-0.0, 0.0}, {false, true});
    normalize_float_hash_key(nullable, nullable_type);
    const auto& nested = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(*nullable).get_nested_column());
    EXPECT_TRUE(is_positive_zero(nested.get_data()[0]));

    auto array_type = std::make_shared<DataTypeArray>(nullable_type);
    auto mutable_array = array_type->create_column();
    Array value;
    value.push_back(Field::create_field<TYPE_DOUBLE>(-0.0));
    value.push_back(Field::create_field<TYPE_DOUBLE>(1.0));
    mutable_array->insert(Field::create_field<TYPE_ARRAY>(value));
    ColumnPtr array = std::move(mutable_array);
    normalize_float_hash_key(array, array_type);
    const auto& array_data = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(assert_cast<const ColumnArray&>(*array).get_data())
                    .get_nested_column());
    EXPECT_TRUE(is_positive_zero(array_data.get_data()[0]));
    EXPECT_EQ(array_data.get_data()[1], 1.0);
}

TEST(HashKeyNormalizeTest, NormalizeMapAndStructLeaves) {
    auto map_type = std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeString>(), make_nullable(std::make_shared<DataTypeFloat64>()));
    auto mutable_map = map_type->create_column();
    Map map_value;
    Array keys;
    keys.push_back(Field::create_field<TYPE_STRING>("k"));
    Array values;
    values.push_back(Field::create_field<TYPE_DOUBLE>(-0.0));
    map_value.push_back(Field::create_field<TYPE_ARRAY>(keys));
    map_value.push_back(Field::create_field<TYPE_ARRAY>(values));
    mutable_map->insert(Field::create_field<TYPE_MAP>(map_value));
    ColumnPtr map = std::move(mutable_map);
    normalize_float_hash_key(map, map_type);
    const auto& map_values = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(assert_cast<const ColumnMap&>(*map).get_values())
                    .get_nested_column());
    EXPECT_TRUE(is_positive_zero(map_values.get_data()[0]));

    auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeFloat32>()});
    auto mutable_struct = struct_type->create_column();
    Struct tuple;
    tuple.push_back(Field::create_field<TYPE_INT>(7));
    tuple.push_back(Field::create_field<TYPE_FLOAT>(-0.0F));
    mutable_struct->insert(Field::create_field<TYPE_STRUCT>(tuple));
    ColumnPtr struct_column = std::move(mutable_struct);
    normalize_float_hash_key(struct_column, struct_type);
    const auto& struct_float = assert_cast<const ColumnFloat32&>(
            assert_cast<const ColumnStruct&>(*struct_column).get_column(1));
    EXPECT_FALSE(std::signbit(struct_float.get_data()[0]));
}

TEST(HashKeyNormalizeTest, SharedNestedColumnIsDetachedNotOverwritten) {
    // The nullable wrapper is uniquely owned but its nested data is shared with `raw`:
    // the copy-on-write mutate must detach the nested column instead of rewriting `raw`.
    ColumnPtr raw = ColumnHelper::create_column<DataTypeFloat64>({-0.0, 2.0});
    auto null_map = ColumnUInt8::create(2, 0);
    ColumnPtr nullable = ColumnNullable::create(raw, std::move(null_map));

    normalize_float_hash_key(nullable, make_nullable(std::make_shared<DataTypeFloat64>()));

    EXPECT_TRUE(std::signbit(assert_cast<const ColumnFloat64&>(*raw).get_data()[0]));
    const auto& nested = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(*nullable).get_nested_column());
    EXPECT_TRUE(is_positive_zero(nested.get_data()[0]));
}

} // namespace doris
