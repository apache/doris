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

#include <optional>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_inner_product.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

MutableColumnPtr make_offsets(const std::vector<size_t>& offsets) {
    auto result = ColumnArray::ColumnOffsets::create();
    for (size_t offset : offsets) {
        result->insert_value(offset);
    }
    return result;
}

template <typename ColumnType, typename ValueType>
MutableColumnPtr make_nullable_vector(const std::vector<std::optional<ValueType>>& values) {
    auto nested = ColumnType::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value.value_or(ValueType {}));
        null_map->insert_value(value.has_value() ? 0 : 1);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

MutableColumnPtr make_nullable_string(const std::vector<std::optional<std::string>>& values) {
    auto nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        const std::string data = value.value_or("");
        nested->insert_data(data.data(), data.size());
        null_map->insert_value(value.has_value() ? 0 : 1);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr make_int_float_map(const std::vector<std::optional<int32_t>>& keys,
                             const std::vector<std::optional<float>>& values,
                             const std::vector<size_t>& offsets) {
    return ColumnMap::create(make_nullable_vector<ColumnInt32>(keys),
                             make_nullable_vector<ColumnFloat32>(values), make_offsets(offsets));
}

ColumnPtr make_largeint_float_map(const std::vector<std::optional<Int128>>& keys,
                                  const std::vector<std::optional<float>>& values,
                                  const std::vector<size_t>& offsets) {
    return ColumnMap::create(make_nullable_vector<ColumnInt128>(keys),
                             make_nullable_vector<ColumnFloat32>(values), make_offsets(offsets));
}

ColumnPtr make_string_float_map(const std::vector<std::optional<std::string>>& keys,
                                const std::vector<std::optional<float>>& values,
                                const std::vector<size_t>& offsets) {
    return ColumnMap::create(make_nullable_string(keys),
                             make_nullable_vector<ColumnFloat32>(values), make_offsets(offsets));
}

Status execute_inner_product(Block& block, const DataTypePtr& return_type) {
    ColumnsWithTypeAndName arguments {block.get_by_position(0), block.get_by_position(1)};
    auto function =
            SimpleFunctionFactory::instance().get_function("inner_product", arguments, return_type);
    if (function == nullptr) {
        return Status::InternalError("function inner_product is not registered");
    }
    return function->execute(nullptr, block, {0, 1}, 2, block.rows());
}

DataTypePtr nullable_int_type() {
    return make_nullable(std::make_shared<DataTypeInt32>());
}

DataTypePtr nullable_float_type() {
    return make_nullable(std::make_shared<DataTypeFloat32>());
}

} // namespace

TEST(FunctionMapInnerProductTest, numeric_keys) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    block.insert({make_int_float_map({1, 2, 1, 3}, {1.0F, 2.0F, -2.0F, 0.5F}, {2, 4, 4}), map_type,
                  "left"});
    block.insert({make_int_float_map({2, 1, 3, 2, 1}, {3.0F, 4.0F, 8.0F, 99.0F, 4.0F}, {2, 5, 5}),
                  map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 3);
    EXPECT_FLOAT_EQ(result[0], 10.0F);
    EXPECT_FLOAT_EQ(result[1], -4.0F);
    EXPECT_FLOAT_EQ(result[2], 0.0F);
}

TEST(FunctionMapInnerProductTest, const_map) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    block.insert({ColumnConst::create(make_int_float_map({1, 2}, {2.0F, 3.0F}, {2}), 2), map_type,
                  "left"});
    block.insert({make_int_float_map({2, 1}, {4.0F, 5.0F}, {1, 2}), map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 2);
    EXPECT_FLOAT_EQ(result[0], 12.0F);
    EXPECT_FLOAT_EQ(result[1], 10.0F);
}

TEST(FunctionMapInnerProductTest, largeint_keys) {
    auto largeint_type = make_nullable(std::make_shared<DataTypeInt128>());
    auto map_type = std::make_shared<DataTypeMap>(largeint_type, nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    block.insert({make_largeint_float_map({Int128 {1}, Int128 {2}}, {2.0F, 3.0F}, {2}), map_type,
                  "left"});
    block.insert({make_largeint_float_map({Int128 {2}, Int128 {3}}, {4.0F, 5.0F}, {2}), map_type,
                  "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 1);
    EXPECT_FLOAT_EQ(result[0], 12.0F);
}

TEST(FunctionMapInnerProductTest, string_and_null_keys) {
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
    auto map_type = std::make_shared<DataTypeMap>(nullable_string, nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    block.insert({make_string_float_map({"a", std::nullopt}, {2.0F, 3.0F}, {2}), map_type, "left"});
    block.insert(
            {make_string_float_map({std::nullopt, "a"}, {4.0F, 5.0F}, {2}), map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 1);
    EXPECT_FLOAT_EQ(result[0], 22.0F);
}

TEST(FunctionMapInnerProductTest, rejects_null_values) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    block.insert({make_int_float_map({1}, {std::nullopt}, {1}), map_type, "left"});
    block.insert({make_int_float_map({1}, {2.0F}, {1}), map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    const auto status = execute_inner_product(block, return_type);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("First argument for function inner_product cannot have null"),
              std::string::npos);
}

TEST(FunctionMapInnerProductTest, rejects_unsupported_key_type) {
    auto double_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto map_type = std::make_shared<DataTypeMap>(double_type, nullable_float_type());
    DataTypes arguments {map_type, map_type};

    try {
        FunctionInnerProduct::create()->get_return_type_impl(arguments);
        FAIL() << "Expected unsupported map key type to be rejected";
    } catch (const doris::Exception& exception) {
        EXPECT_NE(std::string(exception.what())
                          .find("inner_product only supports integer or string map keys"),
                  std::string::npos);
    }
}

} // namespace doris
