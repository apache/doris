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

ColumnPtr make_high_cardinality_map_with_shadowed_nulls(size_t key_count) {
    std::vector<std::optional<int32_t>> keys;
    std::vector<std::optional<float>> values;
    keys.reserve(key_count + 3);
    values.reserve(key_count + 3);
    for (size_t key = 0; key < key_count; ++key) {
        keys.emplace_back(static_cast<int32_t>(key));
        if (key == 5) {
            values.emplace_back(std::nullopt);
        } else {
            values.emplace_back(1.0F);
        }
    }
    keys.emplace_back(5);
    values.emplace_back(2.0F);
    keys.emplace_back(std::nullopt);
    values.emplace_back(std::nullopt);
    keys.emplace_back(std::nullopt);
    values.emplace_back(4.0F);
    return make_int_float_map(keys, values, {key_count + 3});
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

TEST(FunctionMapInnerProductTest, reuses_high_cardinality_const_map) {
    constexpr size_t key_count = 1024;
    constexpr size_t row_count = 3;
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    auto constant_map = make_high_cardinality_map_with_shadowed_nulls(key_count);

    std::vector<std::optional<int32_t>> varying_keys;
    std::vector<std::optional<float>> varying_values;
    std::vector<size_t> varying_offsets;
    varying_keys.reserve(row_count * (key_count + 2));
    varying_values.reserve(row_count * (key_count + 2));
    varying_offsets.reserve(row_count);
    for (size_t row = 0; row < row_count; ++row) {
        for (size_t key = 0; key < key_count; ++key) {
            varying_keys.emplace_back(static_cast<int32_t>(key));
            varying_values.emplace_back(1.0F);
        }
        varying_keys.emplace_back(5);
        varying_values.emplace_back(3.0F);
        varying_keys.emplace_back(std::nullopt);
        varying_values.emplace_back(static_cast<float>(row + 1));
        varying_offsets.emplace_back(varying_keys.size());
    }
    auto varying_map = make_int_float_map(varying_keys, varying_values, varying_offsets);

    auto expect_results = [&](bool constant_is_left) {
        Block block;
        ColumnPtr constant = ColumnConst::create(constant_map, row_count);
        block.insert({constant_is_left ? constant : varying_map, map_type, "left"});
        block.insert({constant_is_left ? varying_map : constant, map_type, "right"});
        block.insert({nullptr, return_type, "result"});

        ASSERT_TRUE(execute_inner_product(block, return_type).ok());
        const auto& result =
                assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
        ASSERT_EQ(result.size(), row_count);
        EXPECT_FLOAT_EQ(result[0], 1033.0F);
        EXPECT_FLOAT_EQ(result[1], 1037.0F);
        EXPECT_FLOAT_EQ(result[2], 1041.0F);
    };

    expect_results(true);
    expect_results(false);
}

TEST(FunctionMapInnerProductTest, avoids_caching_oversized_const_map) {
    constexpr size_t key_count = 1024;
    constexpr size_t row_count = 3;
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    std::vector<std::optional<int32_t>> constant_keys;
    std::vector<std::optional<float>> constant_values;
    constant_keys.reserve(key_count);
    constant_values.reserve(key_count);
    for (size_t key = 0; key < key_count; ++key) {
        constant_keys.emplace_back(static_cast<int32_t>(key));
        constant_values.emplace_back(1.0F);
    }
    auto constant_map = make_int_float_map(constant_keys, constant_values, {key_count});
    auto varying_map = make_int_float_map({5, 7, -1}, {3.0F, 2.0F, 7.0F}, {1, 2, 3});

    auto expect_results = [&](bool constant_is_left) {
        Block block;
        ColumnPtr constant = ColumnConst::create(constant_map, row_count);
        block.insert({constant_is_left ? constant : varying_map, map_type, "left"});
        block.insert({constant_is_left ? varying_map : constant, map_type, "right"});
        block.insert({nullptr, return_type, "result"});

        ASSERT_TRUE(execute_inner_product(block, return_type).ok());
        const auto& result =
                assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
        ASSERT_EQ(result.size(), row_count);
        EXPECT_FLOAT_EQ(result[0], 3.0F);
        EXPECT_FLOAT_EQ(result[1], 2.0F);
        EXPECT_FLOAT_EQ(result[2], 0.0F);
    };

    expect_results(true);
    expect_results(false);
}

TEST(FunctionMapInnerProductTest, empty_map_short_circuits_high_cardinality_row) {
    constexpr size_t key_count = 4096;
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    std::vector<std::optional<int32_t>> keys;
    std::vector<std::optional<float>> values;
    keys.reserve(key_count);
    values.reserve(key_count);
    for (size_t key = 0; key < key_count; ++key) {
        keys.emplace_back(static_cast<int32_t>(key));
        values.emplace_back(1.0F);
    }

    Block block;
    block.insert({make_int_float_map(keys, values, {0, key_count}), map_type, "left"});
    block.insert({make_int_float_map(keys, values, {key_count, key_count}), map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 2);
    EXPECT_FLOAT_EQ(result[0], 0.0F);
    EXPECT_FLOAT_EQ(result[1], 0.0F);
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

TEST(FunctionMapInnerProductTest, duplicate_keys_use_last_value) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    // Rows 0 and 2 build the left map; rows 1 and 3 build the right map.
    // Rows 0 and 1 use ordinary duplicate keys; rows 2 and 3 use duplicate NULL keys.
    block.insert(
            {make_int_float_map(
                     {1, 1, 1, 1, 2, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 2},
                     {2.0F, 3.0F, 4.0F, 5.0F, 7.0F, 2.0F, 3.0F, 4.0F, 5.0F, 7.0F}, {2, 5, 7, 10}),
             map_type, "left"});
    block.insert(
            {make_int_float_map(
                     {1, 1, 2, 1, 1, std::nullopt, std::nullopt, 2, std::nullopt, std::nullopt},
                     {4.0F, 5.0F, 7.0F, 2.0F, 3.0F, 4.0F, 5.0F, 7.0F, 2.0F, 3.0F}, {3, 5, 8, 10}),
             map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 4);
    EXPECT_FLOAT_EQ(result[0], 15.0F);
    EXPECT_FLOAT_EQ(result[1], 15.0F);
    EXPECT_FLOAT_EQ(result[2], 15.0F);
    EXPECT_FLOAT_EQ(result[3], 15.0F);
}

TEST(FunctionMapInnerProductTest, shadowed_null_values_are_ignored) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();
    Block block;
    // Cover a shadowed NULL value on either side and in both build/probe roles. Rows 1 and 3
    // additionally exercise the separate NULL-key bucket.
    block.insert(
            {make_int_float_map(
                     {1, 1, std::nullopt, std::nullopt, 2, 1, 2, std::nullopt, 2, 3},
                     {std::nullopt, 2.0F, std::nullopt, 2.0F, 5.0F, 3.0F, 4.0F, 3.0F, 4.0F, 5.0F},
                     {2, 5, 7, 10}),
             map_type, "left"});
    block.insert(
            {make_int_float_map(
                     {1, 2, 3, std::nullopt, 3, 1, 1, 3, std::nullopt, std::nullopt},
                     {3.0F, 4.0F, 5.0F, 3.0F, 4.0F, std::nullopt, 2.0F, 5.0F, std::nullopt, 2.0F},
                     {3, 5, 8, 10}),
             map_type, "right"});
    block.insert({nullptr, return_type, "result"});

    ASSERT_TRUE(execute_inner_product(block, return_type).ok());
    const auto& result =
            assert_cast<const ColumnFloat32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(result.size(), 4);
    EXPECT_FLOAT_EQ(result[0], 6.0F);
    EXPECT_FLOAT_EQ(result[1], 6.0F);
    EXPECT_FLOAT_EQ(result[2], 6.0F);
    EXPECT_FLOAT_EQ(result[3], 6.0F);
}

TEST(FunctionMapInnerProductTest, rejects_retained_null_values) {
    auto map_type = std::make_shared<DataTypeMap>(nullable_int_type(), nullable_float_type());
    auto return_type = std::make_shared<DataTypeFloat32>();

    auto expect_rejected = [&](ColumnPtr left, ColumnPtr right, const std::string& message) {
        Block block;
        block.insert({std::move(left), map_type, "left"});
        block.insert({std::move(right), map_type, "right"});
        block.insert({nullptr, return_type, "result"});

        const auto status = execute_inner_product(block, return_type);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find(message), std::string::npos);
    };

    const std::string first_argument_error =
            "First argument for function inner_product cannot have null";
    const std::string second_argument_error =
            "Second argument for function inner_product cannot have null";

    // Retained NULL on the left while the left and right maps are selected for build.
    expect_rejected(make_int_float_map({1}, {std::nullopt}, {1}),
                    make_int_float_map({2, 3}, {2.0F, 3.0F}, {2}), first_argument_error);
    expect_rejected(make_int_float_map({std::nullopt, std::nullopt}, {1.0F, std::nullopt}, {2}),
                    make_int_float_map({1}, {3.0F}, {1}), first_argument_error);

    // Retained NULL on the right while the left and right maps are selected for build.
    expect_rejected(make_int_float_map({1}, {3.0F}, {1}),
                    make_int_float_map({std::nullopt, std::nullopt}, {1.0F, std::nullopt}, {2}),
                    second_argument_error);
    expect_rejected(make_int_float_map({2, 3}, {2.0F, 3.0F}, {2}),
                    make_int_float_map({1}, {std::nullopt}, {1}), second_argument_error);

    // Empty-map short-circuiting must not hide a retained NULL in the nonempty map.
    expect_rejected(make_int_float_map({1}, {std::nullopt}, {1}), make_int_float_map({}, {}, {0}),
                    first_argument_error);
    expect_rejected(make_int_float_map({}, {}, {0}), make_int_float_map({1}, {std::nullopt}, {1}),
                    second_argument_error);
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
