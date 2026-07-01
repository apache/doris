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

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/function_utils.h"

namespace doris {

namespace {

using FloatRows = std::vector<std::vector<float>>;

template <PrimitiveType ElementType>
ColumnPtr create_array_column(const FloatRows& rows) {
    using NestedColumnType = typename PrimitiveTypeTraits<ElementType>::ColumnType;
    auto data = NestedColumnType::create();
    auto offsets = ColumnArray::ColumnOffsets::create();

    auto& data_values = data->get_data();
    auto& offset_values = offsets->get_data();
    size_t offset = 0;
    for (const auto& row : rows) {
        for (const auto value : row) {
            data_values.push_back(static_cast<typename NestedColumnType::value_type>(value));
        }
        offset += row.size();
        offset_values.push_back(offset);
    }

    return ColumnArray::create(std::move(data), std::move(offsets));
}

DataTypePtr array_nullable_float_type() {
    return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()));
}

template <PrimitiveType ElementType>
DataTypePtr array_type() {
    return std::make_shared<DataTypeArray>(
            std::make_shared<typename PrimitiveTypeTraits<ElementType>::DataType>());
}

template <PrimitiveType ElementType>
Status execute_cross_product(const std::string& func_name, ColumnPtr lhs, ColumnPtr rhs,
                             size_t rows, Block* block, bool nullable_lhs = false,
                             bool nullable_rhs = false, bool nullable_element = false) {
    auto input_type = nullable_element ? array_nullable_float_type() : array_type<ElementType>();
    auto lhs_type = nullable_lhs ? make_nullable(input_type) : input_type;
    auto rhs_type = nullable_rhs ? make_nullable(input_type) : input_type;
    auto result_type = nullable_lhs || nullable_rhs ? make_nullable(array_nullable_float_type())
                                                    : array_nullable_float_type();
    block->insert({std::move(lhs), lhs_type, "lhs"});
    block->insert({std::move(rhs), rhs_type, "rhs"});

    auto function = SimpleFunctionFactory::instance().get_function(
            func_name, block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);

    FunctionUtils fn_utils(result_type, {lhs_type, rhs_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
    block->insert({nullptr, result_type, "result"});
    auto st = function->execute(fn_ctx, *block, {0, 1}, 2, rows);
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    RETURN_IF_ERROR(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    return st;
}

void expect_array_rows(const Block& block, const FloatRows& expected) {
    auto result_col_holder = block.get_by_position(2).column->convert_to_full_column_if_const();
    const IColumn* result_col = result_col_holder.get();
    const ColumnUInt8::Container* result_null_map = nullptr;
    if (result_col->is_nullable()) {
        const auto& nullable_col = assert_cast<const ColumnNullable&>(*result_col);
        result_null_map = &nullable_col.get_null_map_data();
        result_col = nullable_col.get_nested_column_ptr().get();
    }
    const auto& array_col = assert_cast<const ColumnArray&>(*result_col);
    const IColumn* nested_col = &array_col.get_data();
    if (nested_col->is_nullable()) {
        nested_col = assert_cast<const ColumnNullable&>(*nested_col).get_nested_column_ptr().get();
    }
    const auto& data = assert_cast<const ColumnFloat32&>(*nested_col);
    const auto& values = data.get_data();
    const auto& offsets = array_col.get_offsets();

    size_t offset = 0;
    ASSERT_EQ(expected.size(), offsets.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        if (result_null_map && (*result_null_map)[row]) {
            offset = offsets[row];
            continue;
        }
        ASSERT_EQ(expected[row].size(), offsets[row] - offset);
        for (size_t i = 0; i < expected[row].size(); ++i) {
            EXPECT_FLOAT_EQ(expected[row][i], values[offset + i]);
        }
        offset = offsets[row];
    }
}

void expect_top_nulls(const Block& block, const std::vector<bool>& expected_nulls) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    ASSERT_EQ(expected_nulls.size(), nullable_col.size());
    for (size_t i = 0; i < expected_nulls.size(); ++i) {
        EXPECT_EQ(expected_nulls[i], nullable_col.is_null_at(i));
    }
}

void expect_nested_data_size(const Block& block, size_t expected_size) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& array_col = assert_cast<const ColumnArray&>(*nullable_col.get_nested_column_ptr());
    const auto& nested_nullable_col = assert_cast<const ColumnNullable&>(array_col.get_data());
    EXPECT_EQ(expected_size, nested_nullable_col.get_nested_column().size());
}

} // namespace

TEST(function_array_cross_product_test, basic_and_alias) {
    FloatRows lhs = {{1.0F, 0.0F, 0.0F}, {-2.0F, 3.0F, 4.0F}};
    FloatRows rhs = {{0.0F, 1.0F, 0.0F}, {5.0F, -6.0F, 7.0F}};
    FloatRows expected = {{0.0F, 0.0F, 1.0F}, {45.0F, 34.0F, -3.0F}};

    {
        Block block;
        auto st = execute_cross_product<TYPE_FLOAT>(
                "array_cross_product", create_array_column<TYPE_FLOAT>(lhs),
                create_array_column<TYPE_FLOAT>(rhs), lhs.size(), &block);
        ASSERT_TRUE(st.ok()) << st;
        expect_array_rows(block, expected);
    }

    {
        Block block;
        auto st = execute_cross_product<TYPE_FLOAT>(
                "cross_product", create_array_column<TYPE_FLOAT>(lhs),
                create_array_column<TYPE_FLOAT>(rhs), lhs.size(), &block);
        ASSERT_TRUE(st.ok()) << st;
        expect_array_rows(block, expected);
    }
}

TEST(function_array_cross_product_test, partial_const_arguments) {
    FloatRows const_lhs = {{1.0F, 2.0F, 3.0F}};
    FloatRows rhs = {{4.0F, 5.0F, 6.0F}, {-1.0F, 0.0F, 1.0F}};
    FloatRows expected_left_const = {{-3.0F, 6.0F, -3.0F}, {2.0F, -4.0F, 2.0F}};

    {
        Block block;
        auto st = execute_cross_product<TYPE_FLOAT>(
                "array_cross_product",
                ColumnConst::create(create_array_column<TYPE_FLOAT>(const_lhs), rhs.size()),
                create_array_column<TYPE_FLOAT>(rhs), rhs.size(), &block);
        ASSERT_TRUE(st.ok()) << st;
        expect_array_rows(block, expected_left_const);
    }

    FloatRows lhs = {{4.0F, 5.0F, 6.0F}, {-1.0F, 0.0F, 1.0F}};
    FloatRows const_rhs = {{1.0F, 2.0F, 3.0F}};
    FloatRows expected_right_const = {{3.0F, -6.0F, 3.0F}, {-2.0F, 4.0F, -2.0F}};

    {
        Block block;
        auto st = execute_cross_product<TYPE_FLOAT>(
                "array_cross_product", create_array_column<TYPE_FLOAT>(lhs),
                ColumnConst::create(create_array_column<TYPE_FLOAT>(const_rhs), lhs.size()),
                lhs.size(), &block);
        ASSERT_TRUE(st.ok()) << st;
        expect_array_rows(block, expected_right_const);
    }
}

TEST(function_array_cross_product_test, all_const_arguments) {
    FloatRows lhs = {{1.0F, 2.0F, 3.0F}};
    FloatRows rhs = {{4.0F, 5.0F, 6.0F}};

    Block block;
    auto st = execute_cross_product<TYPE_FLOAT>(
            "array_cross_product", ColumnConst::create(create_array_column<TYPE_FLOAT>(lhs), 2),
            ColumnConst::create(create_array_column<TYPE_FLOAT>(rhs), 2), 2, &block);
    ASSERT_TRUE(st.ok()) << st;
    expect_array_rows(block, {{-3.0F, 6.0F, -3.0F}, {-3.0F, 6.0F, -3.0F}});
}

TEST(function_array_cross_product_test, invalid_dimension) {
    Block block;
    auto st = execute_cross_product<TYPE_FLOAT>(
            "array_cross_product", create_array_column<TYPE_FLOAT>({{1.0F, 2.0F}}),
            create_array_column<TYPE_FLOAT>({{3.0F, 4.0F, 5.0F}}), 1, &block);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("exactly 3 elements") != std::string::npos) << st.to_string();
}

TEST(function_array_cross_product_test, null_element_returns_error) {
    auto data = ColumnFloat32::create();
    auto& data_values = data->get_data();
    data_values.push_back(1.0F);
    data_values.push_back(2.0F);
    data_values.push_back(3.0F);
    auto null_map = ColumnUInt8::create();
    auto& null_values = null_map->get_data();
    null_values.push_back(0);
    null_values.push_back(1);
    null_values.push_back(0);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(3);
    auto lhs = ColumnArray::create(ColumnNullable::create(std::move(data), std::move(null_map)),
                                   std::move(offsets));

    Block block;
    auto st = execute_cross_product<TYPE_FLOAT>(
            "array_cross_product", std::move(lhs),
            create_array_column<TYPE_FLOAT>({{4.0F, 5.0F, 6.0F}}), 1, &block, false, false, true);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("cannot have null") != std::string::npos) << st.to_string();
}

TEST(function_array_cross_product_test, top_null_array_returns_null) {
    auto lhs_nested_data = ColumnFloat32::create();
    auto& lhs_nested_values = lhs_nested_data->get_data();
    lhs_nested_values.push_back(1.0F);
    lhs_nested_values.push_back(0.0F);
    lhs_nested_values.push_back(0.0F);
    lhs_nested_values.push_back(9.0F);
    lhs_nested_values.push_back(9.0F);
    lhs_nested_values.push_back(9.0F);
    auto lhs_nested_null_map = ColumnUInt8::create();
    auto& lhs_nested_null_map_data = lhs_nested_null_map->get_data();
    lhs_nested_null_map_data.push_back(0);
    lhs_nested_null_map_data.push_back(0);
    lhs_nested_null_map_data.push_back(0);
    lhs_nested_null_map_data.push_back(0);
    lhs_nested_null_map_data.push_back(1);
    lhs_nested_null_map_data.push_back(0);
    auto lhs_offsets = ColumnArray::ColumnOffsets::create();
    lhs_offsets->get_data().push_back(3);
    lhs_offsets->get_data().push_back(6);
    auto lhs_nested = ColumnArray::create(
            ColumnNullable::create(std::move(lhs_nested_data), std::move(lhs_nested_null_map)),
            std::move(lhs_offsets));
    auto lhs_null_map = ColumnUInt8::create();
    lhs_null_map->get_data().push_back(0);
    lhs_null_map->get_data().push_back(1);
    auto lhs = ColumnNullable::create(std::move(lhs_nested), std::move(lhs_null_map));

    Block block;
    auto st = execute_cross_product<TYPE_FLOAT>(
            "array_cross_product", std::move(lhs),
            create_array_column<TYPE_FLOAT>({{0.0F, 1.0F, 0.0F}, {4.0F, 5.0F, 6.0F}}), 2, &block,
            true, false, true);
    ASSERT_TRUE(st.ok()) << st;
    expect_top_nulls(block, {false, true});
    expect_nested_data_size(block, 3);
}

} // namespace doris
