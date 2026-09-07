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

#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/function_utils.h"

namespace doris {
namespace {

using RangeRow = std::array<std::optional<Int32>, 3>;

void check_range_result(const ColumnPtr& result,
                        const std::vector<std::optional<std::vector<Int32>>>& expected) {
    ASSERT_EQ(result->size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        SCOPED_TRACE(row);
        if (!expected[row].has_value()) {
            EXPECT_TRUE(result->is_null_at(row));
            continue;
        }
        ASSERT_FALSE(result->is_null_at(row));
        Field field;
        result->get(row, field);
        const auto& array = field.get<TYPE_ARRAY>();
        ASSERT_EQ(array.size(), expected[row]->size());
        for (size_t i = 0; i < array.size(); ++i) {
            ASSERT_FALSE(array[i].is_null());
            EXPECT_EQ(array[i].get<TYPE_INT>(), (*expected[row])[i]);
        }
    }
}

void check_range(const std::string& name, const std::vector<RangeRow>& rows,
                 const std::vector<std::optional<std::vector<Int32>>>& expected,
                 unsigned const_mask = 0, size_t argument_count = 3,
                 bool expect_size_error = false) {
    auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto result_type = make_nullable(std::make_shared<DataTypeArray>(int_type));
    Block block;
    ColumnNumbers arguments;
    std::vector<DataTypePtr> arg_types;
    for (size_t arg = 0; arg < argument_count; ++arg) {
        auto column = int_type->create_column();
        for (const auto& row : rows) {
            if (row[arg].has_value()) {
                column->insert(Field::create_field<TYPE_INT>(*row[arg]));
            } else {
                column->insert_default();
            }
        }
        if (const_mask & (1U << arg)) {
            column = ColumnConst::create(column->clone_resized(1), rows.size());
        }
        block.insert({std::move(column), int_type, "arg"});
        arguments.push_back(static_cast<uint32_t>(arg));
        arg_types.push_back(int_type);
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            name, block.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    FunctionUtils utils(result_type, arg_types, false);
    auto* context = utils.get_fn_ctx();
    ASSERT_TRUE(function->open(context, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::THREAD_LOCAL).ok());
    block.insert({nullptr, result_type, "result"});
    auto status = function->execute(context, block, arguments,
                                    static_cast<uint32_t>(argument_count), rows.size());
    ASSERT_TRUE(function->close(context, FunctionContext::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FRAGMENT_LOCAL).ok());
    if (expect_size_error) {
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("Array size exceeds the limit"), std::string::npos);
        return;
    }
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto result = block.get_by_position(argument_count).column->convert_to_full_column_if_const();
    check_range_result(result, expected);
}

TEST(FunctionArrayRangeTest, OverflowAndOffsets) {
    constexpr Int32 max = std::numeric_limits<Int32>::max();
    const std::vector<RangeRow> rows = {{max, max, 2},
                                        {max - 1, max, 2},
                                        {1, 5, 2},
                                        {max - 3, max, 2},
                                        {1, max, max},
                                        {2, 1, 1},
                                        {std::nullopt, 5, 2},
                                        {1, std::nullopt, 2},
                                        {1, 5, std::nullopt},
                                        {-1, 5, 2},
                                        {1, -1, 2},
                                        {1, 5, 0},
                                        {1, 5, -1},
                                        {0, 3, 1}};
    const std::vector<std::optional<std::vector<Int32>>> expected = {
            std::vector<Int32> {},
            std::vector<Int32> {max - 1},
            std::vector<Int32> {1, 3},
            std::vector<Int32> {max - 3, max - 1},
            std::vector<Int32> {1},
            std::vector<Int32> {},
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::vector<Int32> {0, 1, 2}};
    for (const auto* name : {"array_range", "sequence"}) {
        check_range(name, rows, expected);
        // Repeated rows exercise every mixture of constant and vector arguments.
        for (unsigned mask = 0; mask < 8; ++mask) {
            check_range(
                    name, {{max - 3, max, 2}, {max - 3, max, 2}},
                    {std::vector<Int32> {max - 3, max - 1}, std::vector<Int32> {max - 3, max - 1}},
                    mask);
        }
    }
}

TEST(FunctionArrayRangeTest, DefaultStartAndStep) {
    for (const auto* name : {"array_range", "sequence"}) {
        check_range(name, {{3, 0, 0}, {0, 0, 0}},
                    {std::vector<Int32> {0, 1, 2}, std::vector<Int32> {}}, 0, 1);
        check_range(name, {{1, 4, 0}, {4, 4, 0}},
                    {std::vector<Int32> {1, 2, 3}, std::vector<Int32> {}}, 0, 2);
    }
}

TEST(FunctionArrayRangeTest, GrowingArrays) {
    std::vector<RangeRow> rows;
    std::vector<std::optional<std::vector<Int32>>> expected;
    for (Int32 row = 0; row < 32; ++row) {
        const Int32 length = row * row * 3;
        const Int32 step = row % 7 + 1;
        rows.push_back({row, row + length * step, step});
        std::vector<Int32> values;
        for (Int32 i = 0; i < length; ++i) {
            values.push_back(row + i * step);
        }
        expected.emplace_back(std::move(values));
        rows.push_back({std::nullopt, 1, 1});
        expected.emplace_back(std::nullopt);
        rows.push_back({1, 1, 1});
        expected.emplace_back(std::vector<Int32> {});
    }
    for (const auto* name : {"array_range", "sequence"}) {
        check_range(name, rows, expected);
    }
}

TEST(FunctionArrayRangeTest, ArraySizeLimit) {
    const auto limit = static_cast<Int32>(max_array_size_as_field);
    std::vector<Int32> expected(limit);
    for (Int32 i = 0; i < limit; ++i) {
        expected[i] = i * 2;
    }
    for (const auto* name : {"array_range", "sequence"}) {
        check_range(name, {{0, limit * 2, 2}}, {expected});
        check_range(name, {{0, limit * 2 + 1, 2}}, {}, 0, 3, true);
    }
}

} // namespace
} // namespace doris
