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

#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

static void check_array_trim_case(const DataSet& data_set, bool const_array, bool const_size) {
    auto element_type = make_nullable(std::make_shared<DataTypeInt32>());
    DataTypePtr array_type =
            make_nullable(std::make_shared<DataTypeArray>(std::move(element_type)));
    DataTypePtr size_type = make_nullable(std::make_shared<DataTypeInt64>());
    const size_t row_size = data_set.size();

    MutableColumnPtr array_column = array_type->create_column();
    MutableColumnPtr size_column = size_type->create_column();
    for (size_t row = 0; row < row_size; ++row) {
        if (!const_array || row == 0) {
            ASSERT_TRUE(insert_cell(array_column, array_type, data_set[row].first[0]));
        }
        if (!const_size || row == 0) {
            ASSERT_TRUE(insert_cell(size_column, size_type, data_set[row].first[1]));
        }
    }

    if (const_array) {
        array_column = ColumnConst::create(std::move(array_column), row_size);
    }
    if (const_size) {
        size_column = ColumnConst::create(std::move(size_column), row_size);
    }

    Block block;
    block.insert({std::move(array_column), array_type, "array"});
    block.insert({std::move(size_column), size_type, "size"});

    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "trim_array", block.get_columns_with_type_and_name(), array_type);
    ASSERT_NE(function, nullptr);

    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_columns(2);
    if (const_array) {
        constant_columns[0] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(0).column);
    }
    if (const_size) {
        constant_columns[1] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(1).column);
    }

    FunctionUtils function_utils(array_type, {array_type, size_type}, false);
    auto* function_context = function_utils.get_fn_ctx();
    function_context->set_constant_cols(constant_columns);
    ASSERT_TRUE(function->open(function_context, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(function_context, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, array_type, "result"});
    const auto result = block.columns() - 1;
    ASSERT_TRUE(function->execute(function_context, block, {0, 1}, result, row_size).ok());

    static_cast<void>(function->close(function_context, FunctionContext::THREAD_LOCAL));
    static_cast<void>(function->close(function_context, FunctionContext::FRAGMENT_LOCAL));

    MutableColumnPtr expected_column = array_type->create_column();
    for (const auto& row : data_set) {
        ASSERT_TRUE(insert_cell(expected_column, array_type, row.second));
    }
    const auto result_column =
            block.get_by_position(result).column->convert_to_full_column_if_const();
    for (size_t row = 0; row < row_size; ++row) {
        EXPECT_EQ(0, result_column->compare_at(row, row, *expected_column, 1))
                << "row " << row << ", result: " << array_type->to_string(*result_column, row)
                << ", expected: " << array_type->to_string(*expected_column, row);
    }
}

TEST(FunctionArrayTrimTest, all_argument_combinations) {
    const TestArray empty;
    const TestArray values = {Int32(1), Int32(2), Int32(3), Int32(4)};
    const TestArray short_values = {Int32(5), Int32(6)};
    const TestArray values_with_null = {Int32(1), Null(), Int32(3)};

    check_array_trim_case(
            {{{AnyType(values), Int64(0)}, AnyType(values)},
             {{AnyType(values), Int64(2)}, AnyType(TestArray {Int32(1), Int32(2)})},
             {{AnyType(empty), Int64(0)}, AnyType(empty)},
             {{AnyType(values_with_null), Int64(1)}, AnyType(TestArray {Int32(1), Null()})},
             {{Null(), Int64(1)}, Null()},
             {{AnyType(values), Null()}, Null()}},
            false, false);

    check_array_trim_case({{{AnyType(values), Int64(0)}, AnyType(values)},
                           {{AnyType(values), Int64(2)}, AnyType(TestArray {Int32(1), Int32(2)})},
                           {{AnyType(values), Int64(4)}, AnyType(empty)}},
                          true, false);

    check_array_trim_case(
            {{{AnyType(values), Int64(1)}, AnyType(TestArray {Int32(1), Int32(2), Int32(3)})},
             {{AnyType(short_values), Int64(1)}, AnyType(TestArray {Int32(5)})},
             {{AnyType(values_with_null), Int64(1)}, AnyType(TestArray {Int32(1), Null()})},
             {{Null(), Int64(1)}, Null()}},
            false, true);

    check_array_trim_case({{{AnyType(values), Int64(2)}, AnyType(TestArray {Int32(1), Int32(2)})}},
                          true, true);
}

} // namespace doris
