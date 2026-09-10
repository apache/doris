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
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

namespace {

void check_array_except_all(const DataTypePtr& array_type, const TestArray& left,
                            const TestArray& right, const TestArray& expected, bool left_const,
                            bool right_const) {
    MutableColumnPtr left_column = array_type->create_column();
    ASSERT_TRUE(insert_cell(left_column, array_type, left));
    MutableColumnPtr right_column = array_type->create_column();
    ASSERT_TRUE(insert_cell(right_column, array_type, right));

    constexpr size_t row_count = 1;
    if (left_const) {
        left_column = ColumnConst::create(std::move(left_column), row_count);
    }
    if (right_const) {
        right_column = ColumnConst::create(std::move(right_column), row_count);
    }

    Block block;
    block.insert({std::move(left_column), array_type, "left"});
    block.insert({std::move(right_column), array_type, "right"});
    auto function = SimpleFunctionFactory::instance().get_function(
            "array_except_all", block.get_columns_with_type_and_name(), array_type);
    ASSERT_NE(function, nullptr);

    FunctionUtils function_utils(array_type, {array_type, array_type}, false);
    auto* context = function_utils.get_fn_ctx();
    ASSERT_TRUE(function->open(context, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::THREAD_LOCAL).ok());
    block.insert({nullptr, array_type, "result"});
    ASSERT_TRUE(function->execute(context, block, {0, 1}, 2, row_count).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context, FunctionContext::FRAGMENT_LOCAL).ok());

    MutableColumnPtr expected_column = array_type->create_column();
    ASSERT_TRUE(insert_cell(expected_column, array_type, expected));
    Field actual_value;
    block.get_by_position(2).column->get(0, actual_value);
    Field expected_value;
    expected_column->get(0, expected_value);
    EXPECT_EQ(actual_value, expected_value);
}

} // namespace

TEST(function_array_except_all_test, integer_multiset_semantics) {
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    check_array_except_all(array_type, {Int32(1), Int32(1), Int32(2)}, {Int32(1)},
                           {Int32(1), Int32(2)}, false, false);
    check_array_except_all(array_type, {Int32(1), Int32(1)}, {Int32(1), Int32(1), Int32(1)}, {},
                           true, false);
    check_array_except_all(array_type, {Int32(1), Int32(2)}, {}, {Int32(1), Int32(2)}, false, true);
}

TEST(function_array_except_all_test, null_and_string_counts) {
    auto integer_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    check_array_except_all(integer_array_type, {Null(), Null(), Int32(1)}, {Null()},
                           {Null(), Int32(1)}, false, false);
    check_array_except_all(integer_array_type, {Null()}, {Null(), Null()}, {}, false, false);

    auto string_array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>()));
    check_array_except_all(string_array_type,
                           {std::string("a"), std::string("a"), std::string("b")},
                           {std::string("a")}, {std::string("a"), std::string("b")}, false, false);
}

} // namespace doris
