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
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

static void check_array_remove_case(DataTypePtr array_type, DataTypePtr element_type,
                                    const TestArray& array, const AnyType& target,
                                    const TestArray& expected, bool const_array,
                                    bool const_target) {
    MutableColumnPtr array_column = array_type->create_column();
    ASSERT_TRUE(insert_cell(array_column, array_type, array));
    MutableColumnPtr target_column = element_type->create_column();
    ASSERT_TRUE(insert_cell(target_column, element_type, target));

    constexpr size_t row_size = 1;
    if (const_array) {
        array_column = ColumnConst::create(std::move(array_column), row_size);
    }
    if (const_target) {
        target_column = ColumnConst::create(std::move(target_column), row_size);
    }

    Block block;
    block.insert({std::move(array_column), array_type, "array"});
    block.insert({std::move(target_column), element_type, "target"});

    DataTypePtr return_type = array_type;
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            "array_remove", block.get_columns_with_type_and_name(), return_type);
    ASSERT_NE(func, nullptr);

    ColumnNumbers arguments = {0, 1};
    std::vector<DataTypePtr> arg_types = {array_type, element_type};
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols = {nullptr, nullptr};
    if (const_array) {
        constant_cols[0] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(0).column);
    }
    if (const_target) {
        constant_cols[1] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(1).column);
    }

    FunctionUtils fn_utils(return_type, arg_types, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->set_constant_cols(constant_cols);
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto result_idx = block.columns() - 1;
    ASSERT_TRUE(func->execute(fn_ctx, block, arguments, result_idx, row_size).ok());
    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));

    MutableColumnPtr expected_column = return_type->create_column();
    ASSERT_TRUE(insert_cell(expected_column, return_type, expected));
    Field result_value;
    block.get_by_position(result_idx).column->get(0, result_value);
    Field expected_value;
    expected_column->get(0, expected_value);
    EXPECT_EQ(result_value, expected_value)
            << "result: " << return_type->to_string(*block.get_by_position(result_idx).column, 0)
            << ", expected: " << return_type->to_string(*expected_column, 0);
}

TEST(function_array_remove_test, const_arguments) {
    {
        auto element_type = make_nullable(std::make_shared<DataTypeInt32>());
        auto array_type = std::make_shared<DataTypeArray>(element_type);
        TestArray array = {Int32(1), Null(), Int32(2), Int32(1)};
        TestArray expected = {Null(), Int32(2)};
        check_array_remove_case(array_type, element_type, array, Int32(1), expected, true, false);
    }
    {
        auto element_type = make_nullable(std::make_shared<DataTypeString>());
        auto array_type = std::make_shared<DataTypeArray>(element_type);
        TestArray array = {std::string("abc"), Null(), std::string("def"), std::string("abc")};
        TestArray expected = {Null(), std::string("def")};
        check_array_remove_case(array_type, element_type, array, std::string("abc"), expected,
                                false, true);
    }
    {
        auto element_type = make_nullable(std::make_shared<DataTypeInt32>());
        auto array_type = std::make_shared<DataTypeArray>(element_type);
        TestArray array = {Int32(1), Null(), Int32(2), Null()};
        TestArray expected = {Int32(1), Int32(2)};
        check_array_remove_case(array_type, element_type, array, Null(), expected, true, true);
    }
}

} // namespace doris
