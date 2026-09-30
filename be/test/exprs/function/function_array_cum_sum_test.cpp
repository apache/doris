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

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

// Runs array_cum_sum on one block with one row per element of arrays, and returns each result
// row as a string.
static std::vector<std::string> run_array_cum_sum(const std::vector<TestArray>& arrays) {
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    auto return_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt64>()));
    const size_t row_size = arrays.size();
    // Empty rows make a failed call fail the checks instead of reading past the end.
    std::vector<std::string> results(row_size);

    MutableColumnPtr array_column = array_type->create_column();
    for (const auto& array : arrays) {
        EXPECT_TRUE(insert_cell(array_column, array_type, array));
    }
    Block block;
    block.insert({std::move(array_column), array_type, "array"});

    DataTypePtr result_type = return_type;
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            "array_cum_sum", block.get_columns_with_type_and_name(), result_type);
    EXPECT_NE(func, nullptr);

    FunctionUtils fn_utils(result_type, {array_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->set_constant_cols({nullptr});
    EXPECT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, result_type, "result"});
    auto result_idx = block.columns() - 1;
    auto st = func->execute(fn_ctx, block, {0}, result_idx, row_size);
    EXPECT_TRUE(st.ok()) << st;
    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    if (!st.ok()) {
        return results;
    }

    const auto& result_column = *block.get_by_position(result_idx).column;
    for (size_t i = 0; i < row_size; ++i) {
        results[i] = result_type->to_string(result_column, i);
    }
    return results;
}

// The NULLs before the first non-NULL element of each array stay NULL, and a later NULL keeps the
// running sum. This holds for every row of a block, so each row gives the same result in a block
// with other rows as alone.
TEST(function_array_cum_sum_test, leading_null_per_row) {
    const std::vector<TestArray> arrays = {
            {Int32(1), Int32(2)},
            {Null(), Int32(2)},
            {Null(), Null(), Int32(3)},
            {Int32(5), Null(), Int32(1)},
            {Null(), Null()},
            {},
            {Null(), Int32(1), Null(), Int32(2), Int32(3)},
    };
    auto results = run_array_cum_sum(arrays);
    for (size_t i = 0; i < arrays.size(); ++i) {
        EXPECT_EQ(results[i], run_array_cum_sum({arrays[i]})[0]) << "row " << i;
    }
    EXPECT_EQ(results[1], "[null, 2]");
    EXPECT_EQ(results[6], "[null, 1, 1, 3, 6]");
}

} // namespace doris
