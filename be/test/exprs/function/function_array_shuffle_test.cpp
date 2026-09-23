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

#include <cstdint>
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

static const TestArray kArray = {Int32(1), Int32(2), Int32(3), Int32(4), Int32(5)};

// Runs array_shuffle(array, seed) on one block and returns each result row as a string.
// With const_seed, seeds must hold one value and the seed column is a ColumnConst.
static std::vector<std::string> run_array_shuffle(const std::vector<TestArray>& arrays,
                                                  const std::vector<int64_t>& seeds,
                                                  bool const_seed) {
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    auto seed_type = std::make_shared<DataTypeInt64>();
    const size_t row_size = arrays.size();

    MutableColumnPtr array_column = array_type->create_column();
    for (const auto& array : arrays) {
        EXPECT_TRUE(insert_cell(array_column, array_type, array));
    }
    MutableColumnPtr seed_column = seed_type->create_column();
    for (auto seed : seeds) {
        EXPECT_TRUE(insert_cell(seed_column, seed_type, seed));
    }
    if (const_seed) {
        EXPECT_EQ(seeds.size(), 1);
        seed_column = ColumnConst::create(std::move(seed_column), row_size);
    } else {
        EXPECT_EQ(seeds.size(), row_size);
    }

    Block block;
    block.insert({std::move(array_column), array_type, "array"});
    block.insert({std::move(seed_column), seed_type, "seed"});

    DataTypePtr return_type = array_type;
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            "array_shuffle", block.get_columns_with_type_and_name(), return_type);
    EXPECT_NE(func, nullptr);

    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols = {nullptr, nullptr};
    if (const_seed) {
        constant_cols[1] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(1).column);
    }
    FunctionUtils fn_utils(return_type, {array_type, seed_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->set_constant_cols(constant_cols);
    EXPECT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto result_idx = block.columns() - 1;
    EXPECT_TRUE(func->execute(fn_ctx, block, {0, 1}, result_idx, row_size).ok());
    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));

    std::vector<std::string> results;
    const auto& result_column = *block.get_by_position(result_idx).column;
    for (size_t i = 0; i < row_size; ++i) {
        results.push_back(return_type->to_string(result_column, i));
    }
    return results;
}

// Each row must use its own seed, and give the same result as running that row alone.
TEST(function_array_shuffle_test, seed_per_row) {
    auto seed1 = run_array_shuffle({kArray}, {1}, false)[0];
    auto seed2 = run_array_shuffle({kArray}, {2}, false)[0];
    ASSERT_NE(seed1, seed2);

    auto results = run_array_shuffle({kArray, kArray, kArray}, {1, 2, 1}, false);
    EXPECT_EQ(results[0], seed1);
    EXPECT_EQ(results[1], seed2);
    EXPECT_EQ(results[2], seed1);
}

// A constant seed gives the same result on every row.
TEST(function_array_shuffle_test, const_seed) {
    auto seed1 = run_array_shuffle({kArray}, {1}, false)[0];
    auto results = run_array_shuffle({kArray, kArray, kArray}, {1}, true);
    for (const auto& result : results) {
        EXPECT_EQ(result, seed1);
    }
}

} // namespace doris
