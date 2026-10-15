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
#include <limits>
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
#include "exprs/vectorized_fn_call.h"
#include "gen_cpp/Exprs_types.h"

namespace doris {

static const TestArray kArray = {Int32(1), Int32(2), Int32(3), Int32(4), Int32(5)};

// Two random orders of 20 elements are the same only by a tiny chance, so tests that expect
// different orders use this array.
static const TestArray kLongArray = [] {
    TestArray array;
    for (int32_t i = 0; i < 20; ++i) {
        array.emplace_back(Int32(i));
    }
    return array;
}();

// Runs array_shuffle on one block with one row per element of arrays, and puts each result row
// as a string in results. With const_array, the arrays must all be the same and the array column
// is a ColumnConst. An empty seeds runs array_shuffle(array), one seed is a constant seed, and
// more seeds are a seed column with one seed per row.
// Returns the first failed status of open() and execute().
static Status run_array_shuffle(const std::vector<TestArray>& arrays, bool const_array,
                                const std::vector<int64_t>& seeds,
                                std::vector<std::string>* results) {
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    auto seed_type = std::make_shared<DataTypeInt64>();
    const size_t row_size = arrays.size();
    // Empty rows make a failed call fail the checks instead of reading past the end.
    results->assign(row_size, "");

    MutableColumnPtr array_column = array_type->create_column();
    for (size_t i = 0; i < (const_array ? 1 : row_size); ++i) {
        EXPECT_TRUE(insert_cell(array_column, array_type, arrays[i]));
    }
    ColumnPtr array_ptr = std::move(array_column);
    if (const_array) {
        array_ptr = ColumnConst::create(array_ptr, row_size);
    }

    Block block;
    block.insert({array_ptr, array_type, "array"});
    DataTypes arg_types = {array_type};
    ColumnNumbers arguments = {0};
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols = {nullptr};
    if (const_array) {
        constant_cols[0] = std::make_shared<ColumnPtrWrapper>(array_ptr);
    }
    if (!seeds.empty()) {
        MutableColumnPtr seed_column = seed_type->create_column();
        for (auto seed : seeds) {
            EXPECT_TRUE(insert_cell(seed_column, seed_type, seed));
        }
        ColumnPtr seed_ptr = std::move(seed_column);
        constant_cols.push_back(nullptr);
        if (seeds.size() == 1) {
            seed_ptr = ColumnConst::create(seed_ptr, row_size);
            constant_cols[1] = std::make_shared<ColumnPtrWrapper>(seed_ptr);
        } else {
            EXPECT_EQ(seeds.size(), row_size);
        }
        block.insert({seed_ptr, seed_type, "seed"});
        arg_types.push_back(seed_type);
        arguments.push_back(1);
    }

    DataTypePtr return_type = array_type;
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            "array_shuffle", block.get_columns_with_type_and_name(), return_type);
    EXPECT_NE(func, nullptr);

    FunctionUtils fn_utils(return_type, arg_types, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->set_constant_cols(constant_cols);
    RETURN_IF_ERROR(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(func->open(fn_ctx, FunctionContext::THREAD_LOCAL));

    block.insert({nullptr, return_type, "result"});
    auto result_idx = block.columns() - 1;
    RETURN_IF_ERROR(func->execute(fn_ctx, block, arguments, result_idx, row_size));
    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));

    const auto& result_column = *block.get_by_position(result_idx).column;
    for (size_t i = 0; i < row_size; ++i) {
        (*results)[i] = return_type->to_string(result_column, i);
    }
    return Status::OK();
}

// Builds an array_shuffle(array) call under the given function name.
static VExprSPtr array_shuffle_call(const std::string& function_name) {
    DataTypePtr array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({array_type->to_thrift()});
    fn.__set_ret_type(array_type->to_thrift());
    fn.__set_has_var_args(true);

    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(array_type->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(1);
    node.__set_is_nullable(true);
    return VectorizedFnCall::create_shared(node);
}

// Runs array_shuffle(array, seed) with a constant seed, which must succeed.
static std::vector<std::string> shuffle_with_const_seed(const std::vector<TestArray>& arrays,
                                                        int64_t seed, bool const_array = false) {
    std::vector<std::string> results;
    auto st = run_array_shuffle(arrays, const_array, {seed}, &results);
    EXPECT_TRUE(st.ok()) << st;
    return results;
}

// Runs array_shuffle(array) on a constant array, which must succeed.
static std::vector<std::string> shuffle_const_array_without_seed(const TestArray& array,
                                                                 size_t row_size) {
    std::vector<std::string> results;
    auto st = run_array_shuffle(std::vector<TestArray>(row_size, array), true, {}, &results);
    EXPECT_TRUE(st.ok()) << st;
    return results;
}

// The rows of a block draw from one random sequence that starts from the seed, so a per-row
// seed would be ignored. A non-constant seed is rejected instead.
TEST(function_array_shuffle_test, non_constant_seed) {
    std::vector<std::string> results;
    auto st = run_array_shuffle({kArray, kArray}, false, {1, 2}, &results);
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st;
    EXPECT_NE(st.to_string().find("must be a constant"), std::string::npos) << st;
}

// A constant seed gives the same result each time, and the first row gives the same result as
// running that row alone.
TEST(function_array_shuffle_test, const_seed) {
    auto seed1 = shuffle_with_const_seed({kArray}, 1);
    auto results = shuffle_with_const_seed({kArray, kArray, kArray}, 1);
    EXPECT_EQ(results[0], seed1[0]);
    EXPECT_EQ(shuffle_with_const_seed({kArray, kArray, kArray}, 1), results);
    EXPECT_NE(shuffle_with_const_seed({kLongArray}, 2), shuffle_with_const_seed({kLongArray}, 1));
}

// Any BIGINT is a valid seed, a negative one too. All 64 bits are used, so seeds with the same
// low 32 bits still give different results.
TEST(function_array_shuffle_test, any_bigint_seed) {
    EXPECT_NE(shuffle_with_const_seed({kLongArray}, -1),
              shuffle_with_const_seed({kLongArray}, 4294967295));
    EXPECT_NE(shuffle_with_const_seed({kLongArray}, 4294967301),
              shuffle_with_const_seed({kLongArray}, 5));
    EXPECT_NE(shuffle_with_const_seed({kLongArray}, std::numeric_limits<int64_t>::min()),
              shuffle_with_const_seed({kLongArray}, 0));
    EXPECT_NE(shuffle_with_const_seed({kLongArray}, std::numeric_limits<int64_t>::max()),
              shuffle_with_const_seed({kLongArray}, -1));
}

// Arrays with 0 or 1 element stay the same.
TEST(function_array_shuffle_test, short_arrays) {
    const TestArray empty_array = {};
    const TestArray one_element = {Int32(7)};
    auto results = shuffle_with_const_seed({empty_array, one_element, kArray}, 1);
    EXPECT_EQ(results[0], "[]");
    EXPECT_EQ(results[1], "[7]");
}

// A constant array is still shuffled on each row, so it gives the same rows as the same arrays
// in a column, and the rows do not all get the same order.
TEST(function_array_shuffle_test, const_array) {
    const std::vector<TestArray> arrays(3, kLongArray);
    auto from_column = shuffle_with_const_seed(arrays, 1);
    EXPECT_EQ(shuffle_with_const_seed(arrays, 1, true), from_column);
    EXPECT_NE(from_column[0], from_column[1]);
}

// Without a seed, each row and each call gets a new random order.
TEST(function_array_shuffle_test, no_seed) {
    auto first = shuffle_const_array_without_seed(kLongArray, 2);
    auto second = shuffle_const_array_without_seed(kLongArray, 2);
    EXPECT_NE(first[0], first[1]);
    EXPECT_NE(first[0], second[0]);
}

// Running array_shuffle again on other rows gives other orders, so it is not deterministic, and
// a scan must not run it twice, for example as a file-local filter copy and again in the scanner.
TEST(function_array_shuffle_test, not_deterministic) {
    EXPECT_FALSE(array_shuffle_call("array_shuffle")->is_deterministic());
    EXPECT_FALSE(array_shuffle_call("shuffle")->is_deterministic());
}

} // namespace doris
