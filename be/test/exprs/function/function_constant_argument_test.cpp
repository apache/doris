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
#include <random>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/column_helper.h"
#include "testutil/function_utils.h"

namespace doris {

// FE requires these arguments to be constants, but BE does not evaluate some constant expressions,
// such as arithmetic ones, in open, and some of them, such as IF ones, are not a ColumnConst in
// execute. Then FunctionContext has no constant column for them, and the functions read the value
// from the first row in execute.
class FunctionConstantArgumentTest : public testing::Test {
protected:
    // Executes the function on each block with one FunctionContext. The arguments are the first
    // columns of a block, and constant_cols gives the columns that open sees as constants, nullptr
    // for an argument that is not evaluated in open. The result is appended to each block.
    static Status execute(const std::string& name, std::vector<Block>& blocks,
                          const DataTypePtr& return_type,
                          const std::vector<std::shared_ptr<ColumnPtrWrapper>>& constant_cols) {
        auto arguments = blocks[0].get_columns_with_type_and_name();
        FunctionBasePtr function =
                SimpleFunctionFactory::instance().get_function(name, arguments, return_type);
        EXPECT_TRUE(function != nullptr);

        std::vector<DataTypePtr> arg_types;
        ColumnNumbers argument_indexes;
        for (size_t i = 0; i < arguments.size(); ++i) {
            arg_types.push_back(arguments[i].type);
            argument_indexes.push_back(i);
        }

        FunctionUtils fn_utils(return_type, arg_types, false);
        auto* fn_ctx = fn_utils.get_fn_ctx();
        fn_ctx->set_constant_cols(constant_cols);
        RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
        Status status;
        for (auto& block : blocks) {
            size_t rows = block.rows();
            block.insert({nullptr, return_type, "result"});
            status = function->execute(fn_ctx, block, argument_indexes, arguments.size(), rows);
            if (!status.ok()) {
                break;
            }
        }
        static_cast<void>(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
        static_cast<void>(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        return status;
    }

    static Status execute(const std::string& name, Block& block, const DataTypePtr& return_type,
                          const std::vector<std::shared_ptr<ColumnPtrWrapper>>& constant_cols) {
        std::vector<Block> blocks;
        blocks.push_back(std::move(block));
        Status status = execute(name, blocks, return_type, constant_cols);
        block = std::move(blocks[0]);
        return status;
    }

    static std::shared_ptr<ColumnPtrWrapper> constant(const ColumnPtr& column) {
        return std::make_shared<ColumnPtrWrapper>(column);
    }

    static ColumnPtr strings(const std::vector<std::string>& values) {
        auto column = ColumnString::create();
        for (const auto& value : values) {
            column->insert_data(value.data(), value.size());
        }
        return column;
    }
};

TEST_F(FunctionConstantArgumentTest, regexp_replace_options) {
    auto string_type = std::make_shared<DataTypeString>();
    auto return_type = make_nullable(string_type);
    ColumnPtr pattern = ColumnConst::create(strings({"a"}), 2);
    ColumnPtr replacement = ColumnConst::create(strings({"\\x"}), 2);
    for (std::string name : {"regexp_replace", "regexp_replace_one"}) {
        // the regex is compiled with the options in the first block, and reused in the second one
        std::vector<Block> blocks(2);
        for (auto& block : blocks) {
            block.insert({strings({"aa", "xa"}), string_type, "s"});
            block.insert({pattern, string_type, "pattern"});
            block.insert({replacement, string_type, "replacement"});
            block.insert({strings({"ignore_invalid_escape", "ignore_invalid_escape"}), string_type,
                          "options"});
        }
        Status status = execute(name, blocks, return_type,
                                {nullptr, constant(pattern), constant(replacement), nullptr});
        ASSERT_TRUE(status.ok()) << status.to_string();
        for (const auto& block : blocks) {
            // ignore_invalid_escape replaces with x for the invalid escape \x, without it the
            // replacement fails
            const auto& result = *block.get_by_position(4).column;
            EXPECT_EQ(result.get_data_at(0).to_string(), name == "regexp_replace" ? "xx" : "xa");
            EXPECT_EQ(result.get_data_at(1).to_string(), "xx");
        }
    }

    // an invalid pattern is reported when it is compiled in execute
    Block invalid_block;
    ColumnPtr invalid_pattern = ColumnConst::create(strings({"("}), 1);
    invalid_block.insert({strings({"a"}), string_type, "s"});
    invalid_block.insert({invalid_pattern, string_type, "pattern"});
    invalid_block.insert({ColumnConst::create(strings({"b"}), 1), string_type, "replacement"});
    invalid_block.insert({strings({""}), string_type, "options"});
    Status status = execute("regexp_replace", invalid_block, return_type,
                            {nullptr, constant(invalid_pattern), nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Could not compile regexp pattern"), std::string::npos)
            << status.to_string();

    // an empty block does not compile the regex
    Block empty_block;
    empty_block.insert({strings({}), string_type, "s"});
    empty_block.insert({ColumnConst::create(strings({"a"}), 0), string_type, "pattern"});
    empty_block.insert({ColumnConst::create(strings({"b"}), 0), string_type, "replacement"});
    empty_block.insert({strings({}), string_type, "options"});
    status = execute("regexp_replace", empty_block, return_type,
                     {nullptr, nullptr, nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(empty_block.get_by_position(4).column->size(), 0);
}

TEST_F(FunctionConstantArgumentTest, date_trunc_unit) {
    auto date_type = std::make_shared<DataTypeDateV2>();
    auto string_type = std::make_shared<DataTypeString>();
    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2024, 3, 15, 0, 0, 0, 0);
    DateV2Value<DateV2ValueType> month;
    month.unchecked_set_time(2024, 3, 1, 0, 0, 0, 0);
    DateV2Value<DateV2ValueType> year;
    year.unchecked_set_time(2024, 1, 1, 0, 0, 0, 0);

    // the state is created from the first block, and reused in the second one
    std::vector<Block> blocks(2);
    for (auto& block : blocks) {
        block.insert({ColumnHelper::create_column<DataTypeDateV2>({date, date}), date_type, "d"});
        block.insert({strings({"month", "month"}), string_type, "unit"});
    }
    Status status = execute("date_trunc", blocks, date_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (const auto& block : blocks) {
        const auto& result = assert_cast<const ColumnDateV2&>(*block.get_by_position(2).column);
        EXPECT_EQ(result.get_element(0), month);
        EXPECT_EQ(result.get_element(1), month);
    }

    // the time unit may also be the first argument
    Block unit_first_block;
    unit_first_block.insert({strings({"year"}), string_type, "unit"});
    unit_first_block.insert({ColumnHelper::create_column<DataTypeDateV2>({date}), date_type, "d"});
    status = execute("date_trunc", unit_first_block, date_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(assert_cast<const ColumnDateV2&>(*unit_first_block.get_by_position(2).column)
                      .get_element(0),
              year);

    // an illegal time unit is still reported
    Block illegal_block;
    illegal_block.insert({ColumnHelper::create_column<DataTypeDateV2>({date}), date_type, "d"});
    illegal_block.insert({strings({"x"}), string_type, "unit"});
    status = execute("date_trunc", illegal_block, date_type, {nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Illegal second argument"), std::string::npos)
            << status.to_string();

    // an empty block does not read the time unit
    Block empty_block;
    empty_block.insert({ColumnHelper::create_column<DataTypeDateV2>({}), date_type, "d"});
    empty_block.insert({strings({}), string_type, "unit"});
    status = execute("date_trunc", empty_block, date_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(empty_block.get_by_position(2).column->size(), 0);
}

TEST_F(FunctionConstantArgumentTest, random_seed) {
    auto bigint_type = std::make_shared<DataTypeInt64>();
    auto double_type = std::make_shared<DataTypeFloat64>();
    // the generator is seeded once with the first row of the first non-empty block
    std::vector<Block> blocks(3);
    blocks[0].insert({ColumnHelper::create_column<DataTypeInt64>({}), bigint_type, "seed"});
    blocks[1].insert({ColumnHelper::create_column<DataTypeInt64>({10, 10}), bigint_type, "seed"});
    blocks[2].insert({ColumnHelper::create_column<DataTypeInt64>({10}), bigint_type, "seed"});
    Status status = execute("random", blocks, double_type, {nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();

    std::mt19937_64 generator(10);
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    const auto& first = assert_cast<const ColumnFloat64&>(*blocks[1].get_by_position(1).column);
    const auto& second = assert_cast<const ColumnFloat64&>(*blocks[2].get_by_position(1).column);
    EXPECT_EQ(first.get_element(0), distribution(generator));
    EXPECT_EQ(first.get_element(1), distribution(generator));
    EXPECT_EQ(second.get_element(0), distribution(generator));
}

TEST_F(FunctionConstantArgumentTest, random_range) {
    auto bigint_type = std::make_shared<DataTypeInt64>();
    Block block;
    block.insert({ColumnHelper::create_column<DataTypeInt64>({5, 5, 5}), bigint_type, "min"});
    block.insert({ColumnHelper::create_column<DataTypeInt64>({7, 7, 7}), bigint_type, "max"});
    Status status = execute("random", block, bigint_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result = assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_GE(result.get_element(i), 5);
        EXPECT_LE(result.get_element(i), 7);
    }

    // the bounds are still validated
    Block illegal_block;
    illegal_block.insert({ColumnHelper::create_column<DataTypeInt64>({7}), bigint_type, "min"});
    illegal_block.insert({ColumnHelper::create_column<DataTypeInt64>({5}), bigint_type, "max"});
    status = execute("random", illegal_block, bigint_type, {nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("lower bound should less than upper bound"),
              std::string::npos)
            << status.to_string();
}

TEST_F(FunctionConstantArgumentTest, uniform_bounds) {
    auto bigint_type = std::make_shared<DataTypeInt64>();
    auto expected = [](int64_t seed) {
        std::mt19937_64 generator(seed);
        std::uniform_int_distribution<int64_t> distribution(1, 10);
        return distribution(generator);
    };

    // the bounds are ColumnConst, or full columns holding the same value
    std::vector<ColumnPtr> min_columns {
            ColumnConst::create(ColumnHelper::create_column<DataTypeInt64>({1}), 2),
            ColumnHelper::create_column<DataTypeInt64>({1, 1})};
    std::vector<ColumnPtr> max_columns {
            ColumnConst::create(ColumnHelper::create_column<DataTypeInt64>({10}), 2),
            ColumnHelper::create_column<DataTypeInt64>({10, 10})};
    for (size_t i = 0; i < min_columns.size(); ++i) {
        Block block;
        block.insert({min_columns[i], bigint_type, "min"});
        block.insert({max_columns[i], bigint_type, "max"});
        block.insert({ColumnHelper::create_column<DataTypeInt64>({101, 202}), bigint_type, "gen"});
        Status status = execute("uniform", block, bigint_type, {nullptr, nullptr, nullptr});
        ASSERT_TRUE(status.ok()) << status.to_string();

        const auto& result = assert_cast<const ColumnInt64&>(*block.get_by_position(3).column);
        EXPECT_EQ(result.get_element(0), expected(101));
        EXPECT_EQ(result.get_element(1), expected(202));
    }
}

TEST_F(FunctionConstantArgumentTest, array_apply_op_and_value) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(make_nullable(int_type));
    auto string_type = std::make_shared<DataTypeString>();

    auto nested =
            ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3, 3, 4}, {0, 0, 0, 0, 0});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);
    offsets->insert_value(5);

    // op and val are full columns holding the same value
    Block block;
    block.insert({ColumnArray::create(nested, std::move(offsets)), array_type, "arr"});
    block.insert({strings({">", ">"}), string_type, "op"});
    block.insert({ColumnHelper::create_column<DataTypeInt32>({2, 2}), int_type, "val"});
    Status status = execute("array_apply", block, array_type, {nullptr, nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result = *block.get_by_position(3).column;
    EXPECT_EQ(array_type->to_string(result, 0), "[3]");
    EXPECT_EQ(array_type->to_string(result, 1), "[3, 4]");
}

} // namespace doris
