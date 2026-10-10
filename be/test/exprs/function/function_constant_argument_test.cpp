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

    static ColumnPtr null_string(size_t rows) {
        return ColumnConst::create(ColumnNullable::create(strings({""}), ColumnUInt8::create(1, 1)),
                                   rows);
    }

    static ColumnPtr null_int32(size_t rows) {
        return ColumnConst::create(
                ColumnNullable::create(ColumnHelper::create_column<DataTypeInt32>({0}),
                                       ColumnUInt8::create(1, 1)),
                rows);
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

TEST_F(FunctionConstantArgumentTest, regexp_replace_pattern_changes_across_blocks) {
    // A lazy join can pass a probe-side pattern as a physical ColumnConst for one block while the
    // pattern is not a query-level constant, so FE's constant_cols entry for it is nullptr here
    // even though each block's pattern column is still a ColumnConst. Caching the regex compiled
    // from the first block's pattern must not leak into a later block whose physical constant
    // differs.
    auto string_type = std::make_shared<DataTypeString>();
    auto return_type = make_nullable(string_type);
    for (std::string name : {"regexp_replace", "regexp_replace_one"}) {
        std::vector<Block> blocks(2);
        blocks[0].insert({strings({"a"}), string_type, "s"});
        blocks[0].insert({ColumnConst::create(strings({"a"}), 1), string_type, "pattern"});
        blocks[0].insert({ColumnConst::create(strings({"x"}), 1), string_type, "replacement"});
        blocks[0].insert({strings({"ignore_invalid_escape"}), string_type, "options"});
        blocks[1].insert({strings({"b"}), string_type, "s"});
        blocks[1].insert({ColumnConst::create(strings({"b"}), 1), string_type, "pattern"});
        blocks[1].insert({ColumnConst::create(strings({"x"}), 1), string_type, "replacement"});
        blocks[1].insert({strings({"ignore_invalid_escape"}), string_type, "options"});
        Status status = execute(name, blocks, return_type, {nullptr, nullptr, nullptr, nullptr});
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(blocks[0].get_by_position(4).column->get_data_at(0).to_string(), "x");
        EXPECT_EQ(blocks[1].get_by_position(4).column->get_data_at(0).to_string(), "x");
    }
}

TEST_F(FunctionConstantArgumentTest, regexp_replace_null_options) {
    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_string_type = make_nullable(string_type);
    for (std::string name : {"regexp_replace", "regexp_replace_one"}) {
        Block block;
        ColumnPtr pattern = ColumnConst::create(strings({"["}), 2);
        ColumnPtr options = null_string(2);
        block.insert({strings({"abc", "def"}), string_type, "s"});
        block.insert({pattern, string_type, "pattern"});
        block.insert({ColumnConst::create(strings({"x"}), 2), string_type, "replacement"});
        block.insert({options, nullable_string_type, "options"});
        Status status = execute(name, block, nullable_string_type,
                                {nullptr, constant(pattern), nullptr, constant(options)});
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto& result = *block.get_by_position(4).column;
        ASSERT_EQ(result.size(), 2);
        EXPECT_TRUE(result.is_null_at(0));
        EXPECT_TRUE(result.is_null_at(1));
    }
}

TEST_F(FunctionConstantArgumentTest, regexp_replace_constant_source) {
    auto string_type = std::make_shared<DataTypeString>();
    auto return_type = make_nullable(string_type);
    for (std::string name : {"regexp_replace", "regexp_replace_one"}) {
        Block block;
        block.insert({ColumnConst::create(strings({"abc"}), 2), string_type, "s"});
        block.insert({ColumnConst::create(strings({"a"}), 2), string_type, "pattern"});
        block.insert({strings({"x", "y"}), string_type, "replacement"});
        block.insert({strings({"", ""}), string_type, "options"});
        Status status = execute(name, block, return_type, {nullptr, nullptr, nullptr, nullptr});
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto& result = *block.get_by_position(4).column;
        ASSERT_EQ(result.size(), 2);
        EXPECT_EQ(result.get_data_at(0).to_string(), "xbc");
        EXPECT_EQ(result.get_data_at(1).to_string(), "ybc");
    }
}

TEST_F(FunctionConstantArgumentTest, regexp_replace_empty_pattern_options) {
    // A constant options argument BE evaluates to a full column, e.g. an IF on uniform(), is not
    // compiled in open, and an empty constant pattern leaves no compiled regex either, so each row
    // compiles the pattern. The options must reach that compilation when the pattern and the
    // replacement are physical constants too.
    auto string_type = std::make_shared<DataTypeString>();
    auto return_type = make_nullable(string_type);
    ColumnPtr pattern = ColumnConst::create(strings({""}), 2);
    ColumnPtr replacement = ColumnConst::create(strings({"\\x"}), 2);
    for (std::string name : {"regexp_replace", "regexp_replace_one"}) {
        Block block;
        block.insert({strings({"a", "b"}), string_type, "s"});
        block.insert({pattern, string_type, "pattern"});
        block.insert({replacement, string_type, "replacement"});
        block.insert({strings({"ignore_invalid_escape", "ignore_invalid_escape"}), string_type,
                      "options"});
        Status status = execute(name, block, return_type,
                                {nullptr, constant(pattern), constant(replacement), nullptr});
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto& result = *block.get_by_position(4).column;
        ASSERT_EQ(result.size(), 2);
        // the empty pattern matches before and after each character, and ignore_invalid_escape
        // replaces with x for the invalid escape \x; without it the replacement fails
        EXPECT_EQ(result.get_data_at(0).to_string(), name == "regexp_replace" ? "xax" : "xa");
        EXPECT_EQ(result.get_data_at(1).to_string(), name == "regexp_replace" ? "xbx" : "xb");
    }
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

    // an illegal time unit is still reported, including one that only starts with a legal unit
    for (std::string unit : {"x", "month0", "days"}) {
        Block illegal_block;
        illegal_block.insert({ColumnHelper::create_column<DataTypeDateV2>({date}), date_type, "d"});
        illegal_block.insert({strings({unit}), string_type, "unit"});
        status = execute("date_trunc", illegal_block, date_type, {nullptr, nullptr});
        EXPECT_FALSE(status.ok()) << unit;
        EXPECT_NE(status.to_string().find("Illegal second argument"), std::string::npos)
                << status.to_string();

        Block illegal_unit_first_block;
        illegal_unit_first_block.insert({strings({unit}), string_type, "unit"});
        illegal_unit_first_block.insert(
                {ColumnHelper::create_column<DataTypeDateV2>({date}), date_type, "d"});
        status = execute("date_trunc", illegal_unit_first_block, date_type, {nullptr, nullptr});
        EXPECT_FALSE(status.ok()) << unit;
    }

    // an empty block does not read the time unit
    Block empty_block;
    empty_block.insert({ColumnHelper::create_column<DataTypeDateV2>({}), date_type, "d"});
    empty_block.insert({strings({}), string_type, "unit"});
    status = execute("date_trunc", empty_block, date_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(empty_block.get_by_position(2).column->size(), 0);
}

TEST_F(FunctionConstantArgumentTest, date_trunc_null_unit) {
    auto date_type = std::make_shared<DataTypeDateV2>();
    auto string_type = make_nullable(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(date_type);
    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2024, 3, 15, 0, 0, 0, 0);
    for (bool unit_first : {false, true}) {
        Block block;
        ColumnPtr unit = null_string(2);
        ColumnPtr dates = ColumnHelper::create_column<DataTypeDateV2>({date, date});
        if (unit_first) {
            block.insert({unit, string_type, "unit"});
            block.insert({dates, date_type, "date"});
        } else {
            block.insert({dates, date_type, "date"});
            block.insert({unit, string_type, "unit"});
        }
        std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols(2);
        constant_cols[unit_first ? 0 : 1] = constant(unit);
        Status status = execute("date_trunc", block, return_type, constant_cols);
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto& result = *block.get_by_position(2).column;
        ASSERT_EQ(result.size(), 2);
        EXPECT_TRUE(result.is_null_at(0));
        EXPECT_TRUE(result.is_null_at(1));
    }
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

    // A constant source stays compact even when the value is evaluated as a full column.
    auto const_nested = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 0, 0});
    auto const_offsets = ColumnArray::ColumnOffsets::create();
    const_offsets->insert_value(3);
    Block const_block;
    const_block.insert(
            {ColumnConst::create(ColumnArray::create(const_nested, std::move(const_offsets)), 2),
             array_type, "arr"});
    const_block.insert({strings({">", ">"}), string_type, "op"});
    const_block.insert({ColumnHelper::create_column<DataTypeInt32>({2, 2}), int_type, "val"});
    status = execute("array_apply", const_block, array_type, {nullptr, nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& const_result = *const_block.get_by_position(3).column;
    ASSERT_EQ(const_result.size(), 2);
    EXPECT_TRUE(is_column_const(const_result));
    EXPECT_EQ(array_type->to_string(const_result, 0), "[3]");
    EXPECT_EQ(array_type->to_string(const_result, 1), "[3]");
}

// op can be nullable-typed when it is a BE-only expression such as an IF with a NULL branch, even
// when the branch actually taken, and so the value read, is not NULL. A NULL value is rejected the
// same way a literal NULL op is rejected by FE, instead of the generic nullable-argument shortcut
// silently returning NULL for the whole call.
TEST_F(FunctionConstantArgumentTest, array_apply_rejects_null_op) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(make_nullable(int_type));
    auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());

    auto nested = ColumnHelper::create_nullable_column<DataTypeInt32>({1, 2, 3}, {0, 0, 0});
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(3);

    Block block;
    block.insert({ColumnArray::create(nested, std::move(offsets)), array_type, "arr"});
    block.insert({null_string(1), nullable_string_type, "op"});
    block.insert({ColumnHelper::create_column<DataTypeInt32>({2}), int_type, "val"});
    Status status =
            execute("array_apply", block, make_nullable(array_type), {nullptr, nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("op support const value only"), std::string::npos)
            << status.to_string();
}

// A constant argument that BE evaluates to a full column may come with constant arguments that are
// ColumnConst, over several rows.
TEST_F(FunctionConstantArgumentTest, sha2_constant_input) {
    auto string_type = std::make_shared<DataTypeString>();
    auto int_type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert({ColumnConst::create(strings({"abc"}), 2), string_type, "s"});
    block.insert({ColumnHelper::create_column<DataTypeInt32>({256, 256}), int_type, "length"});
    Status status = execute("sha2", block, string_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result = *block.get_by_position(2).column;
    ASSERT_EQ(result.size(), 2);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_EQ(result.get_data_at(i).to_string(),
                  "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}

// The digest length can be nullable-typed when it is a BE-only expression such as an IF with a
// NULL branch, even when the branch actually taken is not NULL. A NULL value is rejected the same
// way a literal NULL length is rejected by FE, instead of the generic nullable-argument shortcut
// silently returning NULL for the whole call.
TEST_F(FunctionConstantArgumentTest, sha2_rejects_null_digest_length) {
    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_int_type = make_nullable(std::make_shared<DataTypeInt32>());
    Block block;
    block.insert({ColumnConst::create(strings({"abc"}), 1), string_type, "s"});
    block.insert({null_int32(1), nullable_int_type, "length"});
    Status status = execute("sha2", block, make_nullable(string_type), {nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("digest length"), std::string::npos) << status.to_string();
}

TEST_F(FunctionConstantArgumentTest, split_by_regexp_limit) {
    auto string_type = std::make_shared<DataTypeString>();
    auto int_type = std::make_shared<DataTypeInt32>();
    auto return_type = std::make_shared<DataTypeArray>(make_nullable(string_type));

    // the source and the pattern are ColumnConst beside a full column limit
    Block block;
    block.insert({ColumnConst::create(strings({"a,b,c"}), 3), string_type, "s"});
    block.insert({ColumnConst::create(strings({","}), 3), string_type, "pattern"});
    block.insert({ColumnHelper::create_column<DataTypeInt32>({2, 2, 2}), int_type, "limit"});
    Status status = execute("split_by_regexp", block, return_type, {nullptr, nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& result = *block.get_by_position(3).column;
    ASSERT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_EQ(return_type->to_string(result, i), R"(["a", "b,c"])");
    }

    // a negative limit is reported
    Block negative_block;
    negative_block.insert({strings({"a,b,c"}), string_type, "s"});
    negative_block.insert({ColumnConst::create(strings({","}), 1), string_type, "pattern"});
    negative_block.insert({ColumnHelper::create_column<DataTypeInt32>({-1}), int_type, "limit"});
    status = execute("split_by_regexp", negative_block, return_type, {nullptr, nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("must be a positive constant"), std::string::npos)
            << status.to_string();

    // the limit can be nullable-typed when it is a BE-only expression such as an IF with a NULL
    // branch, even when the branch actually taken is not NULL. A NULL value is rejected the same
    // way a literal NULL limit is rejected by FE, instead of the generic nullable-argument
    // shortcut silently returning NULL for the whole call.
    Block null_limit_block;
    null_limit_block.insert({ColumnConst::create(strings({"a,b,c"}), 1), string_type, "s"});
    null_limit_block.insert({ColumnConst::create(strings({","}), 1), string_type, "pattern"});
    null_limit_block.insert({null_int32(1), make_nullable(int_type), "limit"});
    status = execute("split_by_regexp", null_limit_block, make_nullable(return_type),
                     {nullptr, nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("must be a positive constant"), std::string::npos)
            << status.to_string();
}

TEST_F(FunctionConstantArgumentTest, tokenize_properties) {
    auto string_type = std::make_shared<DataTypeString>();
    // every property is a key-value pair, and spaces around them are allowed
    for (std::string properties :
         {"", "parser='none'", " \"parser\" = \"none\" , lower_case=true "}) {
        Block block;
        block.insert({strings({"hello world"}), string_type, "s"});
        block.insert({strings({properties}), string_type, "properties"});
        Status status = execute("tokenize", block, string_type, {nullptr, nullptr});
        ASSERT_TRUE(status.ok()) << properties << ": " << status.to_string();
    }

    // malformed properties are reported instead of being ignored
    for (std::string properties :
         {"x", "parser='none' x", "parser='none',", "parser='none';a=b", ",parser='none'",
          "parser='none',parser='english'", "parser='none',' parser '='english'"}) {
        Block block;
        block.insert({strings({"hello world"}), string_type, "s"});
        block.insert({strings({properties}), string_type, "properties"});
        Status status = execute("tokenize", block, string_type, {nullptr, nullptr});
        EXPECT_FALSE(status.ok()) << properties;
        EXPECT_NE(status.to_string().find("must be properties format"), std::string::npos)
                << status.to_string();
    }

    // the char filter properties are validated as FE validates literal ones
    for (std::string properties :
         {"char_filter_type=x", "char_filter_type=char_replace",
          "char_filter_type=char_replace,char_filter_pattern=''",
          "char_filter_type=char_replace,char_filter_pattern=a,char_filter_replacement=ab",
          "char_filter_type=char_replace,char_filter_pattern=a,char_filter_replacement=''"}) {
        Block block;
        block.insert({strings({"hello world"}), string_type, "s"});
        block.insert({strings({properties}), string_type, "properties"});
        Status status = execute("tokenize", block, string_type, {nullptr, nullptr});
        EXPECT_FALSE(status.ok()) << properties;
        EXPECT_NE(status.to_string().find("char_filter"), std::string::npos) << status.to_string();
    }
    // a quoted key is trimmed as FE does, so the char filter type is still checked
    Block trimmed_key_block;
    trimmed_key_block.insert({strings({"hello world"}), string_type, "s"});
    trimmed_key_block.insert({strings({"' char_filter_type '='x'"}), string_type, "properties"});
    Status status = execute("tokenize", trimmed_key_block, string_type, {nullptr, nullptr});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid 'char_filter_type'"), std::string::npos)
            << status.to_string();

    Block char_filter_block;
    char_filter_block.insert({strings({"a_b"}), string_type, "s"});
    char_filter_block.insert(
            {strings({"parser=unicode,char_filter_type=char_replace,char_filter_pattern=_"}),
             string_type, "properties"});
    status = execute("tokenize", char_filter_block, string_type, {nullptr, nullptr});
    ASSERT_TRUE(status.ok()) << status.to_string();
}

} // namespace doris
