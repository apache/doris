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

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "cpp/sync_point.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/column_helper.h"
#include "testutil/function_utils.h"
#include "util/defer_op.h"

namespace doris {

class FunctionRegexpPositionTest : public testing::Test {
protected:
    static void execute(Block& block, const std::string& expected_error = "") {
        auto int_type = std::make_shared<DataTypeInt64>();
        DataTypes argument_types;
        bool nullable = false;
        for (const auto& argument : block.get_columns_with_type_and_name()) {
            argument_types.push_back(argument.type);
            nullable |= argument.type->is_nullable();
        }
        const DataTypePtr result_type = nullable ? make_nullable(int_type) : int_type;
        auto pattern = block.get_by_position(1).column;
        auto flags = block.get_by_position(5).column;
        auto function = SimpleFunctionFactory::instance().get_function(
                "regexp_position", block.get_columns_with_type_and_name(), result_type);
        ASSERT_NE(function, nullptr);
        EXPECT_EQ(function->get_name(), "regexp_position");
        auto implementation = std::dynamic_pointer_cast<IFunction>(
                assert_cast<DefaultFunction*>(function.get())->function);
        ASSERT_NE(implementation, nullptr);
        EXPECT_FALSE(implementation->is_variadic());
        EXPECT_EQ(implementation->get_number_of_arguments(), 7);
        EXPECT_EQ(implementation->get_arguments_that_are_always_constant(), (ColumnNumbers {1, 5}));
        EXPECT_TRUE(implementation->get_return_type_impl(argument_types)->equals(*result_type));

        FunctionUtils fn_utils(result_type, argument_types, false);
        auto* context = fn_utils.get_fn_ctx();
        context->set_constant_cols({nullptr, std::make_shared<ConstantExprResult>(pattern), nullptr,
                                    nullptr, nullptr, std::make_shared<ConstantExprResult>(flags),
                                    nullptr});
        ASSERT_TRUE(
                function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        ASSERT_TRUE(
                function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        block.insert({nullptr, result_type, "result"});
        auto status = function->execute(context, block, ColumnNumbers {0, 1, 2, 3, 4, 5, 6}, 7,
                                        block.rows());
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        check_execution_status(status, expected_error);
    }

    static void check_execution_status(const Status& status, const std::string& expected_error) {
        if (expected_error.empty()) {
            ASSERT_TRUE(status.ok()) << status.to_string();
        } else {
            ASSERT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
            EXPECT_NE(status.to_string().find(expected_error), std::string::npos)
                    << status.to_string();
        }
    }
    static Block make_shape_block(bool constant_columns, bool nullable_columns, int null_argument) {
        constexpr size_t rows = 2;
        Block block;
        const std::vector<std::string> names = {"haystack",     "pattern",       "position",
                                                "occurrence",   "return_option", "flags",
                                                "subexpression"};
        const std::vector<std::string> string_values = {"xab", "ab", "", "", "", "", ""};
        for (int argument = 0; argument < 7; ++argument) {
            const bool string_argument = argument == 0 || argument == 1 || argument == 5;
            DataTypePtr type;
            ColumnPtr column;
            if (string_argument) {
                const auto& value = string_values[argument];
                type = std::make_shared<DataTypeString>();
                column = ColumnHelper::create_column<DataTypeString>({value, value});
            } else {
                const Int64 value = argument == 2 || argument == 3 ? 1 : 0;
                type = std::make_shared<DataTypeInt64>();
                column = ColumnHelper::create_column<DataTypeInt64>({value, value});
            }
            if (nullable_columns || argument == null_argument) {
                type = make_nullable(type);
                auto null_map = ColumnUInt8::create(rows, 0);
                null_map->get_data()[0] = argument == null_argument;
                column = ColumnNullable::create(column, std::move(null_map));
            }
            if (constant_columns || argument == 1 || argument == 5) {
                column = ColumnConst::create(column->clone_resized(1), rows);
            }
            block.insert({column, type, names[argument]});
        }
        return block;
    }

    static void check_shape(bool constant_columns, bool nullable_columns) {
        constexpr size_t rows = 2;
        auto block = make_shape_block(constant_columns, nullable_columns, -1);
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& result = block.get_by_position(7);
        EXPECT_EQ(result.type->is_nullable(), nullable_columns);
        ASSERT_EQ(result.column->size(), rows);
        const auto full_result = result.column->convert_to_full_column_if_const();
        const IColumn* values = full_result.get();
        if (nullable_columns) {
            const auto* nullable = check_and_get_column<ColumnNullable>(values);
            ASSERT_NE(nullable, nullptr);
            EXPECT_FALSE(nullable->is_null_at(0) || nullable->is_null_at(1));
            values = &nullable->get_nested_column();
        }
        const auto* positions = check_and_get_column<ColumnInt64>(values);
        ASSERT_NE(positions, nullptr);
        EXPECT_EQ(positions->get_data()[0], 2);
        EXPECT_EQ(positions->get_data()[1], 2);
    }

    static void check_null_argument(bool constant_columns, int null_argument) {
        constexpr size_t rows = 2;
        auto block = make_shape_block(constant_columns, false, null_argument);
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& result = block.get_by_position(7);
        EXPECT_TRUE(result.type->is_nullable());
        ASSERT_EQ(result.column->size(), rows);
        const auto full_result = result.column->convert_to_full_column_if_const();
        const auto* nullable = check_and_get_column<ColumnNullable>(full_result.get());
        ASSERT_NE(nullable, nullptr);
        EXPECT_TRUE(nullable->is_null_at(0));
        const bool constant_null = constant_columns || null_argument == 1 || null_argument == 5;
        EXPECT_EQ(nullable->is_null_at(1), constant_null);
        if (!constant_null) {
            const auto& positions = assert_cast<const ColumnInt64&>(nullable->get_nested_column());
            EXPECT_EQ(positions.get_data()[1], 2);
        }
    }
    static Block make_compile_block(size_t rows, bool all_null) {
        auto block = make_shape_block(false, true, 0);
        for (size_t argument = 0; argument < 7; ++argument) {
            auto& column = block.get_by_position(argument).column;
            column = column->clone_resized(rows);
        }
        if (all_null) {
            auto nested = ColumnHelper::create_column<DataTypeString>({"xab", "xab"});
            block.get_by_position(0).column =
                    ColumnNullable::create(nested, ColumnUInt8::create(rows, 1));
        }
        return block;
    }

    static void check_compile_block(const FunctionBasePtr& function, FunctionContext* context,
                                    size_t rows, bool all_null, const size_t& compilations,
                                    size_t expected_compilations) {
        const auto result_type = make_nullable(std::make_shared<DataTypeInt64>());
        auto block = make_compile_block(rows, all_null);
        block.insert({nullptr, result_type, "result"});
        const auto status =
                function->execute(context, block, ColumnNumbers {0, 1, 2, 3, 4, 5, 6}, 7, rows);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(compilations, expected_compilations);
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);
        ASSERT_EQ(result.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(result.is_null_at(row), all_null || row == 0);
        }
        if (!all_null && rows > 0) {
            EXPECT_EQ(assert_cast<const ColumnInt64&>(result.get_nested_column()).get_data()[1], 2);
        }
    }
    static void check_null_error_precedence(bool constant_columns, bool all_null,
                                            int null_argument) {
        SCOPED_TRACE(testing::Message() << "constant=" << constant_columns << ", all NULL="
                                        << all_null << ", NULL argument=" << null_argument);
        auto block = make_shape_block(constant_columns, false, null_argument);
        for (int argument : {2, 3, 4, 6}) {
            const Int64 valid_value = argument == 2 || argument == 3 ? 1 : 0;
            ColumnPtr values =
                    ColumnHelper::create_column<DataTypeInt64>({-1, all_null ? -1 : valid_value});
            if (argument == null_argument) {
                auto null_map = ColumnUInt8::create(2, all_null ? 1 : 0);
                null_map->get_data()[0] = 1;
                values = ColumnNullable::create(values, std::move(null_map));
            }
            if (constant_columns) {
                values = ColumnConst::create(values->clone_resized(1), 2);
            }
            block.get_by_position(argument).column = values;
        }
        if (all_null && null_argument != 2 && null_argument != 3 && null_argument != 4 &&
            null_argument != 6) {
            auto& column = block.get_by_position(null_argument).column;
            // String NULL arguments are constant for pattern/flags and optionally haystack.
            auto full = column->convert_to_full_column_if_const();
            const auto& nullable = assert_cast<const ColumnNullable&>(*full);
            column = ColumnNullable::create(nullable.get_nested_column_ptr(),
                                            ColumnUInt8::create(2, 1));
            if (constant_columns || null_argument == 1 || null_argument == 5) {
                column = ColumnConst::create(column->clone_resized(1), 2);
            }
        }
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto full = block.get_by_position(7).column->convert_to_full_column_if_const();
        const auto& result = assert_cast<const ColumnNullable&>(*full);
        EXPECT_TRUE(result.is_null_at(0));
        const bool second_null =
                all_null || constant_columns || null_argument == 1 || null_argument == 5;
        EXPECT_EQ(result.is_null_at(1), second_null);
        if (!second_null) {
            EXPECT_EQ(assert_cast<const ColumnInt64&>(result.get_nested_column()).get_data()[1], 2);
        }
    }
    struct ErrorCase {
        std::string pattern;
        std::string flags;
        Int64 position;
        Int64 occurrence;
        Int64 return_option;
        Int64 subexpression;
        std::string error;
    };

    static void check_constant_error_precedence(bool all_null, const ErrorCase& test_case) {
        SCOPED_TRACE(testing::Message()
                     << "all NULL=" << all_null << ", expected=" << test_case.error);
        auto block = make_compile_block(2, all_null);
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(5).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.flags}), 2);
        for (size_t argument : {1, 2, 3, 4, 5, 6}) {
            auto& entry = block.get_by_position(argument);
            entry.type = remove_nullable(entry.type);
        }
        const std::vector<Int64> numbers = {test_case.position, test_case.occurrence,
                                            test_case.return_option, test_case.subexpression};
        const std::vector<size_t> arguments = {2, 3, 4, 6};
        for (size_t index = 0; index < arguments.size(); ++index) {
            block.get_by_position(arguments[index]).column =
                    ColumnHelper::create_column<DataTypeInt64>({numbers[index], numbers[index]});
        }
        ASSERT_NO_FATAL_FAILURE(execute(block, all_null ? "" : test_case.error));
        if (all_null) {
            const auto& result =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);
            EXPECT_TRUE(result.is_null_at(0));
            EXPECT_TRUE(result.is_null_at(1));
        }
    }
    static void check_invalid_value_without_match(bool constant_columns, Int64 position,
                                                  size_t argument, Int64 value,
                                                  const std::string& parameter) {
        SCOPED_TRACE(testing::Message() << "constant=" << constant_columns << ", position="
                                        << position << ", invalid " << parameter << "=" << value);
        auto block = make_shape_block(constant_columns, false, -1);
        block.get_by_position(1).column =
                ColumnConst::create(ColumnHelper::create_column<DataTypeString>(
                                            {argument == 2 || position == 1 ? "(z)" : "(a)"}),
                                    2);
        const Int64 valid_value = argument == 2 || argument == 3 ? 1 : 0;
        block.get_by_position(2).column =
                ColumnHelper::create_column<DataTypeInt64>({position, position});
        // The vector's first valid row returns 0; its second row must still report
        // the invalid value, even when the pattern misses or position is out of range.
        block.get_by_position(argument).column =
                ColumnHelper::create_column<DataTypeInt64>({valid_value, value});
        if (constant_columns) {
            block.get_by_position(argument).column =
                    ColumnConst::create(ColumnHelper::create_column<DataTypeInt64>({value}), 2);
            if (argument != 2) {
                block.get_by_position(2).column = ColumnConst::create(
                        ColumnHelper::create_column<DataTypeInt64>({position}), 2);
            }
        }
        ASSERT_NO_FATAL_FAILURE(execute(block, "Invalid regexp_position " + parameter));
    }
};

TEST_F(FunctionRegexpPositionTest, BasicSevenArgs) {
    auto string_type = std::make_shared<DataTypeString>();
    auto int_type = std::make_shared<DataTypeInt64>();
    const size_t rows = 5;
    auto strings = [](const std::vector<std::string>& values) {
        return ColumnHelper::create_column<DataTypeString>(values);
    };

    for (bool constant_haystack : {false, true}) {
        SCOPED_TRACE(constant_haystack ? "constant haystack" : "vector haystack");
        const std::vector<Int64> expected = constant_haystack ? std::vector<Int64> {1, 7, 7, 10, 0}
                                                              : std::vector<Int64> {1, 6, 7, 10, 0};
        ColumnPtr haystack = strings({"ab12", "xy3ab45", "xx12cd345", "zz12cd345", "no_digits"});
        if (constant_haystack) {
            haystack = ColumnConst::create(strings({"ab12cd345"}), rows);
        }
        ColumnPtr pattern = ColumnConst::create(strings({"([a-z]+)([0-9]+)"}), rows);
        ColumnPtr flags = ColumnConst::create(strings({""}), rows);
        Block block;
        block.insert({haystack, string_type, "haystack"});
        block.insert({pattern, string_type, "pattern"});
        block.insert({ColumnHelper::create_column<DataTypeInt64>({1, 1, 3, 5, 1}), int_type,
                      "position"});
        block.insert({ColumnHelper::create_column<DataTypeInt64>({1, 2, 1, 1, 3}), int_type,
                      "occurrence"});
        block.insert({ColumnHelper::create_column<DataTypeInt64>({0, 0, 1, 1, 0}), int_type,
                      "return_option"});
        block.insert({flags, string_type, "flags"});
        block.insert({ColumnHelper::create_column<DataTypeInt64>({0, 2, 1, 2, 0}), int_type,
                      "subexpression"});

        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& result = block.get_by_position(7);
        ASSERT_EQ(result.column->size(), rows);
        ASSERT_TRUE(check_and_get_column<ColumnInt64>(result.column.get()));
        const auto& values = assert_cast<const ColumnInt64&>(*result.column).get_data();
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(values[row], expected[row]) << "row " << row;
        }
    }
}

TEST_F(FunctionRegexpPositionTest, ColumnShapesAndNulls) {
    for (bool constant_columns : {false, true}) {
        for (bool nullable_columns : {false, true}) {
            SCOPED_TRACE(testing::Message()
                         << "constant=" << constant_columns << ", nullable=" << nullable_columns);
            check_shape(constant_columns, nullable_columns);
        }
        for (int null_argument = 0; null_argument < 7; ++null_argument) {
            SCOPED_TRACE(testing::Message()
                         << "constant=" << constant_columns << ", NULL argument=" << null_argument);
            check_null_argument(constant_columns, null_argument);
        }
    }
}

TEST_F(FunctionRegexpPositionTest, CompileOncePerFunction) {
    auto prototype = make_compile_block(2, false);
    DataTypes argument_types;
    for (const auto& argument : prototype.get_columns_with_type_and_name()) {
        argument_types.push_back(argument.type);
    }
    const auto result_type = make_nullable(std::make_shared<DataTypeInt64>());
    auto function = SimpleFunctionFactory::instance().get_function(
            "regexp_position", prototype.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    FunctionUtils fn_utils(result_type, argument_types, false);
    auto* context = fn_utils.get_fn_ctx();
    context->set_constant_cols(
            {nullptr, std::make_shared<ConstantExprResult>(prototype.get_by_position(1).column),
             nullptr, nullptr, nullptr,
             std::make_shared<ConstantExprResult>(prototype.get_by_position(5).column), nullptr});

    size_t compilations = 0;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "FunctionRegexpPosition::compile_pattern", [&](auto&&) { ++compilations; }, &guard);
    sync_point->enable_processing();
    Defer disable_sync_points {[&] { sync_point->disable_processing(); }};
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    EXPECT_EQ(compilations, 0);

    // The first mixed block's NULL row is skipped; its valid row initializes the regex.
    const std::vector<std::pair<size_t, bool>> blocks = {
            {0, false}, {2, true}, {2, false}, {2, false}, {0, false}, {2, true}, {2, false}};
    for (size_t index = 0; index < blocks.size(); ++index) {
        SCOPED_TRACE(index);
        const size_t expected_compilations = index < 2 ? 0 : 1;
        ASSERT_NO_FATAL_FAILURE(check_compile_block(function, context, blocks[index].first,
                                                    blocks[index].second, compilations,
                                                    expected_compilations));
    }
    EXPECT_EQ(compilations, 1);
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

TEST_F(FunctionRegexpPositionTest, Flags) {
    struct FlagCase {
        std::string haystack;
        std::string pattern;
        std::string flags;
        Int64 expected;
    };
    const std::vector<FlagCase> cases = {
            {.haystack = "Ab", .pattern = "ab", .flags = "", .expected = 0},
            {.haystack = "Ab", .pattern = "ab", .flags = "i", .expected = 1},
            {.haystack = "Ab", .pattern = "ab", .flags = "c", .expected = 0},
            {.haystack = "Ab", .pattern = "ab", .flags = "ic", .expected = 0},
            {.haystack = "Ab", .pattern = "ab", .flags = "ci", .expected = 1},
            {.haystack = "Ab", .pattern = "ab", .flags = "ici", .expected = 1},
            {.haystack = "Ab", .pattern = "ab", .flags = "cic", .expected = 0},
            {.haystack = "a\nb\nc", .pattern = "^b$", .flags = "", .expected = 0},
            {.haystack = "a\nb\nc", .pattern = "^b$", .flags = "m", .expected = 3},
            {.haystack = "a\nb\nc", .pattern = "^b$", .flags = "n", .expected = 3},
            {.haystack = "a\nb\nc", .pattern = "^b$", .flags = "s", .expected = 0},
            {.haystack = "a\nb", .pattern = "a.b", .flags = "", .expected = 0},
            {.haystack = "a\nb", .pattern = "a.b", .flags = "m", .expected = 0},
            {.haystack = "a\nb", .pattern = "a.b", .flags = "n", .expected = 0},
            {.haystack = "a\nb", .pattern = "a.b", .flags = "s", .expected = 1},
            {.haystack = "x\nA\nb\ny", .pattern = "^a.b$", .flags = "imns", .expected = 3},
            {.haystack = "Ab", .pattern = "(?i)ab", .flags = "", .expected = 1},
            {.haystack = "Ab", .pattern = "(?i)ab", .flags = "c", .expected = 1},
            {.haystack = "Ab", .pattern = "(?-i)ab", .flags = "i", .expected = 0},
            {.haystack = "a\nb\nc", .pattern = "(?m)^b$", .flags = "c", .expected = 3},
            {.haystack = "a\nb\nc", .pattern = "(?-m)^b$", .flags = "m", .expected = 0},
            {.haystack = "a\nb\nc", .pattern = "(?-m)^b$", .flags = "n", .expected = 0},
            {.haystack = "a\nb", .pattern = "(?s)a.b", .flags = "m", .expected = 1},
            {.haystack = "a\nb", .pattern = "(?-s)a.b", .flags = "s", .expected = 0},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message()
                     << "pattern=" << test_case.pattern << ", flags=" << test_case.flags);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(5).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.flags}), 2);
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& result = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        EXPECT_EQ(result.get_data()[0], test_case.expected);
        EXPECT_EQ(result.get_data()[1], test_case.expected);
    }

    const std::vector<std::string> invalid_flags = {
            "z", "I", "C", "M", "N", "S", "iz", "ciS", " ", std::string(1, '\0'), "\xff"};
    for (const auto& flags : invalid_flags) {
        SCOPED_TRACE(testing::Message() << "invalid flags=" << flags);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(5).column =
                ColumnConst::create(ColumnHelper::create_column<DataTypeString>({flags}), 2);
        ASSERT_NO_FATAL_FAILURE(execute(block, "Invalid regexp_position flag"));
    }
}

TEST_F(FunctionRegexpPositionTest, RegexCompile) {
    struct CompileCase {
        std::string haystack;
        std::string pattern;
        std::string flags;
        Int64 subexpression;
        Int64 expected;
        std::string error;
    };
    std::string max_groups;
    for (size_t group = 0; group < 1023; ++group) {
        max_groups += "()";
    }
    max_groups += "(a)";
    const std::vector<CompileCase> cases = {
            {.haystack = "x\xff",
             .pattern = "\xff",
             .flags = "",
             .subexpression = 0,
             .expected = 2,
             .error = ""},
            // The Latin-1 retry must retain case-insensitive and dot-newline options.
            {.haystack = "x\xff\na",
             .pattern = "\xff.A",
             .flags = "is",
             .subexpression = 0,
             .expected = 2,
             .error = ""},
            {.haystack = "xab",
             .pattern = "\xff[",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "Invalid regexp_position pattern"},
            {.haystack = "xab",
             .pattern = "[",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "Invalid regexp_position pattern"},
            {.haystack = "xaa",
             .pattern = "(a)\\1",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "Invalid regexp_position pattern"},
            {.haystack = "xab",
             .pattern = "a(?=b)",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "Invalid regexp_position pattern"},
            {.haystack = "xab",
             .pattern = "a{1001}",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "Invalid regexp_position pattern"},
            {.haystack = "xa",
             .pattern = max_groups,
             .flags = "",
             .subexpression = 1024,
             .expected = 2,
             .error = ""},
            {.haystack = "xa",
             .pattern = max_groups + "()",
             .flags = "",
             .subexpression = 0,
             .expected = 0,
             .error = "1025 capturing groups exceed the 1024 limit"},
    };
    for (size_t index = 0; index < cases.size(); ++index) {
        SCOPED_TRACE(index);
        const auto& test_case = cases[index];
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(5).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.flags}), 2);
        block.get_by_position(6).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.subexpression, test_case.subexpression});
        ASSERT_NO_FATAL_FAILURE(execute(block, test_case.error));
        if (test_case.error.empty()) {
            const auto& result = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
            EXPECT_EQ(result.get_data()[0], test_case.expected);
            EXPECT_EQ(result.get_data()[1], test_case.expected);
        }
    }
}

TEST_F(FunctionRegexpPositionTest, PositionOccurrenceBounds) {
    constexpr Int64 minimum = std::numeric_limits<Int64>::min();
    constexpr Int64 maximum = std::numeric_limits<Int64>::max();
    struct InvalidCase {
        size_t argument;
        Int64 value;
        std::string name;
    };
    const std::vector<InvalidCase> invalid_cases = {
            {.argument = 2, .value = 0, .name = "position"},
            {.argument = 2, .value = -1, .name = "position"},
            {.argument = 2, .value = minimum, .name = "position"},
            {.argument = 3, .value = 0, .name = "occurrence"},
            {.argument = 3, .value = -1, .name = "occurrence"},
            {.argument = 3, .value = minimum, .name = "occurrence"}};
    for (const auto& test_case : invalid_cases) {
        SCOPED_TRACE(testing::Message() << test_case.name << "=" << test_case.value);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(test_case.argument).column =
                ColumnHelper::create_column<DataTypeInt64>({1, test_case.value});
        ASSERT_NO_FATAL_FAILURE(execute(block, "Invalid regexp_position " + test_case.name));
    }

    struct BoundsCase {
        Int64 position;
        Int64 occurrence;
        Int64 expected;
    };
    const std::vector<BoundsCase> cases = {
            {.position = 1, .occurrence = 1, .expected = 2},
            {.position = maximum, .occurrence = 1, .expected = 0},
            {.position = 1, .occurrence = maximum, .expected = 0},
            {.position = maximum, .occurrence = maximum, .expected = 0},
            {.position = 1, .occurrence = Int64 {1} << 40, .expected = 0},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message() << "position=" << test_case.position
                                        << ", occurrence=" << test_case.occurrence);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(2).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.position, test_case.position});
        block.get_by_position(3).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.occurrence, test_case.occurrence});
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& result = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        EXPECT_EQ(result.get_data()[0], test_case.expected);
        EXPECT_EQ(result.get_data()[1], test_case.expected);
    }
}

TEST_F(FunctionRegexpPositionTest, ReturnOptionBounds) {
    auto block = make_shape_block(false, false, -1);
    block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
    ASSERT_NO_FATAL_FAILURE(execute(block));
    const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
    ASSERT_EQ(positions.size(), 2);
    EXPECT_EQ(positions.get_data()[0], 2);
    EXPECT_EQ(positions.get_data()[1], 4);

    for (Int64 return_option : {-1, 2}) {
        SCOPED_TRACE(testing::Message() << "return_option=" << return_option);
        auto invalid_block = make_shape_block(false, false, -1);
        invalid_block.get_by_position(4).column =
                ColumnHelper::create_column<DataTypeInt64>({0, return_option});
        ASSERT_NO_FATAL_FAILURE(
                execute(invalid_block, "Invalid regexp_position return_option: expected 0 or 1"));
    }
}

TEST_F(FunctionRegexpPositionTest, SubexpressionBounds) {
    auto block = make_shape_block(false, false, -1);
    block.get_by_position(1).column =
            ColumnConst::create(ColumnHelper::create_column<DataTypeString>({"(a)(b)"}), 2);
    block.get_by_position(6).column = ColumnHelper::create_column<DataTypeInt64>({0, 2});
    ASSERT_NO_FATAL_FAILURE(execute(block));
    const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
    ASSERT_EQ(positions.size(), 2);
    EXPECT_EQ(positions.get_data()[0], 2);
    EXPECT_EQ(positions.get_data()[1], 3);

    const std::vector<Int64> invalid_subexpressions = {-1, 3, std::numeric_limits<Int64>::min(),
                                                       std::numeric_limits<Int64>::max()};
    for (Int64 subexpression : invalid_subexpressions) {
        SCOPED_TRACE(testing::Message() << "subexpression=" << subexpression);
        auto invalid_block = make_shape_block(false, false, -1);
        invalid_block.get_by_position(1).column =
                ColumnConst::create(ColumnHelper::create_column<DataTypeString>({"(a)(b)"}), 2);
        invalid_block.get_by_position(6).column =
                ColumnHelper::create_column<DataTypeInt64>({2, subexpression});
        ASSERT_NO_FATAL_FAILURE(
                execute(invalid_block,
                        "Invalid regexp_position subexpression: expected a value from 0 to 2"));
    }

    auto no_groups = make_shape_block(false, false, -1);
    no_groups.get_by_position(6).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
    ASSERT_NO_FATAL_FAILURE(execute(
            no_groups, "Invalid regexp_position subexpression: expected a value from 0 to 0"));
}

TEST_F(FunctionRegexpPositionTest, StartAtEnd) {
    struct BoundaryCase {
        std::string haystack;
        std::string pattern;
        Int64 position;
        Int64 expected_start;
        Int64 expected_end;
    };
    const std::vector<BoundaryCase> cases = {
            {.haystack = "abc",
             .pattern = "c",
             .position = 3,
             .expected_start = 3,
             .expected_end = 4},
            {.haystack = "abc",
             .pattern = "c",
             .position = 4,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "c",
             .position = 5,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "",
             .position = 3,
             .expected_start = 3,
             .expected_end = 3},
            {.haystack = "abc",
             .pattern = "",
             .position = 4,
             .expected_start = 4,
             .expected_end = 4},
            {.haystack = "abc",
             .pattern = "",
             .position = 5,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "$",
             .position = 3,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "$",
             .position = 4,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "$",
             .position = 5,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "", .pattern = "", .position = 1, .expected_start = 1, .expected_end = 1},
            {.haystack = "", .pattern = "", .position = 2, .expected_start = 0, .expected_end = 0},
            {.haystack = "", .pattern = "a", .position = 1, .expected_start = 0, .expected_end = 0},
            {.haystack = "", .pattern = "a", .position = 2, .expected_start = 0, .expected_end = 0},
            {.haystack = "", .pattern = "$", .position = 1, .expected_start = 0, .expected_end = 0},
            {.haystack = "", .pattern = "$", .position = 2, .expected_start = 0, .expected_end = 0},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message()
                     << "haystack=" << test_case.haystack << ", pattern=" << test_case.pattern
                     << ", position=" << test_case.position);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(2).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.position, test_case.position});
        block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        ASSERT_EQ(positions.size(), 2);
        EXPECT_EQ(positions.get_data()[0], test_case.expected_start);
        EXPECT_EQ(positions.get_data()[1], test_case.expected_end);
    }
}

TEST_F(FunctionRegexpPositionTest, WholeInputAndNonOverlap) {
    struct SearchCase {
        std::string haystack;
        std::string pattern;
        Int64 position;
        Int64 occurrence;
        Int64 expected_start;
        Int64 expected_end;
    };
    const std::vector<SearchCase> cases = {
            {.haystack = "abc",
             .pattern = "^.",
             .position = 1,
             .occurrence = 1,
             .expected_start = 1,
             .expected_end = 2},
            {.haystack = "abc",
             .pattern = "^.",
             .position = 1,
             .occurrence = 2,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "abc",
             .pattern = "^b",
             .position = 2,
             .occurrence = 1,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "new york is the greatest",
             .pattern = "\\b\\w",
             .position = 1,
             .occurrence = 2,
             .expected_start = 5,
             .expected_end = 6},
            {.haystack = "new york is the greatest",
             .pattern = "\\b\\w",
             .position = 1,
             .occurrence = 3,
             .expected_start = 10,
             .expected_end = 11},
            // Starting inside "new" must not manufacture a word boundary at "e".
            {.haystack = "new york",
             .pattern = "\\b\\w",
             .position = 2,
             .occurrence = 1,
             .expected_start = 5,
             .expected_end = 6},
            {.haystack = "abcabc",
             .pattern = "abc",
             .position = 1,
             .occurrence = 2,
             .expected_start = 4,
             .expected_end = 7},
            // An overlapping second "aa" starts at byte 2, but is not an occurrence.
            {.haystack = "aaa",
             .pattern = "aa",
             .position = 1,
             .occurrence = 2,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "aaaa",
             .pattern = "aa",
             .position = 1,
             .occurrence = 2,
             .expected_start = 3,
             .expected_end = 5},
            {.haystack = "aaaa",
             .pattern = "aa",
             .position = 1,
             .occurrence = 3,
             .expected_start = 0,
             .expected_end = 0},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message() << "haystack=" << test_case.haystack << ", pattern="
                                        << test_case.pattern << ", position=" << test_case.position
                                        << ", occurrence=" << test_case.occurrence);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(2).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.position, test_case.position});
        block.get_by_position(3).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.occurrence, test_case.occurrence});
        block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        ASSERT_EQ(positions.size(), 2);
        EXPECT_EQ(positions.get_data()[0], test_case.expected_start);
        EXPECT_EQ(positions.get_data()[1], test_case.expected_end);
    }
}

TEST_F(FunctionRegexpPositionTest, ZeroWidthGolden) {
    struct GoldenCase {
        std::string haystack;
        std::string pattern;
        Int64 position;
        Int64 occurrence;
        Int64 return_option;
        std::string flags;
        Int64 subexpression;
        Int64 expected;
    };
    // Keep the rows and expression order from new_func_SPEC.md section 5.
    // Empty literal patterns and RE2 zero-width matches have different golden results.
    const std::vector<std::vector<GoldenCase>> golden_rows = {
            {{.haystack = "abc",
              .pattern = "",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 1},
             {.haystack = "abc",
              .pattern = "",
              .position = 1,
              .occurrence = 4,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 4},
             {.haystack = "abc",
              .pattern = "",
              .position = 1,
              .occurrence = 5,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0}},
            {{.haystack = "abc",
              .pattern = "^",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "abc",
              .pattern = "$",
              .position = 2,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "ba",
              .pattern = "a*",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "ba",
              .pattern = "a*",
              .position = 1,
              .occurrence = 2,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "ba",
              .pattern = "a*",
              .position = 1,
              .occurrence = 3,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0}},
            {{.haystack = "",
              .pattern = "()",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 1,
              .expected = 0},
             {.haystack = "",
              .pattern = "(a)?",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 1,
              .expected = 0},
             {.haystack = "a",
              .pattern = "a()",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 1},
             {.haystack = "a",
              .pattern = "a()",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 1,
              .expected = 0},
             {.haystack = "a",
              .pattern = "a()",
              .position = 1,
              .occurrence = 1,
              .return_option = 1,
              .flags = "",
              .subexpression = 1,
              .expected = 0}},
            {{.haystack = "abc",
              .pattern = "",
              .position = 1,
              .occurrence = 2,
              .return_option = 0,
              .flags = "i",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "abc",
              .pattern = "",
              .position = 1,
              .occurrence = 4,
              .return_option = 1,
              .flags = "",
              .subexpression = 0,
              .expected = 4},
             {.haystack = "",
              .pattern = "",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 1},
             {.haystack = "",
              .pattern = "",
              .position = 1,
              .occurrence = 2,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "abc",
              .pattern = "",
              .position = 4,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 4},
             {.haystack = "abc",
              .pattern = "",
              .position = 5,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0}},
            {{.haystack = "abc",
              .pattern = "^.",
              .position = 1,
              .occurrence = 2,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "abc",
              .pattern = "^b",
              .position = 2,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 0},
             {.haystack = "new york is the greatest",
              .pattern = "\\b\\w",
              .position = 1,
              .occurrence = 2,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 5},
             {.haystack = "new york is the greatest",
              .pattern = "\\b\\w",
              .position = 1,
              .occurrence = 3,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 10}},
            {{.haystack = std::string("a\0b", 3),
              .pattern = "b",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 3},
             {.haystack = std::string("a\0b", 3),
              .pattern = std::string(1, '\0'),
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 2},
             {.haystack = std::string("a\xff"
                                      "b",
                                      3),
              .pattern = "b",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 3},
             {.haystack = std::string("a\xff"
                                      "b",
                                      3),
              .pattern = "\xff",
              .position = 1,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 2}},
            {{.haystack = "äbc",
              .pattern = ".",
              .position = 2,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 3},
             {.haystack = "😀a",
              .pattern = ".",
              .position = 3,
              .occurrence = 1,
              .return_option = 0,
              .flags = "",
              .subexpression = 0,
              .expected = 5}},
    };
    for (size_t row = 0; row < golden_rows.size(); ++row) {
        for (size_t expression = 0; expression < golden_rows[row].size(); ++expression) {
            SCOPED_TRACE(testing::Message()
                         << "golden row=" << row + 1 << ", expression=" << expression + 1);
            const auto& test_case = golden_rows[row][expression];
            auto block = make_shape_block(false, false, -1);
            block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                    {test_case.haystack, test_case.haystack});
            block.get_by_position(1).column = ColumnConst::create(
                    ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
            block.get_by_position(2).column = ColumnHelper::create_column<DataTypeInt64>(
                    {test_case.position, test_case.position});
            block.get_by_position(3).column = ColumnHelper::create_column<DataTypeInt64>(
                    {test_case.occurrence, test_case.occurrence});
            block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>(
                    {test_case.return_option, test_case.return_option});
            block.get_by_position(5).column = ColumnConst::create(
                    ColumnHelper::create_column<DataTypeString>({test_case.flags}), 2);
            block.get_by_position(6).column = ColumnHelper::create_column<DataTypeInt64>(
                    {test_case.subexpression, test_case.subexpression});
            ASSERT_NO_FATAL_FAILURE(execute(block));
            const auto& positions =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
            ASSERT_EQ(positions.size(), 2);
            EXPECT_EQ(positions.get_data()[0], test_case.expected);
            EXPECT_EQ(positions.get_data()[1], test_case.expected);
        }
    }
}

TEST_F(FunctionRegexpPositionTest, CaptureReturnPosition) {
    struct CaptureCase {
        std::string haystack;
        std::string pattern;
        Int64 occurrence;
        Int64 subexpression;
        Int64 expected_start;
        Int64 expected_end;
    };
    const std::vector<CaptureCase> cases = {
            // An optional group can be absent even though the whole pattern matches.
            {.haystack = "b",
             .pattern = "(a)?(b)",
             .occurrence = 1,
             .subexpression = 1,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "b",
             .pattern = "(a)?(b)",
             .occurrence = 1,
             .subexpression = 2,
             .expected_start = 1,
             .expected_end = 2},
            // Participating empty captures follow the golden and also return zero.
            {.haystack = "b",
             .pattern = "(a*)(b)",
             .occurrence = 1,
             .subexpression = 1,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "a",
             .pattern = "a()",
             .occurrence = 1,
             .subexpression = 1,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "a",
             .pattern = "a()",
             .occurrence = 1,
             .subexpression = 0,
             .expected_start = 1,
             .expected_end = 2},
            // Occurrence counts whole matches before selecting a capture.
            {.haystack = "foo123bar456",
             .pattern = "([a-z]+)([0-9]+)",
             .occurrence = 2,
             .subexpression = 0,
             .expected_start = 7,
             .expected_end = 13},
            {.haystack = "foo123bar456",
             .pattern = "([a-z]+)([0-9]+)",
             .occurrence = 2,
             .subexpression = 1,
             .expected_start = 7,
             .expected_end = 10},
            {.haystack = "foo123bar456",
             .pattern = "([a-z]+)([0-9]+)",
             .occurrence = 2,
             .subexpression = 2,
             .expected_start = 10,
             .expected_end = 13},
            // Nested captures use opening-parenthesis order and original input offsets.
            {.haystack = "xabc",
             .pattern = "(ab(c))",
             .occurrence = 1,
             .subexpression = 1,
             .expected_start = 2,
             .expected_end = 5},
            {.haystack = "xabc",
             .pattern = "(ab(c))",
             .occurrence = 1,
             .subexpression = 2,
             .expected_start = 4,
             .expected_end = 5},
            // A later absent group must not retain a capture from the previous match.
            {.haystack = "ab a",
             .pattern = "(a)(b)?",
             .occurrence = 1,
             .subexpression = 2,
             .expected_start = 2,
             .expected_end = 3},
            {.haystack = "ab a",
             .pattern = "(a)(b)?",
             .occurrence = 2,
             .subexpression = 2,
             .expected_start = 0,
             .expected_end = 0},
            {.haystack = "a ab",
             .pattern = "(a)(b)?",
             .occurrence = 2,
             .subexpression = 2,
             .expected_start = 4,
             .expected_end = 5},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(testing::Message()
                     << "haystack=" << test_case.haystack << ", pattern=" << test_case.pattern
                     << ", occurrence=" << test_case.occurrence
                     << ", subexpression=" << test_case.subexpression);
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(3).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.occurrence, test_case.occurrence});
        block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
        block.get_by_position(6).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.subexpression, test_case.subexpression});
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        ASSERT_EQ(positions.size(), 2);
        EXPECT_EQ(positions.get_data()[0], test_case.expected_start);
        EXPECT_EQ(positions.get_data()[1], test_case.expected_end);
    }
}

TEST_F(FunctionRegexpPositionTest, RawBytes) {
    struct TestCase {
        std::string haystack;
        std::string pattern;
        Int64 position;
        Int64 expected_start;
        Int64 expected_end;
    };
    const std::vector<TestCase> cases = {
            // Starting inside a UTF-8 character skips its remaining bytes for a UTF-8 dot.
            {.haystack = "äbc",
             .pattern = ".",
             .position = 2,
             .expected_start = 3,
             .expected_end = 4},
            {.haystack = "😀a",
             .pattern = ".",
             .position = 3,
             .expected_start = 5,
             .expected_end = 6},
            {.haystack = "中文a",
             .pattern = ".",
             .position = 2,
             .expected_start = 4,
             .expected_end = 7},
            {.haystack = "中文a",
             .pattern = "a",
             .position = 5,
             .expected_start = 7,
             .expected_end = 8},
            {.haystack = "😀a",
             .pattern = "😀",
             .position = 2,
             .expected_start = 0,
             .expected_end = 0},
            // Explicit lengths preserve NUL in both the input and the pattern.
            {.haystack = std::string("a\0b", 3),
             .pattern = "b",
             .position = 1,
             .expected_start = 3,
             .expected_end = 4},
            {.haystack = std::string("a\0b", 3),
             .pattern = std::string("\0", 1),
             .position = 1,
             .expected_start = 2,
             .expected_end = 3},
            {.haystack = std::string("a\0b", 3),
             .pattern = ".",
             .position = 2,
             .expected_start = 2,
             .expected_end = 3},
            {.haystack = std::string("a\0b", 3),
             .pattern = std::string("a\0b", 3),
             .position = 1,
             .expected_start = 1,
             .expected_end = 4},
            // Invalid haystack bytes do not prevent finding later valid characters.
            {.haystack = std::string("a\xff"
                                     "b",
                                     3),
             .pattern = "b",
             .position = 1,
             .expected_start = 3,
             .expected_end = 4},
            {.haystack = std::string("a\xff"
                                     "b",
                                     3),
             .pattern = ".",
             .position = 2,
             .expected_start = 3,
             .expected_end = 4},
            {.haystack = std::string("a\xff"
                                     "b",
                                     3),
             .pattern = std::string("\xff", 1),
             .position = 1,
             .expected_start = 2,
             .expected_end = 3},
    };
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(i);
        const auto& test_case = cases[i];
        auto block = make_shape_block(false, false, -1);
        block.get_by_position(0).column = ColumnHelper::create_column<DataTypeString>(
                {test_case.haystack, test_case.haystack});
        block.get_by_position(1).column = ColumnConst::create(
                ColumnHelper::create_column<DataTypeString>({test_case.pattern}), 2);
        block.get_by_position(2).column = ColumnHelper::create_column<DataTypeInt64>(
                {test_case.position, test_case.position});
        block.get_by_position(4).column = ColumnHelper::create_column<DataTypeInt64>({0, 1});
        ASSERT_NO_FATAL_FAILURE(execute(block));
        const auto& positions = assert_cast<const ColumnInt64&>(*block.get_by_position(7).column);
        ASSERT_EQ(positions.size(), 2);
        EXPECT_EQ(positions.get_data()[0], test_case.expected_start);
        EXPECT_EQ(positions.get_data()[1], test_case.expected_end);
    }
}

TEST_F(FunctionRegexpPositionTest, NullAndErrorPrecedence) {
    // Every argument can suppress invalid numeric placeholders on the same row.
    for (bool constant_columns : {false, true}) {
        for (bool all_null : {false, true}) {
            for (int null_argument = 0; null_argument < 7; ++null_argument) {
                check_null_error_precedence(constant_columns, all_null, null_argument);
            }
        }
    }

    // Illegal constants stay unevaluated for an all-NULL block, but precede numeric errors
    // as soon as the mixed block reaches its first non-NULL row.

    const std::vector<ErrorCase> cases = {
            {.pattern = "[",
             .flags = "z",
             .position = 0,
             .occurrence = 0,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position flag"},
            {.pattern = "ab",
             .flags = "z",
             .position = 0,
             .occurrence = 0,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position flag"},
            {.pattern = "[",
             .flags = "",
             .position = 0,
             .occurrence = 0,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position pattern"},
            {.pattern = "ab",
             .flags = "",
             .position = 0,
             .occurrence = 0,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position position"},
            {.pattern = "ab",
             .flags = "",
             .position = 1,
             .occurrence = 0,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position occurrence"},
            {.pattern = "ab",
             .flags = "",
             .position = 1,
             .occurrence = 1,
             .return_option = 2,
             .subexpression = -1,
             .error = "Invalid regexp_position return_option"},
            {.pattern = "ab",
             .flags = "",
             .position = 1,
             .occurrence = 1,
             .return_option = 0,
             .subexpression = -1,
             .error = "Invalid regexp_position subexpression"},
    };
    for (bool all_null : {true, false}) {
        for (const auto& test_case : cases) {
            check_constant_error_precedence(all_null, test_case);
        }
    }
}

TEST_F(FunctionRegexpPositionTest, InvalidValuesDespiteNoMatch) {
    struct InvalidValue {
        size_t argument;
        Int64 value;
        std::string parameter;
    };
    const Int64 minimum = std::numeric_limits<Int64>::min();
    const Int64 maximum = std::numeric_limits<Int64>::max();
    const std::vector<InvalidValue> invalid_values = {
            {.argument = 2, .value = minimum, .parameter = "position"},
            {.argument = 2, .value = -1, .parameter = "position"},
            {.argument = 2, .value = 0, .parameter = "position"},
            {.argument = 3, .value = minimum, .parameter = "occurrence"},
            {.argument = 3, .value = -1, .parameter = "occurrence"},
            {.argument = 3, .value = 0, .parameter = "occurrence"},
            {.argument = 4, .value = minimum, .parameter = "return_option"},
            {.argument = 4, .value = -1, .parameter = "return_option"},
            {.argument = 4, .value = 2, .parameter = "return_option"},
            {.argument = 4, .value = maximum, .parameter = "return_option"},
            {.argument = 6, .value = minimum, .parameter = "subexpression"},
            {.argument = 6, .value = -1, .parameter = "subexpression"},
            {.argument = 6, .value = 2, .parameter = "subexpression"},
            {.argument = 6, .value = maximum, .parameter = "subexpression"},
    };
    for (bool constant_columns : {false, true}) {
        for (Int64 position : {Int64 {1}, Int64 {5}, maximum}) {
            for (const auto& invalid : invalid_values) {
                check_invalid_value_without_match(constant_columns, position, invalid.argument,
                                                  invalid.value, invalid.parameter);
            }
        }
    }
}

} // namespace doris
