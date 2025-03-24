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

#include "vec/utils/stringop_substring.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/logging.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h" // ColumnHelper is used for constructing columns and Block
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(StringOPTest, testPushEmptyStringNullable) {
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>({"abc"}, {false});
    auto rhs_col_nullable =
            ColumnHelper::create_nullable_column<DataTypeString>({"ignored"}, {false});

    auto* rhs_column_nullable = dynamic_cast<const ColumnNullable*>(rhs_col_nullable.get());
    ASSERT_NE(rhs_column_nullable, nullptr);
    auto& rhs_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(rhs_column_nullable->get_nested_column()));
    auto& rhs_chars = rhs_nested_column.get_chars();
    auto& rhs_offsets = rhs_nested_column.get_offsets();

    size_t row_index = 0;
    StringOP::push_empty_string(row_index, rhs_chars, rhs_offsets);

    ASSERT_EQ(col_nullable, rhs_col_nullable);
}

TEST(StringOPTest, testPushNullStringNullable) {
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>({"ignored"}, {true});
    auto rhs_col_nullable =
            ColumnHelper::create_nullable_column<DataTypeString>({"ignored"}, {false});

    auto* rhs_column_nullable = dynamic_cast<const ColumnNullable*>(rhs_col_nullable.get());
    ASSERT_NE(rhs_column_nullable, nullptr);
    auto& rhs_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(rhs_column_nullable->get_nested_column()));
    auto& rhs_chars = rhs_nested_column.get_chars();
    auto& rhs_offsets = rhs_nested_column.get_offsets();
    NullMap rhs_null_map = {false};

    size_t row_index = 0;
    StringOP::push_null_string(row_index, rhs_chars, rhs_offsets, rhs_null_map);

    ASSERT_EQ(col_nullable, rhs_col_nullable);
}

TEST(StringOPTest, testPushValueStringNullable) {
    std::vector<std::string> test_strings = {"",     "hello",      " ",
                                             "你好", "1234567890", std::string(1000, 'a')};

    // Create a non-NULL ColumnNullable with all rows being non-NULL
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            std::vector<std::string>(test_strings), {true, false, false, false, false, false});

    auto rhs_col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            {"", "", "", "", "", ""}, {false, false, false, false, false, false});
    auto* rhs_column_nullable = dynamic_cast<const ColumnNullable*>(rhs_col_nullable.get());
    auto& rhs_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(rhs_column_nullable->get_nested_column()));
    NullMap rhs_null_map = {false, false, false, false, false, false};
    auto& rhs_chars = rhs_nested_column.get_chars();
    auto& rhs_offsets = rhs_nested_column.get_offsets();

    for (size_t i = 0; i < test_strings.size(); ++i) {
        if (col_nullable->is_null_at(i)) {
            StringOP::push_null_string(i, rhs_chars, rhs_offsets, rhs_null_map);
        } else {
            StringOP::push_value_string(test_strings[i], i, rhs_chars, rhs_offsets);
        }
    }

    ASSERT_EQ(col_nullable, rhs_col_nullable);

    // Verify the result written for each row
    for (size_t i = 0; i < test_strings.size(); ++i) {
        size_t row_length = rhs_offsets[i] - rhs_offsets[i - 1];
        // Check if the written length matches the test string length
        ASSERT_EQ(row_length, test_strings[i].size()) << rhs_offsets[i - 1] << ' ' << rhs_offsets[i]
                                                      << ' ' << "Row " << i << " length mismatch.";
        // Check if the written content matches the test string
        std::string actual(reinterpret_cast<const char*>(rhs_chars.data() + rhs_offsets[i - 1]),
                           row_length);
        ASSERT_EQ(actual, test_strings[i]) << "Row " << i << " content mismatch.";
    }
}

TEST(StringOPTest, testPushValueStringReservedAndAllowOverflowNullable) {
    std::vector<std::string> original_test_strings = {"", "normal_string", "unused"};
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            std::vector<std::string>(original_test_strings), {true, false, false});
    auto* column_nullable = dynamic_cast<const ColumnNullable*>(col_nullable.get());
    auto& nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(column_nullable->get_nested_column()));
    auto& chars = nested_column.get_chars();
    auto& offsets = nested_column.get_offsets();

    auto rhs_col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            {"", "", ""}, {true, false, false});
    auto* rhs_column_nullable = dynamic_cast<const ColumnNullable*>(rhs_col_nullable.get());
    auto& rhs_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(rhs_column_nullable->get_nested_column()));
    auto& rhs_chars = rhs_nested_column.get_chars();
    auto& rhs_offsets = rhs_nested_column.get_offsets();
    NullMap rhs_null_map = {false, false, false};

    for (size_t i = 0; i < original_test_strings.size(); ++i) {
        if (col_nullable->is_null_at(i)) {
            StringOP::push_null_string(i, rhs_chars, rhs_offsets, rhs_null_map);
        } else {
            rhs_chars.reserve(rhs_chars.size() + original_test_strings[i].size());
            StringOP::push_value_string_reserved_and_allow_overflow(original_test_strings[i], i,
                                                                    rhs_chars, rhs_offsets);
        }
    }

    ASSERT_EQ(col_nullable, rhs_col_nullable);

    for (size_t i = 0; i < original_test_strings.size(); ++i) {
        size_t row_length = rhs_offsets[i] - (i == 0 ? 0 : rhs_offsets[i - 1]);
        if (col_nullable->is_null_at(i)) {
            ASSERT_EQ(row_length, 0u) << "Row " << i << " expected to be null with 0 length.";
        } else {
            ASSERT_EQ(row_length, original_test_strings[i].size())
                    << "Row " << i << " length mismatch.";
            std::string actual(reinterpret_cast<const char*>(rhs_chars.data() + rhs_offsets[i - 1]),
                               row_length);
            ASSERT_EQ(actual, original_test_strings[i]) << "Row " << i << " content mismatch.";
        }
    }
}

TEST(StringOPTest, ZeroTimes) {
    // When repeat_times <= 0, no copy is performed, and the output buffer is empty
    const std::string src = "example";
    int32_t repeat_times = 0;
    // Allocate enough buffer (when repeat_times is 0, the size is 0)
    std::vector<uint8_t> dst(src.size() * repeat_times);
    StringOP::fast_repeat(dst.data(), reinterpret_cast<const uint8_t*>(src.data()), src.size(),
                          repeat_times);
    // dst length is 0, no content written
    ASSERT_EQ(dst.size(), 0);
}

TEST(StringOPTest, MultipleTimes) {
    // Test multiple repeat times to ensure the result is src repeated repeat_times times
    const std::string src = "abc";
    for (int32_t repeat_times = 1; repeat_times <= 10; ++repeat_times) {
        std::vector<uint8_t> dst(src.size() * repeat_times);
        StringOP::fast_repeat(dst.data(), reinterpret_cast<const uint8_t*>(src.data()), src.size(),
                              repeat_times);
        std::string result(reinterpret_cast<const char*>(dst.data()), dst.size());
        std::string expected;
        for (int i = 0; i < repeat_times; ++i) {
            expected += src;
        }
        ASSERT_EQ(result, expected) << "Failed for repeat_times = " << repeat_times;
    }
}

TEST(StringOPTest, EmptyString) {
    std::vector<std::tuple<int32_t, int32_t>> test_cases = {
            {0, 0},  // Zero parameters
            {1, 5},  // Positive start position
            {-1, 3}, // Negative start position
            {2, -1}  // Negative length
    };

    for (const auto& [start_val, len_val] : test_cases) {
        std::vector<std::string> input_strings = {""};
        std::vector<int32_t> starts = {start_val};
        std::vector<int32_t> lengths = {len_val};

        // Create a non-empty column (using ColumnVector instead of Nullable)
        auto col_strings = ColumnHelper::create_column<DataTypeString>(input_strings);
        auto col_starts = ColumnHelper::create_column<DataTypeInt32>(starts);
        auto col_lengths = ColumnHelper::create_column<DataTypeInt32>(lengths);

        Block block;
        block.insert(ColumnWithTypeAndName(col_strings->clone(), std::make_shared<DataTypeString>(),
                                           "str"));
        block.insert(ColumnWithTypeAndName(col_starts->clone(), std::make_shared<DataTypeInt32>(),
                                           "start"));
        block.insert(ColumnWithTypeAndName(col_lengths->clone(), std::make_shared<DataTypeInt32>(),
                                           "len"));

        auto res_col = DataTypeString().create_column();
        block.insert(ColumnWithTypeAndName(std::move(res_col), std::make_shared<DataTypeString>(),
                                           "result"));

        ColumnNumbers arguments = {0, 1, 2};
        uint32_t result_index = 3;
        SubstringUtil::substring_execute(block, arguments, result_index, input_strings.size());

        // Verify the result
        auto result_column = block.get_by_position(result_index).column;
        ASSERT_EQ(result_column->size(), 1);
        // An empty string input should always return an empty string
        ASSERT_EQ(result_column->get_data_at(0).to_string(), "");
    }
}

TEST(StringOPTest, SingleRow) {
    // Parametric test: using tuple to store (input string, start position, length, expected result)
    std::vector<std::tuple<std::string, int32_t, int32_t, std::string>> test_cases = {
            {"abcdefg", 2, 3, "bcd"}, // Fixed expected result (original "cde" -> "bcd")
            {"hello", -3, 2, "ll"},     {"doris", 10, 5, ""},
            {"中文测试", 3, 2, "测试"}, // Start position changed to 3 (original 2)
            {"example", 3, -1, ""},     {"test", 0, 2, ""}};

    for (const auto& [input_str, start, length, expected] : test_cases) {
        // Create a non-empty column (using ColumnVector)
        auto col_str = ColumnHelper::create_column<DataTypeString>({input_str});
        auto col_start = ColumnHelper::create_column<DataTypeInt32>({start});
        auto col_len = ColumnHelper::create_column<DataTypeInt32>({length});

        Block block;
        block.insert(
                ColumnWithTypeAndName(col_str->clone(), std::make_shared<DataTypeString>(), "str"));
        block.insert(ColumnWithTypeAndName(col_start->clone(), std::make_shared<DataTypeInt32>(),
                                           "start"));
        block.insert(
                ColumnWithTypeAndName(col_len->clone(), std::make_shared<DataTypeInt32>(), "len"));

        auto res_col = DataTypeString().create_column();
        block.insert(ColumnWithTypeAndName(std::move(res_col), std::make_shared<DataTypeString>(),
                                           "result"));

        ColumnNumbers arguments = {0, 1, 2};
        uint32_t result_index = 3;
        SubstringUtil::substring_execute(block, arguments, result_index, 1);

        // Verify the result
        auto result_column = block.get_by_position(result_index).column;
        ASSERT_EQ(result_column->size(), 1);
        ASSERT_EQ(result_column->get_data_at(0).to_string(), expected)
                << "Failed case: input='" << input_str << "' start=" << start << " len=" << length;
    }
}

TEST(StringOPTest, MultipleRows) {
    // Parametric test: tuple(input string, start, len, expected result)
    std::vector<std::tuple<std::string, int32_t, int32_t, std::string>> test_cases = {
            {"abcdefg", 2, 3, "bcd"},    // Normal row
            {"doris", 3, 5, "ris"},      // Long position
            {"中文测试", -2, 2, "测试"}, // Negative start position handling
            {"test", 0, 2, ""}           // Special parameters
    };

    // Prepare column data
    std::vector<std::string> input_strings;
    std::vector<int32_t> starts, lengths;
    std::vector<std::string> expected;

    for (const auto& [str, s, l, exp] : test_cases) {
        input_strings.push_back(str);
        starts.push_back(s);
        lengths.push_back(l);
        expected.push_back(exp);
    }

    // Create non-empty columns
    auto col_strings = ColumnHelper::create_column<DataTypeString>(input_strings);
    auto col_starts = ColumnHelper::create_column<DataTypeInt32>(starts);
    auto col_lengths = ColumnHelper::create_column<DataTypeInt32>(lengths);

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(col_strings), std::make_shared<DataTypeString>(),
                                       "str"));
    block.insert(ColumnWithTypeAndName(std::move(col_starts), std::make_shared<DataTypeInt32>(),
                                       "start"));
    block.insert(ColumnWithTypeAndName(std::move(col_lengths), std::make_shared<DataTypeInt32>(),
                                       "len"));

    auto res_col = DataTypeString().create_column();
    block.insert(ColumnWithTypeAndName(std::move(res_col), std::make_shared<DataTypeString>(),
                                       "result"));

    ColumnNumbers arguments = {0, 1, 2};
    uint32_t result_index = 3;
    SubstringUtil::substring_execute(block, arguments, result_index, test_cases.size());

    // Verify the result
    auto result_column = block.get_by_position(result_index).column;
    ASSERT_EQ(result_column->size(), test_cases.size());

    for (size_t i = 0; i < test_cases.size(); ++i) {
        const auto& exp = expected[i];
        const auto actual = result_column->get_data_at(i).to_string();
        ASSERT_EQ(actual, exp) << "Row " << i << " failed: input='" << input_strings[i]
                               << " start=" << starts[i] << " len=" << lengths[i];
    }
}

} // namespace doris::vectorized
