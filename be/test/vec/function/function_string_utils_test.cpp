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
#include "vec/functions/string_utils.h"

namespace doris::vectorized {

// test ok
TEST(StringOPTest, testPushEmptyStringNullable) {
    // Construct a ColumnNullable string column (single non-NULL row) to test push_empty_string
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>({"dummy"}, {false});
    // Here we are only testing the logic of push_empty_string, manually constructing local chars and offsets
    ColumnString::Chars chars;
    ColumnString::Offsets offsets;
    offsets.resize(1); // Ensure row 0 exists

    size_t row_index = 0;

    StringOP::push_empty_string(row_index, chars, offsets);
    // Check the status of offsets and chars
    ASSERT_EQ(offsets[0], chars.size());
    ASSERT_EQ(chars.size(), 0);
}

//test ok
TEST(StringOPTest, testPushNullStringNullable) {
    // Construct a ColumnNullable string column (single NULL row)
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>({"ignored"}, {true});
    ColumnString::Chars chars;
    ColumnString::Offsets offsets;
    offsets.resize(1, 0);
    NullMap null_map;
    null_map.resize(1);

    size_t row_index = 0;
    StringOP::push_null_string(row_index, chars, offsets, null_map);

    // Check NULL mark and offsets status
    ASSERT_EQ(null_map.size(), 1);
    ASSERT_EQ(null_map[0], 1);
    ASSERT_EQ(offsets[0], chars.size());
}

// test ok
TEST(StringOPTest, testPushValueStringNullable) {
    // Define multiple test cases to cover edge cases:
    // 1. Empty string
    // 2. Normal string
    // 3. Single space
    // 4. Multi-byte character (e.g., Chinese)
    // 5. Numeric string
    // 6. Long string (1000 characters)
    std::vector<std::string> test_strings = {"",     "hello",      " ",
                                             "你好", "1234567890", std::string(1000, 'a')};

    // Create a non-NULL ColumnNullable with all rows being non-NULL
    std::vector<bool> null_flags(test_strings.size(), false);
    // auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>({"ignored"}, {true});
    // auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
    //         {"", "hello", " ", "你好", "1234567890", std::string(1000, 'a')}, {0, 0, 0, 0, 0, 0});
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            std::vector<std::string> {test_strings}, {0, 0, 0, 0, 0, 0});

    // Initialize chars and offsets arrays, with the number of rows matching the test cases
    ColumnString::Chars chars;
    ColumnString::Offsets offsets;
    offsets.resize(test_strings.size(), 0);

    // For each row, call push_value_string to write the corresponding test string
    for (size_t i = 0; i < test_strings.size(); ++i) {
        if (col_nullable->is_null_at(i)) {
            // In this test, all rows are non-NULL, so this branch will not be entered
            NullMap null_map;
            StringOP::push_null_string(i, chars, offsets, null_map);
        } else {
            StringOP::push_value_string(test_strings[i], i, chars, offsets);
        }
    }

    // Verify the result written for each row
    size_t start_offset = 0;
    for (size_t i = 0; i < test_strings.size(); ++i) {
        size_t row_length = offsets[i] - start_offset;
        // Check if the written length matches the test string length
        ASSERT_EQ(row_length, test_strings[i].size()) << "Row " << i << " length mismatch.";
        // Check if the written content matches the test string
        std::string actual(reinterpret_cast<const char*>(chars.data() + start_offset), row_length);
        ASSERT_EQ(actual, test_strings[i]) << "Row " << i << " content mismatch.";
        start_offset = offsets[i];
    }
}

// test ok
TEST(StringOPTest, testPushValueStringReservedAndAllowOverflowNullable) {
    // Define test cases
    std::vector<std::string> original_test_strings = {
            "",              // Empty string
            "normal_string", // Normal string
            "unused",        // Logically NULL string
    };

    // Define NULL marks
    std::vector<uint8_t> null_flags = {1, 0, 0}; // 1 means NULL

    // Create a ColumnNullable string column
    auto col_nullable = ColumnHelper::create_nullable_column<DataTypeString>(
            std::vector<std::string>(original_test_strings), std::move(null_flags));

    // Initialize chars and offsets arrays
    ColumnString::Chars chars;
    ColumnString::Offsets offsets;
    offsets.resize(original_test_strings.size());
    chars.resize(original_test_strings.size());
    // ASSERT_EQ(chars.size(), 0);
    // for (size_t i = 0; i < original_test_strings.size(); ++i) {
    // 	ASSERT_EQ(offsets[i], 0);
    // }
    offsets[-1] = chars.size();

    // Construct a global null map
    NullMap null_map;
    null_map.resize(original_test_strings.size());

    // For each row, call the corresponding write function
    for (size_t i = 0; i < original_test_strings.size(); ++i) {
        if (col_nullable->is_null_at(i)) {
            // For NULL rows, write an empty string
            StringOP::push_null_string(i, chars, offsets, null_map);
        } else {
            // For non-NULL rows, write a string with additional padding
            StringOP::push_value_string_reserved_and_allow_overflow(original_test_strings[i], i,
                                                                    chars, offsets);
            ASSERT_EQ(original_test_strings[i].size(), offsets[i] - offsets[i - 1])
                    << "Row " << i << " length mismatch."
                    << " " << original_test_strings[i].size() << ' ' << offsets[i] << ' '
                    << offsets[i - 1];
        }
    }

    // Verify the result written for each row
    // size_t start_offset = offsets[-1];
    for (size_t i = 0; i < original_test_strings.size(); ++i) {
        size_t row_length = offsets[i] - offsets[i - 1];
        if (col_nullable->is_null_at(i)) {
            // For NULL rows, we expect an empty string (or check special marks based on push_null_string conventions)
            ASSERT_EQ(row_length, 0u) << "Row " << i << " expected to be null with 0 length.";
        } else {
            // For non-NULL rows, the written length should match the original string length
            ASSERT_EQ(row_length, original_test_strings[i].size())
                    << "Row " << i << " length mismatch."
                    << " " << row_length << " " << original_test_strings[i].size() << ' '
                    << original_test_strings[i] << offsets[i] << ' ' << offsets[i - 1];
            std::string actual(reinterpret_cast<const char*>(chars.data() + offsets[i - 1]),
                               row_length);
            ASSERT_EQ(actual, original_test_strings[i])
                    << "Row " << i << " content mismatch."
                    << " " << actual << " " << original_test_strings[i];
        }
        // start_offset = offsets[i];
    }
}

//ok
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

//ok
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

//ok
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

//ok
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
