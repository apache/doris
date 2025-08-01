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

TEST(StringOPTest, testStringPushOperations) {
    // Create expected result column with various string types
    std::vector<std::string> expected_strings = {
            "",                               // Empty string
            "abc",                            // Simple ASCII string
            "中文测试",                       // UTF-8 multi-byte string
            "",                               // NULL marker
            "   ",                            // Whitespace only
            "!@#$%^&*()",                     // Special characters
            "123456789012345678901234567890", // Medium-length string
            std::string(10000, 'x'),          // Very long string
            "混合English和中文",              // Mixed language
            "包含\n换行\t制表符",             // Contains escape characters
            "末尾有空格   "                   // Trailing spaces
    };

    std::vector<uint8_t> null_flags = {false, false, false, true,  false, false,
                                       false, false, false, false, false}; // The fourth one is NULL

    auto expected_col =
            ColumnHelper::create_nullable_column<DataTypeString>(expected_strings, null_flags);

    auto test_col = ColumnHelper::create_nullable_column<DataTypeString>({}, {});
    auto* test_column_nullable = dynamic_cast<const ColumnNullable*>(test_col.get());
    auto& test_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(test_column_nullable->get_nested_column()));
    auto& test_chars = test_nested_column.get_chars();
    auto& test_offsets = test_nested_column.get_offsets();
    test_offsets.resize(expected_strings.size());
    NullMap test_null_map(expected_strings.size(), false);

    // Use loop and conditions to select different push functions to fill the test column
    for (size_t i = 0; i < expected_strings.size(); ++i) {
        if (null_flags[i]) {
            // Use push_null_string for NULL values
            StringOP::push_null_string(i, test_chars, test_offsets, test_null_map);
        } else if (expected_strings[i].empty()) {
            // Use push_empty_string for empty strings
            StringOP::push_empty_string(i, test_chars, test_offsets);
        } else {
            // Use push_value_string for normal strings
            StringOP::push_value_string(expected_strings[i], i, test_chars, test_offsets);
        }
    }

    for (size_t i = 0; i < expected_strings.size(); ++i) {
        if (null_flags[i]) {
            continue; // Skip content validation for NULL values
        }
        ASSERT_EQ(static_cast<bool>(test_null_map[i]), static_cast<bool>(null_flags[i]))
                << "Row " << i << " expected to be non-null.";

        size_t row_length = test_offsets[i] - test_offsets[i - 1];
        ASSERT_EQ(row_length, expected_strings[i].size())
                << "Row " << i << " length mismatch: " << row_length << " vs "
                << expected_strings[i].size();

        std::string actual(static_cast<const char*>(static_cast<const void*>(test_chars.data() +
                                                                             test_offsets[i - 1])),
                           row_length);
        ASSERT_EQ(actual, expected_strings[i]) << "Row " << i << " content mismatch.";
    }
}

TEST(StringOPTest, testPushValueStringReservedAndAllowOverFlow) {
    // Create expected result column with various string types
    std::vector<std::string> expected_strings = {"",    "abc",        "中文测试",   "",
                                                 "   ", "!@#$%^&*()", "1234567890", "xxxxxx",
                                                 "a",   "   ",        "!@#$%^&*()"};
    std::vector<uint8_t> null_flags = {false, false, false, true,  false, false,
                                       false, false, false, false, false}; // The fourth one is NULL

    auto expected_col =
            ColumnHelper::create_nullable_column<DataTypeString>(expected_strings, null_flags);

    auto test_col = ColumnHelper::create_nullable_column<DataTypeString>({}, {});
    auto* test_column_nullable = dynamic_cast<const ColumnNullable*>(test_col.get());
    auto& test_nested_column = const_cast<ColumnString&>(
            static_cast<const ColumnString&>(test_column_nullable->get_nested_column()));
    auto& test_chars = test_nested_column.get_chars();
    auto& test_offsets = test_nested_column.get_offsets();
    test_offsets.resize(expected_strings.size());
    NullMap test_null_map(expected_strings.size(), false);

    // Calculate total length of all strings for reserving space
    size_t total_length = 0;
    for (const auto& str : expected_strings) {
        total_length += str.size();
    }
    test_chars.reserve(total_length);
    for (size_t i = 0; i < expected_strings.size(); ++i) {
        if (null_flags[i]) {
            // Use push_null_string for NULL values
            StringOP::push_null_string(i, test_chars, test_offsets, test_null_map);
        } else if (expected_strings[i].empty()) {
            // Use push_empty_string for empty strings
            StringOP::push_empty_string(i, test_chars, test_offsets);
        } else {
            // Reserve all space at once
            StringOP::push_value_string_reserved_and_allow_overflow(expected_strings[i], i,
                                                                    test_chars, test_offsets);
        }
    }

    for (size_t i = 0; i < expected_strings.size(); ++i) {
        if (null_flags[i]) {
            continue; // Skip content validation for NULL values
        }
        ASSERT_EQ(static_cast<bool>(test_null_map[i]), static_cast<bool>(null_flags[i]))
                << "Row " << i << " expected to be non-null.";

        size_t row_length = test_offsets[i] - test_offsets[i - 1];
        ASSERT_EQ(row_length, expected_strings[i].size())
                << "Row " << i << " length mismatch: " << row_length << " vs "
                << expected_strings[i].size();

        std::string actual(static_cast<const char*>(static_cast<const void*>(test_chars.data() +
                                                                             test_offsets[i - 1])),
                           row_length);
        ASSERT_EQ(actual, expected_strings[i]) << "Row " << i << " content mismatch.";
    }
}
TEST(StringOPTest, testFastRepeat) {
    const std::string src = "example";
    {
        int32_t repeat_times = 0;
        // Allocate enough buffer (when repeat_times is 0, the size is 0)
        std::vector<uint8_t> dst(src.size() * repeat_times);
        StringOP::fast_repeat(dst.data(),
                              static_cast<const uint8_t*>(static_cast<const void*>(src.data())),
                              src.size(), repeat_times);
        // dst length is 0, no content written
        ASSERT_EQ(dst.size(), 0);
    }

    {
        for (int32_t repeat_times = 1; repeat_times <= 10; ++repeat_times) {
            std::vector<uint8_t> dst(src.size() * repeat_times);
            StringOP::fast_repeat(dst.data(),
                                  static_cast<const uint8_t*>(static_cast<const void*>(src.data())),
                                  src.size(), repeat_times);

            // Use std::string constructor with pointer to uint8_t for better safety
            std::string result(static_cast<const char*>(static_cast<const void*>(dst.data())),
                               dst.size());

            std::string expected;
            for (int i = 0; i < repeat_times; ++i) {
                expected += src;
            }
            ASSERT_EQ(result, expected) << "Failed for repeat_times = " << repeat_times;
        }
    }
}

TEST(StringOPTest, testSubstringExecute) {
    // Test case 1: Test empty string with various parameters
    std::vector<std::tuple<std::string, int32_t, int32_t, std::string>> test_cases = {
            {"", 0, 0, ""},  // Empty string, zero parameters
            {"", 1, 5, ""},  // Empty string, positive start position
            {"", -1, 3, ""}, // Empty string, negative start position
            {"", 2, -1, ""}  // Empty string, negative length
    };

    // Test case 2: Add non-empty string test cases
    std::vector<std::tuple<std::string, int32_t, int32_t, std::string>> more_test_cases = {
            {"hello", 1, 2, "he"},           // Normal substring from start
            {"hello", 2, 3, "ell"},          // Substring from middle
            {"hello", -3, 2, "ll"},          // Negative start position
            {"hello", 10, 2, ""},            // Start beyond string length
            {"hello", 1, 10, "hello"},       // Length beyond string end
            {"hello", 1, -1, ""},            // Negative length
            {"中文测试", 1, 2, "中文"},      // UTF-8 multi-byte string
            {"中文测试", 3, 2, "测试"},      // UTF-8 multi-byte string, partial
            {"混合English", 3, 7, "English"} // Mixed language string
    };

    // First test with empty string
    for (const auto& [input_str, start_val, len_val, expected] : test_cases) {
        std::vector<std::string> input_strings = {input_str};
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
        ASSERT_EQ(result_column->get_data_at(0).to_string(), expected)
                << "Failed for input='" << input_str << "', start=" << start_val
                << ", len=" << len_val;
    }

    // Then test with non-empty strings
    for (const auto& [input_str, start_val, len_val, expected] : more_test_cases) {
        std::vector<std::string> input_strings = {input_str};
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
        ASSERT_EQ(result_column->get_data_at(0).to_string(), expected)
                << "Failed for input='" << input_str << "', start=" << start_val
                << ", len=" << len_val;
    }
}

} // namespace doris::vectorized
