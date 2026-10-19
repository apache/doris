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

#include "exprs/function/function_tokenize.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionTokenizeTest : public ::testing::Test {
public:
    void SetUp() override {
        // Register the tokenize function
        SimpleFunctionFactory factory;
        register_function_tokenize(factory);

        // Create argument template for tokenize function (input string, properties string)
        auto input_type = std::make_shared<DataTypeString>();
        auto properties_type = std::make_shared<DataTypeString>();
        auto return_type = std::make_shared<DataTypeString>();

        ColumnsWithTypeAndName argument_template = {{nullptr, input_type, "input"},
                                                    {nullptr, properties_type, "properties"}};

        _function = factory.get_function("tokenize", argument_template, return_type, {},
                                         BeExecVersionManager::get_newest_version());
        ASSERT_NE(_function, nullptr);
    }

protected:
    FunctionBasePtr _function;

    // Helper function to create a block with string columns
    Block create_test_block(const std::vector<std::string>& input_strings,
                            const std::string& properties_str) {
        Block block;

        // Create input string column
        auto input_column = ColumnString::create();
        for (const auto& str : input_strings) {
            input_column->insert_data(str.data(), str.size());
        }
        auto input_type = std::make_shared<DataTypeString>();
        block.insert(ColumnWithTypeAndName(std::move(input_column), input_type, "input"));

        // Create properties string column
        auto properties_column = ColumnString::create();
        properties_column->insert_data(properties_str.data(), properties_str.size());
        auto properties_type = std::make_shared<DataTypeString>();
        block.insert(
                ColumnWithTypeAndName(std::move(properties_column), properties_type, "properties"));

        // Add result column
        auto result_type = std::make_shared<DataTypeString>();
        auto result_column = result_type->create_column();
        block.insert(ColumnWithTypeAndName(std::move(result_column), result_type, "result"));

        return block;
    }

    // Helper function to execute tokenize function
    std::vector<std::string> execute_tokenize(const std::vector<std::string>& input_strings,
                                              const std::string& properties_str) {
        Block block = create_test_block(input_strings, properties_str);
        ColumnNumbers arguments = {0, 1}; // input column and properties column
        uint32_t result = 2;              // result column index
        size_t input_rows_count = input_strings.size();

        auto status = _function->execute(nullptr, block, arguments, result, input_rows_count);
        EXPECT_TRUE(status.ok()) << "Error executing tokenize: " << status.to_string();

        // Extract results
        std::vector<std::string> results;
        auto result_column = block.get_by_position(result).column;
        const auto* string_column = assert_cast<const ColumnString*>(result_column.get());

        for (size_t i = 0; i < input_strings.size(); ++i) {
            StringRef result_str = string_column->get_data_at(i);
            results.emplace_back(result_str.to_string());
        }

        return results;
    }
};

// Test parser=none functionality
TEST_F(FunctionTokenizeTest, ParserNone) {
    std::vector<std::string> input_strings = {"Hello World!", "This is a test.",
                                              "Multiple words here",
                                              "", // empty string
                                              "Single"};

    std::string properties = "parser='none'";
    auto results = execute_tokenize(input_strings, properties);

    ASSERT_EQ(results.size(), input_strings.size());

    // For parser=none, each input should return as a single token in JSON array format
    EXPECT_EQ(results[0], R"([{
        "token": "Hello World!"
    }])");
    EXPECT_EQ(results[1], R"([{
        "token": "This is a test."
    }])");
    EXPECT_EQ(results[2], R"([{
        "token": "Multiple words here"
    }])");
    EXPECT_EQ(results[3], R"([{
        "token": ""
    }])"); // empty string should still create a token
    EXPECT_EQ(results[4], R"([{
        "token": "Single"
    }])");
}

// Test parser=none with special characters
TEST_F(FunctionTokenizeTest, ParserNoneSpecialCharacters) {
    std::vector<std::string> input_strings = {"Hello, World!", "Test with punctuation: 123",
                                              "Unicode: 测试", "Numbers and symbols: 123 @#$%"};

    std::string properties = "parser='none'";
    auto results = execute_tokenize(input_strings, properties);

    ASSERT_EQ(results.size(), input_strings.size());

    // Special characters should be preserved exactly
    EXPECT_EQ(results[0], R"([{
        "token": "Hello, World!"
    }])");
    EXPECT_EQ(results[1], R"([{
        "token": "Test with punctuation: 123"
    }])");
    EXPECT_EQ(results[2], R"([{
        "token": "Unicode: 测试"
    }])");
    EXPECT_EQ(results[3], R"([{
        "token": "Numbers and symbols: 123 @#$%"
    }])");
}

// Test comparison with other parsers to ensure parser=none behaves differently
TEST_F(FunctionTokenizeTest, ParserNoneVsEnglish) {
    std::vector<std::string> input_strings = {"Hello World Test"};

    // Test with parser=none
    std::string properties_none = "parser='none'";
    auto results_none = execute_tokenize(input_strings, properties_none);

    // Should return single token
    EXPECT_EQ(results_none[0], R"([{
        "token": "Hello World Test"
    }])");

    // Test with parser=english for comparison
    std::string properties_english = "parser='english'";
    auto results_english = execute_tokenize(input_strings, properties_english);

    // English parser should return multiple tokens - just verify it's different
    EXPECT_NE(results_english[0], results_none[0]);
    // English parser should contain multiple tokens (should have multiple "token" fields)
    EXPECT_GT(results_english[0].size(), results_none[0].size());
}

// Test parser=none with mixed property formats
TEST_F(FunctionTokenizeTest, ParserNonePropertyFormats) {
    std::vector<std::string> input_strings = {"Test String"};

    // Test different ways to specify parser=none
    std::vector<std::string> property_formats = {"parser='none'", "parser=\"none\"", "parser=none",
                                                 "'parser'='none'", R"("parser"="none")"};

    for (const auto& properties : property_formats) {
        auto results = execute_tokenize(input_strings, properties);
        ASSERT_EQ(results.size(), 1);
        EXPECT_EQ(results[0], R"([{
        "token": "Test String"
    }])") << "Failed for property format: "
          << properties;
    }
}

// Regression for const-first-argument row count.
//
// The fix in FunctionTokenize::execute_impl wraps the dest column in a
// ColumnConst(input_rows_count) when the first argument was already a
// ColumnConst. The generic PreparedFunctionImpl::default_implementation_for_
// constant_arguments path only short-circuits when *all* arguments are const,
// so to reach the new left_const branch we need arg0 const but arg1 non-const.
// FE rejects non-literal second arguments for tokenize, so this path is only
// exercisable from a unit test that builds the block directly.
TEST_F(FunctionTokenizeTest, ConstFirstArgPreservesRowCount) {
    const size_t input_rows_count = 5;
    const std::string input_str = "Hello World Test";
    const std::string properties = "parser='english'";

    auto input_inner = ColumnString::create();
    input_inner->insert_data(input_str.data(), input_str.size());
    auto input_const = ColumnConst::create(std::move(input_inner), input_rows_count);

    auto properties_column = ColumnString::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        properties_column->insert_data(properties.data(), properties.size());
    }

    auto string_type = std::make_shared<DataTypeString>();

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(input_const), string_type, "input"));
    block.insert(ColumnWithTypeAndName(std::move(properties_column), string_type, "properties"));
    block.insert(ColumnWithTypeAndName(string_type->create_column(), string_type, "result"));

    ColumnNumbers arguments = {0, 1};
    uint32_t result = 2;

    auto status = _function->execute(nullptr, block, arguments, result, input_rows_count);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto result_column = block.get_by_position(result).column;
    ASSERT_EQ(result_column->size(), input_rows_count);

    // Source is constant, so every row must carry the same tokenized value.
    StringRef first_token = result_column->get_data_at(0);
    ASSERT_GT(first_token.size, 0);
    EXPECT_NE(std::string(first_token.data, first_token.size).find("hello"), std::string::npos);
    EXPECT_NE(std::string(first_token.data, first_token.size).find("world"), std::string::npos);
    for (size_t i = 1; i < input_rows_count; ++i) {
        StringRef row_i = result_column->get_data_at(i);
        EXPECT_EQ(row_i.size, first_token.size);
        EXPECT_EQ(memcmp(row_i.data, first_token.data, first_token.size), 0);
    }
}

// Same check for the PARSER_NONE early-return branch in execute_impl.
TEST_F(FunctionTokenizeTest, ConstFirstArgParserNonePreservesRowCount) {
    const size_t input_rows_count = 4;
    const std::string input_str = "Hello World";
    const std::string properties = "parser='none'";

    auto input_inner = ColumnString::create();
    input_inner->insert_data(input_str.data(), input_str.size());
    auto input_const = ColumnConst::create(std::move(input_inner), input_rows_count);

    auto properties_column = ColumnString::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        properties_column->insert_data(properties.data(), properties.size());
    }

    auto string_type = std::make_shared<DataTypeString>();

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(input_const), string_type, "input"));
    block.insert(ColumnWithTypeAndName(std::move(properties_column), string_type, "properties"));
    block.insert(ColumnWithTypeAndName(string_type->create_column(), string_type, "result"));

    ColumnNumbers arguments = {0, 1};
    uint32_t result = 2;

    auto status = _function->execute(nullptr, block, arguments, result, input_rows_count);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto result_column = block.get_by_position(result).column;
    ASSERT_EQ(result_column->size(), input_rows_count);

    StringRef first_token = result_column->get_data_at(0);
    const std::string expected = R"([{
        "token": "Hello World"
    }])";
    EXPECT_EQ(std::string(first_token.data, first_token.size), expected);
    for (size_t i = 1; i < input_rows_count; ++i) {
        StringRef row_i = result_column->get_data_at(i);
        EXPECT_EQ(std::string(row_i.data, row_i.size), expected);
    }
}

// Test error cases
TEST_F(FunctionTokenizeTest, InvalidParser) {
    std::vector<std::string> input_strings = {"Test String"};
    std::string properties = "parser='invalid'";

    Block block = create_test_block(input_strings, properties);
    ColumnNumbers arguments = {0, 1};
    uint32_t result = 2;
    size_t input_rows_count = input_strings.size();

    auto status = _function->execute(nullptr, block, arguments, result, input_rows_count);
    EXPECT_FALSE(status.ok()) << "Should fail with invalid parser";
}

} // namespace doris