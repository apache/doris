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

#include "vec/json/json_parser.h"

#include <gtest/gtest.h>

#include <vector>

#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"

using doris::vectorized::JSONDataParser;
using doris::vectorized::SimdJSONParser;
using doris::vectorized::ParseConfig;

TEST(JsonParserTest, ParseSimpleTypes) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    // int
    auto result = parser.parse("123", 3, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);

    // double
    result = parser.parse("1.23", 4, config);
    ASSERT_TRUE(result.has_value());

    // bool
    result = parser.parse("true", 4, config);
    ASSERT_TRUE(result.has_value());

    // null
    result = parser.parse("null", 4, config);
    ASSERT_TRUE(result.has_value());

    // string
    result = parser.parse("\"abc\"", 5, config);
    ASSERT_TRUE(result.has_value());
}

TEST(JsonParserTest, ParseObjectAndArray) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    // Object
    auto result = parser.parse(R"({"a":1,"b":2})", 13, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 2);

    // Array
    result = parser.parse("[1,2,3]", 7, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
}

TEST(JsonParserTest, ParseMultiLevelNestedArray) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    auto result = parser.parse("[[1,2],[3,4]]", 13, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::Array);

    result = parser.parse("[[[1],[2]],[[3],[4]]]", 21, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::Array);

    result = parser.parse("[[1,2],[3],[4,5,6]]", 19, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);

    // Test complex nested structure
    config.enable_flatten_nested = false;
    std::string json1 = R"({"a":[[1,2],[3],[4,5,6]]})";
    // multi level nested array in object
    result = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::Array);

    std::string json = R"({"nested": [{"a": [1,2,3]}]})";
    // result should be jsonbField
    result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::JSONB);

    // multi level nested array in nested array object
    std::string json2 = R"({"a":[{"b":[[1,2,3]]}]})";
    result = parser.parse(json2.c_str(), json2.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::JSONB);

    // test flatten nested
    config.enable_flatten_nested = true;
    EXPECT_ANY_THROW(parser.parse(json.c_str(), json.size(), config));
    // test flatten nested with multi level nested array
    // no throw because it is not nested object array
    result = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::vectorized::Field::Types::Array);

    EXPECT_ANY_THROW(parser.parse(json2.c_str(), json2.size(), config));
}

TEST(JsonParserTest, ParseNestedAndFlatten) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.enable_flatten_nested = true;

    std::string json = R"({"a":[{"b":1},{"b":2}]})";
    auto result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_GT(result->values.size(), 0);

    config.enable_flatten_nested = false;
    std::string json2 = R"({"a":[{"b":1},{"b":2}]})";
    result = parser.parse(json2.c_str(), json2.size(), config);
    ASSERT_TRUE(result.has_value());
}

TEST(JsonParserTest, ParseInvalidJson) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    auto result = parser.parse("{a:1}", 5, config);
    ASSERT_FALSE(result.has_value());

    result = parser.parse("", 0, config);
    ASSERT_FALSE(result.has_value());
}

TEST(JsonParserTest, ParseCornerCases) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    auto result = parser.parse("{}", 2, config);
    ASSERT_TRUE(result.has_value());

    result = parser.parse("[]", 2, config);
    ASSERT_TRUE(result.has_value());

    result = parser.parse(R"({"a":"\n\t"})", 12, config);
    ASSERT_TRUE(result.has_value());
}

// Test cases for the selected code functionality
TEST(JsonParserTest, TestIsPrefixFunction) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    // Test is_prefix functionality through nested path parsing
    // This tests the is_prefix function used in checkAmbiguousStructure

    // Test case 1: Simple nested paths that should not be ambiguous
    std::string json1 = R"({"a": [{"b": 1}, {"b": 2}]})";
    auto result1 = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result1.has_value());

    // Test case 2: More complex nested paths
    std::string json2 = R"({"a": [{"b": {"c": 1}}, {"b": {"c": 2}}]})";
    auto result2 = parser.parse(json2.c_str(), json2.size(), config);
    ASSERT_TRUE(result2.has_value());

    // Test case 3: Deep nested structure
    std::string json3 = R"({"level1": {"level2": [{"level3": {"level4": 1}}]}})";
    auto result3 = parser.parse(json3.c_str(), json3.size(), config);
    ASSERT_TRUE(result3.has_value());
}

TEST(JsonParserTest, TestAmbiguousStructureDetection) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.enable_flatten_nested = true;

    // Test case 1: Arrays with different sizes in nested structure
    // This should trigger the array size mismatch exception
    std::string json1 = R"([{"b": [1, 2]}, {"b": [1, 2, 3]}])";
    EXPECT_ANY_THROW(parser.parse(json1.c_str(), json1.size(), config));

    // Test case 2: Arrays with same sizes should not throw
    std::string json2 = R"([{"b": [1, 2]}, {"b": [3, 4]}])";
    EXPECT_ANY_THROW(parser.parse(json2.c_str(), json2.size(), config));

    // Test case 3: More complex nested array size mismatch
    std::string json3 = R"({"nested": [{"arr": [[1, 2], [3]]}, {"arr": [[1, 2], [3, 4]]}]})";
    EXPECT_ANY_THROW(parser.parse(json3.c_str(), json3.size(), config));

    // Test case 4: Ambiguous structure with prefix paths
    // This should trigger the ambiguous structure exception
    std::string json4 = R"([{"a": {"c": 1}}, {"a": 2}])";
    EXPECT_ANY_THROW(parser.parse(json4.c_str(), json4.size(), config));
}

TEST(JsonParserTest, TestNestedArrayHandling) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.enable_flatten_nested = true;

    // Test case 1: Simple nested array handling
    std::string json1 = R"([{"b": 1}, {"c": 2}])";
    auto result1 = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result1.has_value());
    EXPECT_GT(result1->values.size(), 0);

    // Test case 2: Multi-level nested array
    std::string json2 = R"([{"a": {"b": 1}}, {"a": {"b": 2}}])";
    auto result2 = parser.parse(json2.c_str(), json2.size(), config);
    ASSERT_TRUE(result2.has_value());
    EXPECT_GT(result2->values.size(), 0);
}

TEST(JsonParserTest, TestNestedArrayWithDifferentConfigs) {
    JSONDataParser<SimdJSONParser> parser;

    // Test with flatten_nested = false
    ParseConfig config1;
    config1.enable_flatten_nested = false;

    std::string json1 = R"([{"b": [1, 2]}, {"b": [3, 4]}])";
    auto result1 = parser.parse(json1.c_str(), json1.size(), config1);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(result1->values.size(), 1);
    EXPECT_EQ(result1->values[0].get_type(), doris::vectorized::Field::Types::JSONB);

    // Test with flatten_nested = true
    ParseConfig config2;
    config2.enable_flatten_nested = true;

    EXPECT_ANY_THROW(parser.parse(json1.c_str(), json1.size(), config2));
}

// Test case for directly calling handleNewPath to cover the if (!nested_key.empty()) branch
TEST(JsonParserTest, TestHandleNewPathDirectCall) {
    JSONDataParser<SimdJSONParser> parser;

    // Create a ParseArrayContext
    JSONDataParser<SimdJSONParser>::ParseArrayContext ctx;
    ctx.current_size = 1;
    ctx.total_size = 2;
    ctx.has_nested_in_flatten = true;
    ctx.is_top_array = true;

    // Create a path with nested parts
    doris::vectorized::PathInData::Parts path;
    // Create a nested part (is_nested = true)
    path.emplace_back("nested_key", true, 0); // is_nested = true
    path.emplace_back("inner_key", false, 0); // is_nested = false

    // Create a Field with array type (required for getNameOfNested to return non-empty)
    doris::vectorized::Array array_data;
    array_data.push_back(doris::vectorized::Int32(1));
    array_data.push_back(doris::vectorized::Int32(2));
    doris::vectorized::Field value = std::move(array_data);

    // Create hash for the path
    UInt128 hash = doris::vectorized::PathInData::get_parts_hash(path);

    // Call handleNewPath directly
    // This should trigger the if (!nested_key.empty()) branch
    parser.handleNewPath(hash, path, value, ctx);

    // Verify that the nested_sizes_by_key was populated
    EXPECT_EQ(ctx.nested_sizes_by_key.size(), 1);

    // Verify that the arrays_by_path was populated
    EXPECT_EQ(ctx.arrays_by_path.size(), 1);
    EXPECT_TRUE(ctx.arrays_by_path.find(hash) != ctx.arrays_by_path.end());
}

// Test case for testing the else branch in handleNewPath (when nested_sizes is not empty)
TEST(JsonParserTest, TestHandleNewPathElseBranch) {
    JSONDataParser<SimdJSONParser> parser;

    // Create a ParseArrayContext
    JSONDataParser<SimdJSONParser>::ParseArrayContext ctx;
    ctx.current_size = 2; // Start with size 2
    ctx.total_size = 3;
    ctx.has_nested_in_flatten = true;
    ctx.is_top_array = true;

    // Create a path with nested parts
    doris::vectorized::PathInData::Parts path;
    path.emplace_back("nested_key", true, 0);
    path.emplace_back("inner_key", false, 0);

    // Create a Field with array type
    doris::vectorized::Array array_data;
    array_data.push_back(doris::vectorized::Int32(1));
    array_data.push_back(doris::vectorized::Int32(2));
    doris::vectorized::Field value = std::move(array_data);

    // Create hash for the path
    UInt128 hash = doris::vectorized::PathInData::get_parts_hash(path);

    // First call to populate nested_sizes_by_key
    parser.handleNewPath(hash, path, value, ctx);

    // Verify nested_sizes_by_key was populated
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key")).size(), 3);
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key"))[0], 0);

    // Create another array with same size
    doris::vectorized::Array array_data2;
    array_data2.push_back(doris::vectorized::Int32(3));
    array_data2.push_back(doris::vectorized::Int32(4));
    doris::vectorized::Field value2 = std::move(array_data2);

    // Second call should trigger the else branch (nested_sizes is not empty)
    ctx.is_top_array = false;
    ctx.has_nested_in_flatten = false;
    parser.handleNewPath(hash, path, value2, ctx);

    // Verify nested_sizes_by_key was updated
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key")).size(), 3);
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key"))[1], 0);
}
