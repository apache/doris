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

#include "common/config.h"
#include "vec/common/string_ref.h"

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

    auto parse_result_int = result.value();
    EXPECT_EQ(parse_result_int.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_int.values[0].get_type(), doris::PrimitiveType::TYPE_BIGINT);

    // double
    result = parser.parse("1.23", 4, config);
    ASSERT_TRUE(result.has_value());

    auto parse_result_double = result.value();
    EXPECT_EQ(parse_result_double.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_double.values[0].get_type(), doris::PrimitiveType::TYPE_DOUBLE);

    // bool
    result = parser.parse("true", 4, config);
    ASSERT_TRUE(result.has_value());

    auto parse_result_bool = result.value();
    EXPECT_EQ(parse_result_bool.values[0].get_type(), doris::PrimitiveType::TYPE_BOOLEAN);

    // null
    result = parser.parse("null", 4, config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_null = result.value();
    EXPECT_EQ(parse_result_null.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_null.values[0].get_type(), doris::PrimitiveType::TYPE_NULL);

    // string
    result = parser.parse("\"abc\"", 5, config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_string = result.value();
    EXPECT_EQ(parse_result_string.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_string.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);

    // largeint
    result = parser.parse("12345678901234567890", 20, config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_bigint = result.value();
    EXPECT_EQ(parse_result_bigint.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_bigint.values[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
}

TEST(JsonParserTest, ParseObjectAndArray) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    // Object
    auto result = parser.parse(R"({"a":1,"b":2})", 13, config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_object = result.value();
    EXPECT_EQ(parse_result_object.values.size(), 2);
    EXPECT_EQ(parse_result_object.paths.size(), 2);
    EXPECT_EQ(parse_result_object.values[0].get_type(), doris::PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(parse_result_object.values[1].get_type(), doris::PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(parse_result_object.paths[0].get_path(), "a");
    EXPECT_EQ(parse_result_object.paths[1].get_path(), "b");

    // Array
    result = parser.parse("[1,2,3]", 7, config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_array = result.value();
    EXPECT_EQ(parse_result_array.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_array.values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

    std::string json = R"([1, "string", null, true, 1.23, 12345678901234567890])";
    result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_array_2 = result.value();
    EXPECT_EQ(parse_result_array_2.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_array_2.values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& array_field = parse_result_array_2.values[0].get<doris::PrimitiveType::TYPE_ARRAY>();

    EXPECT_EQ(array_field.size(), 6);
    EXPECT_EQ(array_field[0].get_type(), doris::PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(array_field[1].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(array_field[2].get_type(), doris::PrimitiveType::TYPE_NULL);
    EXPECT_EQ(array_field[3].get_type(), doris::PrimitiveType::TYPE_BOOLEAN);
    EXPECT_EQ(array_field[4].get_type(), doris::PrimitiveType::TYPE_DOUBLE);
    EXPECT_EQ(array_field[5].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
}

TEST(JsonParserTest, ParseMultiLevelNestedArray) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    auto result = parser.parse("[[1,2],[3,4]]", 13, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

    result = parser.parse("[[[1],[2]],[[3],[4]]]", 21, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

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
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

    std::string json = R"({"nested": [{"a": [1,2,3]}]})";
    // result should be jsonbField
    result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);

    // multi level nested array in nested array object
    std::string json2 = R"({"a":[{"b":[[1,2,3]]}]})";
    result = parser.parse(json2.c_str(), json2.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);

    // test flatten nested
    config.enable_flatten_nested = true;
    EXPECT_ANY_THROW(parser.parse(json.c_str(), json.size(), config));
    // test flatten nested with multi level nested array
    // no throw because it is not nested object array
    result = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

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
    EXPECT_EQ(result1->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);

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
    array_data.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(1));
    array_data.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(2));
    doris::vectorized::Field value =
            doris::vectorized::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data));

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
    array_data.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(1));
    array_data.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(2));
    doris::vectorized::Field value =
            doris::vectorized::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data));

    // Create hash for the path
    UInt128 hash = doris::vectorized::PathInData::get_parts_hash(path);

    // First call to populate nested_sizes_by_key
    parser.handleNewPath(hash, path, value, ctx);

    // Verify nested_sizes_by_key was populated
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key")).size(), 3);
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key"))[0], 0);

    // Create another array with same size
    doris::vectorized::Array array_data2;
    array_data2.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(3));
    array_data2.push_back(doris::vectorized::Field::create_field<doris::TYPE_INT>(4));
    doris::vectorized::Field value2 =
            doris::vectorized::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data2));

    // Second call should trigger the else branch (nested_sizes is not empty)
    ctx.is_top_array = false;
    ctx.has_nested_in_flatten = false;
    parser.handleNewPath(hash, path, value2, ctx);

    // Verify nested_sizes_by_key was updated
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key")).size(), 3);
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key"))[1], 0);
}

TEST(JsonParserTest, ParseUInt64) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    std::string json = R"({"a": 18446744073709551615})";
    auto result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->paths[0].get_path(), "a");
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(result->values[0].get<doris::PrimitiveType::TYPE_LARGEINT>(),
              18446744073709551615ULL);

    std::string array_json = R"({"a": [18446744073709551615]})";
    result = parser.parse(array_json.c_str(), array_json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->paths[0].get_path(), "a");
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& array_field = result->values[0].get<doris::PrimitiveType::TYPE_ARRAY>();
    EXPECT_EQ(array_field.size(), 1);
    EXPECT_EQ(array_field[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(array_field[0].get<doris::PrimitiveType::TYPE_LARGEINT>(), 18446744073709551615ULL);

    std::string nested_json = R"({"a": [{"b": 18446744073709551615}]})";
    config.enable_flatten_nested = true;
    result = parser.parse(nested_json.c_str(), nested_json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& array_field_2 = result->values[0].get<doris::PrimitiveType::TYPE_ARRAY>();
    EXPECT_EQ(array_field_2.size(), 1);
    EXPECT_EQ(array_field_2[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(array_field_2[0].get<doris::PrimitiveType::TYPE_LARGEINT>(), 18446744073709551615ULL);
}

TEST(JsonParserTest, KeyLengthLimitByConfig) {
    struct ScopedMaxJsonKeyLength {
        int32_t old_value;
        explicit ScopedMaxJsonKeyLength(int32_t new_value)
                : old_value(doris::config::variant_max_json_key_length) {
            doris::config::variant_max_json_key_length = new_value;
        }
        ~ScopedMaxJsonKeyLength() { doris::config::variant_max_json_key_length = old_value; }
    };

    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;

    {
        ScopedMaxJsonKeyLength guard(10);
        std::string key11(11, 'a');

        std::string obj_json = "{\"" + key11 + "\": 1}";
        EXPECT_ANY_THROW(parser.parse(obj_json.c_str(), obj_json.size(), config));

        config.enable_flatten_nested = false;
        std::string jsonb_json = "{\"a\": [{\"" + key11 + "\": 1}]}";
        EXPECT_ANY_THROW(parser.parse(jsonb_json.c_str(), jsonb_json.size(), config));
    }

    {
        ScopedMaxJsonKeyLength guard(255);
        std::string key255(255, 'b');

        std::string obj_json = "{\"" + key255 + "\": 1}";
        auto result = parser.parse(obj_json.c_str(), obj_json.size(), config);
        ASSERT_TRUE(result.has_value());

        config.enable_flatten_nested = false;
        std::string jsonb_json = "{\"a\": [{\"" + key255 + "\": 1}]}";
        result = parser.parse(jsonb_json.c_str(), jsonb_json.size(), config);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result->values.size(), 1);
        EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);
    }
}
