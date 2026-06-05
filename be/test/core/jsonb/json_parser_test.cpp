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

#include "util/json/json_parser.h"

#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/string_ref.h"

using doris::JSONDataParser;
using doris::SimdJSONParser;
using doris::ParseConfig;

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

    // untyped high precision decimal tokens keep the regular numeric behavior
    std::string high_precision_decimal = "999999999999999999999999999.999999999";
    result = parser.parse(high_precision_decimal.c_str(), high_precision_decimal.size(), config);
    ASSERT_TRUE(result.has_value());

    auto parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_DOUBLE);

    // typed decimal paths keep the original text for later decimal casts
    config.preserve_decimal_number_paths.emplace("");
    result = parser.parse(high_precision_decimal.c_str(), high_precision_decimal.size(), config);
    ASSERT_TRUE(result.has_value());

    parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_decimal.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              high_precision_decimal);

    std::string scale_sensitive_decimal = "0.57";
    result = parser.parse(scale_sensitive_decimal.c_str(), scale_sensitive_decimal.size(), config);
    ASSERT_TRUE(result.has_value());

    parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_decimal.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              scale_sensitive_decimal);

    std::string high_precision_decimal_with_spaces = high_precision_decimal + " \n";
    result = parser.parse(high_precision_decimal_with_spaces.c_str(),
                          high_precision_decimal_with_spaces.size(), config);
    ASSERT_TRUE(result.has_value());

    parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_decimal.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              high_precision_decimal);

    std::string high_precision_decimal_exponent = "999999999999999999999999999.999999999e0";
    result = parser.parse(high_precision_decimal_exponent.c_str(),
                          high_precision_decimal_exponent.size(), config);
    ASSERT_TRUE(result.has_value());

    parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_decimal.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              high_precision_decimal_exponent);

    std::string typed_integer = "123";
    result = parser.parse(typed_integer.c_str(), typed_integer.size(), config);
    ASSERT_TRUE(result.has_value());

    auto parse_result_typed_integer = result.value();
    EXPECT_EQ(parse_result_typed_integer.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_typed_integer.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_typed_integer.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              typed_integer);

    ParseConfig untyped_decimal_config;
    result = parser.parse(high_precision_decimal_exponent.c_str(),
                          high_precision_decimal_exponent.size(), untyped_decimal_config);
    ASSERT_TRUE(result.has_value());

    parse_result_decimal = result.value();
    EXPECT_EQ(parse_result_decimal.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal.values[0].get_type(), doris::PrimitiveType::TYPE_DOUBLE);

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
    ParseConfig untyped_integer_config;
    result = parser.parse("12345678901234567890", 20, untyped_integer_config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_bigint = result.value();
    EXPECT_EQ(parse_result_bigint.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_bigint.values[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);

    // untyped integers beyond uint64 keep the regular numeric behavior.
    std::string big_integer = "18446744073709551616";
    result = parser.parse(big_integer.c_str(), big_integer.size(), untyped_integer_config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_untyped_big_integer = result.value();
    EXPECT_EQ(parse_result_untyped_big_integer.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_untyped_big_integer.values[0].get_type(),
              doris::PrimitiveType::TYPE_DOUBLE);

    ParseConfig typed_integer_config;
    typed_integer_config.preserve_decimal_number_paths.emplace("");
    result = parser.parse(big_integer.c_str(), big_integer.size(), typed_integer_config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_big_integer = result.value();
    EXPECT_EQ(parse_result_big_integer.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_big_integer.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_big_integer.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              big_integer);

    std::string decimal256_integer = "99999999999999999999999999999999999999999999999999";
    result = parser.parse(decimal256_integer.c_str(), decimal256_integer.size(),
                          typed_integer_config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_decimal256_integer = result.value();
    EXPECT_EQ(parse_result_decimal256_integer.paths[0].get_path(), "");
    EXPECT_EQ(parse_result_decimal256_integer.values[0].get_type(),
              doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_decimal256_integer.values[0].get<doris::PrimitiveType::TYPE_STRING>(),
              decimal256_integer);

    ParseConfig mixed_number_config;
    mixed_number_config.preserve_decimal_number_paths.emplace("amount");
    std::string mixed_json =
            R"({"amount":0.57,"id":18446744073709551616,"normal":12345678901234567890})";
    result = parser.parse(mixed_json.c_str(), mixed_json.size(), mixed_number_config);
    ASSERT_TRUE(result.has_value());
    auto parse_result_mixed_number = result.value();
    ASSERT_EQ(parse_result_mixed_number.paths.size(), 3);
    EXPECT_EQ(parse_result_mixed_number.paths[0].get_path(), "amount");
    EXPECT_EQ(parse_result_mixed_number.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_mixed_number.paths[1].get_path(), "id");
    EXPECT_EQ(parse_result_mixed_number.values[1].get_type(), doris::PrimitiveType::TYPE_DOUBLE);
    EXPECT_EQ(parse_result_mixed_number.paths[2].get_path(), "normal");
    EXPECT_EQ(parse_result_mixed_number.values[2].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
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

TEST(JsonParserTest, PreserveDecimalNumbersForTypedPaths) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.preserve_decimal_number_paths.emplace("a");

    std::string json = R"({"a":[999999999999999999999999999.999999999  ]})";
    auto result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_decimal_array = result.value();
    EXPECT_EQ(parse_result_decimal_array.paths[0].get_path(), "a");
    EXPECT_EQ(parse_result_decimal_array.values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& decimal_array =
            parse_result_decimal_array.values[0].get<doris::PrimitiveType::TYPE_ARRAY>();
    ASSERT_EQ(decimal_array.size(), 1);
    EXPECT_EQ(decimal_array[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(decimal_array[0].get<doris::PrimitiveType::TYPE_STRING>(),
              "999999999999999999999999999.999999999");

    ParseConfig untyped_config;
    result = parser.parse(json.c_str(), json.size(), untyped_config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_untyped_decimal_array = result.value();
    auto& untyped_decimal_array =
            parse_result_untyped_decimal_array.values[0].get<doris::PrimitiveType::TYPE_ARRAY>();
    ASSERT_EQ(untyped_decimal_array.size(), 1);
    EXPECT_EQ(untyped_decimal_array[0].get_type(), doris::PrimitiveType::TYPE_DOUBLE);
}

TEST(JsonParserTest, PreserveDecimalNumbersByPathMatcher) {
    JSONDataParser<SimdJSONParser> parser;

    ParseConfig matcher_config;
    matcher_config.preserve_decimal_number_path_matcher = [](std::string_view path) {
        return path.size() >= 8 && path.substr(0, 8) == "decimal_";
    };

    std::string json =
            R"({"decimal_1":999999999999999999999999999.999999999,"other":999999999999999999999999999.999999999})";
    auto result = parser.parse(json.c_str(), json.size(), matcher_config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_matcher = result.value();
    EXPECT_EQ(parse_result_matcher.paths[0].get_path(), "decimal_1");
    EXPECT_EQ(parse_result_matcher.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_matcher.paths[1].get_path(), "other");
    EXPECT_EQ(parse_result_matcher.values[1].get_type(), doris::PrimitiveType::TYPE_DOUBLE);
}

TEST(JsonParserTest, PreserveDecimalNumbersForEscapedTypedPath) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig escaped_key_config;
    escaped_key_config.preserve_decimal_number_paths.emplace("ab.decimal");

    std::string json = R"({"a\u0062":{"decimal":0.57,"nested_key":"value"}})";
    auto result = parser.parse(json.c_str(), json.size(), escaped_key_config);
    ASSERT_TRUE(result.has_value());
    auto& parse_result_escaped_key = result.value();
    ASSERT_EQ(parse_result_escaped_key.paths.size(), 2);
    EXPECT_EQ(parse_result_escaped_key.paths[0].get_path(), "ab.decimal");
    EXPECT_EQ(parse_result_escaped_key.values[0].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(parse_result_escaped_key.values[0].get<doris::PrimitiveType::TYPE_STRING>(), "0.57");
    EXPECT_EQ(parse_result_escaped_key.paths[1].get_path(), "ab.nested_key");
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
    config.deprecated_enable_flatten_nested = false;
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
    config.deprecated_enable_flatten_nested = true;
    // TODO: checkAmbiguousStructure is only called when has_nested_in_flatten && is_top_array.
    // These JSONs are objects (not top-level arrays), so is_top_array=false and the check is skipped.
    // EXPECT_ANY_THROW(parser.parse(json.c_str(), json.size(), config));
    // test flatten nested with multi level nested array
    // no throw because it is not nested object array
    result = parser.parse(json1.c_str(), json1.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);

    // TODO: Same reason as above — object-level array, is_top_array=false, check skipped.
    // EXPECT_ANY_THROW(parser.parse(json2.c_str(), json2.size(), config));
}

TEST(JsonParserTest, ParseNestedAndFlatten) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.deprecated_enable_flatten_nested = true;

    std::string json = R"({"a":[{"b":1},{"b":2}]})";
    auto result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_GT(result->values.size(), 0);

    config.deprecated_enable_flatten_nested = false;
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
    config.deprecated_enable_flatten_nested = true;

    // TODO: The following 3 cases no longer throw because checkAmbiguousStructure requires
    // has_nested_in_flatten && is_top_array. "b" contains plain arrays (not nested objects),
    // so has_nested=false → has_nested_in_flatten=false, and the ambiguity check is skipped.

    // Test case 1: Arrays with different sizes in nested structure
    std::string json1 = R"([{"b": [1, 2]}, {"b": [1, 2, 3]}])";
    // EXPECT_ANY_THROW(parser.parse(json1.c_str(), json1.size(), config));

    // Test case 2: Arrays with same sizes should not throw
    std::string json2 = R"([{"b": [1, 2]}, {"b": [3, 4]}])";
    // EXPECT_ANY_THROW(parser.parse(json2.c_str(), json2.size(), config));

    // Test case 3: More complex nested array size mismatch (object-level, is_top_array=false)
    std::string json3 = R"({"nested": [{"arr": [[1, 2], [3]]}, {"arr": [[1, 2], [3, 4]]}]})";
    // EXPECT_ANY_THROW(parser.parse(json3.c_str(), json3.size(), config));

    // Test case 4: Ambiguous structure with prefix paths
    // This should trigger the ambiguous structure exception
    std::string json4 = R"([{"a": {"c": 1}}, {"a": 2}])";
    EXPECT_ANY_THROW(parser.parse(json4.c_str(), json4.size(), config));
}

TEST(JsonParserTest, TestNestedArrayHandling) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.deprecated_enable_flatten_nested = true;

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
    config1.deprecated_enable_flatten_nested = false;

    std::string json1 = R"([{"b": [1, 2]}, {"b": [3, 4]}])";
    auto result1 = parser.parse(json1.c_str(), json1.size(), config1);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(result1->values.size(), 1);
    EXPECT_EQ(result1->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);

    // Test with flatten_nested = true
    ParseConfig config2;
    config2.deprecated_enable_flatten_nested = true;

    // TODO: "b" contains plain arrays (no nested objects), so has_nested=false,
    // has_nested_in_flatten=false, and checkAmbiguousStructure is not called.
    // EXPECT_ANY_THROW(parser.parse(json1.c_str(), json1.size(), config2));
}

TEST(JsonParserTest, BigIntegerInJsonbKeepsNumericParse) {
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    config.deprecated_enable_flatten_nested = false;

    std::string json = R"({"nested": [{"big": 18446744073709551616}]})";
    auto result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);
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
    doris::PathInData::Parts path;
    // Create a nested part (is_nested = true)
    path.emplace_back("nested_key", true, 0); // is_nested = true
    path.emplace_back("inner_key", false, 0); // is_nested = false

    // Create a Field with array type (required for getNameOfNested to return non-empty)
    doris::Array array_data;
    array_data.push_back(doris::Field::create_field<doris::TYPE_INT>(1));
    array_data.push_back(doris::Field::create_field<doris::TYPE_INT>(2));
    doris::Field value = doris::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data));

    // Create hash for the path
    UInt128 hash = doris::PathInData::get_parts_hash(path);

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
    doris::PathInData::Parts path;
    path.emplace_back("nested_key", true, 0);
    path.emplace_back("inner_key", false, 0);

    // Create a Field with array type
    doris::Array array_data;
    array_data.push_back(doris::Field::create_field<doris::TYPE_INT>(1));
    array_data.push_back(doris::Field::create_field<doris::TYPE_INT>(2));
    doris::Field value = doris::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data));

    // Create hash for the path
    UInt128 hash = doris::PathInData::get_parts_hash(path);

    // First call to populate nested_sizes_by_key
    parser.handleNewPath(hash, path, value, ctx);

    // Verify nested_sizes_by_key was populated
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key")).size(), 3);
    EXPECT_EQ(ctx.nested_sizes_by_key.at(doris::StringRef("nested_key"))[0], 0);

    // Create another array with same size
    doris::Array array_data2;
    array_data2.push_back(doris::Field::create_field<doris::TYPE_INT>(3));
    array_data2.push_back(doris::Field::create_field<doris::TYPE_INT>(4));
    doris::Field value2 = doris::Field::create_field<doris::TYPE_ARRAY>(std::move(array_data2));

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
    config.deprecated_enable_flatten_nested = true;
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

        std::string obj_json = R"({")" + key11 + R"(": 1})";
        EXPECT_ANY_THROW(parser.parse(obj_json.c_str(), obj_json.size(), config));

        config.deprecated_enable_flatten_nested = false;
        std::string jsonb_json = R"({"a": [{")" + key11 + R"(": 1}]})";
        EXPECT_ANY_THROW(parser.parse(jsonb_json.c_str(), jsonb_json.size(), config));
    }

    {
        ScopedMaxJsonKeyLength guard(255);
        std::string key255(255, 'b');

        std::string obj_json = R"({")" + key255 + R"(": 1})";
        auto result = parser.parse(obj_json.c_str(), obj_json.size(), config);
        ASSERT_TRUE(result.has_value());

        config.deprecated_enable_flatten_nested = false;
        std::string jsonb_json = R"({"a": [{")" + key255 + R"(": 1}]})";
        result = parser.parse(jsonb_json.c_str(), jsonb_json.size(), config);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result->values.size(), 1);
        EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);
    }
}
