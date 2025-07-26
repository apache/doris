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
    EXPECT_EQ(parse_result_bool.values[0].get_type(), doris::PrimitiveType::TYPE_BIGINT);

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
    auto& array_field = parse_result_array_2.values[0].get<doris::vectorized::Array>();

    EXPECT_EQ(array_field.size(), 6);
    EXPECT_EQ(array_field[0].get_type(), doris::PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(array_field[1].get_type(), doris::PrimitiveType::TYPE_STRING);
    EXPECT_EQ(array_field[2].get_type(), doris::PrimitiveType::TYPE_NULL);
    EXPECT_EQ(array_field[3].get_type(), doris::PrimitiveType::TYPE_BIGINT);
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
    EXPECT_EQ(result->values[0].get<doris::vectorized::Int128>(), 18446744073709551615ULL);

    std::string array_json = R"({"a": [18446744073709551615]})";
    result = parser.parse(array_json.c_str(), array_json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->paths[0].get_path(), "a");
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& array_field = result->values[0].get<doris::vectorized::Array>();
    EXPECT_EQ(array_field.size(), 1);
    EXPECT_EQ(array_field[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(array_field[0].get<doris::vectorized::Int128>(), 18446744073709551615ULL);

    std::string nested_json = R"({"a": [{"b": 18446744073709551615}]})";
    config.enable_flatten_nested = true;
    result = parser.parse(nested_json.c_str(), nested_json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values.size(), 1);
    EXPECT_EQ(result->paths.size(), 1);
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_ARRAY);
    auto& array_field_2 = result->values[0].get<doris::vectorized::Array>();
    EXPECT_EQ(array_field_2.size(), 1);
    EXPECT_EQ(array_field_2[0].get_type(), doris::PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(array_field_2[0].get<doris::vectorized::Int128>(), 18446744073709551615ULL);
}