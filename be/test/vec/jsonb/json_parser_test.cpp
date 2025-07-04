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
    std::string json = R"({"nested": [{"a": [1,2,3]}]})";
    // result should be jsonbField
    result = parser.parse(json.c_str(), json.size(), config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->values[0].get_type(), doris::PrimitiveType::TYPE_JSONB);

    config.enable_flatten_nested = true;
    EXPECT_ANY_THROW(parser.parse(json.c_str(), json.size(), config));
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
