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

#include "util/jsonb_parser_simd.h"

#include "common/status.h"
#include "gtest/gtest.h"
#include "util/jsonb_error.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {
class JsonbParserTest : public testing::Test {};

static JsonbErrType parse_json_and_check(std::string_view json_str, std::string_view expected_str) {
    JsonbParser parser;
    if (!parser.parse(json_str.data(), json_str.length())) {
        return parser.getErrorCode();
    }
    const char* ptr = parser.getWriter().getOutput()->getBuffer();
    size_t len = (unsigned)parser.getWriter().getOutput()->getSize();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(ptr, len), expected_str);
    return JsonbErrType::E_NONE;
}

TEST_F(JsonbParserTest, ParseSimpleJson) {
    std::string_view simple_json = R"({"key":"value"})";
    std::string_view expected_simple_json = R"({"key":"value"})";
    EXPECT_EQ(parse_json_and_check(simple_json, expected_simple_json), JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInt8) {
    std::string_view json_with_int8 = R"({"min_int8":-128,"max_int8":127})";
    std::string_view expected_json_with_int8 = R"({"min_int8":-128,"max_int8":127})";
    EXPECT_EQ(parse_json_and_check(json_with_int8, expected_json_with_int8), JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInt16) {
    std::string_view json_with_int16 = R"({"min_int16":-32768,"max_int16":32767})";
    std::string_view expected_json_with_int16 = R"({"min_int16":-32768,"max_int16":32767})";
    EXPECT_EQ(parse_json_and_check(json_with_int16, expected_json_with_int16),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInt32) {
    std::string_view json_with_int32 = R"({"min_int32":-2147483648,"max_int32":2147483647})";
    std::string_view expected_json_with_int32 =
            R"({"min_int32":-2147483648,"max_int32":2147483647})";
    EXPECT_EQ(parse_json_and_check(json_with_int32, expected_json_with_int32),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInt64) {
    std::string_view json_with_int64 =
            R"({"min_int64":-9223372036854775808,"max_int64":9223372036854775807})";
    std::string_view expected_json_with_int64 =
            R"({"min_int64":-9223372036854775808,"max_int64":9223372036854775807})";
    EXPECT_EQ(parse_json_and_check(json_with_int64, expected_json_with_int64),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithFloat) {
    std::string_view json_with_float =
            R"({"min_float":-3.40282e+38,"max_float":3.40282e+38,"small_float":1.17549e-38})";
    std::string_view expected_json_with_float =
            R"({"min_float":-3.40282e+38,"max_float":3.40282e+38,"small_float":1.17549e-38})";
    EXPECT_EQ(parse_json_and_check(json_with_float, expected_json_with_float),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithDouble) {
    std::string_view json_with_double =
            R"({"min_double":-1.79769e+308,"max_double":1.79769e+308,"small_double":2.22507e-308})";
    std::string_view expected_json_with_double =
            R"({"min_double":-1.79769e+308,"max_double":1.79769e+308,"small_double":2.22507e-308})";
    EXPECT_EQ(parse_json_and_check(json_with_double, expected_json_with_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithScientificNotation) {
    std::string_view json_with_scientific_notation =
            R"({"small_positive":1.23e-10,"large_positive":1.23e+10,"small_negative":-1.23e-10,"large_negative":-1.23e+10})";
    std::string_view expected_json_with_scientific_notation =
            R"({"small_positive":1.23e-10,"large_positive":12300000000,"small_negative":-1.23e-10,"large_negative":-12300000000})";
    EXPECT_EQ(parse_json_and_check(json_with_scientific_notation,
                                   expected_json_with_scientific_notation),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMixedIntegers) {
    std::string_view json_with_mixed_integers =
            R"({"int8":42,"int16":32767,"int32":2147483647,"int64":9223372036854775807})";
    std::string_view expected_json_with_mixed_integers =
            R"({"int8":42,"int16":32767,"int32":2147483647,"int64":9223372036854775807})";
    EXPECT_EQ(parse_json_and_check(json_with_mixed_integers, expected_json_with_mixed_integers),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMixedFloats) {
    std::string_view json_with_mixed_floats =
            R"({"float_value":123.456,"double_value":123456.789012,"scientific_value":1.23e+10})";
    std::string_view expected_json_with_mixed_floats =
            R"({"float_value":123.456,"double_value":123456.789012,"scientific_value":12300000000})";
    EXPECT_EQ(parse_json_and_check(json_with_mixed_floats, expected_json_with_mixed_floats),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithSimpleArray) {
    std::string_view json_with_simple_array = R"({"array":[1,2,3,4,5]})";
    std::string_view expected_json_with_simple_array = R"({"array":[1,2,3,4,5]})";
    EXPECT_EQ(parse_json_and_check(json_with_simple_array, expected_json_with_simple_array),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNestedArray) {
    std::string_view json_with_nested_array = R"({"nested_array":[[1,2],[3,4],[5,6]]})";
    std::string_view expected_json_with_nested_array = R"({"nested_array":[[1,2],[3,4],[5,6]]})";
    EXPECT_EQ(parse_json_and_check(json_with_nested_array, expected_json_with_nested_array),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMixedArray) {
    std::string_view json_with_mixed_array = R"({"mixed_array":[1,"two",3.1,true,null]})";
    std::string_view expected_json_with_mixed_array = R"({"mixed_array":[1,"two",3.1,true,null]})";
    EXPECT_EQ(parse_json_and_check(json_with_mixed_array, expected_json_with_mixed_array),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithSimpleObject) {
    std::string_view json_with_simple_object = R"({"object":{"key1":"value1","key2":"value2"}})";
    std::string_view expected_json_with_simple_object =
            R"({"object":{"key1":"value1","key2":"value2"}})";
    EXPECT_EQ(parse_json_and_check(json_with_simple_object, expected_json_with_simple_object),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNestedObject) {
    std::string_view json_with_nested_object =
            R"({"nested_object":{"level1":{"level2":{"level3":"value"}}}})";
    std::string_view expected_json_with_nested_object =
            R"({"nested_object":{"level1":{"level2":{"level3":"value"}}}})";
    EXPECT_EQ(parse_json_and_check(json_with_nested_object, expected_json_with_nested_object),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMixedObject) {
    std::string_view json_with_mixed_object =
            R"({"mixed_object":{"int_key":42,"string_key":"value","bool_key":true,"null_key":null}})";
    std::string_view expected_json_with_mixed_object =
            R"({"mixed_object":{"int_key":42,"string_key":"value","bool_key":true,"null_key":null}})";
    EXPECT_EQ(parse_json_and_check(json_with_mixed_object, expected_json_with_mixed_object),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithArrayOfObjects) {
    std::string_view json_with_array_of_objects =
            R"({"array_of_objects":[{"key1":"value1"},{"key2":"value2"}]})";
    std::string_view expected_json_with_array_of_objects =
            R"({"array_of_objects":[{"key1":"value1"},{"key2":"value2"}]})";
    EXPECT_EQ(parse_json_and_check(json_with_array_of_objects, expected_json_with_array_of_objects),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithObjectOfArrays) {
    std::string_view json_with_object_of_arrays =
            R"({"object_of_arrays":{"array1":[1,2,3],"array2":["a","b","c"]}})";
    std::string_view expected_json_with_object_of_arrays =
            R"({"object_of_arrays":{"array1":[1,2,3],"array2":["a","b","c"]}})";
    EXPECT_EQ(parse_json_and_check(json_with_object_of_arrays, expected_json_with_object_of_arrays),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithComplexNestedStructure) {
    std::string_view json_with_complex_nested_structure =
            R"({"complex":{"array":[1,{"key":"value"},[3,4]],"object":{"nested_array":[1,2,3],"nested_object":{"key":"value"}}}})";
    std::string_view expected_json_with_complex_nested_structure =
            R"({"complex":{"array":[1,{"key":"value"},[3,4]],"object":{"nested_array":[1,2,3],"nested_object":{"key":"value"}}}})";
    EXPECT_EQ(parse_json_and_check(json_with_complex_nested_structure,
                                   expected_json_with_complex_nested_structure),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithString) {
    std::string_view json_with_string = R"({"string":"hello world"})";
    std::string_view expected_json_with_string = R"({"string":"hello world"})";
    EXPECT_EQ(parse_json_and_check(json_with_string, expected_json_with_string),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithSpecialCharactersInString) {
    std::string_view json_with_special_chars =
            R"({"special_chars":"!@#$%^&*()_+-=[]{}|;':,./<>?"})";
    std::string_view expected_json_with_special_chars =
            R"({"special_chars":"!@#$%^&*()_+-=[]{}|;':,./<>?"})";
    EXPECT_EQ(parse_json_and_check(json_with_special_chars, expected_json_with_special_chars),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithUnicodeInString) {
    std::string_view json_with_unicode = R"({"unicode":"你好，世界"})";
    std::string_view expected_json_with_unicode = R"({"unicode":"你好，世界"})";
    EXPECT_EQ(parse_json_and_check(json_with_unicode, expected_json_with_unicode),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithEscapedCharactersInString) {
    std::string_view json_with_escaped_chars = R"({"escaped_chars":"\"\/\b\f\n\r\t"})";
    std::string_view expected_json_with_escaped_chars = R"({"escaped_chars":"\"/\b\f\n\r\t"})";
    EXPECT_EQ(parse_json_and_check(json_with_escaped_chars, expected_json_with_escaped_chars),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithLongInt) {
    std::string_view json_with_long_int = R"({"long_int":19389892839283982938923})";
    EXPECT_EQ(parse_json_and_check(json_with_long_int, json_with_long_int),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseInvalidJsonFormat) {
    std::string_view invalid_json = R"({"key": "value")";
    EXPECT_NE(parse_json_and_check(invalid_json, invalid_json), JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidKeyType) {
    std::string_view json_with_invalid_key = R"({123: "value"})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_key, json_with_invalid_key),
              JsonbErrType::E_INVALID_KEY_STRING);
}

TEST_F(JsonbParserTest, ParseEmptyJson) {
    std::string_view empty_json = "";
    EXPECT_EQ(parse_json_and_check(empty_json, empty_json), JsonbErrType::E_EMPTY_DOCUMENT);
}

TEST_F(JsonbParserTest, ParseJsonWithDeepNesting) {
    std::string_view json_with_deep_nesting =
            R"({"level1":{"level2":{"level3":{"level4":{"level5":{"level6":"value"}}}}}})";
    EXPECT_EQ(parse_json_and_check(json_with_deep_nesting, json_with_deep_nesting),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidNumberFormat) {
    std::string_view json_with_invalid_number = R"({"invalid_number": 1.23e})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_number, json_with_invalid_number),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidBoolean) {
    std::string_view json_with_invalid_boolean = R"({"invalid_bool": True})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_boolean, json_with_invalid_boolean),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidEscapeCharacter) {
    std::string_view json_with_invalid_escape = R"({"invalid_escape": "hello\xworld"})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_escape, json_with_invalid_escape),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidArrayFormat) {
    std::string_view json_with_invalid_array = R"({"array": [1 2 3]})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_array, json_with_invalid_array),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidObjectFormat) {
    std::string_view json_with_invalid_object = R"({"key1" "value1", "key2": "value2"})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_object, json_with_invalid_object),
              JsonbErrType::E_INVALID_KEY_STRING);
}

TEST_F(JsonbParserTest, ParseJsonWithUnsupportedSpecialValue) {
    std::string_view json_with_unsupported_value = R"({"unsupported_value": NaN})";
    EXPECT_EQ(parse_json_and_check(json_with_unsupported_value, json_with_unsupported_value),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithLongString) {
    std::string long_string(1000000, 'a');
    std::string json_with_long_string = R"({"long_string":")" + long_string + R"("})";
    EXPECT_EQ(parse_json_and_check(json_with_long_string, json_with_long_string),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithInvalidUnicode) {
    std::string_view json_with_invalid_unicode = R"({"invalid_unicode": "\uZZZZ"})";
    EXPECT_EQ(parse_json_and_check(json_with_invalid_unicode, json_with_invalid_unicode),
              JsonbErrType::E_EXCEPTION);
}

TEST_F(JsonbParserTest, ParseJsonWithNestedKey) {
    std::string_view json_with_nested_key = R"({"nested": {"": "value"}})";
    std::string_view excepted_json_with_nested_key = R"({"nested":{"":"value"}})";
    EXPECT_EQ(parse_json_and_check(json_with_nested_key, excepted_json_with_nested_key),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNestedArrayElement) {
    std::string_view json_with_nested_array = R"({"array": [1, "two", true, null, {}]})";
    std::string_view excepted_json_with_nested_array = R"({"array":[1,"two",true,null,{}]})";
    EXPECT_EQ(parse_json_and_check(json_with_nested_array, excepted_json_with_nested_array),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNestedObjectValue) {
    std::string_view json_with_nested_object = R"({"nested": {"key": [1, 2, 3]}})";
    std::string_view excepted_json_with_nested_object = R"({"nested":{"key":[1,2,3]}})";
    EXPECT_EQ(parse_json_and_check(json_with_nested_object, excepted_json_with_nested_object),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithLongDouble) {
    std::string_view json_with_long_double = R"({"long_double": 3.1982938928398232132})";
    std::string_view excepted_json_with_long_double = R"({"long_double":3.19829389283982})";
    EXPECT_EQ(parse_json_and_check(json_with_long_double, excepted_json_with_long_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWith50DigitDouble) {
    std::string_view json_with_50_digit_double =
            R"({"double_value": 1.2345678901234567890123456789012345678901234567890})";
    std::string_view expected_json_with_50_digit_double = R"({"double_value":1.23456789012346})";
    EXPECT_EQ(parse_json_and_check(json_with_50_digit_double, expected_json_with_50_digit_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithScientificNotationDouble) {
    std::string_view json_with_scientific_double =
            R"({"scientific_double": 1.234567890123456789e+50})";
    std::string_view expected_json_with_scientific_double =
            R"({"scientific_double":1.23456789012346e+50})";
    EXPECT_EQ(
            parse_json_and_check(json_with_scientific_double, expected_json_with_scientific_double),
            JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMaxDouble) {
    std::string_view json_with_max_double = R"({"max_double": 1.7976931348623157e+308})";
    std::string_view expected_json_with_max_double = R"({"max_double":1.79769313486232e+308})";
    EXPECT_EQ(parse_json_and_check(json_with_max_double, expected_json_with_max_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithMinDouble) {
    std::string_view json_with_min_double = R"({"min_double": 2.2250738585072014e-308})";
    std::string_view expected_json_with_min_double = R"({"min_double":2.2250738585072e-308})";
    EXPECT_EQ(parse_json_and_check(json_with_min_double, expected_json_with_min_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNegativeDouble) {
    std::string_view json_with_negative_double =
            R"({"negative_double": -1.2345678901234567890123456789012345678901234567890})";
    std::string_view expected_json_with_negative_double =
            R"({"negative_double":-1.23456789012346})";
    EXPECT_EQ(parse_json_and_check(json_with_negative_double, expected_json_with_negative_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNegativeScientificDouble) {
    std::string_view json_with_negative_scientific_double =
            R"({"negative_scientific_double": -1.234567890123456789e-50})";
    std::string_view expected_json_with_negative_scientific_double =
            R"({"negative_scientific_double":-1.23456789012346e-50})";
    EXPECT_EQ(parse_json_and_check(json_with_negative_scientific_double,
                                   expected_json_with_negative_scientific_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithZeroDouble) {
    std::string_view json_with_zero_double = R"({"zero_double": 0.0})";
    std::string_view expected_json_with_zero_double = R"({"zero_double":0})";
    EXPECT_EQ(parse_json_and_check(json_with_zero_double, expected_json_with_zero_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithZeroScientificDouble) {
    std::string_view json_with_zero_scientific_double = R"({"zero_scientific_double": 0.0e+50})";
    std::string_view expected_json_with_zero_scientific_double = R"({"zero_scientific_double":0})";
    EXPECT_EQ(parse_json_and_check(json_with_zero_scientific_double,
                                   expected_json_with_zero_scientific_double),
              JsonbErrType::E_NONE);
}

TEST_F(JsonbParserTest, ParseJsonWithNearMaxDouble) {
    std::string_view json_with_near_max_double = R"({"near_max_double": 1.7976931348623156e+308})";
    std::string_view expected_json_with_near_max_double =
            R"({"near_max_double":1.79769313486232e+308})";
    EXPECT_EQ(parse_json_and_check(json_with_near_max_double, expected_json_with_near_max_double),
              JsonbErrType::E_NONE);
}
} // namespace doris