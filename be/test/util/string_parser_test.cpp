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

#include "util/string_parser.hpp"

#include <gtest/gtest.h>

#include <boost/lexical_cast.hpp>
#include <cstdint>
#include <cstdio>
#include <string>

#include "util/logging.h"

namespace doris {

std::string space[] = {"", "   ", "\t\t\t", "\n\n\n", "\v\v\v", "\f\f\f", "\r\r\r"};
const int space_len = 7;

// Tests conversion of s to integer with and without leading/trailing whitespace
template <typename T>
void test_int_value(const char* s, T exp_val, StringParser::ParseResult exp_result) {
    for (int i = 0; i < space_len; ++i) {
        for (int j = 0; j < space_len; ++j) {
            // All combinations of leading and/or trailing whitespace.
            std::string str = space[i] + s + space[j];
            StringParser::ParseResult result;
            T val = StringParser::string_to_int<T>(str.data(), str.length(), &result);
            EXPECT_EQ(exp_val, val) << str;
            EXPECT_EQ(result, exp_result);
        }
    }
}

// Tests conversion of s to integer with and without leading/trailing whitespace
template <typename T>
void test_unsigned_int_value(const char* s, T exp_val, StringParser::ParseResult exp_result) {
    for (int i = 0; i < space_len; ++i) {
        for (int j = 0; j < space_len; ++j) {
            // All combinations of leading and/or trailing whitespace.
            std::string str = space[i] + s + space[j];
            StringParser::ParseResult result;
            T val = StringParser::string_to_unsigned_int<T>(str.data(), str.length(), &result);
            EXPECT_EQ(exp_val, val) << str;
            EXPECT_EQ(result, exp_result);
        }
    }
}

// Tests conversion of s, given a base, to an integer with and without leading/trailing whitespace
template <typename T>
void test_int_value(const char* s, int base, T exp_val, StringParser::ParseResult exp_result) {
    for (int i = 0; i < space_len; ++i) {
        for (int j = 0; j < space_len; ++j) {
            // All combinations of leading and/or trailing whitespace.
            std::string str = space[i] + s + space[j];
            StringParser::ParseResult result;
            T val = StringParser::string_to_int<T>(str.data(), str.length(), base, &result);
            EXPECT_EQ(exp_val, val) << str;
            EXPECT_EQ(result, exp_result);
        }
    }
}

void test_bool_value(const char* s, bool exp_val, StringParser::ParseResult exp_result) {
    for (int i = 0; i < space_len; ++i) {
        for (int j = 0; j < space_len; ++j) {
            // All combinations of leading and/or trailing whitespace.
            std::string str = space[i] + s + space[j];
            StringParser::ParseResult result;
            bool val = StringParser::string_to_bool(str.data(), str.length(), &result);
            EXPECT_EQ(exp_val, val) << s;
            EXPECT_EQ(result, exp_result);
        }
    }
}

// Compare Impala's float conversion function against strtod.
template <typename T>
void test_float_value(const std::string& s, StringParser::ParseResult exp_result) {
    StringParser::ParseResult result;
    T val = StringParser::string_to_float<T>(s.data(), s.length(), &result);
    EXPECT_EQ(exp_result, result);

    if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
        T exp_val = strtod(s.c_str(), nullptr);
        EXPECT_EQ(exp_val, val);
    }
}

template <typename T>
void test_float_value_is_nan(const std::string& s, StringParser::ParseResult exp_result) {
    StringParser::ParseResult result;
    T val = StringParser::string_to_float<T>(s.data(), s.length(), &result);
    EXPECT_EQ(exp_result, result);

    if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
        EXPECT_TRUE(std::isnan(val));
    }
}

// Tests conversion of s to double and float with +/- prefixing (and no prefix) and with
// and without leading/trailing whitespace
void test_all_float_variants(const std::string& s, StringParser::ParseResult exp_result) {
    std::string sign[] = {"", "+", "-"};
    for (int i = 0; i < space_len; ++i) {
        for (int j = 0; j < space_len; ++j) {
            for (int k = 0; k < 3; ++k) {
                // All combinations of leading and/or trailing whitespace and +/- sign.
                std::string str = space[i] + sign[k] + s + space[j];
                test_float_value<float>(str, exp_result);
                test_float_value<double>(str, exp_result);
            }
        }
    }
}

template <typename T>
void TestFloatBruteForce() {
    T min_val = std::numeric_limits<T>::min();
    T max_val = std::numeric_limits<T>::max();

    // Keep multiplying by 2.
    T cur_val = 1.0;
    while (cur_val < max_val) {
        std::string s = boost::lexical_cast<std::string>(cur_val);
        test_float_value<T>(s, StringParser::PARSE_SUCCESS);
        cur_val *= 2;
    }

    // Keep dividing by 2.
    cur_val = 1.0;
    while (cur_val > min_val) {
        std::string s = boost::lexical_cast<std::string>(cur_val);
        test_float_value<T>(s, StringParser::PARSE_SUCCESS);
        cur_val /= 2;
    }
}

class StringParserTest : public testing::Test {
public:
    StringParserTest() {}
    ~StringParserTest() {}

protected:
    virtual void SetUp() { init(); }
    virtual void TearDown() {}

    void init();

private:
}; // end class StringParserTest

TEST(StringToInt, Basic) {
    test_int_value<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("123", 123, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("12345", 12345, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("12345678", 12345678, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("12345678901234", 12345678901234, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("-10", -10, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-10", -10, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("-10", -10, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-10", -10, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("+1", 1, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("+1", 1, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("+1", 1, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("+1", 1, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("+0", 0, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-0", 0, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("+0", 0, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-0", 0, StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, InvalidLeadingTrailing) {
    // Test that trailing garbage is not allowed.
    test_int_value<int8_t>("123xyz   ", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("-123xyz   ", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   123xyz   ", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   -12  3xyz ", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("12 3", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("-12 3", 0, StringParser::PARSE_FAILURE);

    // Must have at least one leading valid digit.
    test_int_value<int8_t>("x123", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   x123", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   -x123", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   x-123", 0, StringParser::PARSE_FAILURE);

    // Test empty string and string with only whitespaces.
    test_int_value<int8_t>("", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToInt, Limit) {
    test_int_value<int8_t>("127", 127, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("-128", -128, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("32767", 32767, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-32768", -32768, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("2147483647", 2147483647, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("-2147483648", -2147483648, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("9223372036854775807", std::numeric_limits<int64_t>::max(),
                            StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-9223372036854775808", std::numeric_limits<int64_t>::min(),
                            StringParser::PARSE_SUCCESS);
}

TEST(StringToUnsignedInt, Basic) {
    test_unsigned_int_value<uint8_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint16_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint32_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint64_t>("123", 123, StringParser::PARSE_SUCCESS);

    test_unsigned_int_value<uint8_t>("123", 123, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint16_t>("12345", 12345, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint32_t>("12345678", 12345678, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint64_t>("12345678901234", 12345678901234,
                                      StringParser::PARSE_SUCCESS);

    test_unsigned_int_value<uint8_t>("-10", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint16_t>("-10", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint32_t>("-10", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint64_t>("-10", 0, StringParser::PARSE_FAILURE);

    test_unsigned_int_value<uint8_t>("+1", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint16_t>("+1", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint32_t>("+1", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint64_t>("+1", 0, StringParser::PARSE_FAILURE);

    test_unsigned_int_value<uint8_t>("+0", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint16_t>("-0", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint32_t>("+0", 0, StringParser::PARSE_FAILURE);
    test_unsigned_int_value<uint64_t>("-0", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToUnsignedInt, Limit) {
    test_unsigned_int_value<uint8_t>("255", 255, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint16_t>("65535", 65535, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint32_t>("4294967295", 4294967295, StringParser::PARSE_SUCCESS);
    test_unsigned_int_value<uint64_t>("18446744073709551615", std::numeric_limits<uint64_t>::max(),
                                      StringParser::PARSE_SUCCESS);
}

TEST(StringToUnsignedInt, Overflow) {
    test_unsigned_int_value<uint8_t>("256", 255, StringParser::PARSE_OVERFLOW);
    test_unsigned_int_value<uint16_t>("65536", 65535, StringParser::PARSE_OVERFLOW);
    test_unsigned_int_value<uint32_t>("4294967296", 4294967295, StringParser::PARSE_OVERFLOW);
    test_unsigned_int_value<uint64_t>("18446744073709551616", std::numeric_limits<uint64_t>::max(),
                                      StringParser::PARSE_OVERFLOW);
}

TEST(StringToInt, Overflow) {
    test_int_value<int8_t>("128", 127, StringParser::PARSE_OVERFLOW);
    test_int_value<int8_t>("-129", -128, StringParser::PARSE_OVERFLOW);
    test_int_value<int16_t>("32768", 32767, StringParser::PARSE_OVERFLOW);
    test_int_value<int16_t>("-32769", -32768, StringParser::PARSE_OVERFLOW);
    test_int_value<int32_t>("2147483648", 2147483647, StringParser::PARSE_OVERFLOW);
    test_int_value<int32_t>("-2147483649", -2147483648, StringParser::PARSE_OVERFLOW);
    test_int_value<int64_t>("9223372036854775808", 9223372036854775807LL,
                            StringParser::PARSE_OVERFLOW);
    test_int_value<int64_t>("-9223372036854775809", std::numeric_limits<int64_t>::min(),
                            StringParser::PARSE_OVERFLOW);
}

TEST(StringToInt, Int8_Exhaustive) {
    char buffer[5];
    for (int i = -256; i <= 256; ++i) {
        snprintf(buffer, 5, "%d", i);
        int8_t expected = i;
        if (i > 127) {
            expected = 127;
        } else if (i < -128) {
            expected = -128;
        }
        test_int_value<int8_t>(
                buffer, expected,
                i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
    }
}

TEST(StringToIntWithBase, Basic) {
    test_int_value<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("123", 10, 123, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("12345", 10, 12345, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("12345678", 10, 12345678, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("12345678901234", 10, 12345678901234, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);

    test_int_value<int8_t>("a", 16, 10, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("A", 16, 10, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("b", 20, 11, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("B", 20, 11, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("z", 36, 35, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("f0a", 16, 3850, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("7", 8, 7, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("10", 2, 2, StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, NonNumericCharacters) {
    // Alphanumeric digits that are not in base are ok
    test_int_value<int8_t>("123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("-123abc   ", 10, -123, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("   123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("a123", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("   a123", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("   -a123", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);

    // Trailing white space + digits is not ok
    test_int_value<int8_t>("   -12  3xyz ", 10, 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("12 3", 10, 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("-12 3", 10, 0, StringParser::PARSE_FAILURE);

    // Must have at least one leading valid digit.
    test_int_value<int8_t>("!123", 0, StringParser::PARSE_FAILURE);

    // Test empty string and string with only whitespaces.
    test_int_value<int8_t>("", 0, StringParser::PARSE_FAILURE);
    test_int_value<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToIntWithBase, Limit) {
    test_int_value<int8_t>("127", 10, 127, StringParser::PARSE_SUCCESS);
    test_int_value<int8_t>("-128", 10, -128, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("32767", 10, 32767, StringParser::PARSE_SUCCESS);
    test_int_value<int16_t>("-32768", 10, -32768, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("2147483647", 10, 2147483647, StringParser::PARSE_SUCCESS);
    test_int_value<int32_t>("-2147483648", 10, -2147483648, StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("9223372036854775807", 10, std::numeric_limits<int64_t>::max(),
                            StringParser::PARSE_SUCCESS);
    test_int_value<int64_t>("-9223372036854775808", 10, std::numeric_limits<int64_t>::min(),
                            StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, Overflow) {
    test_int_value<int8_t>("128", 10, 127, StringParser::PARSE_OVERFLOW);
    test_int_value<int8_t>("-129", 10, -128, StringParser::PARSE_OVERFLOW);
    test_int_value<int16_t>("32768", 10, 32767, StringParser::PARSE_OVERFLOW);
    test_int_value<int16_t>("-32769", 10, -32768, StringParser::PARSE_OVERFLOW);
    test_int_value<int32_t>("2147483648", 10, 2147483647, StringParser::PARSE_OVERFLOW);
    test_int_value<int32_t>("-2147483649", 10, -2147483648, StringParser::PARSE_OVERFLOW);
    test_int_value<int64_t>("9223372036854775808", 10, 9223372036854775807LL,
                            StringParser::PARSE_OVERFLOW);
    test_int_value<int64_t>("-9223372036854775809", 10, std::numeric_limits<int64_t>::min(),
                            StringParser::PARSE_OVERFLOW);
}

TEST(StringToIntWithBase, Int8_Exhaustive) {
    char buffer[5];
    for (int i = -256; i <= 256; ++i) {
        snprintf(buffer, 5, "%d", i);
        int8_t expected = i;
        if (i > 127) {
            expected = 127;
        } else if (i < -128) {
            expected = -128;
        }
        test_int_value<int8_t>(
                buffer, 10, expected,
                i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
    }
}

TEST(StringToBool, Basic) {
    test_bool_value("true", true, StringParser::PARSE_SUCCESS);
    test_bool_value("false", false, StringParser::PARSE_SUCCESS);

    test_bool_value("false xdfsd", false, StringParser::PARSE_FAILURE);
    test_bool_value("true xdfsd", false, StringParser::PARSE_FAILURE);
    test_bool_value("ffffalse xdfsd", false, StringParser::PARSE_FAILURE);
    test_bool_value("tttfalse xdfsd", false, StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, Basic) {
    test_all_float_variants("0", StringParser::PARSE_SUCCESS);
    test_all_float_variants("123", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.456", StringParser::PARSE_SUCCESS);
    test_all_float_variants(".456", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.0", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.789", StringParser::PARSE_SUCCESS);

    // Scientific notation.
    test_all_float_variants("1e10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1E10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1e-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1E-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.456e10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.456E10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.456e-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.456E-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.789e10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.789E10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.789e-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("456.789E-10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.7e-294", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.7E-294", StringParser::PARSE_SUCCESS);

    // Min/max values.
    std::string float_min = boost::lexical_cast<std::string>(std::numeric_limits<float>::min());
    std::string float_max = boost::lexical_cast<std::string>(std::numeric_limits<float>::max());
    test_float_value<float>(float_min, StringParser::PARSE_SUCCESS);
    test_float_value<float>(float_max, StringParser::PARSE_SUCCESS);
    std::string double_min = boost::lexical_cast<std::string>(std::numeric_limits<double>::min());
    std::string double_max = boost::lexical_cast<std::string>(std::numeric_limits<double>::max());
    test_float_value<double>(double_min, StringParser::PARSE_SUCCESS);
    test_float_value<double>(double_max, StringParser::PARSE_SUCCESS);

    // Non-finite values
    test_all_float_variants("INFinity", StringParser::PARSE_SUCCESS);
    test_all_float_variants("infinity", StringParser::PARSE_SUCCESS);
    test_all_float_variants("inf", StringParser::PARSE_SUCCESS);

    test_float_value_is_nan<float>("nan", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<double>("nan", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<float>("NaN", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<double>("NaN", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<float>("nana", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<double>("nana", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<float>("naN", StringParser::PARSE_SUCCESS);
    test_float_value_is_nan<double>("naN", StringParser::PARSE_SUCCESS);

    test_float_value_is_nan<float>("n aN", StringParser::PARSE_FAILURE);
    test_float_value_is_nan<float>("nnaN", StringParser::PARSE_FAILURE);

    // Overflow.
    test_float_value<float>(float_max + "11111", StringParser::PARSE_OVERFLOW);
    test_float_value<double>(double_max + "11111", StringParser::PARSE_OVERFLOW);
    test_float_value<float>("-" + float_max + "11111", StringParser::PARSE_OVERFLOW);
    test_float_value<double>("-" + double_max + "11111", StringParser::PARSE_OVERFLOW);

    // Precision limits
    // Regression test for IMPALA-1622 (make sure we get correct result with many digits
    // after decimal)
    test_all_float_variants("1.12345678912345678912", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.1234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.01234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.01111111111111111111111", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.1234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants(".1234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
    test_all_float_variants("12345678901234567890.1234567890123456789012",
                            StringParser::PARSE_SUCCESS);
    test_all_float_variants("12345678901234567890.01234567890123456789012",
                            StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.000000000000000000001234", StringParser::PARSE_SUCCESS);
    test_all_float_variants("1.000000000000000000001234", StringParser::PARSE_SUCCESS);
    test_all_float_variants(".000000000000000000001234", StringParser::PARSE_SUCCESS);
    test_all_float_variants("0.000000000000000000001234e10", StringParser::PARSE_SUCCESS);
    test_all_float_variants("00000000000000000000.000000000000000000000",
                            StringParser::PARSE_SUCCESS);
    test_all_float_variants("00000000000000000000.000000000000000000001",
                            StringParser::PARSE_SUCCESS);
    test_all_float_variants("12345678901234567890123456", StringParser::PARSE_SUCCESS);
    test_all_float_variants("12345678901234567890123456e10", StringParser::PARSE_SUCCESS);

    // Invalid floats.
    test_all_float_variants("x456.789e10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456x.789e10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.x789e10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.789xe10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.789a10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.789ex10", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.789e10x", StringParser::PARSE_FAILURE);
    test_all_float_variants("456.789e10   sdfs ", StringParser::PARSE_FAILURE);
    test_all_float_variants("1e10   sdfs", StringParser::PARSE_FAILURE);
    test_all_float_variants("in", StringParser::PARSE_FAILURE);
    test_all_float_variants("in finity", StringParser::PARSE_FAILURE);
    test_all_float_variants("na", StringParser::PARSE_FAILURE);
    test_all_float_variants("ThisIsANaN", StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, InvalidLeadingTrailing) {
    // Test that trailing garbage is not allowed.
    test_float_value<double>("123xyz   ", StringParser::PARSE_FAILURE);
    test_float_value<double>("-123xyz   ", StringParser::PARSE_FAILURE);
    test_float_value<double>("   123xyz   ", StringParser::PARSE_FAILURE);
    test_float_value<double>("   -12  3xyz ", StringParser::PARSE_FAILURE);
    test_float_value<double>("12 3", StringParser::PARSE_FAILURE);
    test_float_value<double>("-12 3", StringParser::PARSE_FAILURE);

    // Must have at least one leading valid digit.
    test_float_value<double>("x123", StringParser::PARSE_FAILURE);
    test_float_value<double>("   x123", StringParser::PARSE_FAILURE);
    test_float_value<double>("   -x123", StringParser::PARSE_FAILURE);
    test_float_value<double>("   x-123", StringParser::PARSE_FAILURE);

    // Test empty string and string with only whitespaces.
    test_float_value<double>("", StringParser::PARSE_FAILURE);
    test_float_value<double>("   ", StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, BruteForce) {
    TestFloatBruteForce<float>();
    TestFloatBruteForce<double>();
}

} // end namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
