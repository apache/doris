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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <limits>

#include "gtest/gtest_pred_impl.h"
#include "util/mysql_global.h"
#include "util/to_string.h"

namespace doris {

class NumbersTest : public testing::Test {};

TEST_F(NumbersTest, test_float_to_buffer) {
    char buffer1[100];
    char buffer2[100];
    float v1 = 0;
    int len1 = to_buffer(v1, MAX_FLOAT_STR_LENGTH, buffer1);
    int len2 = fast_to_buffer(v1, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    float v2 = 0.00;
    len1 = to_buffer(v2, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v2, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    float v3 = 20001230;
    len1 = to_buffer(v3, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v3, buffer2);
    EXPECT_EQ(std::string("20001230"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("20001230"), std::string(buffer2, len2));

    float v4 = static_cast<float>(200012303131);
    len1 = to_buffer(v4, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v4, buffer2);
    EXPECT_EQ(std::string("2.000123e+11"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("200012300000"), std::string(buffer2, len2));

    float v5 = -3167.3131;
    len1 = to_buffer(v5, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v5, buffer2);
    EXPECT_EQ(std::string("-3167.313"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3167.313"), std::string(buffer2, len2));

    float v6 = std::numeric_limits<float>::max();
    len1 = to_buffer(v6, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v6, buffer2);
    EXPECT_EQ(std::string("3.4028235e+38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("3.4028235e+38"), std::string(buffer2, len2));

    float v7 = std::numeric_limits<float>::min();
    len1 = to_buffer(v7, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v7, buffer2);
    EXPECT_EQ(std::string("1.1754944e-38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("1.1754944e-38"), std::string(buffer2, len2));

    float v8 = 0 - std::numeric_limits<float>::max();
    len1 = to_buffer(v8, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v8, buffer2);
    EXPECT_EQ(std::string("-3.4028235e+38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3.4028235e+38"), std::string(buffer2, len2));
}

TEST_F(NumbersTest, test_double_to_buffer) {
    char buffer1[100];
    char buffer2[100];
    double v1 = 0;
    int len1 = to_buffer(v1, MAX_DOUBLE_STR_LENGTH, buffer1);
    int len2 = fast_to_buffer(v1, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    double v2 = 0.00;
    len1 = to_buffer(v2, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v2, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    double v3 = 20001230;
    len1 = to_buffer(v3, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v3, buffer2);
    EXPECT_EQ(std::string("20001230"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("20001230"), std::string(buffer2, len2));

    double v4 = 200012303131;
    len1 = to_buffer(v4, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v4, buffer2);
    EXPECT_EQ(std::string("200012303131"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("200012303131"), std::string(buffer2, len2));

    double v5 = -3167.3131;
    len1 = to_buffer(v5, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v5, buffer2);
    EXPECT_EQ(std::string("-3167.3131"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3167.3131"), std::string(buffer2, len2));

    double v6 = std::numeric_limits<double>::max();
    len1 = to_buffer(v6, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v6, buffer2);
    EXPECT_EQ(std::string("1.7976931348623157e+308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("1.7976931348623157e+308"), std::string(buffer2, len2));

    double v7 = std::numeric_limits<double>::min();
    len1 = to_buffer(v7, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v7, buffer2);
    EXPECT_EQ(std::string("2.2250738585072014e-308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("2.2250738585072014e-308"), std::string(buffer2, len2));

    double v8 = 0 - std::numeric_limits<double>::max();
    len1 = to_buffer(v8, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = fast_to_buffer(v8, buffer2);
    EXPECT_EQ(std::string("-1.7976931348623157e+308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-1.7976931348623157e+308"), std::string(buffer2, len2));
}

/*
TEST_F(NumbersTest, test_float_to_buffer2) {
    std::vector<std::pair<float, std::string>> input_values = {
            {123.456F, "123.456001"},
            {123.456789F, "123.456787"},
            {123.456789123F, "123.456787"},
            {123456.123456789F, "123456.125"},
            {123456789.12345F, "123456792"},
            {1234567890.12345F, "1.23456794e+09"},
            {0.123456789F, "0.123456791"},
            {0.000123456F, "0.000123456004"},
            {0.0000123456F, "1.23456002e-05"},
            {0.0000123456789F, "1.23456794e-05"},

            {1234567890123456.12345F, "1.23456795e+15"},
            {12345678901234567.12345F, "1.23456784e+16"},

            {0.0, "0"},
            {-0.0, "-0"},
            {std::numeric_limits<float>::infinity(), "inf"},
            {-std::numeric_limits<float>::infinity(), "-inf"},
            {std::numeric_limits<float>::quiet_NaN(), "nan"}};
    for (const auto& value : input_values) {
        std::string str;
        str.resize(64);
        auto len = fast_to_buffer(value.first, str.data());
        str.resize(len);
        EXPECT_EQ(str, value.second);
    }
}
TEST_F(NumbersTest, test_double_to_buffer2) {
    std::vector<std::pair<double, std::string>> input_values = {
            {123.456, "123.456"},
            {123.456789, "123.456789"},
            {123.456789123, "123.45678912299999"},
            {123456.123456789, "123456.12345678901"},
            {123456789.12345, "123456789.12345"},

            {1234567890123456.12345, "1234567890123456"},
            {12345678901234567.12345, "12345678901234568"},
            {123456789012345678.12345, "1.2345678901234568e+17"},
            {1234567890123456789.12345, "1.2345678901234568e+18"},
            {0.123456789, "0.123456789"},
            {0.000123456, "0.00012345600000000001"},
            {0.0000123456, "1.2345599999999999e-05"},

            {0.0, "0"},
            {-0.0, "-0"},
            {std::numeric_limits<float>::infinity(), "inf"},
            {-std::numeric_limits<float>::infinity(), "-inf"},
            {std::numeric_limits<float>::quiet_NaN(), "nan"}};
    for (const auto& value : input_values) {
        std::string str;
        str.resize(64);
        auto len = fast_to_buffer(value.first, str.data());
        str.resize(len);
        EXPECT_EQ(str, value.second);
    }
}
*/
} // namespace doris
