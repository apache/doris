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

#include "gutil/strings/numbers.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <limits>

#include "gtest/gtest_pred_impl.h"
#include "util/mysql_global.h"

namespace doris {

class NumbersTest : public testing::Test {};

TEST_F(NumbersTest, test_float_to_buffer) {
    char buffer1[100];
    char buffer2[100];
    float v1 = 0;
    int len1 = FloatToBuffer(v1, MAX_FLOAT_STR_LENGTH, buffer1);
    int len2 = FastFloatToBuffer(v1, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    float v2 = 0.00;
    len1 = FloatToBuffer(v2, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v2, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    float v3 = 20001230;
    len1 = FloatToBuffer(v3, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v3, buffer2);
    EXPECT_EQ(std::string("20001230"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("20001230"), std::string(buffer2, len2));

    float v4 = static_cast<float>(200012303131);
    len1 = FloatToBuffer(v4, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v4, buffer2);
    EXPECT_EQ(std::string("2.000123e+11"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("200012300000"), std::string(buffer2, len2));

    float v5 = -3167.3131;
    len1 = FloatToBuffer(v5, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v5, buffer2);
    EXPECT_EQ(std::string("-3167.313"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3167.313"), std::string(buffer2, len2));

    float v6 = std::numeric_limits<float>::max();
    len1 = FloatToBuffer(v6, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v6, buffer2);
    EXPECT_EQ(std::string("3.4028235e+38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("3.4028235e+38"), std::string(buffer2, len2));

    float v7 = std::numeric_limits<float>::min();
    len1 = FloatToBuffer(v7, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v7, buffer2);
    EXPECT_EQ(std::string("1.1754944e-38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("1.1754944e-38"), std::string(buffer2, len2));

    float v8 = 0 - std::numeric_limits<float>::max();
    len1 = FloatToBuffer(v8, MAX_FLOAT_STR_LENGTH, buffer1);
    len2 = FastFloatToBuffer(v8, buffer2);
    EXPECT_EQ(std::string("-3.4028235e+38"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3.4028235e+38"), std::string(buffer2, len2));
}

TEST_F(NumbersTest, test_double_to_buffer) {
    char buffer1[100];
    char buffer2[100];
    double v1 = 0;
    int len1 = DoubleToBuffer(v1, MAX_DOUBLE_STR_LENGTH, buffer1);
    int len2 = FastDoubleToBuffer(v1, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    double v2 = 0.00;
    len1 = DoubleToBuffer(v2, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v2, buffer2);
    EXPECT_EQ(std::string("0"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("0"), std::string(buffer2, len2));

    double v3 = 20001230;
    len1 = DoubleToBuffer(v3, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v3, buffer2);
    EXPECT_EQ(std::string("20001230"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("20001230"), std::string(buffer2, len2));

    double v4 = 200012303131;
    len1 = DoubleToBuffer(v4, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v4, buffer2);
    EXPECT_EQ(std::string("200012303131"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("200012303131"), std::string(buffer2, len2));

    double v5 = -3167.3131;
    len1 = DoubleToBuffer(v5, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v5, buffer2);
    EXPECT_EQ(std::string("-3167.3131"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-3167.3131"), std::string(buffer2, len2));

    double v6 = std::numeric_limits<double>::max();
    len1 = DoubleToBuffer(v6, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v6, buffer2);
    EXPECT_EQ(std::string("1.7976931348623157e+308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("1.7976931348623157e+308"), std::string(buffer2, len2));

    double v7 = std::numeric_limits<double>::min();
    len1 = DoubleToBuffer(v7, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v7, buffer2);
    EXPECT_EQ(std::string("2.2250738585072014e-308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("2.2250738585072014e-308"), std::string(buffer2, len2));

    double v8 = 0 - std::numeric_limits<double>::max();
    len1 = DoubleToBuffer(v8, MAX_DOUBLE_STR_LENGTH, buffer1);
    len2 = FastDoubleToBuffer(v8, buffer2);
    EXPECT_EQ(std::string("-1.7976931348623157e+308"), std::string(buffer1, len1));
    EXPECT_EQ(std::string("-1.7976931348623157e+308"), std::string(buffer2, len2));
}

} // namespace doris
