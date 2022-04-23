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

#include "runtime/decimalv2_value.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "util/logging.h"

namespace doris {

class DecimalV2ValueTest : public testing::Test {
public:
    DecimalV2ValueTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(DecimalV2ValueTest, string_to_decimal) {
    char buffer[100];
    DecimalV2Value value(std::string("1.23"));
    EXPECT_EQ("1.230", value.to_string(3));
    EXPECT_EQ("1.23", value.to_string());
    int len = value.to_buffer(buffer, 3);
    EXPECT_EQ("1.230", std::string(buffer, len));
    len = value.to_buffer(buffer, -1);
    EXPECT_EQ("1.23", std::string(buffer, len));

    DecimalV2Value value1(std::string("-0.23"));
    EXPECT_EQ("-0.230", value1.to_string(3));
    EXPECT_EQ("-0.23", value1.to_string());
    len = value1.to_buffer(buffer, 3);
    EXPECT_EQ("-0.230", std::string(buffer, len));
    len = value1.to_buffer(buffer, -1);
    EXPECT_EQ("-0.23", std::string(buffer, len));

    DecimalV2Value value2(std::string("1234567890123456789.0"));
    EXPECT_EQ("1234567890123456789.000", value2.to_string(3));
    EXPECT_EQ("1234567890123456789", value2.to_string());
    len = value2.to_buffer(buffer, 3);
    EXPECT_EQ("1234567890123456789.000", std::string(buffer, len));
    len = value2.to_buffer(buffer, -1);
    EXPECT_EQ("1234567890123456789", std::string(buffer, len));

    DecimalV2Value value3(std::string("0"));
    EXPECT_EQ("0.000", value3.to_string(3));
    EXPECT_EQ("0", value3.to_string());
    len = value3.to_buffer(buffer, 3);
    EXPECT_EQ("0.000", std::string(buffer, len));
    len = value3.to_buffer(buffer, -1);
    EXPECT_EQ("0", std::string(buffer, len));

    DecimalV2Value value4(std::string("22"));
    EXPECT_EQ("22.00", value4.to_string(2));
    EXPECT_EQ("22", value4.to_string());
    len = value4.to_buffer(buffer, 2);
    EXPECT_EQ("22.00", std::string(buffer, len));
    len = value4.to_buffer(buffer, -1);
    EXPECT_EQ("22", std::string(buffer, len));

    DecimalV2Value value5(std::string("-0"));
    EXPECT_EQ("0", value5.to_string());
    len = value5.to_buffer(buffer, -1);
    EXPECT_EQ("0", std::string(buffer, len));

    DecimalV2Value value6(std::string("999999999999999999.999999999"));
    EXPECT_EQ("999999999999999999.999", value6.to_string(3));
    EXPECT_EQ("999999999999999999.999999999", value6.to_string());
    len = value6.to_buffer(buffer, 3);
    EXPECT_EQ("999999999999999999.999", std::string(buffer, len));
    len = value6.to_buffer(buffer, -1);
    EXPECT_EQ("999999999999999999.999999999", std::string(buffer, len));

    DecimalV2Value value7(std::string("-999999999999999999.999999999"));
    EXPECT_EQ("-999999999999999999.999", value7.to_string(3));
    EXPECT_EQ("-999999999999999999.999999999", value7.to_string());
    len = value7.to_buffer(buffer, 3);
    EXPECT_EQ("-999999999999999999.999", std::string(buffer, len));
    len = value7.to_buffer(buffer, -1);
    EXPECT_EQ("-999999999999999999.999999999", std::string(buffer, len));

    DecimalV2Value value8(std::string("100.001"));
    EXPECT_EQ("100.00100", value8.to_string(5));
    EXPECT_EQ("100.001", value8.to_string());
    len = value8.to_buffer(buffer, 5);
    EXPECT_EQ("100.00100", std::string(buffer, len));
    len = value8.to_buffer(buffer, -1);
    EXPECT_EQ("100.001", std::string(buffer, len));
}

TEST_F(DecimalV2ValueTest, negative_zero) {
    DecimalV2Value value(std::string("-0.00"));
    {
        // positive zero VS negative zero
        DecimalV2Value value2(std::string("0.00"));
        EXPECT_TRUE(value == value2);
        EXPECT_FALSE(value < value2);
        EXPECT_FALSE(value < value2);
        EXPECT_TRUE(value <= value2);
        EXPECT_TRUE(value >= value2);
    }
    {
        // from string, positive
        DecimalV2Value value3(std::string("5.0"));
        EXPECT_TRUE(value < value3);
        EXPECT_TRUE(value <= value3);
        EXPECT_TRUE(value3 > value);
        EXPECT_TRUE(value3 >= value);
    }
    {
        // from string, negative
        DecimalV2Value value3(std::string("-5.0"));
        EXPECT_TRUE(value > value3);
        EXPECT_TRUE(value >= value3);
        EXPECT_TRUE(value3 < value);
        EXPECT_TRUE(value3 <= value);
    }
    {
        // from int
        DecimalV2Value value3(6);
        EXPECT_TRUE(value < value3);
        EXPECT_TRUE(value <= value3);
        EXPECT_TRUE(value3 > value);
        EXPECT_TRUE(value3 >= value);

        EXPECT_FALSE(!(value < value3));
        EXPECT_FALSE(!(value <= value3));
        EXPECT_FALSE(!(value3 > value));
        EXPECT_FALSE(!(value3 >= value));
    }
    {
        // from int
        DecimalV2Value value3(4, 0);
        EXPECT_TRUE(value < value3);
        EXPECT_TRUE(value <= value3);
        EXPECT_TRUE(value3 > value);
        EXPECT_TRUE(value3 >= value);
    }
    {
        // from int
        DecimalV2Value value3(3, -0);
        EXPECT_TRUE(value < value3);
        EXPECT_TRUE(value <= value3);
        EXPECT_TRUE(value3 > value);
        EXPECT_TRUE(value3 >= value);
    }
}

TEST_F(DecimalV2ValueTest, int_to_decimal) {
    DecimalV2Value value1(0);
    EXPECT_EQ("0.000", value1.to_string(3));

    DecimalV2Value value2(111111111, 0); // 9 digits
    EXPECT_EQ("111111111.000", value2.to_string(3));

    DecimalV2Value value3(111111111, 222222222); // 9 digits
    EXPECT_EQ("111111111.222", value3.to_string(3));

    DecimalV2Value value4(0, 222222222); // 9 digits
    EXPECT_EQ("0.222", value4.to_string(3));

    DecimalV2Value value5(111111111, 0); // 9 digits
    EXPECT_EQ("111111111.000", value5.to_string(3));

    DecimalV2Value value6(0, 0); // 9 digits
    EXPECT_EQ("0.000", value6.to_string(3));

    DecimalV2Value value7(0, 12345); // 9 digits
    EXPECT_EQ("0.000012", value7.to_string(6));

    DecimalV2Value value8(11, 0);
    EXPECT_EQ("11.000", value8.to_string(3));

    // more than 9digit, fraction will be truncated to 999999999
    DecimalV2Value value9(1230123456789, 1230123456789);
    EXPECT_EQ("1230123456789.999999999", value9.to_string(10));

    // negative
    {
        DecimalV2Value value2(-111111111, 0); // 9 digits
        EXPECT_EQ("-111111111.000", value2.to_string(3));

        DecimalV2Value value3(-111111111, 222222222); // 9 digits
        EXPECT_EQ("-111111111.222", value3.to_string(3));

        DecimalV2Value value4(0, -222222222); // 9 digits
        EXPECT_EQ("-0.222", value4.to_string(3));

        DecimalV2Value value5(-111111111, 0); // 9 digits
        EXPECT_EQ("-111111111.000", value5.to_string(3));

        DecimalV2Value value7(0, -12345); // 9 digits
        EXPECT_EQ("-0.000012", value7.to_string(6));

        DecimalV2Value value8(-11, 0);
        EXPECT_EQ("-11.000", value8.to_string(3));
    }
}

TEST_F(DecimalV2ValueTest, add) {
    DecimalV2Value value11(std::string("1111111111.222222222")); // 9 digits
    DecimalV2Value value12(std::string("2222222222.111111111")); // 9 digits
    DecimalV2Value add_result1 = value11 + value12;
    EXPECT_EQ("3333333333.333333333", add_result1.to_string(9));

    DecimalV2Value value21(std::string("-3333333333.222222222")); // 9 digits
    DecimalV2Value value22(std::string("2222222222.111111111"));  // 9 digits
    DecimalV2Value add_result2 = value21 + value22;
    EXPECT_EQ("-1111111111.111111111", add_result2.to_string(9));
}

TEST_F(DecimalV2ValueTest, compound_add) {
    {
        DecimalV2Value value1(std::string("111111111.222222222"));
        DecimalV2Value value2(std::string("111111111.222222222"));
        value1 += value2;
        EXPECT_EQ("222222222.444444444", value1.to_string(9));
    }
}

TEST_F(DecimalV2ValueTest, sub) {
    DecimalV2Value value11(std::string("3333333333.222222222")); // 9 digits
    DecimalV2Value value12(std::string("2222222222.111111111")); // 9 digits
    DecimalV2Value sub_result1 = value11 - value12;
    EXPECT_EQ("1111111111.111111111", sub_result1.to_string(9));

    DecimalV2Value value21(std::string("-2222222222.111111111")); // 9 digits
    DecimalV2Value sub_result2 = value11 - value21;
    EXPECT_EQ("5555555555.333333333", sub_result2.to_string(9));

    // small - big
    {
        DecimalV2Value value1(std::string("8.0"));
        DecimalV2Value value2(std::string("0"));
        DecimalV2Value sub_result = value2 - value1;
        DecimalV2Value expected_value(std::string("-8.0"));
        EXPECT_EQ(expected_value, sub_result);
        EXPECT_FALSE(sub_result.is_zero());
    }
    // minimum - maximal
    {
        DecimalV2Value value1(std::string("999999999999999999.999999999"));  // 27 digits
        DecimalV2Value value2(std::string("-999999999999999999.999999999")); // 27 digits
        DecimalV2Value sub_result = value2 - value1;
        EXPECT_STREQ("-1999999999999999999.999999998", sub_result.to_string().c_str());
        EXPECT_FALSE(sub_result.is_zero());
        EXPECT_TRUE(value1 > value2);
    }
}

TEST_F(DecimalV2ValueTest, mul) {
    DecimalV2Value value11(std::string("333333333.2222"));
    DecimalV2Value value12(std::string("-222222222.1111"));
    DecimalV2Value mul_result1 = value11 * value12;
    EXPECT_EQ(DecimalV2Value(std::string("-74074074012337037.04938642")), mul_result1);

    DecimalV2Value value21(std::string("0")); // zero
    DecimalV2Value mul_result2 = value11 * value21;
    EXPECT_EQ(DecimalV2Value(std::string("0")), mul_result2);
}

TEST_F(DecimalV2ValueTest, div) {
    DecimalV2Value value11(std::string("-74074074012337037.04938642"));
    DecimalV2Value value12(std::string("-222222222.1111"));
    DecimalV2Value div_result1 = value11 / value12;
    EXPECT_EQ(DecimalV2Value(std::string("333333333.2222")), div_result1);
    EXPECT_EQ("333333333.2222", div_result1.to_string());
    {
        DecimalV2Value value11(std::string("32766.999943536"));
        DecimalV2Value value12(std::string("604587"));
        DecimalV2Value div_result1 = value11 / value12;
        EXPECT_EQ(DecimalV2Value(std::string("0.054197328")), div_result1);
    }
}

TEST_F(DecimalV2ValueTest, unary_minus_operator) {
    {
        DecimalV2Value value1(std::string("111111111.222222222"));
        DecimalV2Value value2 = -value1;
        EXPECT_EQ("111111111.222222222", value1.to_string(10));
        EXPECT_EQ("-111111111.222222222", value2.to_string(10));
    }
}

TEST_F(DecimalV2ValueTest, to_int_frac_value) {
    // positive & negative
    {
        DecimalV2Value value(std::string("123456789123456789.987654321"));
        EXPECT_EQ(123456789123456789, value.int_value());
        EXPECT_EQ(987654321, value.frac_value());

        DecimalV2Value value2(std::string("-123456789123456789.987654321"));
        EXPECT_EQ(-123456789123456789, value2.int_value());
        EXPECT_EQ(-987654321, value2.frac_value());
    }
    // int or frac part is 0
    {
        DecimalV2Value value(std::string("-123456789123456789"));
        EXPECT_EQ(-123456789123456789, value.int_value());
        EXPECT_EQ(0, value.frac_value());

        DecimalV2Value value2(std::string("0.987654321"));
        EXPECT_EQ(0, value2.int_value());
        EXPECT_EQ(987654321, value2.frac_value());
    }
    // truncate frac part
    {
        DecimalV2Value value(std::string("-123456789.987654321987654321"));
        EXPECT_EQ(-123456789, value.int_value());
        EXPECT_EQ(-987654322, value.frac_value());
    }
}

// Half up
TEST_F(DecimalV2ValueTest, round_ops) {
    // less than 5
    DecimalV2Value value(std::string("1.249"));
    {
        DecimalV2Value dst(0);
        value.round(&dst, -1, HALF_UP);
        EXPECT_EQ("0", dst.to_string());

        value.round(&dst, -1, CEILING);
        EXPECT_EQ("10", dst.to_string());

        value.round(&dst, -1, FLOOR);
        EXPECT_EQ("0", dst.to_string());

        value.round(&dst, -1, TRUNCATE);
        EXPECT_EQ("0", dst.to_string());
    }
    {
        DecimalV2Value dst(0);
        value.round(&dst, 0, HALF_UP);
        EXPECT_EQ("1", dst.to_string());

        value.round(&dst, 0, CEILING);
        EXPECT_EQ("2", dst.to_string());

        value.round(&dst, 0, FLOOR);
        EXPECT_EQ("1", dst.to_string());

        value.round(&dst, 0, TRUNCATE);
        EXPECT_EQ("1", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 1, HALF_UP);
        EXPECT_EQ("1.2", dst.to_string());

        value.round(&dst, 1, CEILING);
        EXPECT_EQ("1.3", dst.to_string());

        value.round(&dst, 1, FLOOR);
        EXPECT_EQ("1.2", dst.to_string());

        value.round(&dst, 1, TRUNCATE);
        EXPECT_EQ("1.2", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 2, HALF_UP);
        EXPECT_EQ("1.25", dst.to_string());

        value.round(&dst, 2, CEILING);
        EXPECT_EQ("1.25", dst.to_string());

        value.round(&dst, 2, FLOOR);
        EXPECT_EQ("1.24", dst.to_string());

        value.round(&dst, 2, TRUNCATE);
        EXPECT_EQ("1.24", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 3, HALF_UP);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, CEILING);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, FLOOR);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, TRUNCATE);
        EXPECT_EQ("1.249", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 4, HALF_UP);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, CEILING);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, FLOOR);
        EXPECT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, TRUNCATE);
        EXPECT_EQ("1.249", dst.to_string());
    }
}

// Half up
TEST_F(DecimalV2ValueTest, round_minus) {
    // less than 5
    DecimalV2Value value(std::string("-1.249"));
    {
        DecimalV2Value dst(0);
        value.round(&dst, -1, HALF_UP);
        EXPECT_EQ("0", dst.to_string());

        value.round(&dst, -1, CEILING);
        EXPECT_EQ("0", dst.to_string());

        value.round(&dst, -1, FLOOR);
        EXPECT_EQ("-10", dst.to_string());

        value.round(&dst, -1, TRUNCATE);
        EXPECT_EQ("0", dst.to_string());
    }
    {
        DecimalV2Value dst(0);
        value.round(&dst, 0, HALF_UP);
        EXPECT_EQ("-1", dst.to_string());

        value.round(&dst, 0, CEILING);
        EXPECT_EQ("-1", dst.to_string());

        value.round(&dst, 0, FLOOR);
        EXPECT_EQ("-2", dst.to_string());

        value.round(&dst, 0, TRUNCATE);
        EXPECT_EQ("-1", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 1, HALF_UP);
        EXPECT_EQ("-1.2", dst.to_string());

        value.round(&dst, 1, CEILING);
        EXPECT_EQ("-1.2", dst.to_string());

        value.round(&dst, 1, FLOOR);
        EXPECT_EQ("-1.3", dst.to_string());

        value.round(&dst, 1, TRUNCATE);
        EXPECT_EQ("-1.2", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 2, HALF_UP);
        EXPECT_EQ("-1.25", dst.to_string());

        value.round(&dst, 2, CEILING);
        EXPECT_EQ("-1.24", dst.to_string());

        value.round(&dst, 2, FLOOR);
        EXPECT_EQ("-1.25", dst.to_string());

        value.round(&dst, 2, TRUNCATE);
        EXPECT_EQ("-1.24", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 3, HALF_UP);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, CEILING);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, FLOOR);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, TRUNCATE);
        EXPECT_EQ("-1.249", dst.to_string());
    }

    {
        DecimalV2Value dst(0);
        value.round(&dst, 4, HALF_UP);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, CEILING);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, FLOOR);
        EXPECT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, TRUNCATE);
        EXPECT_EQ("-1.249", dst.to_string());
    }
}

// Half up
TEST_F(DecimalV2ValueTest, round_to_int) {
    {
        DecimalV2Value value(std::string("99.99"));
        {
            DecimalV2Value dst;
            value.round(&dst, 1, HALF_UP);
            EXPECT_EQ("100", dst.to_string());
        }
    }
    {
        DecimalV2Value value(std::string("123.12399"));
        {
            DecimalV2Value dst;
            value.round(&dst, 4, HALF_UP);
            EXPECT_EQ("123.124", dst.to_string());
        }
    }
}

TEST_F(DecimalV2ValueTest, double_to_decimal) {
    double i = 1.2;
    DecimalV2Value* value = new DecimalV2Value(100, 9876);
    value->assign_from_double(i);
    EXPECT_STREQ("1.2", value->to_string().c_str());
    delete value;
}

TEST_F(DecimalV2ValueTest, float_to_decimal) {
    float i = 1.2;
    DecimalV2Value* value = new DecimalV2Value(100, 9876);
    value->assign_from_float(i);
    EXPECT_STREQ("1.2", value->to_string().c_str());
    delete value;
}
} // end namespace doris
