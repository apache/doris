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

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "runtime/decimalv2_value.h"
#include "util/logging.h"

namespace doris {

class DecimalValueTest : public testing::Test {
public:
    DecimalValueTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(DecimalValueTest, string_to_decimal) {
    DecimalValue value(std::string("1.23"));
    ASSERT_EQ("1.23", value.to_string(3));

    DecimalValue value1(std::string("0.23"));
    ASSERT_EQ("0.23", value1.to_string(3));

    DecimalValue value2(std::string("1234567890123456789.0"));
    ASSERT_EQ("1234567890123456789.0", value2.to_string(3));
}

TEST_F(DecimalValueTest, negative_zero) {
    DecimalValue value(std::string("-0.00"));
    std::cout << "value: " << value.get_debug_info() << std::endl;
    {
        // positive zero VS negative zero
        DecimalValue value2(std::string("0.00"));
        std::cout << "value2: " << value2.get_debug_info() << std::endl;
        ASSERT_TRUE(value == value2);
        ASSERT_FALSE(value < value2);
        ASSERT_FALSE(value < value2);
        ASSERT_TRUE(value <= value2);
        ASSERT_TRUE(value >= value2);
    }
    {
        // from string, positive
        DecimalValue value3(std::string("5.0"));
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_TRUE(value < value3);
        ASSERT_TRUE(value <= value3);
        ASSERT_TRUE(value3 > value);
        ASSERT_TRUE(value3 >= value);
    }
    {
        // from string, negative
        DecimalValue value3(std::string("-5.0"));
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_TRUE(value > value3);
        ASSERT_TRUE(value >= value3);
        ASSERT_TRUE(value3 < value);
        ASSERT_TRUE(value3 <= value);
    }
    {
        // from int
        DecimalValue value3(6);
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_TRUE(value < value3);
        ASSERT_TRUE(value <= value3);
        ASSERT_TRUE(value3 > value);
        ASSERT_TRUE(value3 >= value);

        ASSERT_FALSE(!(value < value3));
        ASSERT_FALSE(!(value <= value3));
        ASSERT_FALSE(!(value3 > value));
        ASSERT_FALSE(!(value3 >= value));
    }
    {
        // from int
        DecimalValue value3(4, 0);
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_TRUE(value < value3);
        ASSERT_TRUE(value <= value3);
        ASSERT_TRUE(value3 > value);
        ASSERT_TRUE(value3 >= value);
    }
    {
        // from int
        DecimalValue value3(3, -0);
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_TRUE(value < value3);
        ASSERT_TRUE(value <= value3);
        ASSERT_TRUE(value3 > value);
        ASSERT_TRUE(value3 >= value);
    }
}

TEST_F(DecimalValueTest, int_to_decimal) {
    DecimalValue value1;
    ASSERT_EQ("0", value1.to_string(3));

    DecimalValue value2(111111111); // 9 digits
    std::cout << "value2: " << value2.get_debug_info() << std::endl;
    ASSERT_EQ("111111111", value2.to_string(3));

    DecimalValue value3(111111111, 222222222); // 9 digits
    std::cout << "value3: " << value3.get_debug_info() << std::endl;
    ASSERT_EQ("111111111.222", value3.to_string(3));

    DecimalValue value4(0, 222222222); // 9 digits
    std::cout << "value4: " << value4.get_debug_info() << std::endl;
    ASSERT_EQ("0.222", value4.to_string(3));

    DecimalValue value5(111111111, 0); // 9 digits
    std::cout << "value5: " << value5.get_debug_info() << std::endl;
    ASSERT_EQ("111111111", value5.to_string(3));

    DecimalValue value6(0, 0); // 9 digits
    std::cout << "value6: " << value6.get_debug_info() << std::endl;
    ASSERT_EQ("0", value6.to_string(3));

    DecimalValue value7(0, 12345); // 9 digits
    std::cout << "value7: " << value7.get_debug_info() << std::endl;
    ASSERT_EQ("0.000012", value7.to_string(6));

    DecimalValue value8(11, 0);
    std::cout << "value8: " << value8.get_debug_info() << std::endl;
    ASSERT_EQ("11", value8.to_string(3));

    // more than 9digit, fraction will be trancated to 999999999
    DecimalValue value9(1230123456789, 1230123456789);
    std::cout << "value9: " << value9.get_debug_info() << std::endl;
    ASSERT_EQ("1230123456789.999999999", value9.to_string(10));

    // negative
    {
        DecimalValue value2(-111111111); // 9 digits
        std::cout << "value2: " << value2.get_debug_info() << std::endl;
        ASSERT_EQ("-111111111", value2.to_string(3));

        DecimalValue value3(-111111111, 222222222); // 9 digits
        std::cout << "value3: " << value3.get_debug_info() << std::endl;
        ASSERT_EQ("-111111111.222", value3.to_string(3));

        DecimalValue value4(0, -222222222); // 9 digits
        std::cout << "value4: " << value4.get_debug_info() << std::endl;
        ASSERT_EQ("-0.222", value4.to_string(3));

        DecimalValue value5(-111111111, 0); // 9 digits
        std::cout << "value5: " << value5.get_debug_info() << std::endl;
        ASSERT_EQ("-111111111", value5.to_string(3));

        DecimalValue value7(0, -12345); // 9 digits
        std::cout << "value7: " << value7.get_debug_info() << std::endl;
        ASSERT_EQ("-0.000012", value7.to_string(6));

        DecimalValue value8(-11, 0);
        std::cout << "value8: " << value8.get_debug_info() << std::endl;
        ASSERT_EQ("-11", value8.to_string(3));
    }
}

TEST_F(DecimalValueTest, add) {
    DecimalValue value11(std::string("1111111111.2222222222")); // 10 digits
    DecimalValue value12(std::string("2222222222.1111111111")); // 10 digits
    DecimalValue add_result1 = value11 + value12;
    std::cout << "add_result1: " << add_result1.get_debug_info() << std::endl;
    ASSERT_EQ("3333333333.3333333333", add_result1.to_string(10));

    DecimalValue value21(std::string("-3333333333.2222222222")); // 10 digits
    DecimalValue value22(std::string("2222222222.1111111111"));  // 10 digits
    DecimalValue add_result2 = value21 + value22;
    std::cout << "add_result2: " << add_result2.get_debug_info() << std::endl;
    ASSERT_EQ("-1111111111.1111111111", add_result2.to_string(10));
}

TEST_F(DecimalValueTest, compound_add) {
    {
        DecimalValue value1(std::string("111111111.222222222"));
        DecimalValue value2(std::string("111111111.222222222"));
        value1 += value2;
        std::cout << "value1: " << value1.get_debug_info() << std::endl;
        ASSERT_EQ("222222222.444444444", value1.to_string(10));
    }
}

TEST_F(DecimalValueTest, sub) {
    DecimalValue value11(std::string("3333333333.2222222222")); // 10 digits
    DecimalValue value12(std::string("2222222222.1111111111")); // 10 digits
    DecimalValue sub_result1 = value11 - value12;
    std::cout << "sub_result1: " << sub_result1.get_debug_info() << std::endl;
    ASSERT_EQ("1111111111.1111111111", sub_result1.to_string(10));

    DecimalValue value21(std::string("-2222222222.1111111111")); // 10 digits
    DecimalValue sub_result2 = value11 - value21;
    std::cout << "sub_result2: " << sub_result2.get_debug_info() << std::endl;
    ASSERT_EQ("5555555555.3333333333", sub_result2.to_string(10));

    // small - big
    {
        DecimalValue value1(std::string("8.0"));
        DecimalValue value2(std::string("0"));
        DecimalValue sub_result = value2 - value1;
        LOG(INFO) << "sub_result: " << sub_result.get_debug_info() << std::endl;
        DecimalValue expected_value(std::string("-8.0"));
        ASSERT_EQ(expected_value, sub_result);
        ASSERT_FALSE(sub_result.is_zero());
    }
    // minimum - maximal
    {
        DecimalValue value1(
                std::string("9999999999999999999999999999999999999999"
                            "99999999999999999999999999999999999999999")); // 81 digits
        DecimalValue value2(
                std::string("-9999999999999999999999999999999999999999"
                            "99999999999999999999999999999999999999999")); // 81 digits
        DecimalValue sub_result = value2 - value1;
        LOG(INFO) << "sub_result: " << sub_result.get_debug_info() << std::endl;
        DecimalValue expected_value = value2;
        ASSERT_EQ(expected_value, sub_result);
        ASSERT_FALSE(sub_result.is_zero());
        ASSERT_TRUE(value1 > value2);
    }
}

TEST_F(DecimalValueTest, mul) {
    DecimalValue value11(std::string("3333333333.2222222222"));  // 10 digits
    DecimalValue value12(std::string("-2222222222.1111111111")); // 10 digits
    DecimalValue mul_result1 = value11 * value12;
    std::cout << "mul_result1: " << mul_result1.get_debug_info() << std::endl;
    ASSERT_EQ(DecimalValue(std::string("-7407407406790123456.71604938271975308642")), mul_result1);

    DecimalValue value21(std::string("0")); // zero
    DecimalValue mul_result2 = value11 * value21;
    std::cout << "mul_result2: " << mul_result2.get_debug_info() << std::endl;
    ASSERT_EQ(DecimalValue(std::string("0")), mul_result2);

    {
        // test when carry is needed
        DecimalValue value1(std::string("3074062.5421333313"));
        DecimalValue value2(std::string("2169.957745029689045693"));
        DecimalValue mul_result = value1 * value2;
        std::cout << "mul_result=" << mul_result.get_debug_info() << std::endl;
        ASSERT_EQ(DecimalValue(std::string("6670585822.0078770603624547106640070909")), mul_result);
    }
}

TEST_F(DecimalValueTest, div) {
    DecimalValue value11(std::string("-7407407406790123456.71604938271975308642"));
    DecimalValue value12(std::string("-2222222222.1111111111")); // 10 digits
    DecimalValue div_result1 = value11 / value12;
    std::cout << "div_result1: " << div_result1.get_debug_info() << std::endl;
    ASSERT_EQ(DecimalValue(std::string("3333333333.2222222222")), div_result1);
    ASSERT_EQ("3333333333.222222222200000", div_result1.to_string(15));
    {
        DecimalValue value11(std::string("32767"));
        DecimalValue value12(std::string("604587"));
        DecimalValue div_result1 = value11 / value12;
        std::cout << "div_result1: " << div_result1.get_debug_info() << std::endl;
        ASSERT_EQ(DecimalValue(std::string("0.054197328")), div_result1);
    }
}

TEST_F(DecimalValueTest, unary_minus_operator) {
    {
        DecimalValue value1(std::string("111111111.222222222"));
        DecimalValue value2 = -value1;
        std::cout << "value1: " << value1.get_debug_info() << std::endl;
        std::cout << "value2: " << value2.get_debug_info() << std::endl;
        ASSERT_EQ("111111111.222222222", value1.to_string(10));
        ASSERT_EQ("-111111111.222222222", value2.to_string(10));
    }
}

TEST_F(DecimalValueTest, to_int_frac_value) {
    // positive & negative
    {
        DecimalValue value(std::string("123456789123456789.987654321"));
        ASSERT_EQ(123456789123456789, value.int_value());
        ASSERT_EQ(987654321, value.frac_value());

        DecimalValue value2(std::string("-123456789123456789.987654321"));
        ASSERT_EQ(-123456789123456789, value2.int_value());
        ASSERT_EQ(-987654321, value2.frac_value());
    }
    // int or frac part is 0
    {
        DecimalValue value(std::string("-123456789123456789"));
        ASSERT_EQ(-123456789123456789, value.int_value());
        ASSERT_EQ(0, value.frac_value());

        DecimalValue value2(std::string("0.987654321"));
        ASSERT_EQ(0, value2.int_value());
        ASSERT_EQ(987654321, value2.frac_value());
    }
    // truncate frac part
    {
        DecimalValue value(std::string("-123456789.987654321987654321"));
        ASSERT_EQ(-123456789, value.int_value());
        ASSERT_EQ(-987654321, value.frac_value());
    }
}

// Half up
TEST_F(DecimalValueTest, round_ops) {
    // less than 5
    DecimalValue value(std::string("1.249"));
    {
        DecimalValue dst;
        value.round(&dst, -1, HALF_UP);
        ASSERT_EQ("0", dst.to_string());

        value.round(&dst, -1, CEILING);
        ASSERT_EQ("10", dst.to_string());

        value.round(&dst, -1, FLOOR);
        ASSERT_EQ("0", dst.to_string());

        value.round(&dst, -1, TRUNCATE);
        ASSERT_EQ("0", dst.to_string());
    }
    {
        DecimalValue dst;
        value.round(&dst, 0, HALF_UP);
        ASSERT_EQ("1", dst.to_string());

        value.round(&dst, 0, CEILING);
        ASSERT_EQ("2", dst.to_string());

        value.round(&dst, 0, FLOOR);
        ASSERT_EQ("1", dst.to_string());

        value.round(&dst, 0, TRUNCATE);
        ASSERT_EQ("1", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 1, HALF_UP);
        ASSERT_EQ("1.2", dst.to_string());

        value.round(&dst, 1, CEILING);
        ASSERT_EQ("1.3", dst.to_string());

        value.round(&dst, 1, FLOOR);
        ASSERT_EQ("1.2", dst.to_string());

        value.round(&dst, 1, TRUNCATE);
        ASSERT_EQ("1.2", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 2, HALF_UP);
        ASSERT_EQ("1.25", dst.to_string());

        value.round(&dst, 2, CEILING);
        ASSERT_EQ("1.25", dst.to_string());

        value.round(&dst, 2, FLOOR);
        ASSERT_EQ("1.24", dst.to_string());

        value.round(&dst, 2, TRUNCATE);
        ASSERT_EQ("1.24", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 3, HALF_UP);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, CEILING);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, FLOOR);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 3, TRUNCATE);
        ASSERT_EQ("1.249", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 4, HALF_UP);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, CEILING);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, FLOOR);
        ASSERT_EQ("1.249", dst.to_string());

        value.round(&dst, 4, TRUNCATE);
        ASSERT_EQ("1.249", dst.to_string());
    }
}

// Half up
TEST_F(DecimalValueTest, round_minus) {
    // less than 5
    DecimalValue value(std::string("-1.249"));
    {
        DecimalValue dst;
        value.round(&dst, -1, HALF_UP);
        ASSERT_EQ("0", dst.to_string());

        value.round(&dst, -1, CEILING);
        ASSERT_EQ("0", dst.to_string());

        value.round(&dst, -1, FLOOR);
        ASSERT_EQ("-10", dst.to_string());

        value.round(&dst, -1, TRUNCATE);
        ASSERT_EQ("0", dst.to_string());
    }
    {
        DecimalValue dst;
        value.round(&dst, 0, HALF_UP);
        ASSERT_EQ("-1", dst.to_string());

        value.round(&dst, 0, CEILING);
        ASSERT_EQ("-1", dst.to_string());

        value.round(&dst, 0, FLOOR);
        ASSERT_EQ("-2", dst.to_string());

        value.round(&dst, 0, TRUNCATE);
        ASSERT_EQ("-1", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 1, HALF_UP);
        ASSERT_EQ("-1.2", dst.to_string());

        value.round(&dst, 1, CEILING);
        ASSERT_EQ("-1.2", dst.to_string());

        value.round(&dst, 1, FLOOR);
        ASSERT_EQ("-1.3", dst.to_string());

        value.round(&dst, 1, TRUNCATE);
        ASSERT_EQ("-1.2", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 2, HALF_UP);
        ASSERT_EQ("-1.25", dst.to_string());

        value.round(&dst, 2, CEILING);
        ASSERT_EQ("-1.24", dst.to_string());

        value.round(&dst, 2, FLOOR);
        ASSERT_EQ("-1.25", dst.to_string());

        value.round(&dst, 2, TRUNCATE);
        ASSERT_EQ("-1.24", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 3, HALF_UP);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, CEILING);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, FLOOR);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 3, TRUNCATE);
        ASSERT_EQ("-1.249", dst.to_string());
    }

    {
        DecimalValue dst;
        value.round(&dst, 4, HALF_UP);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, CEILING);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, FLOOR);
        ASSERT_EQ("-1.249", dst.to_string());

        value.round(&dst, 4, TRUNCATE);
        ASSERT_EQ("-1.249", dst.to_string());
    }
}

// Half up
TEST_F(DecimalValueTest, round_to_int) {
    {
        DecimalValue value(std::string("99.99"));
        {
            DecimalValue dst;
            value.round(&dst, 1, HALF_UP);
            ASSERT_EQ("100.0", dst.to_string());
        }
    }
    {
        DecimalValue value(std::string("123.12399"));
        {
            DecimalValue dst;
            value.round(&dst, 4, HALF_UP);
            ASSERT_EQ("123.124", dst.to_string());
        }
    }
}

TEST_F(DecimalValueTest, double_to_decimal) {
    double i = 1.2;
    DecimalValue* value = new DecimalValue(100, 9876);
    value->assign_from_double(i);
    ASSERT_STREQ("1.2", value->to_string().c_str());
    delete value;
}

TEST_F(DecimalValueTest, float_to_decimal) {
    float i = 1.2;
    DecimalValue* value = new DecimalValue(100, 9876);
    value->assign_from_float(i);
    ASSERT_STREQ("1.2", value->to_string().c_str());
    delete value;
}
} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
