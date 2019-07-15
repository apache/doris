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

#include "exprs/string_functions.h"
#include "util/logging.h"
#include "exprs/anyval_util.h"
#include <iostream>
#include <string>

#include <gtest/gtest.h>

namespace doris {

class StringFunctionsTest : public testing::Test {
public:
    StringFunctionsTest() {
    }
};

TEST_F(StringFunctionsTest, money_format_bigint) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::BigIntVal(123456));
    StringVal expected = AnyValUtil::from_string_temp(context, std::string("123,456.00"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(-123456));
    expected = AnyValUtil::from_string_temp(context, std::string("-123,456.00"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(9223372036854775807));
    expected = AnyValUtil::from_string_temp(context, std::string("9,223,372,036,854,775,807.00"));
    ASSERT_EQ(expected, result);
}

TEST_F(StringFunctionsTest, money_format_large_int) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    std::string str("170141183460469231731687303715884105727");
    std::stringstream ss;
    ss << str;
    __int128 value;
    ss >> value;

    std::cout << "value: " << value << std::endl;

    StringVal result = StringFunctions::money_format(context, doris_udf::LargeIntVal(value));
    StringVal expected = AnyValUtil::from_string_temp(context, std::string("170,141,183,460,469,231,731,687,303,715,884,105,727.00"));
    ASSERT_EQ(expected, result);
}

TEST_F(StringFunctionsTest, money_format_double) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.456));
    StringVal expected = AnyValUtil::from_string_temp(context, std::string("1,234.46"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.45));
    expected = AnyValUtil::from_string_temp(context, std::string("1,234.45"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.4));
    expected = AnyValUtil::from_string_temp(context, std::string("1,234.40"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.454));
    expected = AnyValUtil::from_string_temp(context, std::string("1,234.45"));
    ASSERT_EQ(expected, result);
}

TEST_F(StringFunctionsTest, money_format_decimal) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DecimalValue dv1(std::string("3333333333.2222222222"));
    DecimalVal value1;
    dv1.to_decimal_val(&value1);

    StringVal result = StringFunctions::money_format(context, value1);
    StringVal expected = AnyValUtil::from_string_temp(context, std::string("3,333,333,333.22"));
    ASSERT_EQ(expected, result);

    DecimalValue dv2(std::string("-7407407406790123456.71604938271975308642"));
    DecimalVal value2;
    dv2.to_decimal_val(&value2);

    result = StringFunctions::money_format(context, value2);
    expected = AnyValUtil::from_string_temp(context, std::string("-7,407,407,406,790,123,456.72"));
    ASSERT_EQ(expected, result);
}

TEST_F(StringFunctionsTest, money_format_decimal_v2) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DecimalV2Value dv1(std::string("3333333333.2222222222"));
    DecimalV2Val value1;
    dv1.to_decimal_val(&value1);

    StringVal result = StringFunctions::money_format(context, value1);
    StringVal expected = AnyValUtil::from_string_temp(context, std::string("3,333,333,333.22"));
    ASSERT_EQ(expected, result);

    DecimalV2Value dv2(std::string("-740740740.71604938271975308642"));
    DecimalV2Val value2;
    dv2.to_decimal_val(&value2);

    result = StringFunctions::money_format(context, value2);
    expected = AnyValUtil::from_string_temp(context, std::string("-740,740,740.72"));
    ASSERT_EQ(expected, result);
}

TEST_F(StringFunctionsTest, split_part) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("hello")),
            StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 1));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("word")),
            StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 2));

    ASSERT_EQ(StringVal::null(),
            StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 3));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 1));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string(" word")),
            StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 2));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("2019年9")),
            StringFunctions::split_part(context, StringVal("2019年9月8日"), StringVal("月"), 1));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 1));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("bcd")),
            StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 2));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("bd")),
            StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 3));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 4));
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
