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
#include "testutil/function_utils.h"
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
    delete context;
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
    delete context;
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
    delete context;
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
    delete context;
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
    delete context;
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
    delete context;
}

TEST_F(StringFunctionsTest, ends_with) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal nullRet = doris_udf::BooleanVal::null();

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal(""), StringVal("")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal("")));

    ASSERT_EQ(falseRet, StringFunctions::ends_with(context, StringVal(""), StringVal("hello")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal("hello")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal(" "), StringVal(" ")));

    ASSERT_EQ(falseRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal(" ")));

    ASSERT_EQ(falseRet, StringFunctions::ends_with(context, StringVal(" "), StringVal("hello")));

    ASSERT_EQ(falseRet, StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("hello")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("doris")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("hello doris")));

    ASSERT_EQ(nullRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal::null()));

    ASSERT_EQ(nullRet, StringFunctions::ends_with(context, StringVal::null(), StringVal("hello")));

    ASSERT_EQ(nullRet, StringFunctions::ends_with(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, starts_with) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal nullRet = doris_udf::BooleanVal::null();

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal(""), StringVal("")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal(" "), StringVal(" ")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal("")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal(""), StringVal("hello")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal("hello")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal(" ")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal(" "), StringVal("world")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello world"), StringVal("hello")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal("hello world"), StringVal("world")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello world"), StringVal("hello world")));

    ASSERT_EQ(nullRet, StringFunctions::starts_with(context, StringVal("hello world"), StringVal::null()));

    ASSERT_EQ(nullRet, StringFunctions::starts_with(context, StringVal::null(), StringVal("hello world")));

    ASSERT_EQ(nullRet, StringFunctions::starts_with(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, null_or_empty) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);

    ASSERT_EQ(trueRet, StringFunctions::null_or_empty(context, StringVal("")));

    ASSERT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal(" ")));

    ASSERT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("hello")));

    ASSERT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("doris")));

    ASSERT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("111")));

    ASSERT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal(".")));

    ASSERT_EQ(trueRet, StringFunctions::null_or_empty(context, StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, substring) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("hello")),
            StringFunctions::substring(context, StringVal("hello word"), 1, 5));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("word")),
            StringFunctions::substring(context, StringVal("hello word"), 7, 4));

    ASSERT_EQ(StringVal::null(),
            StringFunctions::substring(context, StringVal::null(), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::substring(context, StringVal("hello word"), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string(" word")),
            StringFunctions::substring(context, StringVal("hello word"), -5, 5));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("hello word 你")),
            StringFunctions::substring(context, StringVal("hello word 你好"), 1, 12));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("好")),
            StringFunctions::substring(context, StringVal("hello word 你好"), 13, 1));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::substring(context, StringVal("hello word 你好"), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("rd 你好")),
            StringFunctions::substring(context, StringVal("hello word 你好"), -5, 5));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("h")),
            StringFunctions::substring(context, StringVal("hello word 你好"), 1, 1));
}

TEST_F(StringFunctionsTest, reverse) {
    FunctionUtils fu;
    doris_udf::FunctionContext* context = fu.get_fn_ctx();

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("olleh")),
            StringFunctions::reverse(context, StringVal("hello")));
    ASSERT_EQ(StringVal::null(),
            StringFunctions::reverse(context, StringVal::null()));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("")),
            StringFunctions::reverse(context, StringVal("")));

    ASSERT_EQ(AnyValUtil::from_string_temp(context,std::string("好你olleh")),
            StringFunctions::reverse(context, StringVal("hello你好")));
}

TEST_F(StringFunctionsTest, length) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(IntVal(5),
            StringFunctions::length(context, StringVal("hello")));
    ASSERT_EQ(IntVal(5),
            StringFunctions::char_utf8_length(context, StringVal("hello")));
    ASSERT_EQ(IntVal::null(),
            StringFunctions::length(context, StringVal::null()));
    ASSERT_EQ(IntVal::null(),
            StringFunctions::char_utf8_length(context, StringVal::null()));

    ASSERT_EQ(IntVal(0),
            StringFunctions::length(context, StringVal("")));
    ASSERT_EQ(IntVal(0),
            StringFunctions::char_utf8_length(context, StringVal("")));
            
    ASSERT_EQ(IntVal(11),
            StringFunctions::length(context, StringVal("hello你好")));
            
    ASSERT_EQ(IntVal(7),
            StringFunctions::char_utf8_length(context, StringVal("hello你好")));
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
