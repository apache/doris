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

#include <fmt/os.h>
#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "exprs/anyval_util.h"
#include "testutil/function_utils.h"
#include "testutil/test_util.h"
#include "util/logging.h"
#include "util/simd/vstring_function.h"

namespace doris {

class StringFunctionsTest : public testing::Test {
public:
    StringFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(StringFunctionsTest, do_money_format_for_bigint_bench) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    StringVal expected = AnyValUtil::from_string(ctx, std::string("9,223,372,036,854,775,807.00"));
    BigIntVal bigIntVal(9223372036854775807);
    for (int i = 0; i < LOOP_LESS_OR_MORE(10, 10000000); i++) {
        StringVal result = StringFunctions::money_format(context, bigIntVal);
        EXPECT_EQ(expected, result);
    }
    delete context;
}

TEST_F(StringFunctionsTest, do_money_format_for_decimalv2_bench) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    StringVal expected = AnyValUtil::from_string(ctx, std::string("9,223,372,085.87"));
    DecimalV2Value dv1(std::string("9223372085.8678"));
    DecimalV2Val decimalV2Val;
    dv1.to_decimal_val(&decimalV2Val);
    for (int i = 0; i < LOOP_LESS_OR_MORE(10, 10000000); i++) {
        StringVal result = StringFunctions::money_format(context, decimalV2Val);
        EXPECT_EQ(expected, result);
    }
    delete context;
}

TEST_F(StringFunctionsTest, money_format_bigint) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::BigIntVal(123456));
    StringVal expected = AnyValUtil::from_string(ctx, std::string("123,456.00"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(-123456));
    expected = AnyValUtil::from_string(ctx, std::string("-123,456.00"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(9223372036854775807));
    expected = AnyValUtil::from_string(ctx, std::string("9,223,372,036,854,775,807.00"));
    EXPECT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, money_format_large_int) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    __int128 value = MAX_INT128;
    StringVal result = StringFunctions::money_format(context, doris_udf::LargeIntVal(value));
    StringVal expected = AnyValUtil::from_string_temp(
            context, std::string("170,141,183,460,469,231,731,687,303,715,884,105,727.00"));
    EXPECT_EQ(expected, result);

    value = MIN_INT128;
    result = StringFunctions::money_format(context, doris_udf::LargeIntVal(value));
    expected = AnyValUtil::from_string_temp(
            context, std::string("-170,141,183,460,469,231,731,687,303,715,884,105,728.00"));
    EXPECT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, money_format_double) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.456));
    StringVal expected = AnyValUtil::from_string(ctx, std::string("1,234.46"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.45));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.45"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.4));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.40"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.454));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.45"));
    EXPECT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(-36854775807.039));
    expected = AnyValUtil::from_string(ctx, std::string("-36,854,775,807.04"));
    EXPECT_EQ(expected, result);

    delete context;
}

TEST_F(StringFunctionsTest, money_format_decimal_v2) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DecimalV2Value dv1(std::string("3333333333.2222222222"));
    DecimalV2Val value1;
    dv1.to_decimal_val(&value1);

    StringVal result = StringFunctions::money_format(context, value1);
    StringVal expected = AnyValUtil::from_string(ctx, std::string("3,333,333,333.22"));
    EXPECT_EQ(expected, result);

    DecimalV2Value dv2(std::string("-740740740.71604938271975308642"));
    DecimalV2Val value2;
    dv2.to_decimal_val(&value2);

    result = StringFunctions::money_format(context, value2);
    expected = AnyValUtil::from_string(ctx, std::string("-740,740,740.72"));
    EXPECT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, split_part) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("hello")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 1));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("word")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 2));

    EXPECT_EQ(StringVal::null(),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 3));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 1));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string(" word")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 2));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("2019年9")),
              StringFunctions::split_part(context, StringVal("2019年9月8日"), StringVal("月"), 1));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 1));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("bcd")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 2));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("bd")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 3));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 4));

    EXPECT_EQ(
            AnyValUtil::from_string(ctx, std::string("#123")),
            StringFunctions::split_part(context, StringVal("abc###123###234"), StringVal("##"), 2));

    delete context;
}

TEST_F(StringFunctionsTest, ends_with) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal nullRet = doris_udf::BooleanVal::null();

    EXPECT_EQ(trueRet, StringFunctions::ends_with(context, StringVal(""), StringVal("")));

    EXPECT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal("")));

    EXPECT_EQ(falseRet, StringFunctions::ends_with(context, StringVal(""), StringVal("hello")));

    EXPECT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal("hello")));

    EXPECT_EQ(trueRet, StringFunctions::ends_with(context, StringVal(" "), StringVal(" ")));

    EXPECT_EQ(falseRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal(" ")));

    EXPECT_EQ(falseRet, StringFunctions::ends_with(context, StringVal(" "), StringVal("hello")));

    EXPECT_EQ(falseRet,
              StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("hello")));

    EXPECT_EQ(trueRet,
              StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("doris")));

    EXPECT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello doris"),
                                                  StringVal("hello doris")));

    EXPECT_EQ(nullRet, StringFunctions::ends_with(context, StringVal("hello"), StringVal::null()));

    EXPECT_EQ(nullRet, StringFunctions::ends_with(context, StringVal::null(), StringVal("hello")));

    EXPECT_EQ(nullRet, StringFunctions::ends_with(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, starts_with) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);
    doris_udf::BooleanVal nullRet = doris_udf::BooleanVal::null();

    EXPECT_EQ(trueRet, StringFunctions::starts_with(context, StringVal(""), StringVal("")));

    EXPECT_EQ(trueRet, StringFunctions::starts_with(context, StringVal(" "), StringVal(" ")));

    EXPECT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal("")));

    EXPECT_EQ(falseRet, StringFunctions::starts_with(context, StringVal(""), StringVal("hello")));

    EXPECT_EQ(trueRet,
              StringFunctions::starts_with(context, StringVal("hello"), StringVal("hello")));

    EXPECT_EQ(falseRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal(" ")));

    EXPECT_EQ(falseRet, StringFunctions::starts_with(context, StringVal(" "), StringVal("world")));

    EXPECT_EQ(trueRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal("hello")));

    EXPECT_EQ(falseRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal("world")));

    EXPECT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello world"),
                                                    StringVal("hello world")));

    EXPECT_EQ(nullRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal::null()));

    EXPECT_EQ(nullRet,
              StringFunctions::starts_with(context, StringVal::null(), StringVal("hello world")));

    EXPECT_EQ(nullRet, StringFunctions::starts_with(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, null_or_empty) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::BooleanVal falseRet = doris_udf::BooleanVal(false);
    doris_udf::BooleanVal trueRet = doris_udf::BooleanVal(true);

    EXPECT_EQ(trueRet, StringFunctions::null_or_empty(context, StringVal("")));

    EXPECT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal(" ")));

    EXPECT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("hello")));

    EXPECT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("doris")));

    EXPECT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal("111")));

    EXPECT_EQ(falseRet, StringFunctions::null_or_empty(context, StringVal(".")));

    EXPECT_EQ(trueRet, StringFunctions::null_or_empty(context, StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, left) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::left(context, StringVal(""), 10));
    delete context;
}

TEST_F(StringFunctionsTest, substring) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word"), 0, 5));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("hello")),
              StringFunctions::substring(context, StringVal("hello word"), 1, 5));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("word")),
              StringFunctions::substring(context, StringVal("hello word"), 7, 4));

    EXPECT_EQ(StringVal::null(), StringFunctions::substring(context, StringVal::null(), 1, 0));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word"), 1, 0));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string(" word")),
              StringFunctions::substring(context, StringVal("hello word"), -5, 5));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("hello word 你")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 12));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("好")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 13, 1));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 0));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("rd 你好")),
              StringFunctions::substring(context, StringVal("hello word 你好"), -5, 5));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("h")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 1));
    delete context;
}

TEST_F(StringFunctionsTest, reverse) {
    FunctionUtils fu;
    doris_udf::FunctionContext* context = fu.get_fn_ctx();

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("olleh")),
              StringFunctions::reverse(context, StringVal("hello")));
    EXPECT_EQ(StringVal::null(), StringFunctions::reverse(context, StringVal::null()));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::reverse(context, StringVal("")));

    EXPECT_EQ(AnyValUtil::from_string(ctx, std::string("好你olleh")),
              StringFunctions::reverse(context, StringVal("hello你好")));
}

TEST_F(StringFunctionsTest, length) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(IntVal(5), StringFunctions::length(context, StringVal("hello")));
    EXPECT_EQ(IntVal(5), StringFunctions::char_utf8_length(context, StringVal("hello")));
    EXPECT_EQ(IntVal::null(), StringFunctions::length(context, StringVal::null()));
    EXPECT_EQ(IntVal::null(), StringFunctions::char_utf8_length(context, StringVal::null()));

    EXPECT_EQ(IntVal(0), StringFunctions::length(context, StringVal("")));
    EXPECT_EQ(IntVal(0), StringFunctions::char_utf8_length(context, StringVal("")));

    EXPECT_EQ(IntVal(11), StringFunctions::length(context, StringVal("hello你好")));

    EXPECT_EQ(IntVal(7), StringFunctions::char_utf8_length(context, StringVal("hello你好")));
    delete context;
}

TEST_F(StringFunctionsTest, append_trailing_char_if_absent) {
    EXPECT_EQ(StringVal("ac"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal("a"), StringVal("c")));

    EXPECT_EQ(StringVal("c"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal("c"), StringVal("c")));

    EXPECT_EQ(StringVal("123c"), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("123c"), StringVal("c")));

    EXPECT_EQ(StringVal("c"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal(""), StringVal("c")));

    EXPECT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal::null(), StringVal("c")));

    EXPECT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("a"), StringVal::null()));

    EXPECT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("a"), StringVal("abc")));
}

TEST_F(StringFunctionsTest, instr) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    EXPECT_EQ(IntVal(4), StringFunctions::instr(context, StringVal("foobarbar"), StringVal("bar")));
    EXPECT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("foobar"), StringVal("xbar")));
    EXPECT_EQ(IntVal(2), StringFunctions::instr(context, StringVal("123456234"), StringVal("234")));
    EXPECT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("123456"), StringVal("567")));
    EXPECT_EQ(IntVal(2), StringFunctions::instr(context, StringVal("1.234"), StringVal(".234")));
    EXPECT_EQ(IntVal(1), StringFunctions::instr(context, StringVal("1.234"), StringVal("")));
    EXPECT_EQ(IntVal(0), StringFunctions::instr(context, StringVal(""), StringVal("123")));
    EXPECT_EQ(IntVal(1), StringFunctions::instr(context, StringVal(""), StringVal("")));
    EXPECT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好世界"), StringVal("世界")));
    EXPECT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("你好世界"), StringVal("您好")));
    EXPECT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好abc"), StringVal("a")));
    EXPECT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好abc"), StringVal("abc")));
    EXPECT_EQ(IntVal::null(), StringFunctions::instr(context, StringVal::null(), StringVal("2")));
    EXPECT_EQ(IntVal::null(), StringFunctions::instr(context, StringVal(""), StringVal::null()));
    EXPECT_EQ(IntVal::null(),
              StringFunctions::instr(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, locate) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    EXPECT_EQ(IntVal(4),
              StringFunctions::locate(context, StringVal("bar"), StringVal("foobarbar")));
    EXPECT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("xbar"), StringVal("foobar")));
    EXPECT_EQ(IntVal(2),
              StringFunctions::locate(context, StringVal("234"), StringVal("123456234")));
    EXPECT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("567"), StringVal("123456")));
    EXPECT_EQ(IntVal(2), StringFunctions::locate(context, StringVal(".234"), StringVal("1.234")));
    EXPECT_EQ(IntVal(1), StringFunctions::locate(context, StringVal(""), StringVal("1.234")));
    EXPECT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("123"), StringVal("")));
    EXPECT_EQ(IntVal(1), StringFunctions::locate(context, StringVal(""), StringVal("")));
    EXPECT_EQ(IntVal(3),
              StringFunctions::locate(context, StringVal("世界"), StringVal("你好世界")));
    EXPECT_EQ(IntVal(0),
              StringFunctions::locate(context, StringVal("您好"), StringVal("你好世界")));
    EXPECT_EQ(IntVal(3), StringFunctions::locate(context, StringVal("a"), StringVal("你好abc")));
    EXPECT_EQ(IntVal(3), StringFunctions::locate(context, StringVal("abc"), StringVal("你好abc")));
    EXPECT_EQ(IntVal::null(), StringFunctions::locate(context, StringVal::null(), StringVal("2")));
    EXPECT_EQ(IntVal::null(), StringFunctions::locate(context, StringVal(""), StringVal::null()));
    EXPECT_EQ(IntVal::null(),
              StringFunctions::locate(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, locate_pos) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    EXPECT_EQ(IntVal(7), StringFunctions::locate_pos(context, StringVal("bar"),
                                                     StringVal("foobarbar"), IntVal(5)));
    EXPECT_EQ(IntVal(0), StringFunctions::locate_pos(context, StringVal("xbar"),
                                                     StringVal("foobar"), IntVal(1)));
    EXPECT_EQ(IntVal(2),
              StringFunctions::locate_pos(context, StringVal(""), StringVal("foobar"), IntVal(2)));
    EXPECT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal("foobar"), StringVal(""), IntVal(1)));
    EXPECT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal(""), StringVal(""), IntVal(2)));
    EXPECT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal("A"), StringVal("AAAAAA"), IntVal(0)));
    EXPECT_EQ(IntVal(0), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(0)));
    EXPECT_EQ(IntVal(2), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(1)));
    EXPECT_EQ(IntVal(2), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(2)));
    EXPECT_EQ(IntVal(5), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(3)));
    EXPECT_EQ(IntVal(7), StringFunctions::locate_pos(context, StringVal("BaR"),
                                                     StringVal("foobarBaR"), IntVal(5)));
    EXPECT_EQ(IntVal::null(),
              StringFunctions::locate_pos(context, StringVal::null(), StringVal("2"), IntVal(1)));
    EXPECT_EQ(IntVal::null(),
              StringFunctions::locate_pos(context, StringVal(""), StringVal::null(), IntVal(4)));
    EXPECT_EQ(IntVal::null(), StringFunctions::locate_pos(context, StringVal::null(),
                                                          StringVal::null(), IntVal(4)));
    EXPECT_EQ(IntVal::null(), StringFunctions::locate_pos(context, StringVal::null(),
                                                          StringVal::null(), IntVal(-1)));
    delete context;
}

TEST_F(StringFunctionsTest, lpad) {
    EXPECT_EQ(StringVal("???hi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("?")));
    EXPECT_EQ(StringVal("g8%7IgY%AHx7luNtf8Kh"),
              StringFunctions::lpad(ctx, StringVal("g8%7IgY%AHx7luNtf8Kh"), IntVal(20),
                                    StringVal("")));
    EXPECT_EQ(StringVal("h"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal("你"),
              StringFunctions::lpad(ctx, StringVal("你好"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal("你"),
              StringFunctions::lpad(ctx, StringVal("你"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal(""),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(0), StringVal("?")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(-1), StringVal("?")));
    EXPECT_EQ(StringVal("h"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(1), StringVal("")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("")));
    EXPECT_EQ(StringVal("abahi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("ab")));
    EXPECT_EQ(StringVal("ababhi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(6), StringVal("ab")));
    EXPECT_EQ(StringVal("呵呵呵hi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("呵呵")));
    EXPECT_EQ(StringVal("hih呵呵"),
              StringFunctions::lpad(ctx, StringVal("呵呵"), IntVal(5), StringVal("hi")));
}

TEST_F(StringFunctionsTest, rpad) {
    EXPECT_EQ(StringVal("hi???"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("?")));
    EXPECT_EQ(StringVal("g8%7IgY%AHx7luNtf8Kh"),
              StringFunctions::rpad(ctx, StringVal("g8%7IgY%AHx7luNtf8Kh"), IntVal(20),
                                    StringVal("")));
    EXPECT_EQ(StringVal("h"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal("你"),
              StringFunctions::rpad(ctx, StringVal("你好"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal("你"),
              StringFunctions::rpad(ctx, StringVal("你"), IntVal(1), StringVal("?")));
    EXPECT_EQ(StringVal(""),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(0), StringVal("?")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(-1), StringVal("?")));
    EXPECT_EQ(StringVal("h"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(1), StringVal("")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("")));
    EXPECT_EQ(StringVal("hiaba"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("ab")));
    EXPECT_EQ(StringVal("hiabab"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(6), StringVal("ab")));
    EXPECT_EQ(StringVal("hi呵呵呵"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("呵呵")));
    EXPECT_EQ(StringVal("呵呵hih"),
              StringFunctions::rpad(ctx, StringVal("呵呵"), IntVal(5), StringVal("hi")));
}

TEST_F(StringFunctionsTest, replace) {
    //exist substring
    EXPECT_EQ(StringVal("http://www.baidu.com:8080"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9090"), StringVal("8080")));

    //not exist substring
    EXPECT_EQ(StringVal("http://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9070"), StringVal("8080")));

    //old substring is empty
    EXPECT_EQ(StringVal("http://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"), StringVal(""),
                                       StringVal("8080")));

    //new substring is empty
    EXPECT_EQ(StringVal("http://www.baidu.com:"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9090"), StringVal("")));

    //origin string is null
    EXPECT_EQ(StringVal::null(), StringFunctions::replace(ctx, StringVal::null(),
                                                          StringVal("hello"), StringVal("8080")));

    //old substring is null
    EXPECT_EQ(StringVal::null(),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal::null(), StringVal("8080")));

    //new substring is null
    EXPECT_EQ(StringVal::null(),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("hello"), StringVal::null()));

    //substring contains Chinese character
    EXPECT_EQ(StringVal("http://华夏zhongguo:9090"),
              StringFunctions::replace(ctx, StringVal("http://中国hello:9090"),
                                       StringVal("中国hello"), StringVal("华夏zhongguo")));

    //old substring is at the beginning of string
    EXPECT_EQ(StringVal("ftp://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("http"), StringVal("ftp")));
}

TEST_F(StringFunctionsTest, parse_url) {
    EXPECT_EQ(StringVal("facebook.com"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("AUTHORITY")));
    EXPECT_EQ(StringVal("facebook.com"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("authority")));

    EXPECT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("FILE")));
    EXPECT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("file")));

    EXPECT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("PATH")));
    EXPECT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("path")));

    EXPECT_EQ(StringVal("www.baidu.com"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090"),
                                         StringVal("HOST")));
    EXPECT_EQ(StringVal("www.baidu.com"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090"),
                                         StringVal("host")));

    EXPECT_EQ(StringVal("http"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("PROTOCOL")));
    EXPECT_EQ(StringVal("http"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("protocol")));

    EXPECT_EQ(StringVal("a=b"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("QUERY")));
    EXPECT_EQ(StringVal("a=b"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("query")));

    EXPECT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("REF")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("ref")));

    EXPECT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("USERINFO")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("userinfo")));

    EXPECT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("PORT")));
    EXPECT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c?a=b"),
                                         StringVal("PORT")));
    EXPECT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com?a=b"),
                                         StringVal("PORT")));
    EXPECT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("port")));
}

TEST_F(StringFunctionsTest, bit_length) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(IntVal(40), StringFunctions::bit_length(context, StringVal("hello")));

    EXPECT_EQ(IntVal::null(), StringFunctions::bit_length(context, StringVal::null()));

    EXPECT_EQ(IntVal(0), StringFunctions::bit_length(context, StringVal("")));

    EXPECT_EQ(IntVal(88), StringFunctions::bit_length(context, StringVal("hello你好")));

    delete context;
}

TEST_F(StringFunctionsTest, lower) {
    EXPECT_EQ(StringVal("hello"), StringFunctions::lower(ctx, StringVal("hello")));
    EXPECT_EQ(StringVal("hello"), StringFunctions::lower(ctx, StringVal("HELLO")));
    EXPECT_EQ(StringVal("hello123"), StringFunctions::lower(ctx, StringVal("HELLO123")));
    EXPECT_EQ(StringVal("hello, 123"), StringFunctions::lower(ctx, StringVal("HELLO, 123")));
    EXPECT_EQ(StringVal::null(), StringFunctions::lower(ctx, StringVal::null()));
    EXPECT_EQ(StringVal(""), StringFunctions::lower(ctx, StringVal("")));
}

TEST_F(StringFunctionsTest, upper) {
    // function test
    EXPECT_EQ(StringVal("HELLO"), StringFunctions::upper(ctx, StringVal("HELLO")));
    EXPECT_EQ(StringVal("HELLO"), StringFunctions::upper(ctx, StringVal("hello")));
    EXPECT_EQ(StringVal("HELLO123"), StringFunctions::upper(ctx, StringVal("hello123")));
    EXPECT_EQ(StringVal("HELLO, 123"), StringFunctions::upper(ctx, StringVal("hello, 123")));
    EXPECT_EQ(StringVal::null(), StringFunctions::upper(ctx, StringVal::null()));
    EXPECT_EQ(StringVal(""), StringFunctions::upper(ctx, StringVal("")));
}

TEST_F(StringFunctionsTest, ltrim) {
    // no blank
    StringVal src("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    StringVal res = simd::VStringFunctions::ltrim(src);
    EXPECT_EQ(src, res);
    // empty string
    StringVal src1("");
    res = simd::VStringFunctions::ltrim(src1);
    EXPECT_EQ(src1, res);
    // null string
    StringVal src2(StringVal::null());
    res = simd::VStringFunctions::ltrim(src2);
    EXPECT_EQ(src2, res);
    // less than 16 blanks
    StringVal src3("       hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    res = simd::VStringFunctions::ltrim(src3);
    EXPECT_EQ(src, res);
    // more than 16 blanks
    StringVal src4("                   hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    res = simd::VStringFunctions::ltrim(src4);
    EXPECT_EQ(src, res);
    // all are blanks, less than 16 blanks
    StringVal src5("       ");
    res = simd::VStringFunctions::ltrim(src5);
    EXPECT_EQ(StringVal(""), res);
    // all are blanks, more than 16 blanks
    StringVal src6("                  ");
    res = simd::VStringFunctions::ltrim(src6);
    EXPECT_EQ(StringVal(""), res);
    // src less than 16 length
    StringVal src7(" 12345678910");
    res = simd::VStringFunctions::ltrim(src7);
    EXPECT_EQ(StringVal("12345678910"), res);
}

TEST_F(StringFunctionsTest, rtrim) {
    // no blank
    StringVal src("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    StringVal res = simd::VStringFunctions::rtrim(src);
    EXPECT_EQ(src, res);
    // empty string
    StringVal src1("");
    res = simd::VStringFunctions::rtrim(src1);
    EXPECT_EQ(src1, res);
    // null string
    StringVal src2(StringVal::null());
    res = simd::VStringFunctions::rtrim(src2);
    EXPECT_EQ(src2, res);
    // less than 16 blanks
    StringVal src3("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa       ");
    res = simd::VStringFunctions::rtrim(src3);
    EXPECT_EQ(src, res);
    // more than 16 blanks
    StringVal src4("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                      ");
    res = simd::VStringFunctions::rtrim(src4);
    EXPECT_EQ(src, res);
    // all are blanks, less than 16 blanks
    StringVal src5("       ");
    res = simd::VStringFunctions::rtrim(src5);
    EXPECT_EQ(StringVal(""), res);
    // all are blanks, more than 16 blanks
    StringVal src6("                  ");
    res = simd::VStringFunctions::rtrim(src6);
    EXPECT_EQ(StringVal(""), res);
    // src less than 16 length
    StringVal src7("12345678910 ");
    res = simd::VStringFunctions::rtrim(src7);
    EXPECT_EQ(StringVal("12345678910"), res);
}

TEST_F(StringFunctionsTest, is_ascii) {
    EXPECT_EQ(true, simd::VStringFunctions::is_ascii(StringVal("hello123")));
    EXPECT_EQ(true, simd::VStringFunctions::is_ascii(
                            StringVal("hello123fwrewerwerwerwrsfqrwerwefwfwrwfsfwe")));
    EXPECT_EQ(false, simd::VStringFunctions::is_ascii(StringVal("运维组123")));
    EXPECT_EQ(false, simd::VStringFunctions::is_ascii(
                             StringVal("hello123运维组fwrewerwerwerwrsfqrwerwefwfwrwfsfwe")));
    EXPECT_EQ(true, simd::VStringFunctions::is_ascii(StringVal::null()));
    EXPECT_EQ(true, simd::VStringFunctions::is_ascii(StringVal("")));
}
} // namespace doris
