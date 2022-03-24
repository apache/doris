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
#include "test_util/test_util.h"
#include "testutil/function_utils.h"
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
        ASSERT_EQ(expected, result);
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
        ASSERT_EQ(expected, result);
    }
    delete context;
}

TEST_F(StringFunctionsTest, money_format_bigint) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::BigIntVal(123456));
    StringVal expected = AnyValUtil::from_string(ctx, std::string("123,456.00"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(-123456));
    expected = AnyValUtil::from_string(ctx, std::string("-123,456.00"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::BigIntVal(9223372036854775807));
    expected = AnyValUtil::from_string(ctx, std::string("9,223,372,036,854,775,807.00"));
    ASSERT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, money_format_large_int) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    __int128 value = MAX_INT128;
    StringVal result = StringFunctions::money_format(context, doris_udf::LargeIntVal(value));
    StringVal expected = AnyValUtil::from_string_temp(
            context, std::string("170,141,183,460,469,231,731,687,303,715,884,105,727.00"));
    ASSERT_EQ(expected, result);

    value = MIN_INT128;
    result = StringFunctions::money_format(context, doris_udf::LargeIntVal(value));
    expected = AnyValUtil::from_string_temp(
            context, std::string("-170,141,183,460,469,231,731,687,303,715,884,105,728.00"));
    ASSERT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, money_format_double) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    StringVal result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.456));
    StringVal expected = AnyValUtil::from_string(ctx, std::string("1,234.46"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.45));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.45"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.4));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.40"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(1234.454));
    expected = AnyValUtil::from_string(ctx, std::string("1,234.45"));
    ASSERT_EQ(expected, result);

    result = StringFunctions::money_format(context, doris_udf::DoubleVal(-36854775807.039));
    expected = AnyValUtil::from_string(ctx, std::string("-36,854,775,807.04"));
    ASSERT_EQ(expected, result);

    delete context;
}

TEST_F(StringFunctionsTest, money_format_decimal_v2) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DecimalV2Value dv1(std::string("3333333333.2222222222"));
    DecimalV2Val value1;
    dv1.to_decimal_val(&value1);

    StringVal result = StringFunctions::money_format(context, value1);
    StringVal expected = AnyValUtil::from_string(ctx, std::string("3,333,333,333.22"));
    ASSERT_EQ(expected, result);

    DecimalV2Value dv2(std::string("-740740740.71604938271975308642"));
    DecimalV2Val value2;
    dv2.to_decimal_val(&value2);

    result = StringFunctions::money_format(context, value2);
    expected = AnyValUtil::from_string(ctx, std::string("-740,740,740.72"));
    ASSERT_EQ(expected, result);
    delete context;
}

TEST_F(StringFunctionsTest, split_part) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("hello")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 1));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("word")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 2));

    ASSERT_EQ(StringVal::null(),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal(" "), 3));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 1));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string(" word")),
              StringFunctions::split_part(context, StringVal("hello word"), StringVal("hello"), 2));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("2019年9")),
              StringFunctions::split_part(context, StringVal("2019年9月8日"), StringVal("月"), 1));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 1));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("bcd")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 2));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("bd")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 3));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::split_part(context, StringVal("abcdabda"), StringVal("a"), 4));

    ASSERT_EQ(
            AnyValUtil::from_string(ctx, std::string("#123")),
            StringFunctions::split_part(context, StringVal("abc###123###234"), StringVal("##"), 2));

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

    ASSERT_EQ(falseRet,
              StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("hello")));

    ASSERT_EQ(trueRet,
              StringFunctions::ends_with(context, StringVal("hello doris"), StringVal("doris")));

    ASSERT_EQ(trueRet, StringFunctions::ends_with(context, StringVal("hello doris"),
                                                  StringVal("hello doris")));

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

    ASSERT_EQ(trueRet,
              StringFunctions::starts_with(context, StringVal("hello"), StringVal("hello")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal("hello"), StringVal(" ")));

    ASSERT_EQ(falseRet, StringFunctions::starts_with(context, StringVal(" "), StringVal("world")));

    ASSERT_EQ(trueRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal("hello")));

    ASSERT_EQ(falseRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal("world")));

    ASSERT_EQ(trueRet, StringFunctions::starts_with(context, StringVal("hello world"),
                                                    StringVal("hello world")));

    ASSERT_EQ(nullRet,
              StringFunctions::starts_with(context, StringVal("hello world"), StringVal::null()));

    ASSERT_EQ(nullRet,
              StringFunctions::starts_with(context, StringVal::null(), StringVal("hello world")));

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

TEST_F(StringFunctionsTest, left) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::left(context, StringVal(""), 10));
    delete context;
}

TEST_F(StringFunctionsTest, substring) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word"), 0, 5));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("hello")),
              StringFunctions::substring(context, StringVal("hello word"), 1, 5));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("word")),
              StringFunctions::substring(context, StringVal("hello word"), 7, 4));

    ASSERT_EQ(StringVal::null(), StringFunctions::substring(context, StringVal::null(), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word"), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string(" word")),
              StringFunctions::substring(context, StringVal("hello word"), -5, 5));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("hello word 你")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 12));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("好")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 13, 1));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 0));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("rd 你好")),
              StringFunctions::substring(context, StringVal("hello word 你好"), -5, 5));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("h")),
              StringFunctions::substring(context, StringVal("hello word 你好"), 1, 1));
    delete context;
}

TEST_F(StringFunctionsTest, reverse) {
    FunctionUtils fu;
    doris_udf::FunctionContext* context = fu.get_fn_ctx();

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("olleh")),
              StringFunctions::reverse(context, StringVal("hello")));
    ASSERT_EQ(StringVal::null(), StringFunctions::reverse(context, StringVal::null()));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("")),
              StringFunctions::reverse(context, StringVal("")));

    ASSERT_EQ(AnyValUtil::from_string(ctx, std::string("好你olleh")),
              StringFunctions::reverse(context, StringVal("hello你好")));
}

TEST_F(StringFunctionsTest, length) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(IntVal(5), StringFunctions::length(context, StringVal("hello")));
    ASSERT_EQ(IntVal(5), StringFunctions::char_utf8_length(context, StringVal("hello")));
    ASSERT_EQ(IntVal::null(), StringFunctions::length(context, StringVal::null()));
    ASSERT_EQ(IntVal::null(), StringFunctions::char_utf8_length(context, StringVal::null()));

    ASSERT_EQ(IntVal(0), StringFunctions::length(context, StringVal("")));
    ASSERT_EQ(IntVal(0), StringFunctions::char_utf8_length(context, StringVal("")));

    ASSERT_EQ(IntVal(11), StringFunctions::length(context, StringVal("hello你好")));

    ASSERT_EQ(IntVal(7), StringFunctions::char_utf8_length(context, StringVal("hello你好")));
    delete context;
}

TEST_F(StringFunctionsTest, append_trailing_char_if_absent) {
    ASSERT_EQ(StringVal("ac"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal("a"), StringVal("c")));

    ASSERT_EQ(StringVal("c"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal("c"), StringVal("c")));

    ASSERT_EQ(StringVal("123c"), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("123c"), StringVal("c")));

    ASSERT_EQ(StringVal("c"),
              StringFunctions::append_trailing_char_if_absent(ctx, StringVal(""), StringVal("c")));

    ASSERT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal::null(), StringVal("c")));

    ASSERT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("a"), StringVal::null()));

    ASSERT_EQ(StringVal::null(), StringFunctions::append_trailing_char_if_absent(
                                         ctx, StringVal("a"), StringVal("abc")));
}

TEST_F(StringFunctionsTest, instr) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    ASSERT_EQ(IntVal(4), StringFunctions::instr(context, StringVal("foobarbar"), StringVal("bar")));
    ASSERT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("foobar"), StringVal("xbar")));
    ASSERT_EQ(IntVal(2), StringFunctions::instr(context, StringVal("123456234"), StringVal("234")));
    ASSERT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("123456"), StringVal("567")));
    ASSERT_EQ(IntVal(2), StringFunctions::instr(context, StringVal("1.234"), StringVal(".234")));
    ASSERT_EQ(IntVal(1), StringFunctions::instr(context, StringVal("1.234"), StringVal("")));
    ASSERT_EQ(IntVal(0), StringFunctions::instr(context, StringVal(""), StringVal("123")));
    ASSERT_EQ(IntVal(1), StringFunctions::instr(context, StringVal(""), StringVal("")));
    ASSERT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好世界"), StringVal("世界")));
    ASSERT_EQ(IntVal(0), StringFunctions::instr(context, StringVal("你好世界"), StringVal("您好")));
    ASSERT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好abc"), StringVal("a")));
    ASSERT_EQ(IntVal(3), StringFunctions::instr(context, StringVal("你好abc"), StringVal("abc")));
    ASSERT_EQ(IntVal::null(), StringFunctions::instr(context, StringVal::null(), StringVal("2")));
    ASSERT_EQ(IntVal::null(), StringFunctions::instr(context, StringVal(""), StringVal::null()));
    ASSERT_EQ(IntVal::null(),
              StringFunctions::instr(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, locate) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    ASSERT_EQ(IntVal(4),
              StringFunctions::locate(context, StringVal("bar"), StringVal("foobarbar")));
    ASSERT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("xbar"), StringVal("foobar")));
    ASSERT_EQ(IntVal(2),
              StringFunctions::locate(context, StringVal("234"), StringVal("123456234")));
    ASSERT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("567"), StringVal("123456")));
    ASSERT_EQ(IntVal(2), StringFunctions::locate(context, StringVal(".234"), StringVal("1.234")));
    ASSERT_EQ(IntVal(1), StringFunctions::locate(context, StringVal(""), StringVal("1.234")));
    ASSERT_EQ(IntVal(0), StringFunctions::locate(context, StringVal("123"), StringVal("")));
    ASSERT_EQ(IntVal(1), StringFunctions::locate(context, StringVal(""), StringVal("")));
    ASSERT_EQ(IntVal(3),
              StringFunctions::locate(context, StringVal("世界"), StringVal("你好世界")));
    ASSERT_EQ(IntVal(0),
              StringFunctions::locate(context, StringVal("您好"), StringVal("你好世界")));
    ASSERT_EQ(IntVal(3), StringFunctions::locate(context, StringVal("a"), StringVal("你好abc")));
    ASSERT_EQ(IntVal(3), StringFunctions::locate(context, StringVal("abc"), StringVal("你好abc")));
    ASSERT_EQ(IntVal::null(), StringFunctions::locate(context, StringVal::null(), StringVal("2")));
    ASSERT_EQ(IntVal::null(), StringFunctions::locate(context, StringVal(""), StringVal::null()));
    ASSERT_EQ(IntVal::null(),
              StringFunctions::locate(context, StringVal::null(), StringVal::null()));
    delete context;
}

TEST_F(StringFunctionsTest, locate_pos) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    ASSERT_EQ(IntVal(7), StringFunctions::locate_pos(context, StringVal("bar"),
                                                     StringVal("foobarbar"), IntVal(5)));
    ASSERT_EQ(IntVal(0), StringFunctions::locate_pos(context, StringVal("xbar"),
                                                     StringVal("foobar"), IntVal(1)));
    ASSERT_EQ(IntVal(2),
              StringFunctions::locate_pos(context, StringVal(""), StringVal("foobar"), IntVal(2)));
    ASSERT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal("foobar"), StringVal(""), IntVal(1)));
    ASSERT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal(""), StringVal(""), IntVal(2)));
    ASSERT_EQ(IntVal(0),
              StringFunctions::locate_pos(context, StringVal("A"), StringVal("AAAAAA"), IntVal(0)));
    ASSERT_EQ(IntVal(0), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(0)));
    ASSERT_EQ(IntVal(2), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(1)));
    ASSERT_EQ(IntVal(2), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(2)));
    ASSERT_EQ(IntVal(5), StringFunctions::locate_pos(context, StringVal("A"), StringVal("大A写的A"),
                                                     IntVal(3)));
    ASSERT_EQ(IntVal(7), StringFunctions::locate_pos(context, StringVal("BaR"),
                                                     StringVal("foobarBaR"), IntVal(5)));
    ASSERT_EQ(IntVal::null(),
              StringFunctions::locate_pos(context, StringVal::null(), StringVal("2"), IntVal(1)));
    ASSERT_EQ(IntVal::null(),
              StringFunctions::locate_pos(context, StringVal(""), StringVal::null(), IntVal(4)));
    ASSERT_EQ(IntVal::null(), StringFunctions::locate_pos(context, StringVal::null(),
                                                          StringVal::null(), IntVal(4)));
    ASSERT_EQ(IntVal::null(), StringFunctions::locate_pos(context, StringVal::null(),
                                                          StringVal::null(), IntVal(-1)));
    delete context;
}

TEST_F(StringFunctionsTest, lpad) {
    ASSERT_EQ(StringVal("???hi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("?")));
    ASSERT_EQ(StringVal("g8%7IgY%AHx7luNtf8Kh"),
              StringFunctions::lpad(ctx, StringVal("g8%7IgY%AHx7luNtf8Kh"), IntVal(20),
                                    StringVal("")));
    ASSERT_EQ(StringVal("h"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal("你"),
              StringFunctions::lpad(ctx, StringVal("你好"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal("你"),
              StringFunctions::lpad(ctx, StringVal("你"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal(""),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(0), StringVal("?")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(-1), StringVal("?")));
    ASSERT_EQ(StringVal("h"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(1), StringVal("")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("")));
    ASSERT_EQ(StringVal("abahi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("ab")));
    ASSERT_EQ(StringVal("ababhi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(6), StringVal("ab")));
    ASSERT_EQ(StringVal("呵呵呵hi"),
              StringFunctions::lpad(ctx, StringVal("hi"), IntVal(5), StringVal("呵呵")));
    ASSERT_EQ(StringVal("hih呵呵"),
              StringFunctions::lpad(ctx, StringVal("呵呵"), IntVal(5), StringVal("hi")));
}

TEST_F(StringFunctionsTest, rpad) {
    ASSERT_EQ(StringVal("hi???"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("?")));
    ASSERT_EQ(StringVal("g8%7IgY%AHx7luNtf8Kh"),
              StringFunctions::rpad(ctx, StringVal("g8%7IgY%AHx7luNtf8Kh"), IntVal(20),
                                    StringVal("")));
    ASSERT_EQ(StringVal("h"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal("你"),
              StringFunctions::rpad(ctx, StringVal("你好"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal("你"),
              StringFunctions::rpad(ctx, StringVal("你"), IntVal(1), StringVal("?")));
    ASSERT_EQ(StringVal(""),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(0), StringVal("?")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(-1), StringVal("?")));
    ASSERT_EQ(StringVal("h"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(1), StringVal("")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("")));
    ASSERT_EQ(StringVal("hiaba"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("ab")));
    ASSERT_EQ(StringVal("hiabab"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(6), StringVal("ab")));
    ASSERT_EQ(StringVal("hi呵呵呵"),
              StringFunctions::rpad(ctx, StringVal("hi"), IntVal(5), StringVal("呵呵")));
    ASSERT_EQ(StringVal("呵呵hih"),
              StringFunctions::rpad(ctx, StringVal("呵呵"), IntVal(5), StringVal("hi")));
}

TEST_F(StringFunctionsTest, replace) {
    //exist substring
    ASSERT_EQ(StringVal("http://www.baidu.com:8080"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9090"), StringVal("8080")));

    //not exist substring
    ASSERT_EQ(StringVal("http://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9070"), StringVal("8080")));

    //old substring is empty
    ASSERT_EQ(StringVal("http://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"), StringVal(""),
                                       StringVal("8080")));

    //new substring is empty
    ASSERT_EQ(StringVal("http://www.baidu.com:"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("9090"), StringVal("")));

    //origin string is null
    ASSERT_EQ(StringVal::null(), StringFunctions::replace(ctx, StringVal::null(),
                                                          StringVal("hello"), StringVal("8080")));

    //old substring is null
    ASSERT_EQ(StringVal::null(),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal::null(), StringVal("8080")));

    //new substring is null
    ASSERT_EQ(StringVal::null(),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("hello"), StringVal::null()));

    //substring contains Chinese character
    ASSERT_EQ(StringVal("http://华夏zhongguo:9090"),
              StringFunctions::replace(ctx, StringVal("http://中国hello:9090"),
                                       StringVal("中国hello"), StringVal("华夏zhongguo")));

    //old substring is at the beginning of string
    ASSERT_EQ(StringVal("ftp://www.baidu.com:9090"),
              StringFunctions::replace(ctx, StringVal("http://www.baidu.com:9090"),
                                       StringVal("http"), StringVal("ftp")));
}

TEST_F(StringFunctionsTest, parse_url) {
    ASSERT_EQ(StringVal("facebook.com"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("AUTHORITY")));
    ASSERT_EQ(StringVal("facebook.com"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("authority")));

    ASSERT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("FILE")));
    ASSERT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("file")));

    ASSERT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("PATH")));
    ASSERT_EQ(StringVal("/a/b/c.php"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c.php"),
                                         StringVal("path")));

    ASSERT_EQ(StringVal("www.baidu.com"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090"),
                                         StringVal("HOST")));
    ASSERT_EQ(StringVal("www.baidu.com"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090"),
                                         StringVal("host")));

    ASSERT_EQ(StringVal("http"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("PROTOCOL")));
    ASSERT_EQ(StringVal("http"),
              StringFunctions::parse_url(ctx, StringVal("http://facebook.com/path/p1.php?query=1"),
                                         StringVal("protocol")));

    ASSERT_EQ(StringVal("a=b"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("QUERY")));
    ASSERT_EQ(StringVal("a=b"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("query")));

    ASSERT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("REF")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("ref")));

    ASSERT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("USERINFO")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("userinfo")));

    ASSERT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("PORT")));
    ASSERT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090/a/b/c?a=b"),
                                         StringVal("PORT")));
    ASSERT_EQ(StringVal::null(),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com?a=b"),
                                         StringVal("PORT")));
    ASSERT_EQ(StringVal("9090"),
              StringFunctions::parse_url(ctx, StringVal("http://www.baidu.com:9090?a=b"),
                                         StringVal("port")));
}

TEST_F(StringFunctionsTest, bit_length) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(IntVal(40), StringFunctions::bit_length(context, StringVal("hello")));

    ASSERT_EQ(IntVal::null(), StringFunctions::bit_length(context, StringVal::null()));

    ASSERT_EQ(IntVal(0), StringFunctions::bit_length(context, StringVal("")));

    ASSERT_EQ(IntVal(88), StringFunctions::bit_length(context, StringVal("hello你好")));

    delete context;
}

TEST_F(StringFunctionsTest, lower) {
    ASSERT_EQ(StringVal("hello"), StringFunctions::lower(ctx, StringVal("hello")));
    ASSERT_EQ(StringVal("hello"), StringFunctions::lower(ctx, StringVal("HELLO")));
    ASSERT_EQ(StringVal("hello123"), StringFunctions::lower(ctx, StringVal("HELLO123")));
    ASSERT_EQ(StringVal("hello, 123"), StringFunctions::lower(ctx, StringVal("HELLO, 123")));
    ASSERT_EQ(StringVal::null(), StringFunctions::lower(ctx, StringVal::null()));
    ASSERT_EQ(StringVal(""), StringFunctions::lower(ctx, StringVal("")));
}

TEST_F(StringFunctionsTest, upper) {
    // function test
    ASSERT_EQ(StringVal("HELLO"), StringFunctions::upper(ctx, StringVal("HELLO")));
    ASSERT_EQ(StringVal("HELLO"), StringFunctions::upper(ctx, StringVal("hello")));
    ASSERT_EQ(StringVal("HELLO123"), StringFunctions::upper(ctx, StringVal("hello123")));
    ASSERT_EQ(StringVal("HELLO, 123"), StringFunctions::upper(ctx, StringVal("hello, 123")));
    ASSERT_EQ(StringVal::null(), StringFunctions::upper(ctx, StringVal::null()));
    ASSERT_EQ(StringVal(""), StringFunctions::upper(ctx, StringVal("")));
}

TEST_F(StringFunctionsTest, ltrim) {
    // no blank
    StringVal src("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    StringVal res = simd::VStringFunctions::ltrim(src);
    ASSERT_EQ(src, res);
    // empty string
    StringVal src1("");
    res = simd::VStringFunctions::ltrim(src1);
    ASSERT_EQ(src1, res);
    // null string
    StringVal src2(StringVal::null());
    res = simd::VStringFunctions::ltrim(src2);
    ASSERT_EQ(src2, res);
    // less than 16 blanks
    StringVal src3("       hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    res = simd::VStringFunctions::ltrim(src3);
    ASSERT_EQ(src, res);
    // more than 16 blanks
    StringVal src4("                   hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    res = simd::VStringFunctions::ltrim(src4);
    ASSERT_EQ(src, res);
    // all are blanks, less than 16 blanks
    StringVal src5("       ");
    res = simd::VStringFunctions::ltrim(src5);
    ASSERT_EQ(StringVal(""), res);
    // all are blanks, more than 16 blanks
    StringVal src6("                  ");
    res = simd::VStringFunctions::ltrim(src6);
    ASSERT_EQ(StringVal(""), res);
    // src less than 16 length
    StringVal src7(" 12345678910");
    res = simd::VStringFunctions::ltrim(src7);
    ASSERT_EQ(StringVal("12345678910"), res);
}

TEST_F(StringFunctionsTest, rtrim) {
    // no blank
    StringVal src("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    StringVal res = simd::VStringFunctions::rtrim(src);
    ASSERT_EQ(src, res);
    // empty string
    StringVal src1("");
    res = simd::VStringFunctions::rtrim(src1);
    ASSERT_EQ(src1, res);
    // null string
    StringVal src2(StringVal::null());
    res = simd::VStringFunctions::rtrim(src2);
    ASSERT_EQ(src2, res);
    // less than 16 blanks
    StringVal src3("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa       ");
    res = simd::VStringFunctions::rtrim(src3);
    ASSERT_EQ(src, res);
    // more than 16 blanks
    StringVal src4("hello worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                      ");
    res = simd::VStringFunctions::rtrim(src4);
    ASSERT_EQ(src, res);
    // all are blanks, less than 16 blanks
    StringVal src5("       ");
    res = simd::VStringFunctions::rtrim(src5);
    ASSERT_EQ(StringVal(""), res);
    // all are blanks, more than 16 blanks
    StringVal src6("                  ");
    res = simd::VStringFunctions::rtrim(src6);
    ASSERT_EQ(StringVal(""), res);
    // src less than 16 length
    StringVal src7("12345678910 ");
    res = simd::VStringFunctions::rtrim(src7);
    ASSERT_EQ(StringVal("12345678910"), res);
}

TEST_F(StringFunctionsTest, is_ascii) {
    ASSERT_EQ(true, simd::VStringFunctions::is_ascii(StringVal("hello123")));
    ASSERT_EQ(true, simd::VStringFunctions::is_ascii(
                            StringVal("hello123fwrewerwerwerwrsfqrwerwefwfwrwfsfwe")));
    ASSERT_EQ(false, simd::VStringFunctions::is_ascii(StringVal("运维组123")));
    ASSERT_EQ(false, simd::VStringFunctions::is_ascii(
                             StringVal("hello123运维组fwrewerwerwerwrsfqrwerwefwfwrwfsfwe")));
    ASSERT_EQ(true, simd::VStringFunctions::is_ascii(StringVal::null()));
    ASSERT_EQ(true, simd::VStringFunctions::is_ascii(StringVal("")));
}
} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
