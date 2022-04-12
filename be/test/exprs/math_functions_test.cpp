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

#include "exprs/math_functions.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "testutil/function_utils.h"
#include "testutil/test_util.h"
#include "util/logging.h"

namespace doris {

class MathFunctionsTest : public testing::Test {
public:
    MathFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(MathFunctionsTest, abs) {
    // FloatVal
    FloatVal fv1(0.0f);
    FloatVal fv2(0.1f);
    FloatVal fv3(FLT_MAX);
    FloatVal fv4(FLT_MIN);
    EXPECT_EQ(fv1, MathFunctions::abs(ctx, FloatVal(0.0)));
    EXPECT_EQ(fv1, MathFunctions::abs(ctx, FloatVal(-0.0)));
    EXPECT_EQ(fv2, MathFunctions::abs(ctx, FloatVal(0.1)));
    EXPECT_EQ(fv2, MathFunctions::abs(ctx, FloatVal(-0.1)));
    EXPECT_EQ(fv3, MathFunctions::abs(ctx, FloatVal(FLT_MAX)));
    EXPECT_EQ(fv3, MathFunctions::abs(ctx, FloatVal(-FLT_MAX)));
    EXPECT_EQ(fv4, MathFunctions::abs(ctx, FloatVal(FLT_MIN)));
    EXPECT_EQ(fv4, MathFunctions::abs(ctx, FloatVal(-FLT_MIN)));

    // DoubleVal
    DoubleVal dv1(0.0);
    DoubleVal dv2(0.1);
    DoubleVal dv3(DBL_MAX);
    DoubleVal dv4(DBL_MIN);
    EXPECT_EQ(dv1, MathFunctions::abs(ctx, DoubleVal(0.0)));
    EXPECT_EQ(dv1, MathFunctions::abs(ctx, DoubleVal(-0.0)));
    EXPECT_EQ(dv2, MathFunctions::abs(ctx, DoubleVal(0.1)));
    EXPECT_EQ(dv2, MathFunctions::abs(ctx, DoubleVal(-0.1)));
    EXPECT_EQ(dv3, MathFunctions::abs(ctx, DoubleVal(DBL_MAX)));
    EXPECT_EQ(dv3, MathFunctions::abs(ctx, DoubleVal(-DBL_MAX)));
    EXPECT_EQ(dv4, MathFunctions::abs(ctx, DoubleVal(DBL_MIN)));
    EXPECT_EQ(dv4, MathFunctions::abs(ctx, DoubleVal(-DBL_MIN)));

    // LargeIntVal
    LargeIntVal liv1(0);
    LargeIntVal liv2(1);
    LargeIntVal liv3(MAX_INT128);
    LargeIntVal liv4(__int128(INT64_MAX));
    LargeIntVal liv5(-__int128(INT64_MIN));

    EXPECT_EQ(liv1, MathFunctions::abs(ctx, LargeIntVal(0)));
    EXPECT_EQ(liv1, MathFunctions::abs(ctx, LargeIntVal(-0)));
    EXPECT_EQ(liv2, MathFunctions::abs(ctx, LargeIntVal(1)));
    EXPECT_EQ(liv2, MathFunctions::abs(ctx, LargeIntVal(-1)));
    EXPECT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(MAX_INT128)));
    EXPECT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(-MAX_INT128)));
    EXPECT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(MIN_INT128 + 1)));
    // BigIntVal
    EXPECT_EQ(liv1, MathFunctions::abs(ctx, BigIntVal(0)));
    EXPECT_EQ(liv1, MathFunctions::abs(ctx, BigIntVal(-0)));
    EXPECT_EQ(liv2, MathFunctions::abs(ctx, BigIntVal(1)));
    EXPECT_EQ(liv2, MathFunctions::abs(ctx, BigIntVal(-1)));
    EXPECT_EQ(liv4, MathFunctions::abs(ctx, BigIntVal(INT64_MAX)));
    EXPECT_EQ(liv5, MathFunctions::abs(ctx, BigIntVal(INT64_MIN)));

    // IntVal
    BigIntVal biv1(0);
    BigIntVal biv2(1);
    BigIntVal biv3(int64_t(INT32_MAX));
    BigIntVal biv4(-int64_t(INT32_MIN));

    EXPECT_EQ(biv1, MathFunctions::abs(ctx, IntVal(0)));
    EXPECT_EQ(biv1, MathFunctions::abs(ctx, IntVal(-0)));
    EXPECT_EQ(biv2, MathFunctions::abs(ctx, IntVal(1)));
    EXPECT_EQ(biv2, MathFunctions::abs(ctx, IntVal(-1)));
    EXPECT_EQ(biv3, MathFunctions::abs(ctx, IntVal(INT32_MAX)));
    EXPECT_EQ(biv4, MathFunctions::abs(ctx, IntVal(INT32_MIN)));

    // SmallIntVal
    IntVal iv1(0);
    IntVal iv2(1);
    IntVal iv3(int32_t(INT16_MAX));
    IntVal iv4(-int32_t(INT16_MIN));
    EXPECT_EQ(iv1, MathFunctions::abs(ctx, SmallIntVal(0)));
    EXPECT_EQ(iv1, MathFunctions::abs(ctx, SmallIntVal(-0)));
    EXPECT_EQ(iv2, MathFunctions::abs(ctx, SmallIntVal(1)));
    EXPECT_EQ(iv2, MathFunctions::abs(ctx, SmallIntVal(-1)));
    EXPECT_EQ(iv3, MathFunctions::abs(ctx, SmallIntVal(INT16_MAX)));
    EXPECT_EQ(iv4, MathFunctions::abs(ctx, SmallIntVal(INT16_MIN)));

    //TinyIntVal
    SmallIntVal siv1(0);
    SmallIntVal siv2(1);
    SmallIntVal siv3(int16_t(INT8_MAX));
    SmallIntVal siv4(-int16_t(INT8_MIN));
    EXPECT_EQ(siv1, MathFunctions::abs(ctx, TinyIntVal(0)));
    EXPECT_EQ(siv1, MathFunctions::abs(ctx, TinyIntVal(-0)));
    EXPECT_EQ(siv2, MathFunctions::abs(ctx, TinyIntVal(1)));
    EXPECT_EQ(siv2, MathFunctions::abs(ctx, TinyIntVal(-1)));
    EXPECT_EQ(siv3, MathFunctions::abs(ctx, TinyIntVal(INT8_MAX)));
    EXPECT_EQ(siv4, MathFunctions::abs(ctx, TinyIntVal(INT8_MIN)));
}

TEST_F(MathFunctionsTest, rand) {
    doris_udf::FunctionContext::TypeDesc type;
    type.type = doris_udf::FunctionContext::TYPE_DOUBLE;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    doris_udf::FunctionContext::TypeDesc type1;
    type1.type = doris_udf::FunctionContext::TYPE_BIGINT;
    arg_types.push_back(type1);
    FunctionUtils* utils1 = new FunctionUtils(type, arg_types, 8);
    FunctionContext* ctx1 = utils1->get_fn_ctx();
    std::vector<doris_udf::AnyVal*> constant_args;
    BigIntVal bi(1);
    constant_args.push_back(&bi);
    ctx1->impl()->set_constant_args(constant_args);

    MathFunctions::rand_prepare(ctx1, FunctionContext::THREAD_LOCAL);
    DoubleVal dv1 = MathFunctions::rand_seed(ctx1, BigIntVal(0));
    MathFunctions::rand_close(ctx1, FunctionContext::THREAD_LOCAL);

    MathFunctions::rand_prepare(ctx1, FunctionContext::THREAD_LOCAL);
    DoubleVal dv2 = MathFunctions::rand_seed(ctx1, BigIntVal(0));
    MathFunctions::rand_close(ctx1, FunctionContext::THREAD_LOCAL);

    EXPECT_EQ(dv1.val, dv2.val);
    delete utils1;

    MathFunctions::rand_prepare(ctx, FunctionContext::THREAD_LOCAL);
    DoubleVal dv3 = MathFunctions::rand(ctx);
    MathFunctions::rand_close(ctx, FunctionContext::THREAD_LOCAL);

    MathFunctions::rand_prepare(ctx, FunctionContext::THREAD_LOCAL);
    DoubleVal dv4 = MathFunctions::rand(ctx);
    MathFunctions::rand_close(ctx, FunctionContext::THREAD_LOCAL);

    EXPECT_NE(dv3.val, dv4.val);
}

TEST_F(MathFunctionsTest, hex_int) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(StringVal::null(), MathFunctions::hex_string(context, StringVal::null()));

    EXPECT_EQ(
            StringVal("7FFFFFFFFFFFFFFF"),
            MathFunctions::hex_int(context, BigIntVal(9223372036854775807))); //BigIntVal max_value

    EXPECT_EQ(StringVal("FFE5853AB393E6C0"),
              MathFunctions::hex_int(context, BigIntVal(-7453337203775808)));

    EXPECT_EQ(StringVal("0"), MathFunctions::hex_int(context, BigIntVal(0)));

    EXPECT_EQ(StringVal("C"), MathFunctions::hex_int(context, BigIntVal(12)));

    EXPECT_EQ(StringVal("90"), MathFunctions::hex_int(context, BigIntVal(144)));

    EXPECT_EQ(StringVal("FFFFFFFFFFFFFFFF"), MathFunctions::hex_int(context, BigIntVal(-1)));

    EXPECT_EQ(StringVal("FFFFFFFFFFFFFFFE"), MathFunctions::hex_int(context, BigIntVal(-2)));

    EXPECT_EQ(StringVal("24EC1"), MathFunctions::hex_int(context, BigIntVal(151233)));

    delete context;
}

TEST_F(MathFunctionsTest, hex_string) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(StringVal::null(), MathFunctions::hex_string(context, StringVal::null()));

    EXPECT_EQ(StringVal("30"), MathFunctions::hex_string(context, StringVal("0")));

    EXPECT_EQ(StringVal("31"), MathFunctions::hex_string(context, StringVal("1")));

    EXPECT_EQ(StringVal("313233"), MathFunctions::hex_string(context, StringVal("123")));

    EXPECT_EQ(StringVal("41"), MathFunctions::hex_string(context, StringVal("A")));

    EXPECT_EQ(StringVal("61"), MathFunctions::hex_string(context, StringVal("a")));

    EXPECT_EQ(StringVal("E68891"), MathFunctions::hex_string(context, StringVal("我")));

    EXPECT_EQ(StringVal("3F"), MathFunctions::hex_string(context, StringVal("?")));

    delete context;
}

TEST_F(MathFunctionsTest, unhex) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    EXPECT_EQ(StringVal::null(), MathFunctions::unhex(context, StringVal::null()));

    EXPECT_EQ(StringVal("123"), MathFunctions::unhex(context, StringVal("313233")));

    EXPECT_EQ(StringVal(""), MathFunctions::unhex(context, StringVal("@!#")));

    EXPECT_EQ(StringVal(""), MathFunctions::unhex(context, StringVal("@@")));

    EXPECT_EQ(StringVal("a"), MathFunctions::unhex(context, StringVal("61")));

    EXPECT_EQ(StringVal("123"), MathFunctions::unhex(context, StringVal("313233")));

    EXPECT_EQ(StringVal(""), MathFunctions::unhex(context, StringVal("我")));

    EXPECT_EQ(StringVal("？"), MathFunctions::unhex(context, StringVal("EFBC9F")));

    delete context;
}

TEST_F(MathFunctionsTest, round_up_to) {
    DoubleVal r0(0);
    DoubleVal r1(1);
    DoubleVal r2(3);
    DoubleVal r3(4);
    DoubleVal r4(3.5);
    DoubleVal r5(3.55);

    DoubleVal r6(222500);

    EXPECT_EQ(r0, MathFunctions::round_up_to(ctx, DoubleVal(0), IntVal(0)));
    EXPECT_EQ(r1, MathFunctions::round_up_to(ctx, DoubleVal(0.5), IntVal(0)));
    EXPECT_EQ(r1, MathFunctions::round_up_to(ctx, DoubleVal(0.51), IntVal(0)));
    // not 2
    EXPECT_EQ(r2, MathFunctions::round_up_to(ctx, DoubleVal(2.5), IntVal(0)));
    EXPECT_EQ(r3, MathFunctions::round_up_to(ctx, DoubleVal(3.5), IntVal(0)));

    EXPECT_EQ(r4, MathFunctions::round_up_to(ctx, DoubleVal(3.5451), IntVal(1)));
    EXPECT_EQ(r5, MathFunctions::round_up_to(ctx, DoubleVal(3.5451), IntVal(2)));

    // not 3.54
    EXPECT_EQ(r5, MathFunctions::round_up_to(ctx, DoubleVal(3.5450), IntVal(2)));

    // not 222400
    EXPECT_EQ(r6, MathFunctions::round_up_to(ctx, DoubleVal(222450.00), IntVal(-2)));
}

} // namespace doris
