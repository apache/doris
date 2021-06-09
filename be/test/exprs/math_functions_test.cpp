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
    ASSERT_EQ(fv1, MathFunctions::abs(ctx, FloatVal(0.0)));
    ASSERT_EQ(fv1, MathFunctions::abs(ctx, FloatVal(-0.0)));
    ASSERT_EQ(fv2, MathFunctions::abs(ctx, FloatVal(0.1)));
    ASSERT_EQ(fv2, MathFunctions::abs(ctx, FloatVal(-0.1)));
    ASSERT_EQ(fv3, MathFunctions::abs(ctx, FloatVal(FLT_MAX)));
    ASSERT_EQ(fv3, MathFunctions::abs(ctx, FloatVal(-FLT_MAX)));
    ASSERT_EQ(fv4, MathFunctions::abs(ctx, FloatVal(FLT_MIN)));
    ASSERT_EQ(fv4, MathFunctions::abs(ctx, FloatVal(-FLT_MIN)));

    // DoubleVal
    DoubleVal dv1(0.0);
    DoubleVal dv2(0.1);
    DoubleVal dv3(DBL_MAX);
    DoubleVal dv4(DBL_MIN);
    ASSERT_EQ(dv1, MathFunctions::abs(ctx, DoubleVal(0.0)));
    ASSERT_EQ(dv1, MathFunctions::abs(ctx, DoubleVal(-0.0)));
    ASSERT_EQ(dv2, MathFunctions::abs(ctx, DoubleVal(0.1)));
    ASSERT_EQ(dv2, MathFunctions::abs(ctx, DoubleVal(-0.1)));
    ASSERT_EQ(dv3, MathFunctions::abs(ctx, DoubleVal(DBL_MAX)));
    ASSERT_EQ(dv3, MathFunctions::abs(ctx, DoubleVal(-DBL_MAX)));
    ASSERT_EQ(dv4, MathFunctions::abs(ctx, DoubleVal(DBL_MIN)));
    ASSERT_EQ(dv4, MathFunctions::abs(ctx, DoubleVal(-DBL_MIN)));

    // LargeIntVal
    LargeIntVal liv1(0);
    LargeIntVal liv2(1);
    LargeIntVal liv3(MAX_INT128);
    LargeIntVal liv4(__int128(INT64_MAX));
    LargeIntVal liv5(-__int128(INT64_MIN));

    ASSERT_EQ(liv1, MathFunctions::abs(ctx, LargeIntVal(0)));
    ASSERT_EQ(liv1, MathFunctions::abs(ctx, LargeIntVal(-0)));
    ASSERT_EQ(liv2, MathFunctions::abs(ctx, LargeIntVal(1)));
    ASSERT_EQ(liv2, MathFunctions::abs(ctx, LargeIntVal(-1)));
    ASSERT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(MAX_INT128)));
    ASSERT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(-MAX_INT128)));
    ASSERT_EQ(liv3, MathFunctions::abs(ctx, LargeIntVal(MIN_INT128 + 1)));
    // BigIntVal
    ASSERT_EQ(liv1, MathFunctions::abs(ctx, BigIntVal(0)));
    ASSERT_EQ(liv1, MathFunctions::abs(ctx, BigIntVal(-0)));
    ASSERT_EQ(liv2, MathFunctions::abs(ctx, BigIntVal(1)));
    ASSERT_EQ(liv2, MathFunctions::abs(ctx, BigIntVal(-1)));
    ASSERT_EQ(liv4, MathFunctions::abs(ctx, BigIntVal(INT64_MAX)));
    ASSERT_EQ(liv5, MathFunctions::abs(ctx, BigIntVal(INT64_MIN)));

    // IntVal
    BigIntVal biv1(0);
    BigIntVal biv2(1);
    BigIntVal biv3(int64_t(INT32_MAX));
    BigIntVal biv4(-int64_t(INT32_MIN));

    ASSERT_EQ(biv1, MathFunctions::abs(ctx, IntVal(0)));
    ASSERT_EQ(biv1, MathFunctions::abs(ctx, IntVal(-0)));
    ASSERT_EQ(biv2, MathFunctions::abs(ctx, IntVal(1)));
    ASSERT_EQ(biv2, MathFunctions::abs(ctx, IntVal(-1)));
    ASSERT_EQ(biv3, MathFunctions::abs(ctx, IntVal(INT32_MAX)));
    ASSERT_EQ(biv4, MathFunctions::abs(ctx, IntVal(INT32_MIN)));

    // SmallIntVal
    IntVal iv1(0);
    IntVal iv2(1);
    IntVal iv3(int32_t(INT16_MAX));
    IntVal iv4(-int32_t(INT16_MIN));
    ASSERT_EQ(iv1, MathFunctions::abs(ctx, SmallIntVal(0)));
    ASSERT_EQ(iv1, MathFunctions::abs(ctx, SmallIntVal(-0)));
    ASSERT_EQ(iv2, MathFunctions::abs(ctx, SmallIntVal(1)));
    ASSERT_EQ(iv2, MathFunctions::abs(ctx, SmallIntVal(-1)));
    ASSERT_EQ(iv3, MathFunctions::abs(ctx, SmallIntVal(INT16_MAX)));
    ASSERT_EQ(iv4, MathFunctions::abs(ctx, SmallIntVal(INT16_MIN)));

    //TinyIntVal
    SmallIntVal siv1(0);
    SmallIntVal siv2(1);
    SmallIntVal siv3(int16_t(INT8_MAX));
    SmallIntVal siv4(-int16_t(INT8_MIN));
    ASSERT_EQ(siv1, MathFunctions::abs(ctx, TinyIntVal(0)));
    ASSERT_EQ(siv1, MathFunctions::abs(ctx, TinyIntVal(-0)));
    ASSERT_EQ(siv2, MathFunctions::abs(ctx, TinyIntVal(1)));
    ASSERT_EQ(siv2, MathFunctions::abs(ctx, TinyIntVal(-1)));
    ASSERT_EQ(siv3, MathFunctions::abs(ctx, TinyIntVal(INT8_MAX)));
    ASSERT_EQ(siv4, MathFunctions::abs(ctx, TinyIntVal(INT8_MIN)));
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

    ASSERT_EQ(dv1.val, dv2.val);
    delete utils1;

    MathFunctions::rand_prepare(ctx, FunctionContext::THREAD_LOCAL);
    DoubleVal dv3 = MathFunctions::rand(ctx);
    MathFunctions::rand_close(ctx, FunctionContext::THREAD_LOCAL);

    MathFunctions::rand_prepare(ctx, FunctionContext::THREAD_LOCAL);
    DoubleVal dv4 = MathFunctions::rand(ctx);
    MathFunctions::rand_close(ctx, FunctionContext::THREAD_LOCAL);

    ASSERT_NE(dv3.val, dv4.val);
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