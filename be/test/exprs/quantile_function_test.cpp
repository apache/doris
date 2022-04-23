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

#include "exprs/quantile_function.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "exprs/anyval_util.h"
#include "testutil/function_utils.h"
#include "udf/udf_internal.h"
#include "util/quantile_state.h"

namespace doris {
using DoubleQuantileState = QuantileState<double>;

StringVal convert_quantile_state_to_string(FunctionContext* ctx, DoubleQuantileState& state) {
    StringVal result(ctx, state.get_serialized_size());
    state.serialize(result.ptr);
    return result;
}

class QuantileStateFunctionsTest : public testing::Test {
public:
    QuantileStateFunctionsTest() = default;
    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(QuantileStateFunctionsTest, to_quantile_state) {
    FloatVal percentile = FloatVal(2048);
    std::vector<doris_udf::AnyVal*> constant_args;
    constant_args.push_back(nullptr);
    constant_args.push_back(&percentile);

    ctx->impl()->set_constant_args(constant_args);
    StringVal input = AnyValUtil::from_string_temp(ctx, std::to_string(5000));
    StringVal result = QuantileStateFunctions::to_quantile_state(ctx, input);
    float compression = 2048;
    DoubleQuantileState state(compression);
    state.add_value(5000);
    StringVal expected = convert_quantile_state_to_string(ctx, state);
    EXPECT_EQ(expected, result);
}

TEST_F(QuantileStateFunctionsTest, quantile_union) {
    StringVal dst;
    QuantileStateFunctions::quantile_state_init(ctx, &dst);
    DoubleQuantileState state1;
    state1.add_value(1);
    StringVal src1 = convert_quantile_state_to_string(ctx, state1);
    QuantileStateFunctions::quantile_union(ctx, src1, &dst);

    DoubleQuantileState state2;
    state2.add_value(2);
    StringVal src2 = convert_quantile_state_to_string(ctx, state2);
    QuantileStateFunctions::quantile_union(ctx, src2, &dst);

    DoubleQuantileState state3;
    state3.add_value(3);
    StringVal src3 = convert_quantile_state_to_string(ctx, state3);
    QuantileStateFunctions::quantile_union(ctx, src3, &dst);

    DoubleQuantileState state4;
    state4.add_value(4);
    StringVal src4 = convert_quantile_state_to_string(ctx, state4);
    QuantileStateFunctions::quantile_union(ctx, src4, &dst);

    DoubleQuantileState state5;
    state5.add_value(5);
    StringVal src5 = convert_quantile_state_to_string(ctx, state5);
    QuantileStateFunctions::quantile_union(ctx, src5, &dst);

    DoubleQuantileState expect;
    expect.add_value(1);
    expect.add_value(2);
    expect.add_value(3);
    expect.add_value(4);
    expect.add_value(5);

    StringVal result = QuantileStateFunctions::quantile_state_serialize(ctx, dst);
    StringVal expected = convert_quantile_state_to_string(ctx, expect);

    EXPECT_EQ(result, expected);
}

TEST_F(QuantileStateFunctionsTest, quantile_percent) {
    FloatVal percentile = FloatVal(0.5);
    std::vector<doris_udf::AnyVal*> constant_args;
    constant_args.push_back(nullptr);
    constant_args.push_back(&percentile);
    ctx->impl()->set_constant_args(constant_args);

    DoubleQuantileState state;
    state.add_value(1);
    state.add_value(2);
    state.add_value(3);
    state.add_value(4);
    state.add_value(5);
    StringVal input = convert_quantile_state_to_string(ctx, state);
    DoubleVal result = QuantileStateFunctions::quantile_percent(ctx, input);
    DoubleVal expected(3);
    EXPECT_EQ(result, expected);
}

} // namespace doris
