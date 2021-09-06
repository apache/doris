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

#include "exprs/aggregate_functions.h"
#include "testutil/function_utils.h"

namespace doris {

class PercentileTest : public testing::Test {
public:
    PercentileTest() {}
};

TEST_F(PercentileTest, testSample) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.9);

    StringVal stringVal1;
    BigIntVal int1(1);
    AggregateFunctions::percentile_init(context, &stringVal1);
    AggregateFunctions::percentile_update(context, int1, doubleQ, &stringVal1);
    BigIntVal int2(2);
    AggregateFunctions::percentile_update(context, int2, doubleQ, &stringVal1);

    StringVal s = AggregateFunctions::percentile_serialize(context, stringVal1);

    StringVal stringVal2;
    AggregateFunctions::percentile_init(context, &stringVal2);
    AggregateFunctions::percentile_merge(context, s, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_finalize(context, stringVal2);
    ASSERT_EQ(v.val, 1.9);
    delete futil;
}

TEST_F(PercentileTest, testNoMerge) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.9);

    StringVal stringVal1;
    BigIntVal val(1);
    AggregateFunctions::percentile_init(context, &stringVal1);
    AggregateFunctions::percentile_update(context, val, doubleQ, &stringVal1);
    BigIntVal val2(2);
    AggregateFunctions::percentile_update(context, val2, doubleQ, &stringVal1);

    DoubleVal v = AggregateFunctions::percentile_finalize(context, stringVal1);
    ASSERT_EQ(v.val, 1.9);
    delete futil;
}

TEST_F(PercentileTest, testSerialize) {
    FunctionUtils* futil = new FunctionUtils();
    doris_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.999);
    StringVal stringVal;
    AggregateFunctions::percentile_init(context, &stringVal);

    for (int i = 1; i <= 100000; i++) {
        BigIntVal val(i);
        AggregateFunctions::percentile_update(context, val, doubleQ, &stringVal);
    }
    StringVal serialized = AggregateFunctions::percentile_serialize(context, stringVal);

    // mock serialize
    StringVal stringVal2;
    AggregateFunctions::percentile_init(context, &stringVal2);
    AggregateFunctions::percentile_merge(context, serialized, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_finalize(context, stringVal2);
    ASSERT_DOUBLE_EQ(v.val, 99900.001);

    // merge init percentile stringVal3 should not change the correct result
    AggregateFunctions::percentile_init(context, &stringVal);

    for (int i = 1; i <= 100000; i++) {
        BigIntVal val(i);
        AggregateFunctions::percentile_update(context, val, doubleQ, &stringVal);
    }
    serialized = AggregateFunctions::percentile_serialize(context, stringVal);

    StringVal stringVal3;
    AggregateFunctions::percentile_init(context, &stringVal2);
    AggregateFunctions::percentile_init(context, &stringVal3);
    StringVal serialized2 = AggregateFunctions::percentile_serialize(context, stringVal3);

    AggregateFunctions::percentile_merge(context, serialized, &stringVal2);
    AggregateFunctions::percentile_merge(context, serialized2, &stringVal2);
    v = AggregateFunctions::percentile_finalize(context, stringVal2);
    ASSERT_DOUBLE_EQ(v.val, 99900.001);

    delete futil;
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
