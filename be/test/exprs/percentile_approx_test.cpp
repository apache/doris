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

#include "exprs/aggregate_functions.h"
#include <gtest/gtest.h>

namespace doris {

    class PercentileApproxTest : public testing::Test {
    public:
        PercentileApproxTest() { }
    };

    TEST_F(PercentileApproxTest, test1) {
        doris_udf::FunctionContext *context = new doris_udf::FunctionContext();
        DoubleVal doubleQ(0.9);
 
        StringVal stringVal1;
        BigIntVal int1(1);
        AggregateFunctions::percentile_approx_init(context, &stringVal1);
        AggregateFunctions::percentile_approx_update(context, int1, doubleQ, &stringVal1);
        BigIntVal int2(2);
        AggregateFunctions::percentile_approx_update(context, int2, doubleQ, &stringVal1);
        DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal1);
        ASSERT_EQ(v.val, 2);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
