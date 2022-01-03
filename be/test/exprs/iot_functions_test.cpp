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

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/iot_functions.h"
#include "testutil/function_utils.h"
#include "test_util/test_util.h"

#include <gtest/gtest.h>
#include <unordered_map>

namespace doris {

class IoTFunctionsTest : public testing::Test {
public:
    IoTFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }

    void TearDown() {
        delete utils;
    }

private:
    FunctionUtils *utils;
    FunctionContext *ctx;
};

TEST_F(IoTFunctionsTest, iot_first_last_update) {
    // first
    StringVal dst;
    dst.is_null = true;
    IoTFunctions::iot_first_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst);
    IoTFunctions::iot_first_update(ctx, BigIntVal(102), DoubleVal(2.2), &dst);
    IoTFunctions::iot_first_update(ctx, BigIntVal(100), DoubleVal(1.1), &dst);

    DoubleVal result = IoTFunctions::iot_first_last_finalize(ctx, dst);
    ASSERT_EQ(DoubleVal(1.1), result);

    // last
    StringVal dst2;
    dst2.is_null = true;
    IoTFunctions::iot_last_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst2);
    IoTFunctions::iot_last_update(ctx, BigIntVal(102), DoubleVal(2.2), &dst2);
    IoTFunctions::iot_last_update(ctx, BigIntVal(100), DoubleVal(1.1), &dst2);

    result = IoTFunctions::iot_first_last_finalize(ctx, dst2);
    ASSERT_EQ(DoubleVal(2.2), result);
}

TEST_F(IoTFunctionsTest, iot_first_last_merge) {
    // first
    StringVal dst1;
    dst1.is_null = true;
    IoTFunctions::iot_first_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst1);
    IoTFunctions::iot_first_update(ctx, BigIntVal(102), DoubleVal(2.2), &dst1);
    IoTFunctions::iot_first_update(ctx, BigIntVal(100), DoubleVal(1.1), &dst1);

    StringVal dst2;
    dst2.is_null = true;
    IoTFunctions::iot_first_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst2);
    IoTFunctions::iot_first_update(ctx, BigIntVal(99), DoubleVal(3.3), &dst2);

    StringVal ser1 = IoTFunctions::iot_first_last_serialize(ctx, dst1);
    StringVal ser2 = IoTFunctions::iot_first_last_serialize(ctx, dst2);

    StringVal dst;
    dst.is_null = true;
    IoTFunctions::iot_first_merge(ctx, ser1, &dst);
    IoTFunctions::iot_first_merge(ctx, ser2, &dst);

    DoubleVal result = IoTFunctions::iot_first_last_finalize(ctx, dst);
    ASSERT_EQ(DoubleVal(3.3), result);

    // last
    StringVal dst3;
    dst3.is_null = true;
    IoTFunctions::iot_last_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst3);
    IoTFunctions::iot_last_update(ctx, BigIntVal(102), DoubleVal(2.2), &dst3);
    IoTFunctions::iot_last_update(ctx, BigIntVal(100), DoubleVal(1.1), &dst3);

    StringVal dst4;
    dst4.is_null = true;
    IoTFunctions::iot_last_update(ctx, BigIntVal::null(), DoubleVal(1.1), &dst4);
    IoTFunctions::iot_last_update(ctx, BigIntVal(99), DoubleVal(3.3), &dst4);

    ser1 = IoTFunctions::iot_first_last_serialize(ctx, dst3);
    ser2 = IoTFunctions::iot_first_last_serialize(ctx, dst4);

    StringVal dst5;
    dst5.is_null = true;
    IoTFunctions::iot_last_merge(ctx, ser1, &dst5);
    IoTFunctions::iot_last_merge(ctx, ser2, &dst5);

    result = IoTFunctions::iot_first_last_finalize(ctx, dst5);
    ASSERT_EQ(DoubleVal(2.2), result);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
