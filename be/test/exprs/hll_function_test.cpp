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

#include <iostream>
#include <string>

#include "exprs/aggregate_functions.h"
#include "exprs/anyval_util.h"
#include "exprs/hll_function.h"
#include "testutil/function_utils.h"
#include "olap/hll.h"
#include "util/logging.h"

#include <gtest/gtest.h>

namespace doris {

StringVal convert_hll_to_string(FunctionContext* ctx, HyperLogLog& hll) {
    std::string buf;
    buf.resize(HLL_COLUMN_DEFAULT_LEN);
    int size = hll.serialize((char*)buf.c_str());
    buf.resize(size);
    return AnyValUtil::from_string_temp(ctx, buf);
}

class HllFunctionsTest : public testing::Test {
public:
    HllFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() {
        delete utils;
    }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(HllFunctionsTest, hll_hash) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("1024"));
    StringVal result = HllFunctions::hll_hash(ctx, input);

    HyperLogLog hll((char*)result.ptr);
    int64_t cardinality = hll.estimate_cardinality();
    int64_t expected = 1;

    ASSERT_EQ(expected, cardinality);
}

TEST_F(HllFunctionsTest, hll_hash_null) {
    StringVal input = StringVal::null();
    StringVal result = HllFunctions::hll_hash(ctx, input);

    HyperLogLog hll((char*)result.ptr);
    int64_t cardinality = hll.estimate_cardinality();
    int64_t expected = 0;

    ASSERT_EQ(expected, cardinality);
}

TEST_F(HllFunctionsTest, hll_update) {
    StringVal dst;
    HllFunctions::hll_init(ctx, &dst);
    IntVal src1(1);
    HllFunctions::hll_update(ctx, src1, &dst);
    IntVal src2(1234567);
    HllFunctions::hll_update(ctx, src2, &dst);

    BigIntVal result = HllFunctions::hll_finalize(ctx, dst);
    BigIntVal expected(2);
    ASSERT_EQ(expected, result);
}

TEST_F(HllFunctionsTest, hll_merge) {
    StringVal dst;
    HllFunctions::hll_init(ctx, &dst);

    HyperLogLog hll1(1024);
    StringVal src1 = convert_hll_to_string(ctx, hll1);
    HllFunctions::hll_merge(ctx, src1, &dst);

    HyperLogLog hll2;
    StringVal src2 = convert_hll_to_string(ctx, hll2);
    HllFunctions::hll_merge(ctx, src2, &dst);

    StringVal serialized = HllFunctions::hll_serialize(ctx, dst);
    HyperLogLog hll((char*)serialized.ptr);

    BigIntVal expected(1);
    ASSERT_EQ(expected, hll.estimate_cardinality());
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
