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

#include <iostream>
#include <string>

#include "exprs/anyval_util.h"
#include "exprs/hash_functions.h"
#include "testutil/function_utils.h"
#include "testutil/test_util.h"

namespace doris {

class HashFunctionsTest : public testing::Test {
public:
    HashFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(HashFunctionsTest, murmur_hash3_64) {
    StringVal input = AnyValUtil::from_string_temp(ctx, std::string("hello"));
    BigIntVal result = HashFunctions::murmur_hash3_64(ctx, 1, &input);
    BigIntVal expected((int64_t)-3215607508166160593);

    EXPECT_EQ(expected, result);
}
} // namespace doris