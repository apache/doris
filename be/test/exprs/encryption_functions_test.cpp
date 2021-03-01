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

#include "exprs/encryption_functions.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "exprs/anyval_util.h"
#include "testutil/function_utils.h"
#include "util/logging.h"

namespace doris {
class EncryptionFunctionsTest : public testing::Test {
public:
    EncryptionFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(EncryptionFunctionsTest, from_base64) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());
    {
        StringVal result =
                EncryptionFunctions::from_base64(context.get(), doris_udf::StringVal("aGVsbG8="));
        StringVal expected = doris_udf::StringVal("hello");
        ASSERT_EQ(expected, result);
    }

    {
        StringVal result =
                EncryptionFunctions::from_base64(context.get(), doris_udf::StringVal::null());
        StringVal expected = doris_udf::StringVal::null();
        ASSERT_EQ(expected, result);
    }
}

TEST_F(EncryptionFunctionsTest, to_base64) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());

    {
        StringVal result =
                EncryptionFunctions::to_base64(context.get(), doris_udf::StringVal("hello"));
        StringVal expected = doris_udf::StringVal("aGVsbG8=");
        ASSERT_EQ(expected, result);
    }
    {
        StringVal result =
                EncryptionFunctions::to_base64(context.get(), doris_udf::StringVal::null());
        StringVal expected = doris_udf::StringVal::null();
        ASSERT_EQ(expected, result);
    }
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