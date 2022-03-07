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

#include "exprs/utility_functions.h"
#include "testutil/function_utils.h"
#include "util/logging.h"

#include <gtest/gtest.h>

#include <iostream>
#include <limits>
#include <string>
#include <vector>
#include <memory>

using namespace doris_udf;

namespace doris {

class UtilityFunctionsTest : public ::testing::Test {
public:
    UtilityFunctionsTest() = default;
    ~UtilityFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
        utility_functions_ptr = std::make_unique<UtilityFunctions>();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
    std::unique_ptr<UtilityFunctions> utility_functions_ptr;
};

TEST_F(UtilityFunctionsTest, compare_version_empty_version_or_op) {
    { // case1: version1 and version2 is empty
        StringVal version1("");
        StringVal version2("");
        StringVal op("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(false, res.val);
    }

    { // case2: op is empty
        StringVal version1("10.1.1");
        StringVal version2("10.1.1.0");
        StringVal op("");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(false, res.val);
    }
}

TEST_F(UtilityFunctionsTest, compare_version_larger_than) {
    { // case1: 10.1.1 >= 10.1.1.0
        StringVal version1("10.1.1");
        StringVal version2("10.1.1.0");
        StringVal op(">=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(true, res.val);
    }
    { // case2: 10.1.1.0 >= 9.1.1
        StringVal version1("9.1.1");
        StringVal version2("10.1.1.0");
        StringVal op(">=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(false, res.val);
    }
}

TEST_F(UtilityFunctionsTest, compare_version_less_than) {
    { // case1: 9.1.1.0 <= 10.1.1.0
        StringVal version1("9.1.1.0");
        StringVal version2("10.1.1.0");
        StringVal op("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(true, res.val);
    }
    { // case2: 9.1.1.0 <= 10.1.1
        StringVal version1("9.1.1.0");
        StringVal version2("10.1.1");
        StringVal op("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(true, res.val);
    }
}

TEST_F(UtilityFunctionsTest, compare_version_equal) {
    StringVal version1("10.1.1.0");
    StringVal version2("10.1.1.0");
    StringVal op("<=");
    BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
    EXPECT_EQ(true, res.val);
}

TEST_F(UtilityFunctionsTest, compare_version_non_digital) {
    { // case1: "aaa.bbb.1.0" is not digital
        StringVal version1("aaa.bbb.1.0");
        StringVal version2("10.1.1.0");
        StringVal op = StringVal("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(false, res.val);
    }
    { // case2: 9.1.1. <= 10.1.1
        StringVal version1("9.1.1.");
        StringVal version2("10.1.1");
        StringVal op("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(true, res.val);
    }
    { // case3: .9.1.1. <= 10.1.1
        StringVal version1(".9.1.1");
        StringVal version2("10.1.1");
        StringVal op("<=");
        BooleanVal res = utility_functions_ptr->compare_version(ctx, version1, op, version2);
        EXPECT_EQ(true, res.val);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
