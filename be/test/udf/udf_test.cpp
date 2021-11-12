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

#include "common/logging.h"
#include "udf/udf_test_harness.hpp"
#include "util/logging.h"

namespace doris_udf {

DoubleVal zero_udf(FunctionContext* context) {
    return DoubleVal(0);
}

StringVal log_udf(FunctionContext* context, const StringVal& arg1) {
    std::cerr << (arg1.is_null ? "nullptr" : std::string((char*)arg1.ptr, arg1.len)) << std::endl;
    return arg1;
}

StringVal upper_udf(FunctionContext* context, const StringVal& input) {
    if (input.is_null) {
        return StringVal::null();
    }

    // Create a new StringVal object that's the same length as the input
    StringVal result = StringVal(context, input.len);

    for (int i = 0; i < input.len; ++i) {
        result.ptr[i] = toupper(input.ptr[i]);
    }

    return result;
}

FloatVal min3(FunctionContext* context, const FloatVal& f1, const FloatVal& f2,
              const FloatVal& f3) {
    bool is_null = true;
    float v = 0.0;

    if (!f1.is_null) {
        if (is_null) {
            v = f1.val;
            is_null = false;
        } else {
            v = std::min(v, f1.val);
        }
    }

    if (!f2.is_null) {
        if (is_null) {
            v = f2.val;
            is_null = false;
        } else {
            v = std::min(v, f2.val);
        }
    }

    if (!f3.is_null) {
        if (is_null) {
            v = f3.val;
            is_null = false;
        } else {
            v = std::min(v, f3.val);
        }
    }

    return is_null ? FloatVal::null() : FloatVal(v);
}

StringVal concat(FunctionContext* context, int n, const StringVal* args) {
    int size = 0;
    bool all_null = true;

    for (int i = 0; i < n; ++i) {
        if (args[i].is_null) {
            continue;
        }

        size += args[i].len;
        all_null = false;
    }

    if (all_null) {
        return StringVal::null();
    }

    int offset = 0;
    StringVal result(context, size);

    for (int i = 0; i < n; ++i) {
        if (args[i].is_null) {
            continue;
        }

        memcpy(result.ptr + offset, args[i].ptr, args[i].len);
        offset += args[i].len;
    }

    return result;
}

IntVal num_var_args(FunctionContext*, const BigIntVal& dummy, int n, const IntVal* args) {
    return IntVal(n);
}

IntVal validat_udf(FunctionContext* context) {
    EXPECT_EQ(context->version(), FunctionContext::V2_0);
    EXPECT_FALSE(context->has_error());
    EXPECT_TRUE(context->error_msg() == nullptr);
    return IntVal::null();
}

IntVal validate_fail(FunctionContext* context) {
    EXPECT_FALSE(context->has_error());
    EXPECT_TRUE(context->error_msg() == nullptr);
    context->set_error("Fail");
    EXPECT_TRUE(context->has_error());
    EXPECT_TRUE(strcmp(context->error_msg(), "Fail") == 0);
    return IntVal::null();
}

IntVal validate_mem(FunctionContext* context) {
    EXPECT_TRUE(context->allocate(0) == nullptr);
    uint8_t* buffer = context->allocate(10);
    EXPECT_TRUE(buffer != nullptr);
    memset(buffer, 0, 10);
    context->free(buffer);
    return IntVal::null();
}

TEST(UdfTest, TestFunctionContext) {
    EXPECT_TRUE(UdfTestHarness::validat_udf<IntVal>(validat_udf, IntVal::null()));
    EXPECT_FALSE(UdfTestHarness::validat_udf<IntVal>(validate_fail, IntVal::null()));
    EXPECT_TRUE(UdfTestHarness::validat_udf<IntVal>(validate_mem, IntVal::null()));
}

TEST(UdfTest, TestValidate) {
    EXPECT_TRUE(UdfTestHarness::validat_udf<DoubleVal>(zero_udf, DoubleVal(0)));
    EXPECT_FALSE(UdfTestHarness::validat_udf<DoubleVal>(zero_udf, DoubleVal(10)));

    EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, StringVal>(log_udf, StringVal("abcd"),
                                                                   StringVal("abcd"))));

    EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, StringVal>(upper_udf, StringVal("abcd"),
                                                                   StringVal("ABCD"))));

    EXPECT_TRUE((UdfTestHarness::validat_udf<FloatVal, FloatVal, FloatVal, FloatVal>(
            min3, FloatVal(1), FloatVal(2), FloatVal(3), FloatVal(1))));
    EXPECT_TRUE((UdfTestHarness::validat_udf<FloatVal, FloatVal, FloatVal, FloatVal>(
            min3, FloatVal(1), FloatVal::null(), FloatVal(3), FloatVal(1))));
    EXPECT_TRUE((UdfTestHarness::validat_udf<FloatVal, FloatVal, FloatVal, FloatVal>(
            min3, FloatVal::null(), FloatVal::null(), FloatVal::null(), FloatVal::null())));
}

// TEST(UdfTest, TestTimestampVal) {
//     boost::gregorian::date d(2003, 3, 15);
//     TimestampVal t1(*(int32_t*)&d);
//     EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, TimestampVal>(time_to_string, t1,
//                                                                       "2003-03-15 00:00:00")));

//     TimestampVal t2(*(int32_t*)&d, 1000L * 1000L * 5000L);
//     EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, TimestampVal>(time_to_string, t2,
//                                                                       "2003-03-15 00:00:05")));
// }

TEST(UdfTest, TestVarArgs) {
    std::vector<StringVal> input;
    input.push_back(StringVal("Hello"));
    input.push_back(StringVal("World"));

    EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, StringVal>(concat, input,
                                                                   StringVal("HelloWorld"))));

    input.push_back(StringVal("More"));
    EXPECT_TRUE((UdfTestHarness::validat_udf<StringVal, StringVal>(concat, input,
                                                                   StringVal("HelloWorldMore"))));

    std::vector<IntVal> args;
    args.resize(10);
    EXPECT_TRUE((UdfTestHarness::validat_udf<IntVal, BigIntVal, IntVal>(
            num_var_args, BigIntVal(0), args, IntVal(args.size()))));
}
} // namespace doris_udf

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
