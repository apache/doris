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
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "runtime/runtime_state.h"
#include "runtime/test_env.h"
#include "exprs/anyval_util.h"

using namespace doris;
using namespace std;

namespace doris_udf {
class FunctionContextImpl;
class ReplaceFunctionTest : public testing::Test {
    public:
        ReplaceFunctionTest() { }

        void SetUp() {
            TQueryGlobals globals;
            globals.__set_now_string("2019-08-06 01:38:57");
            globals.__set_timestamp_ms(1565080737805);
            globals.__set_time_zone("America/Los_Angeles");
            state = new RuntimeState(globals);
            utils = new FunctionUtils(state);
            ctx = utils->get_fn_ctx();
        }

        void TearDown() {
            delete state;
            delete utils;
        }

    private:
        doris::RuntimeState* state = nullptr;
        doris::FunctionUtils* utils = nullptr;
        FunctionContext* ctx = nullptr;
};

StringVal ReplaceUdf(FunctionContext *context, const StringVal &origStr, const StringVal &oldStr, const StringVal &newStr) {
    if (origStr.is_null || oldStr.is_null || newStr.is_null) {
        return origStr;
    }
    std::string orig_str = std::string(reinterpret_cast<const char *>(origStr.ptr), origStr.len);
    std::string old_str = std::string(reinterpret_cast<const char *>(oldStr.ptr), oldStr.len);
    std::string new_str = std::string(reinterpret_cast<const char *>(newStr.ptr), newStr.len);
    std::string::size_type pos = 0;
    std::string::size_type oldLen = old_str.size();
    std::string::size_type newLen = new_str.size();
    while(pos = orig_str.find(old_str, pos))
    {
        if(pos == std::string::npos) break;
        orig_str.replace(pos, oldLen, new_str);
        pos += newLen;
    }
    return AnyValUtil::from_string_temp(context, orig_str);
}

TEST_F(ReplaceFunctionTest, now) {
    StringVal res = ReplaceUdf(ctx, "http://www.baidu.com:9090", "9090", "100");
    std::string data = std::string(reinterpret_cast<const char *>(res.ptr), res.len);
    cout << "data1: " << data << endl;
    ASSERT_EQ("http://www.baidu.com:100", data);

    res = ReplaceUdf(ctx, "http://www.baidu.com:9090", "9080", "100");
    data = std::string(reinterpret_cast<const char *>(res.ptr), res.len);
    cout << "data2: " << data << endl;
    ASSERT_EQ("http://www.baidu.com:9090", data);

    res = ReplaceUdf(ctx, "http://www.baidu.com:9090", ".", "#");
    data = std::string(reinterpret_cast<const char *>(res.ptr), res.len);
    cout << "data3: " << data << endl;
    ASSERT_EQ("http://www#baidu#com:9090", data);

    res = ReplaceUdf(ctx, "http://www.baidu.com:9090", "", "8080");
    data = std::string(reinterpret_cast<const char *>(res.ptr), res.len);
    cout << "data4: " << data << endl;
    ASSERT_EQ("http://www.baidu.com:9090", data);
}

}
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

