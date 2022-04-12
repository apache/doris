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

#include "util/json_util.h"

#include <gtest/gtest.h>

#include "common/logging.h"

namespace doris {

class JsonUtilTest : public testing::Test {
public:
    JsonUtilTest() {}
    virtual ~JsonUtilTest() {}
};

TEST_F(JsonUtilTest, success) {
    Status status;

    auto str = to_json(status);

    const char* result =
            "{\n"
            "    \"status\": \"Success\",\n"
            "    \"msg\": \"OK\"\n}";
    EXPECT_STREQ(result, str.c_str());
}

TEST_F(JsonUtilTest, normal_fail) {
    Status status = Status::InternalError("so bad");

    auto str = to_json(status);

    const char* result =
            "{\n"
            "    \"status\": \"Fail\",\n"
            "    \"msg\": \"so bad\"\n}";
    EXPECT_STREQ(result, str.c_str());
}

TEST_F(JsonUtilTest, normal_fail_str) {
    Status status = Status::InternalError("\"so bad\"");

    auto str = to_json(status);

    // "msg": "\"so bad\""
    const char* result =
            "{\n"
            "    \"status\": \"Fail\",\n"
            "    \"msg\": \"\\\"so bad\\\"\"\n}";
    LOG(INFO) << "str: " << str;
    EXPECT_STREQ(result, str.c_str());
}

} // namespace doris
