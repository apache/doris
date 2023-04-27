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

#include "runtime/jsonb_value.h"

#include <gtest/gtest.h>

#include <string>

#include "util/cpu_info.h"

using std::string;

namespace doris {

JsonBinaryValue FromStdString(const string& str) {
    char* ptr = const_cast<char*>(str.c_str());
    int len = str.size();
    return JsonBinaryValue(ptr, len);
}

TEST(JsonBinaryValueTest, TestValidation) {
    JsonbErrType err;
    JsonBinaryValue json_val;

    // single value not wrapped as an arrar or object is invalid
    std::vector<string> invalid_strs = {"", "1", "null", "false", "abc"};
    for (size_t i = 0; i < invalid_strs.size(); i++) {
        err = json_val.from_json_string(invalid_strs[i].c_str(), invalid_strs[i].size());
        EXPECT_NE(err, JsonbErrType::E_NONE);
    }

    // valid enums
    std::vector<string> valid_strs;
    valid_strs.push_back("[false]");
    valid_strs.push_back("[-123]");
    valid_strs.push_back("[\"abc\"]");
    valid_strs.push_back("[\"val1\", \"val2\"]");
    valid_strs.push_back("{\"key1\": \"js6\", \"key2\": [\"val1\", \"val2\"]}");
    valid_strs.push_back("[123, {\"key1\": null, \"key2\": [\"val1\", \"val2\"]}]");
    for (size_t i = 0; i < valid_strs.size(); i++) {
        err = json_val.from_json_string(valid_strs[i].c_str(), valid_strs[i].size());
        EXPECT_EQ(err, JsonbErrType::E_NONE);
    }
}
} // namespace doris