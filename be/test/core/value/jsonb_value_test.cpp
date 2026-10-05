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

#include "core/value/jsonb_value.h"

#include <gtest/gtest.h>

#include <string>

using std::string;

namespace doris {

TEST(JsonBinaryValueTest, TestValidation) {
    JsonBinaryValue json_val;

    // Empty string and non-JSON text should fail to parse
    std::vector<string> invalid_strs = {"", "abc"};
    for (size_t i = 0; i < invalid_strs.size(); i++) {
        auto status = json_val.from_json_string(invalid_strs[i].c_str(), invalid_strs[i].size());
        EXPECT_FALSE(status.ok()) << "Should fail for: [" << invalid_strs[i] << "]";
    }

    // Scalar JSON values (number, null, boolean) are valid JSON and should parse OK
    std::vector<string> scalar_strs = {"1", "null", "false"};
    for (size_t i = 0; i < scalar_strs.size(); i++) {
        auto status = json_val.from_json_string(scalar_strs[i].c_str(), scalar_strs[i].size());
        EXPECT_TRUE(status.ok()) << "Should succeed for: [" << scalar_strs[i] << "]";
    }

    // valid arrays and objects
    std::vector<string> valid_strs;
    valid_strs.push_back("[false]");
    valid_strs.push_back("[-123]");
    valid_strs.push_back("[\"abc\"]");
    valid_strs.push_back("[\"val1\", \"val2\"]");
    valid_strs.push_back("{\"key1\": \"js6\", \"key2\": [\"val1\", \"val2\"]}");
    valid_strs.push_back("[123, {\"key1\": null, \"key2\": [\"val1\", \"val2\"]}]");
    for (size_t i = 0; i < valid_strs.size(); i++) {
        auto status = json_val.from_json_string(valid_strs[i].c_str(), valid_strs[i].size());
        EXPECT_TRUE(status.ok()) << "Should succeed for: [" << valid_strs[i] << "]";
    }
}
} // namespace doris