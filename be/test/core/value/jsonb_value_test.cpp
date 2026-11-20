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
#include <vector>

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

TEST(JsonBinaryValueTest, ReuseParserAcrossDocumentsAndErrors) {
    JsonBinaryValue value;
    const std::vector<std::string> valid_json = {R"({"id":1})",
                                                 "\"" + std::string(8192, 'x') + "\"",
                                                 "null",
                                                 "true",
                                                 "-123",
                                                 "18446744073709551616",
                                                 R"({"nested":["a\nb\u4e2d",false,null,3.25]})",
                                                 "[]"};
    const std::vector<std::string> invalid_json = {
            "", "abc", R"({"a":[1,})", R"({"a":"\uZZZZ"})", "{} {}", "01"};

    std::vector<std::string> expected_jsonb;
    for (const auto& json : valid_json) {
        JsonbWriter expected;
        ASSERT_TRUE(JsonbParser::parse(json.data(), json.size(), expected).ok()) << json;
        expected_jsonb.emplace_back(expected.getOutput()->getBuffer(),
                                    expected.getOutput()->getSize());
    }

    for (const auto& invalid : invalid_json) {
        // Exercise failure after valid input and recovery with the same parser, including
        // string-buffer growth followed by smaller documents and failures during traversal.
        for (size_t i = 0; i < valid_json.size(); ++i) {
            ASSERT_TRUE(value.from_json_string(valid_json[i]).ok()) << valid_json[i];
            EXPECT_EQ(std::string(value.value(), value.size()), expected_jsonb[i]);

            EXPECT_FALSE(value.from_json_string(invalid).ok()) << invalid;
            EXPECT_EQ(value.value(), nullptr);
            EXPECT_EQ(value.size(), 0);
        }
    }
}

} // namespace doris
