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

#include "exprs/json_functions.h"

#include <gtest/gtest.h>

namespace doris {

// These tests focus on the robustness of JSON path parsing when using
// boost::escaped_list_separator internally. In particular, they verify
// that malformed escape sequences in the path will NOT crash the process
// and will be converted to an invalid JsonPath entry (is_valid == false).

class JsonFunctionEscapedListTest : public testing::Test {};

TEST_F(JsonFunctionEscapedListTest, ParseJsonPaths_InvalidEscape_TrailingBackslash) {
    std::vector<JsonPath> parsed_paths;

    // A path ending with a single backslash used to trigger
    // boost::escaped_list_error inside boost::tokenizer.
    // The function should catch the exception (or otherwise treat it as invalid)
    // and mark the resulting JsonPath as invalid instead of crashing.
    JsonFunctions::parse_json_paths("$.[*].field\\", &parsed_paths);

    ASSERT_FALSE(parsed_paths.empty());
    // The first element represents root '$', the second (or later) element
    // corresponding to the malformed segment should be marked invalid.
    bool has_invalid = false;
    for (const auto& p : parsed_paths) {
        if (!p.is_valid) {
            has_invalid = true;
            break;
        }
    }
    EXPECT_TRUE(has_invalid);
}

TEST_F(JsonFunctionEscapedListTest, ParseJsonPaths_InvalidEscape_InQuotedKey) {
    std::vector<JsonPath> parsed_paths;

    // Another malformed path intended to exercise the escaped_list_separator
    // error handling. Exact behavior of boost is implementation-defined, the
    // important contract is: no crash and some path element is marked invalid.
    JsonFunctions::parse_json_paths("$.\"bad_key\\", &parsed_paths);

    ASSERT_FALSE(parsed_paths.empty());
    bool has_invalid = false;
    for (const auto& p : parsed_paths) {
        if (!p.is_valid) {
            has_invalid = true;
            break;
        }
    }
    EXPECT_TRUE(has_invalid);
}

} // namespace doris

