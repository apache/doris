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

#include <string_view>

#include "core/value/jsonb_value.h"
#include "util/jsonb_document.h"

namespace doris {
namespace {

void expect_jsonb_contains(std::string_view target_json, std::string_view candidate_json,
                           bool expected) {
    JsonBinaryValue target;
    auto st = target.from_json_string(target_json.data(), target_json.size());
    ASSERT_TRUE(st.ok()) << st.to_string();

    JsonBinaryValue candidate;
    st = candidate.from_json_string(candidate_json.data(), candidate_json.size());
    ASSERT_TRUE(st.ok()) << st.to_string();

    const JsonbDocument* target_doc = nullptr;
    st = JsonbDocument::checkAndCreateDocument(target.value(), target.size(), &target_doc);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const JsonbDocument* candidate_doc = nullptr;
    st = JsonbDocument::checkAndCreateDocument(candidate.value(), candidate.size(), &candidate_doc);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(target_doc->getValue()->contains(candidate_doc->getValue()), expected);
}

} // namespace

TEST(JsonbContainsTest, ArrayCandidateDoesNotConsumeTargetElements) {
    expect_jsonb_contains("[1,1,1]", "[1,1]", true);
    expect_jsonb_contains("[1]", "[1,1]", true);
    expect_jsonb_contains("[1,2,3]", "[2,1]", true);

    expect_jsonb_contains("[1,2,3]", "[2,4]", false);
}

TEST(JsonbContainsTest, ArrayCandidateUsesRecursiveContains) {
    expect_jsonb_contains(R"([{"a":1,"b":2},[3,4]])", R"([{"a":1},[4]])", true);
    expect_jsonb_contains(R"([{"a":1},[3,4]])", R"([{"a":2}])", false);
}

} // namespace doris
