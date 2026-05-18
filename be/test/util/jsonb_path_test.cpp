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

#include "util/jsonb_document.h"

namespace doris {
namespace {

bool is_valid_jsonb_path(std::string_view path) {
    JsonbPath jsonb_path;
    return jsonb_path.seek(path.data(), path.size());
}

} // namespace

TEST(JsonbPathTest, RejectsInvalidArrayPathSyntax) {
    EXPECT_FALSE(is_valid_jsonb_path("$[1.5]"));
    EXPECT_FALSE(is_valid_jsonb_path("$[Last]"));
    EXPECT_FALSE(is_valid_jsonb_path("$[LAST]"));
    EXPECT_FALSE(is_valid_jsonb_path("$.[0]"));

    EXPECT_FALSE(is_valid_jsonb_path("$.a[1.5]"));
    EXPECT_FALSE(is_valid_jsonb_path("$.a[Last]"));
    EXPECT_FALSE(is_valid_jsonb_path("$**.[0]"));
}

TEST(JsonbPathTest, AcceptsValidArrayPathSyntax) {
    EXPECT_TRUE(is_valid_jsonb_path("$[0]"));
    EXPECT_TRUE(is_valid_jsonb_path("$.a[1]"));
    EXPECT_TRUE(is_valid_jsonb_path("$.a[last]"));
    EXPECT_TRUE(is_valid_jsonb_path("$.a[last-0]"));
    EXPECT_TRUE(is_valid_jsonb_path("$.a[last-1]"));
    EXPECT_TRUE(is_valid_jsonb_path("$[*]"));
    EXPECT_TRUE(is_valid_jsonb_path("$.a[*]"));
    EXPECT_TRUE(is_valid_jsonb_path("$**[0]"));
    EXPECT_TRUE(is_valid_jsonb_path("$**.a[0]"));
}

} // namespace doris
