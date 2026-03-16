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

// Unit tests for nested_group_path.h utility functions
// (contains_nested_group_marker, strip_nested_group_marker,
//  build_nested_group_offsets_column_name, ends_with, constants)

#include "olap/rowset/segment_v2/variant/nested_group_path.h"

#include <gtest/gtest.h>

namespace doris::segment_v2 {

// ===========================================================================
// Constants
// ===========================================================================

TEST(NestedGroupPathTest, Constants) {
    EXPECT_EQ("__D0_ng__", std::string(kNestedGroupMarker));
    EXPECT_EQ("__D0_root__", std::string(kRootNestedGroupPath));
    EXPECT_EQ(".__offsets", std::string(kNestedGroupOffsetsSuffix));
}

// ===========================================================================
// contains_nested_group_marker
// ===========================================================================

TEST(NestedGroupPathTest, ContainsNestedGroupMarker) {
    EXPECT_TRUE(contains_nested_group_marker("data.__D0_ng__.items"));
    EXPECT_TRUE(contains_nested_group_marker("__D0_ng__"));
    EXPECT_TRUE(contains_nested_group_marker("prefix.__D0_ng__"));
    EXPECT_FALSE(contains_nested_group_marker("data.items"));
    EXPECT_FALSE(contains_nested_group_marker(""));
    EXPECT_FALSE(contains_nested_group_marker("__D0_root__"));
}

// ===========================================================================
// nested_group_marker_token
// ===========================================================================

TEST(NestedGroupPathTest, NestedGroupMarkerToken) {
    EXPECT_EQ(".__D0_ng__.", nested_group_marker_token());
}

// ===========================================================================
// strip_nested_group_marker
// ===========================================================================

TEST(NestedGroupPathTest, StripNestedGroupMarkerSingle) {
    auto result = strip_nested_group_marker("data.__D0_ng__.items");
    EXPECT_EQ("data.items", result);
}

TEST(NestedGroupPathTest, StripNestedGroupMarkerMultiple) {
    auto result = strip_nested_group_marker("a.__D0_ng__.b.__D0_ng__.c");
    EXPECT_EQ("a.b.c", result);
}

TEST(NestedGroupPathTest, StripNestedGroupMarkerNoMarker) {
    auto result = strip_nested_group_marker("data.items");
    EXPECT_EQ("data.items", result);
}

TEST(NestedGroupPathTest, StripNestedGroupMarkerEmpty) {
    auto result = strip_nested_group_marker("");
    EXPECT_EQ("", result);
}

// ===========================================================================
// build_nested_group_offsets_column_name
// ===========================================================================

TEST(NestedGroupPathTest, BuildNestedGroupOffsetsColumnName) {
    auto name = build_nested_group_offsets_column_name("variant_col", "items");
    EXPECT_EQ("variant_col.__D0_ng__.items.__offsets", name);
}

TEST(NestedGroupPathTest, BuildNestedGroupOffsetsColumnNameNestedPath) {
    auto name = build_nested_group_offsets_column_name("col", "level1.level2");
    EXPECT_EQ("col.__D0_ng__.level1.level2.__offsets", name);
}

// ===========================================================================
// ends_with
// ===========================================================================

TEST(NestedGroupPathTest, EndsWithPositive) {
    EXPECT_TRUE(ends_with("hello__offsets", "__offsets"));
    EXPECT_TRUE(ends_with("__offsets", "__offsets"));
    EXPECT_TRUE(ends_with("abc", "c"));
}

TEST(NestedGroupPathTest, EndsWithNegative) {
    EXPECT_FALSE(ends_with("hello", "__offsets"));
    EXPECT_FALSE(ends_with("", "__offsets"));
    EXPECT_FALSE(ends_with("off", "__offsets"));
}

} // namespace doris::segment_v2
