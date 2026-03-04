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

// Unit tests for NestedGroupPathFilter (defined in nested_group_provider.h)

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/variant/nested_group_provider.h"

namespace doris::segment_v2 {

// ===========================================================================
// NestedGroupPathFilter::empty()
// ===========================================================================

TEST(NestedGroupPathFilterTest, EmptyFilterIsEmpty) {
    NestedGroupPathFilter filter;
    EXPECT_TRUE(filter.empty());
    EXPECT_FALSE(filter.allow_all);
    EXPECT_TRUE(filter.allowed_paths.empty());
}

// ===========================================================================
// NestedGroupPathFilter::set_allow_all()
// ===========================================================================

TEST(NestedGroupPathFilterTest, SetAllowAllClearsPathsAndSetsFlag) {
    NestedGroupPathFilter filter;
    filter.add_path("a");
    filter.add_path("b");
    EXPECT_FALSE(filter.empty());

    filter.set_allow_all();
    EXPECT_TRUE(filter.allow_all);
    EXPECT_TRUE(filter.allowed_paths.empty());
    EXPECT_FALSE(filter.empty()); // not empty because allow_all is true
}

// ===========================================================================
// NestedGroupPathFilter::add_path()
// ===========================================================================

TEST(NestedGroupPathFilterTest, AddPathNormal) {
    NestedGroupPathFilter filter;
    filter.add_path("items.msg");
    filter.add_path("items.title");

    EXPECT_FALSE(filter.empty());
    EXPECT_EQ(2, filter.allowed_paths.size());
    EXPECT_TRUE(filter.allowed_paths.contains("items.msg"));
    EXPECT_TRUE(filter.allowed_paths.contains("items.title"));
}

TEST(NestedGroupPathFilterTest, AddPathEmptyIsSkipped) {
    NestedGroupPathFilter filter;
    filter.add_path("");
    EXPECT_TRUE(filter.empty());
    EXPECT_TRUE(filter.allowed_paths.empty());
}

// ===========================================================================
// NestedGroupPathFilter::matches_child()
// ===========================================================================

TEST(NestedGroupPathFilterTest, MatchesChildExactMatch) {
    NestedGroupPathFilter filter;
    filter.add_path("msg");
    filter.add_path("title");

    EXPECT_TRUE(filter.matches_child("msg"));
    EXPECT_TRUE(filter.matches_child("title"));
    EXPECT_FALSE(filter.matches_child("other"));
}

TEST(NestedGroupPathFilterTest, MatchesChildAllowAll) {
    NestedGroupPathFilter filter;
    filter.set_allow_all();

    EXPECT_TRUE(filter.matches_child("anything"));
    EXPECT_TRUE(filter.matches_child(""));
    EXPECT_TRUE(filter.matches_child("a.b.c"));
}

TEST(NestedGroupPathFilterTest, MatchesChildPrefixMatch) {
    // If allowed_paths has "items.msg" and we check "items",
    // it should match because "items.msg" starts with "items."
    NestedGroupPathFilter filter;
    filter.add_path("items.msg");

    EXPECT_TRUE(filter.matches_child("items"));
}

TEST(NestedGroupPathFilterTest, MatchesChildNoMatch) {
    NestedGroupPathFilter filter;
    filter.add_path("items.msg");

    EXPECT_FALSE(filter.matches_child("other"));
    EXPECT_FALSE(filter.matches_child("item")); // not a prefix with dot
}

// ===========================================================================
// NestedGroupPathFilter::sub_filter()
// ===========================================================================

TEST(NestedGroupPathFilterTest, SubFilterExactPrefixReturnsAllowAll) {
    NestedGroupPathFilter filter;
    filter.add_path("items");
    filter.add_path("other.path");

    auto sub = filter.sub_filter("items");
    EXPECT_TRUE(sub.allow_all);
}

TEST(NestedGroupPathFilterTest, SubFilterStripsPrefix) {
    NestedGroupPathFilter filter;
    filter.add_path("items.msg");
    filter.add_path("items.title");
    filter.add_path("other.path");

    auto sub = filter.sub_filter("items");
    EXPECT_FALSE(sub.allow_all);
    EXPECT_EQ(2, sub.allowed_paths.size());
    EXPECT_TRUE(sub.allowed_paths.contains("msg"));
    EXPECT_TRUE(sub.allowed_paths.contains("title"));
}

TEST(NestedGroupPathFilterTest, SubFilterAllowAllPropagates) {
    NestedGroupPathFilter filter;
    filter.set_allow_all();

    auto sub = filter.sub_filter("anything");
    EXPECT_TRUE(sub.allow_all);
}

TEST(NestedGroupPathFilterTest, SubFilterNoMatchReturnsEmpty) {
    NestedGroupPathFilter filter;
    filter.add_path("items.msg");

    auto sub = filter.sub_filter("other");
    EXPECT_TRUE(sub.empty());
    EXPECT_FALSE(sub.allow_all);
    EXPECT_TRUE(sub.allowed_paths.empty());
}

} // namespace doris::segment_v2
