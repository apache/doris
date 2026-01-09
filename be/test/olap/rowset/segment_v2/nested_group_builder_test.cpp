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

#include <string>
#include <vector>

#include "olap/rowset/segment_v2/variant/nested_group_builder.h"
#include "runtime/jsonb_value.h"
#include "vec/columns/column_string.h"

namespace doris::segment_v2 {

static vectorized::ColumnPtr make_jsonb_column(const std::vector<std::string>& json_strings) {
    auto col = vectorized::ColumnString::create();
    doris::JsonBinaryValue jsonb_value;
    for (const auto& s : json_strings) {
        EXPECT_TRUE(jsonb_value.from_json_string(s).ok());
        col->insert_data(jsonb_value.value(), jsonb_value.size());
    }
    return col->get_ptr();
}

TEST(NestedGroupBuilderTest, BuildSimpleArrayObject) {
    NestedGroupBuilder builder;
    builder.set_max_depth(8);

    auto jsonb_col = make_jsonb_column({R"({"items":[{"a":1},{"a":2}]})"});
    NestedGroupsMap groups;
    ASSERT_TRUE(builder.build_from_jsonb(jsonb_col, groups, jsonb_col->size()).ok());

    auto it = groups.find(vectorized::PathInData("items"));
    ASSERT_TRUE(it != groups.end());
    ASSERT_TRUE(it->second);

    const auto& group = *it->second;
    ASSERT_TRUE(group.offsets);
    const auto& offsets = assert_cast<const vectorized::ColumnOffset64&>(*group.offsets).get_data();
    ASSERT_EQ(offsets.size(), 1);
    EXPECT_EQ(offsets[0], 2);

    ASSERT_EQ(group.children.size(), 1);
    auto it_child = group.children.find(vectorized::PathInData("a"));
    ASSERT_TRUE(it_child != group.children.end());
    EXPECT_EQ(it_child->second.size(), 2);
}

TEST(NestedGroupBuilderTest, ConflictArrayObjectWinsOverScalar) {
    NestedGroupBuilder builder;
    builder.set_max_depth(8);

    // First element: a is scalar. Second element: a becomes array<object>.
    auto jsonb_col = make_jsonb_column({R"({"items":[{"a":1},{"a":[{"b":2}]}]})"});
    NestedGroupsMap groups;
    ASSERT_TRUE(builder.build_from_jsonb(jsonb_col, groups, jsonb_col->size()).ok());

    auto it = groups.find(vectorized::PathInData("items"));
    ASSERT_TRUE(it != groups.end());
    ASSERT_TRUE(it->second);

    const auto& group = *it->second;
    // Scalar child "a" should be discarded once we see array<object> on the same path.
    EXPECT_TRUE(group.children.find(vectorized::PathInData("a")) == group.children.end());
    ASSERT_TRUE(group.nested_groups.contains(vectorized::PathInData("a")));

    const auto& nested = *group.nested_groups.at(vectorized::PathInData("a"));
    ASSERT_TRUE(nested.offsets);
    const auto& offsets = assert_cast<const vectorized::ColumnOffset64&>(*nested.offsets).get_data();
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(offsets[0], 0); // first element has empty nested array
    EXPECT_EQ(offsets[1], 1); // second element has 1 nested object
}

} // namespace doris::segment_v2

