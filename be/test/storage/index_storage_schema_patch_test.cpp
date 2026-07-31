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

#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {

TEST(IndexStorageSchemaPatchTest, AddAndDropTextColumns) {
    IndexTabletOptions options;
    options.tablet_id = 110014;
    options.text_columns = {
            TextColumnSpec {.unique_id = 2, .name = "title"},
            TextColumnSpec {.unique_id = 3, .name = "obsolete"},
    };
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_obsolete", 3));
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    IndexSchemaPatch patch;
    patch.drop_column_uids.insert(3);
    patch.add_text_columns.push_back(TextColumnSpec {.unique_id = 4, .name = "body"});

    auto patched_schema = build_patched_tablet_schema(*base_schema, patch);
    ASSERT_NE(patched_schema, nullptr);

    ASSERT_TRUE(patched_schema->has_column_unique_id(2));
    EXPECT_FALSE(patched_schema->has_column_unique_id(3));
    ASSERT_TRUE(patched_schema->has_column_unique_id(4));
    const auto& body = patched_schema->column_by_uid(4);
    EXPECT_EQ(body.name(), "body");
    EXPECT_EQ(body.type(), FieldType::OLAP_FIELD_TYPE_STRING);
    EXPECT_EQ(patched_schema->next_column_unique_id(), 5);
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(3, "").empty());
}

} // namespace doris::index_storage_test
