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

TEST(IndexStorageVariantSchemaPatchTest, AddDropIndexAndModifyVariantProperties) {
    IndexTabletOptions options;
    options.tablet_id = 110003;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.inverted_indexes.push_back(
            IndexSpec::field_pattern_index(20001, "idx_v_old", 2, "old"));
    auto base_schema = build_tablet_schema(options);

    VariantColumnSpec modified = options.variant_columns[0];
    modified.max_subcolumns_count = 16;
    modified.enable_doc_mode = true;
    modified.doc_materialization_min_rows = 128;

    VariantColumnSpec added;
    added.unique_id = 3;
    added.name = "v2";
    added.max_subcolumns_count = 2;

    IndexSchemaPatch patch;
    patch.modify_variant_columns.emplace(2, modified);
    patch.add_variant_columns.push_back(added);
    patch.drop_index_names.insert("idx_v_old");
    patch.add_inverted_indexes.push_back(
            IndexSpec::field_pattern_index(20002, "idx_v_new", 2, "a"));

    auto patched_schema = build_patched_tablet_schema(*base_schema, patch);
    ASSERT_NE(patched_schema, nullptr);

    ASSERT_TRUE(patched_schema->has_column_unique_id(2));
    ASSERT_TRUE(patched_schema->has_column_unique_id(3));
    const auto& variant = patched_schema->column_by_uid(2);
    EXPECT_EQ(variant.variant_max_subcolumns_count(), 16);
    EXPECT_TRUE(variant.variant_enable_doc_mode());
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(2, "old").empty());
    auto new_indexes = patched_schema->inverted_index_by_field_pattern(2, "a");
    ASSERT_EQ(new_indexes.size(), 1);
    EXPECT_EQ(new_indexes[0]->index_name(), "idx_v_new");
}

TEST(IndexStorageVariantSchemaPatchTest, VariantDocAndNestedGroupOptionsArePreserved) {
    IndexTabletOptions options;
    options.tablet_id = 110009;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.variant_columns[0].enable_doc_mode = true;
    options.variant_columns[0].doc_materialization_min_rows = 64;
    options.variant_columns[0].doc_hash_shard_count = 8;
    options.variant_columns[0].enable_nested_group = true;

    auto schema = build_tablet_schema(options);
    ASSERT_NE(schema, nullptr);
    ASSERT_TRUE(schema->has_column_unique_id(2));
    const auto& variant = schema->column_by_uid(2);
    EXPECT_TRUE(variant.variant_enable_doc_mode());
    EXPECT_EQ(variant.variant_doc_materialization_min_rows(), 64);
    EXPECT_EQ(variant.variant_doc_hash_shard_count(), 8);
    EXPECT_TRUE(variant.variant_enable_nested_group());
}

} // namespace doris::index_storage_test
