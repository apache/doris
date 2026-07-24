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

#include "exec/common/variant_util.h"
#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {

class IndexStorageVariantSchemaEvolutionTest : public IndexStorageTestFixture {
protected:
    void run_multi_variant_columns_keep_shared_path_isolated(bool external_segment_meta,
                                                             int64_t tablet_id);
};

// DORIS-26466 is intentionally kept out for now; the product path is not being fixed yet.
#if 0
TEST_F(IndexStorageVariantSchemaEvolutionTest, PatchedSchemaAddDropVariantColumnCompactsNewRows) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 4;

    VariantColumnSpec obsolete;
    obsolete.unique_id = 3;
    obsolete.name = "old_v";
    obsolete.max_subcolumns_count = 4;

    IndexTabletOptions options;
    options.tablet_id = 110017;
    options.variant_columns = {std::move(variant), std::move(obsolete)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    IndexBatch batch0;
    batch0.keys = {0, 1};
    batch0.variant_jsons_by_column = {
            {R"({"a": "old-one"})", R"({"a": "old-two"})"},
            {R"({"gone": "drop-one"})", R"({"gone": "drop-two"})"},
    };
    rowset0.batches.push_back(std::move(batch0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    VariantColumnSpec added;
    added.unique_id = 4;
    added.name = "v2";
    added.nullable = true;
    added.max_subcolumns_count = 4;
    added.predefined_paths = {
            VariantPathSpec {.path = "x",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexSchemaPatch patch;
    patch.drop_column_uids.insert(3);
    patch.add_variant_columns.push_back(std::move(added));
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_column_unique_id(2));
    EXPECT_FALSE(patched_schema->has_column_unique_id(3));
    ASSERT_TRUE(patched_schema->has_column_unique_id(4));

    auto patched_old_rowsets =
            inject_reader_schema_for_rowsets({rowset0_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_old_rowsets.has_value()) << patched_old_rowsets.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    IndexBatch batch1;
    batch1.keys = {2, 3};
    batch1.variant_jsons_by_column = {
            {R"({"a": "new-one"})", R"({"a": "new-two"})"},
            {R"({"x": "added-one"})", R"({"x": "added-two"})"},
    };
    rowset1.batches.push_back(std::move(batch1));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto read_result = read_rowsets({patched_old_rowsets->front(), rowset1_result.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {patched_old_rowsets->front(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 4, "x"));
    EXPECT_FALSE(has_variant_parent(compacted_probe.value(), 3));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}
#endif

TEST_F(IndexStorageVariantSchemaEvolutionTest,
       MissingRequiredVariantColumnFailsExtendedInfoAggregation) {
    IndexTabletOptions options;
    options.tablet_id = 110018;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    VariantColumnSpec missing_required_variant;
    missing_required_variant.unique_id = 3;
    missing_required_variant.name = "required_v";
    missing_required_variant.nullable = false;

    IndexSchemaPatch patch;
    patch.add_variant_columns.push_back(std::move(missing_required_variant));
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_column_unique_id(3));

    auto patched_rowsets =
            inject_reader_schema_for_rowsets({rowset_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();

    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> variant_extended_info;
    auto status = variant_util::VariantCompactionUtil::aggregate_variant_extended_info(
            patched_rowsets->front(), &variant_extended_info);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("column not found in segment"), std::string::npos)
            << status.to_string();
}

void IndexStorageVariantSchemaEvolutionTest::run_multi_variant_columns_keep_shared_path_isolated(
        bool external_segment_meta, int64_t tablet_id) {
    VariantColumnSpec left;
    left.unique_id = 2;
    left.name = "v_left";
    left.max_subcolumns_count = 2;

    VariantColumnSpec right;
    right.unique_id = 3;
    right.name = "v_right";
    right.max_subcolumns_count = 2;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(left), std::move(right)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    IndexBatch batch0;
    batch0.keys = {0, 1};
    batch0.variant_jsons_by_column = {
            {R"({"shared": "left-0", "left_only": "l0"})",
             R"({"shared": "left-1", "left_only": "l1"})"},
            {R"({"shared": "right-0", "right_only": "r0"})",
             R"({"shared": "right-1", "right_only": "r1"})"},
    };
    rowset0.batches.push_back(std::move(batch0));

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    IndexBatch batch1;
    batch1.keys = {2, 3};
    batch1.variant_jsons_by_column = {
            {R"({"shared": "left-2", "left_only": "l2"})",
             R"({"shared": "left-3", "left_only": "l3"})"},
            {R"({"shared": "right-2", "right_only": "r2"})",
             R"({"shared": "right-3", "right_only": "r3"})"},
    };
    rowset1.batches.push_back(std::move(batch1));

    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> extended_info;
    for (const auto& rowset : rowsets.value()) {
        auto status = variant_util::VariantCompactionUtil::aggregate_variant_extended_info(
                rowset, &extended_info);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    ASSERT_TRUE(extended_info.contains(2));
    ASSERT_TRUE(extended_info.contains(3));
    EXPECT_EQ(extended_info.at(2).path_to_none_null_values.at("shared"), 4);
    EXPECT_EQ(extended_info.at(3).path_to_none_null_values.at("shared"), 4);
    EXPECT_TRUE(extended_info.at(2).path_to_none_null_values.contains("left_only"));
    EXPECT_TRUE(extended_info.at(3).path_to_none_null_values.contains("right_only"));
    EXPECT_FALSE(extended_info.at(2).path_to_none_null_values.contains("right_only"));
    EXPECT_FALSE(extended_info.at(3).path_to_none_null_values.contains("left_only"));

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    ASSERT_EQ(compacted.value()->num_rows(), 4);

    auto probe = probe_rowset(compacted.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "shared"));
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "left_only"));
    EXPECT_TRUE(has_variant_layout(probe.value(), 3, "shared"));
    EXPECT_TRUE(has_variant_layout(probe.value(), 3, "right_only"));
    EXPECT_FALSE(has_variant_layout(probe.value(), 2, "right_only"));
    EXPECT_FALSE(has_variant_layout(probe.value(), 3, "left_only"));

    auto read_result = read_rowsets({compacted.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);
}

TEST_F(IndexStorageVariantSchemaEvolutionTest,
       MultiVariantColumnsKeepSharedPathIsolatedWithExternalSegmentMeta) {
    run_multi_variant_columns_keep_shared_path_isolated(true, 110047);
}

TEST_F(IndexStorageVariantSchemaEvolutionTest,
       MultiVariantColumnsKeepSharedPathIsolatedWithoutExternalSegmentMeta) {
    run_multi_variant_columns_keep_shared_path_isolated(false, 110053);
}

TEST_F(IndexStorageVariantSchemaEvolutionTest, SchemaPatchHelperKeepsVariantPathMetadataIsolated) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexTabletOptions options;
    options.tablet_id = 110048;
    options.variant_columns = {std::move(variant)};
    options.inverted_indexes = {IndexSpec::field_pattern_index(230001, "idx_v_a", 2, "a")};
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    VariantColumnSpec modified;
    modified.unique_id = 2;
    modified.name = "v";
    modified.predefined_paths = {
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    VariantColumnSpec added;
    added.unique_id = 3;
    added.name = "v_added";
    added.predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexSchemaPatch patch;
    patch.modify_variant_columns.emplace(2, std::move(modified));
    patch.add_variant_columns.push_back(std::move(added));
    patch.add_inverted_indexes.push_back(
            IndexSpec::field_pattern_index(230002, "idx_v_added_a", 3, "a"));
    auto patched_schema = build_patched_tablet_schema(*base_schema, patch);
    ASSERT_NE(patched_schema, nullptr);

    EXPECT_TRUE(schema_has_variant_path(*patched_schema, 2, "a"));
    EXPECT_TRUE(schema_has_variant_path(*patched_schema, 2, "b"));
    EXPECT_TRUE(schema_has_variant_path(*patched_schema, 3, "a"));
    EXPECT_FALSE(schema_has_variant_path(*patched_schema, 3, "b"));
    EXPECT_FALSE(patched_schema->inverted_index_by_field_pattern(2, "a").empty());
    EXPECT_FALSE(patched_schema->inverted_index_by_field_pattern(3, "a").empty());
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(2, "missing").empty());
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(3, "b").empty());
}

} // namespace doris::index_storage_test
