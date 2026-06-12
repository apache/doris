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

#include <algorithm>
#include <string_view>
#include <unordered_map>

#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

std::shared_ptr<ColumnPredicate> string_equals(int32_t column_id, std::string column_name,
                                               std::string value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeString>(),
            Field::create_field<TYPE_STRING>(std::move(value)), false);
}

bool has_doc_value_column(const IndexRowsetProbe& probe) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [](const auto& column) { return column.is_doc_value_column; });
    });
}

bool has_variant_layout(const IndexRowsetProbe& probe, int32_t parent_unique_id,
                        std::string_view relative_path) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [&](const auto& column) {
                               return column.parent_unique_id == parent_unique_id &&
                                      column.relative_path == relative_path;
                           });
    });
}

bool has_variant_parent(const IndexRowsetProbe& probe, int32_t parent_unique_id) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(
                segment.variant_columns.begin(), segment.variant_columns.end(),
                [&](const auto& column) { return column.parent_unique_id == parent_unique_id; });
    });
}

bool has_sparse_path_stat(const IndexRowsetProbe& probe, std::string_view relative_path) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [&](const auto& column) {
                               auto it =
                                       column.sparse_non_null_size.find(std::string(relative_path));
                               return it != column.sparse_non_null_size.end() && it->second > 0;
                           });
    });
}

} // namespace

class IndexStorageVariantLifecycleTest : public IndexStorageTestFixture {
protected:
    void run_deep_sparse_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_nested_group_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
};

void IndexStorageVariantLifecycleTest::run_deep_sparse_variant_lifecycle(bool external_segment_meta,
                                                                         int64_t tablet_id) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.sparse_hash_shard_count = 2;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h0", "deep": {"rare_a": "a0"}, "cold0": "c0"})",
             R"({"hot": "h1", "deep": {"rare_b": "b0"}, "cold1": "c1"})"},
            0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    auto rowset0_probe = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(rowset0_probe.has_value()) << rowset0_probe.error();
    EXPECT_TRUE(has_variant_layout(rowset0_probe.value(), 2, "hot"));
    EXPECT_TRUE(has_sparse_path_stat(rowset0_probe.value(), "deep.rare_a"));
    EXPECT_TRUE(has_sparse_path_stat(rowset0_probe.value(), "deep.rare_b"));

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h2", "deep": {"rare_a": "a1"}, "cold2": "c2"})",
             R"({"hot": "h3", "deep": {"rare_c": "c0"}, "cold3": "c3"})"},
            100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto read_result = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "hot"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_a"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_b"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_c"));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

void IndexStorageVariantLifecycleTest::run_nested_group_variant_lifecycle(
        bool external_segment_meta, int64_t tablet_id) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.sparse_hash_shard_count = 2;
    variant.enable_nested_group = true;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"owner": "alice", "profile": {"region": "us"}, "items": [{"sku": "a", "qty": 1}]})",
             R"({"owner": "bob", "profile": {"region": "eu"}, "items": [{"sku": "b", "qty": 2}]})"},
            0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"owner": "carol", "profile": {"region": "apac"}, "items": [{"sku": "c", "qty": 3}]})",
             R"({"owner": "dave", "profile": {"region": "us"}, "items": [{"sku": "d", "qty": 4}]})"},
            100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);
    ASSERT_TRUE(compacted.value()->tablet_schema()->has_column_unique_id(2));
    EXPECT_TRUE(compacted.value()->tablet_schema()->column_by_uid(2).variant_enable_nested_group());

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_variant_parent(compacted_probe.value(), 2));
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "owner"));
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "profile.region"));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

TEST_F(IndexStorageVariantLifecycleTest, BuildAndDropVariantPathIndexAfterExistingRows) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    const auto index_case =
            IndexStorageCaseBuilder("build_and_drop_variant_path_index_after_existing_rows")
                    .tablet_id(110018)
                    .variant_column(std::move(variant))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"b": "one"})", R"({"b": "other"})"}, 0))
                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                       {R"({"b": "other"})", R"({"b": "one"})"}, 100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();

    const int32_t path_column_id = column_id_by_path("v.b");
    ASSERT_GE(path_column_id, 0);
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] =
            std::make_shared<DataTypeString>();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));

    auto before_build = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_build.has_value()) << before_build.error();
    EXPECT_EQ(before_build->rows_read, 2);
    EXPECT_EQ(before_build->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(before_build.value());

    const auto path_index = IndexSpec::field_pattern_index(20004, "idx_v_b_built", 2, "b");
    auto built_rowsets = build_inverted_indexes({path_index});
    ASSERT_TRUE(built_rowsets.has_value()) << built_rowsets.error();
    ASSERT_EQ(built_rowsets->size(), 2);
    for (const auto& rowset : built_rowsets.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), true);
    }

    auto reloaded_built_rowsets = reload_rowsets(built_rowsets.value());
    ASSERT_TRUE(reloaded_built_rowsets.has_value()) << reloaded_built_rowsets.error();
    auto after_build = read_rowsets(reloaded_built_rowsets.value(), read_options);
    ASSERT_TRUE(after_build.has_value()) << after_build.error();
    EXPECT_EQ(after_build->rows_read, 2);
    expect_applied_variant_path_index(after_build.value(), "b", 20004, 2);

    auto dropped_rowsets = drop_inverted_indexes({path_index});
    ASSERT_TRUE(dropped_rowsets.has_value()) << dropped_rowsets.error();
    ASSERT_EQ(dropped_rowsets->size(), 2);
    for (const auto& rowset : dropped_rowsets.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), false);
    }

    auto reloaded_dropped_rowsets = reload_rowsets(dropped_rowsets.value());
    ASSERT_TRUE(reloaded_dropped_rowsets.has_value()) << reloaded_dropped_rowsets.error();
    auto after_drop = read_rowsets(reloaded_dropped_rowsets.value(), read_options);
    ASSERT_TRUE(after_drop.has_value()) << after_drop.error();
    EXPECT_EQ(after_drop->rows_read, 2);
    EXPECT_EQ(after_drop->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(after_drop.value());
}

TEST_F(IndexStorageVariantLifecycleTest,
       DropVariantPathIndexWithoutMaterializedPathRemovesMetadata) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";

    IndexTabletOptions options;
    options.tablet_id = 110021;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(
            IndexBatch::single_variant({R"({"b": "one"})", R"({"b": "other"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    const auto missing_path_index =
            IndexSpec::field_pattern_index(20005, "idx_v_missing", 2, "missing");
    auto built_rowsets = build_inverted_indexes({missing_path_index});
    ASSERT_TRUE(built_rowsets.has_value()) << built_rowsets.error();
    ASSERT_EQ(built_rowsets->size(), 1);
    EXPECT_FALSE(built_rowsets->front()
                         ->tablet_schema()
                         ->inverted_index_by_field_pattern(2, "missing")
                         .empty());

    auto dropped_rowsets = drop_inverted_indexes({missing_path_index});
    ASSERT_TRUE(dropped_rowsets.has_value()) << dropped_rowsets.error();
    ASSERT_EQ(dropped_rowsets->size(), 1);
    EXPECT_TRUE(dropped_rowsets->front()
                        ->tablet_schema()
                        ->inverted_index_by_field_pattern(2, "missing")
                        .empty());
}

TEST_F(IndexStorageVariantLifecycleTest, DropOneVariantPathIndexKeepsSiblingPathIndexUsable) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "c",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    const auto index_case =
            IndexStorageCaseBuilder("drop_one_variant_path_index_keeps_sibling_path_index_usable")
                    .tablet_id(110022)
                    .variant_column(std::move(variant))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant({R"({"b": "drop", "c": "keep"})",
                                                                 R"({"b": "other", "c": "other"})"},
                                                                0))
                    .rowset(1,
                            IndexDataSourceSpec::inline_variant({R"({"b": "other", "c": "other"})",
                                                                 R"({"b": "drop", "c": "keep"})"},
                                                                100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();

    const int32_t b_column_id = column_id_by_path("v.b");
    const int32_t c_column_id = column_id_by_path("v.c");
    ASSERT_GE(b_column_id, 0);
    ASSERT_GE(c_column_id, 0);
    const auto& b_column = tablet_schema()->column(b_column_id);
    const auto& c_column = tablet_schema()->column(c_column_id);

    IndexReadOptions b_read_options;
    b_read_options.return_columns = {0, static_cast<uint32_t>(b_column_id)};
    b_read_options.target_cast_type_for_variants[b_column.name()] =
            std::make_shared<DataTypeString>();
    b_read_options.predicates.push_back(string_equals(b_column_id, b_column.name(), "drop"));

    IndexReadOptions c_read_options;
    c_read_options.return_columns = {0, static_cast<uint32_t>(c_column_id)};
    c_read_options.target_cast_type_for_variants[c_column.name()] =
            std::make_shared<DataTypeString>();
    c_read_options.predicates.push_back(string_equals(c_column_id, c_column.name(), "keep"));

    const auto b_index = IndexSpec::field_pattern_index(20006, "idx_v_b_drop", 2, "b");
    const auto c_index = IndexSpec::field_pattern_index(20007, "idx_v_c_keep", 2, "c");
    auto built_rowsets = build_inverted_indexes({b_index, c_index});
    ASSERT_TRUE(built_rowsets.has_value()) << built_rowsets.error();
    ASSERT_EQ(built_rowsets->size(), 2);
    for (const auto& rowset : built_rowsets.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), true);
    }

    auto reloaded_built_rowsets = reload_rowsets(built_rowsets.value());
    ASSERT_TRUE(reloaded_built_rowsets.has_value()) << reloaded_built_rowsets.error();
    auto b_after_build = read_rowsets(reloaded_built_rowsets.value(), b_read_options);
    ASSERT_TRUE(b_after_build.has_value()) << b_after_build.error();
    EXPECT_EQ(b_after_build->rows_read, 2);
    expect_applied_variant_path_index(b_after_build.value(), "b", 20006, 2);
    auto c_after_build = read_rowsets(reloaded_built_rowsets.value(), c_read_options);
    ASSERT_TRUE(c_after_build.has_value()) << c_after_build.error();
    EXPECT_EQ(c_after_build->rows_read, 2);
    expect_applied_variant_path_index(c_after_build.value(), "c", 20007, 2);

    auto dropped_rowsets = drop_inverted_indexes({b_index});
    ASSERT_TRUE(dropped_rowsets.has_value()) << dropped_rowsets.error();
    ASSERT_EQ(dropped_rowsets->size(), 2);
    for (const auto& rowset : dropped_rowsets.value()) {
        EXPECT_TRUE(rowset->tablet_schema()->inverted_index_by_field_pattern(2, "b").empty());
        EXPECT_FALSE(rowset->tablet_schema()->inverted_index_by_field_pattern(2, "c").empty());
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), true);
    }

    auto reloaded_dropped_rowsets = reload_rowsets(dropped_rowsets.value());
    ASSERT_TRUE(reloaded_dropped_rowsets.has_value()) << reloaded_dropped_rowsets.error();
    auto b_after_drop = read_rowsets(reloaded_dropped_rowsets.value(), b_read_options);
    ASSERT_TRUE(b_after_drop.has_value()) << b_after_drop.error();
    EXPECT_EQ(b_after_drop->rows_read, 2);
    EXPECT_EQ(b_after_drop->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(b_after_drop.value());

    auto c_after_drop = read_rowsets(reloaded_dropped_rowsets.value(), c_read_options);
    ASSERT_TRUE(c_after_drop.has_value()) << c_after_drop.error();
    EXPECT_EQ(c_after_drop->rows_read, 2);
    expect_applied_variant_path_index(c_after_drop.value(), "c", 20007, 2);
}

TEST_F(IndexStorageVariantLifecycleTest, PatchedSchemaAddDropVariantColumnCompactsNewRows) {
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

TEST_F(IndexStorageVariantLifecycleTest, MissingRequiredVariantColumnFailsExtendedInfoAggregation) {
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

TEST_F(IndexStorageVariantLifecycleTest, VariantPathIndexHitAfterCumulativeCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    const auto index_case =
            IndexStorageCaseBuilder("variant_path_index_hit_after_cumulative_compaction")
                    .tablet_id(110006)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::field_pattern_index(20002, "idx_v_b", 2, "b"))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"b": "one"})", R"({"b": "other"})"}, 0))
                    .rowset(1, IndexDataSourceSpec::variant_jsonl(
                                       "be/test/storage/test_data/index_storage/"
                                       "variant_path_index.jsonl",
                                       100, 1))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.b");
    ASSERT_GE(path_column_id, 0);
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] =
            std::make_shared<DataTypeString>();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));
    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_applied_variant_path_index(read_result.value(), "b", 20002, 1);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.b");
    ASSERT_EQ(compacted_path_column_id, path_column_id);
    auto compacted_read = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 2);
    expect_applied_variant_path_index(compacted_read.value(), "b", 20002, 2);
}

TEST_F(IndexStorageVariantLifecycleTest, VariantPathEqualityChoosesStringIndexAfterCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 10;
    variant.predefined_paths = {
            VariantPathSpec {.path = "string1",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "string2",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "array_string",
                             .type = FieldType::OLAP_FIELD_TYPE_ARRAY,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .array_item_nullable = true},
    };

    IndexSpec phrase_index =
            IndexSpec::field_pattern_index(210101, "idx_v_string1_phrase", 2, "string1");
    phrase_index.properties["parser"] = "unicode";
    phrase_index.properties["support_phrase"] = "true";
    const auto string_index =
            IndexSpec::field_pattern_index(210102, "idx_v_string1_string", 2, "string1");
    IndexSpec sibling_phrase_index =
            IndexSpec::field_pattern_index(210103, "idx_v_string2_phrase", 2, "string2");
    sibling_phrase_index.properties["parser"] = "unicode";
    sibling_phrase_index.properties["support_phrase"] = "false";
    const auto sibling_string_index =
            IndexSpec::field_pattern_index(210104, "idx_v_string2_string", 2, "string2");
    const auto array_string_index =
            IndexSpec::field_pattern_index(210105, "idx_v_array_string", 2, "array_string");

    const auto index_case =
            IndexStorageCaseBuilder("variant_path_equality_chooses_string_index_after_compaction")
                    .tablet_id(110031)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::column_index(210100, "idx_v_parent", 2))
                    .inverted_index(std::move(phrase_index))
                    .inverted_index(string_index)
                    .inverted_index(std::move(sibling_phrase_index))
                    .inverted_index(sibling_string_index)
                    .inverted_index(array_string_index)
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"string1": "hello", "string2": "hello", "array_string": ["hello"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})",
                                     R"({"string1": "hello", "string2": "hello", "array_string": ["hello"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})",
                                     R"({"string1": "hello", "string2": "hello", "array_string": ["hello"]})"},
                                    0))
                    .rowset(1,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"string1": "hello", "string2": "hello", "array_string": ["hello"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})",
                                     R"({"string1": "hello", "string2": "hello", "array_string": ["hello"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})"},
                                    100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.string1");
    ASSERT_GE(path_column_id, 0);
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] =
            std::make_shared<DataTypeString>();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "hello"));

    auto before_compaction = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_compaction.has_value()) << before_compaction.error();
    EXPECT_EQ(before_compaction->rows_read, 5);
    expect_applied_variant_path_index(before_compaction.value(), "string1", 210102, 5);
    expect_index_not_filtering(before_compaction.value(), 210100);
    expect_index_not_filtering(before_compaction.value(), 210101);
    expect_index_not_filtering(before_compaction.value(), 210103);
    expect_index_not_filtering(before_compaction.value(), 210104);
    expect_index_not_filtering(before_compaction.value(), 210105);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 10);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.string1");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    auto after_compaction = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(after_compaction.has_value()) << after_compaction.error();
    EXPECT_EQ(after_compaction->rows_read, 5);
    expect_applied_variant_path_index(after_compaction.value(), "string1", 210102, 5);
    expect_index_not_filtering(after_compaction.value(), 210100);
    expect_index_not_filtering(after_compaction.value(), 210101);
    expect_index_not_filtering(after_compaction.value(), 210103);
    expect_index_not_filtering(after_compaction.value(), 210104);
    expect_index_not_filtering(after_compaction.value(), 210105);
}

TEST_F(IndexStorageVariantLifecycleTest, WriteReadProbeAndCumulativeCompact) {
    IndexTabletOptions options;
    options.tablet_id = 110002;
    options.external_segment_meta = true;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.variant_columns[0].max_subcolumns_count = 4;
    options.variant_columns[0].sparse_hash_shard_count = 2;
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"a": 1, "b": "one"})", R"({"a": 2, "c": 20})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(
            IndexBatch::single_variant({R"({"a": 3, "b": "three"})", R"({"a": 4, "d": 40})"}, 100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto read_result = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);

    auto probe_result = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(probe_result.has_value()) << probe_result.error();
    EXPECT_EQ(probe_result->num_rows, 2);
    EXPECT_EQ(probe_result->num_segments, 1);
    EXPECT_TRUE(probe_result->contains_relative_path("a"));
    EXPECT_TRUE(probe_result->contains_relative_path("b"));
    expect_index_files(probe_result.value(), false);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(compacted_probe->contains_relative_path("a"));
    EXPECT_TRUE(compacted_probe->contains_relative_path("b"));
}

TEST_F(IndexStorageVariantLifecycleTest, DeepSparseVariantCompactsWithExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(true, 110023);
}

TEST_F(IndexStorageVariantLifecycleTest, DeepSparseVariantCompactsWithoutExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(false, 110024);
}

TEST_F(IndexStorageVariantLifecycleTest,
       ExactSparsePathReadsHiddenChildAfterSparseStatsLimitTruncated) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = 2;

    IndexTabletOptions options;
    options.tablet_id = 110028;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"a": "hot-0", "b": "scalar-0", "d": "dense-0"})",
                                        R"({"a": "hot-1", "b": "scalar-1", "d": "dense-1"})",
                                        R"({"a": "hot-2", "b": "scalar-2", "d": "dense-2"})"},
                                       0));

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(
            IndexBatch::single_variant({R"({"a": "hot-3", "b": "scalar-3", "d": "dense-3"})",
                                        R"({"a": "hot-4", "b": {"c": "child-0"}})"},
                                       100));

    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    ASSERT_EQ(compacted.value()->num_rows(), 5);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    auto read_schema = build_schema_with_variant_path_column(*tablet_schema(), 2, "b",
                                                             FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(read_schema, nullptr);
    auto readable_compacted =
            inject_reader_schema_for_rowsets(reloaded.value(), std::move(read_schema));
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();

    auto compacted_probe = probe_rowset(readable_compacted->front());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    ASSERT_TRUE(has_variant_layout(compacted_probe.value(), 2, "a"));
    ASSERT_TRUE(has_sparse_path_stat(compacted_probe.value(), "b"));
    ASSERT_FALSE(has_sparse_path_stat(compacted_probe.value(), "b.c"));

    const int32_t b_column_id = column_id_by_path("v.b");
    ASSERT_GE(b_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& b_column = tablet_schema()->column(b_column_id);
    ASSERT_TRUE(b_column.is_variant_type());

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(b_column_id)};
    read_options.collect_variant_values = true;
    auto read_result = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    ASSERT_EQ(read_result->rows_read, 5);
    ASSERT_TRUE(read_result->variant_values_by_uid.contains(b_column.unique_id()));

    const auto& b_values = read_result->variant_values_by_uid.at(b_column.unique_id());
    ASSERT_EQ(b_values.size(), 5);
    const auto has_hidden_child = std::any_of(b_values.begin(), b_values.end(), [](const auto& v) {
        return v.has_value() && v->find("child-0") != std::string::npos;
    });

    std::ostringstream serialized_values;
    for (const auto& value : b_values) {
        serialized_values << (value.has_value() ? value.value() : "NULL") << '\n';
    }
    EXPECT_TRUE(has_hidden_child) << serialized_values.str();
}

TEST_F(IndexStorageVariantLifecycleTest, NestedGroupVariantCompactsWithExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(true, 110025);
}

TEST_F(IndexStorageVariantLifecycleTest, NestedGroupVariantCompactsWithoutExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(false, 110026);
}

TEST_F(IndexStorageVariantLifecycleTest, VariantDocModeWritesDocValueColumnsAfterCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 4;
    variant.enable_doc_mode = true;
    variant.doc_materialization_min_rows = 100000;
    variant.doc_hash_shard_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110012;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"a": "one", "b": 1})", R"({"a": "two", "c": 2})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"a": "three", "d": 3})", R"({"a": "four", "e": 4})"}, 100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto before_compaction_read = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(before_compaction_read.has_value()) << before_compaction_read.error();
    EXPECT_EQ(before_compaction_read->rows_read, 4);

    auto rowset0_probe = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(rowset0_probe.has_value()) << rowset0_probe.error();
    EXPECT_TRUE(has_doc_value_column(rowset0_probe.value()));

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_doc_value_column(compacted_probe.value()));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

} // namespace doris::index_storage_test
