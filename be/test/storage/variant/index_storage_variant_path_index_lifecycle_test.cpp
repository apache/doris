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

#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {

class IndexStorageVariantPathIndexLifecycleTest : public IndexStorageTestFixture {
protected:
    void run_variant_path_count_on_index_filters_rows(bool verify_key_column_not_read);
};

void IndexStorageVariantPathIndexLifecycleTest::run_variant_path_count_on_index_filters_rows(
        bool verify_key_column_not_read) {
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
            IndexStorageCaseBuilder("variant_path_count_on_index_skips_data")
                    .tablet_id(110037)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::field_pattern_index(220401, "idx_v_b_count", 2, "b"))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"b": "one"})", R"({"b": "other"})", R"({"b": "one"})"}, 0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.b");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));
    if (verify_key_column_not_read) {
        read_options.return_columns = {static_cast<uint32_t>(path_column_id)};
    }

    auto verify_read_result = [&](const Result<IndexReadResult>& read_result) {
        ASSERT_TRUE(read_result.has_value()) << read_result.error();
        EXPECT_EQ(read_result->rows_read, 2);
        EXPECT_EQ(read_result->stats.raw_rows_read, 2);
        expect_index_filter_stats(read_result.value(), 1);
    };

    auto read_and_verify = [&](const std::vector<RowsetSharedPtr>& readable) {
        if (verify_key_column_not_read) {
            ScopedDebugPoint fail_if_key_column_is_read("segment_iterator._read_columns_by_index",
                                                        {{"column_name", "k"}});
            auto read_result = read_rowsets(readable, read_options);
            verify_read_result(read_result);
            return;
        }
        auto read_result = read_rowsets(readable, read_options);
        verify_read_result(read_result);
    };

    read_and_verify(readable_rowsets.value());

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 3);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.b");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    read_and_verify(readable_compacted.value());
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest,
       DISABLED_BuildAndDropVariantPathIndexAfterExistingRows) {
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
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));

    auto before_build = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_build.has_value()) << before_build.error();
    EXPECT_EQ(before_build->rows_read, 2);
    expect_index_filter_stats(before_build.value(), 0);

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
    expect_index_filter_stats(after_build.value(), 2);

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
    expect_index_filter_stats(after_drop.value(), 0);
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest,
       DISABLED_DropVariantPathIndexWithoutMaterializedPathRemovesMetadata) {
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

TEST_F(IndexStorageVariantPathIndexLifecycleTest,
       DISABLED_DropOneVariantPathIndexKeepsSiblingPathIndexUsable) {
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
    b_read_options.target_cast_type_for_variants[b_column.name()] = nullable_string_target_type();
    b_read_options.predicates.push_back(string_equals(b_column_id, b_column.name(), "drop"));

    IndexReadOptions c_read_options;
    c_read_options.return_columns = {0, static_cast<uint32_t>(c_column_id)};
    c_read_options.target_cast_type_for_variants[c_column.name()] = nullable_string_target_type();
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
    expect_index_filter_stats(b_after_build.value(), 2);
    auto c_after_build = read_rowsets(reloaded_built_rowsets.value(), c_read_options);
    ASSERT_TRUE(c_after_build.has_value()) << c_after_build.error();
    EXPECT_EQ(c_after_build->rows_read, 2);
    expect_index_filter_stats(c_after_build.value(), 2);

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
    expect_index_filter_stats(b_after_drop.value(), 0);

    auto c_after_drop = read_rowsets(reloaded_dropped_rowsets.value(), c_read_options);
    ASSERT_TRUE(c_after_drop.has_value()) << c_after_drop.error();
    EXPECT_EQ(c_after_drop->rows_read, 2);
    expect_index_filter_stats(c_after_drop.value(), 2);
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest, VariantPathIndexHitAfterCumulativeCompaction) {
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
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));
    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_index_filter_stats(read_result.value(), 1);

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
    expect_index_filter_stats(compacted_read.value(), 2);
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest, VariantPathCountOnIndexFiltersRows) {
    run_variant_path_count_on_index_filters_rows(false);
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest, VariantPathCountOnIndexSkipsReadingKeyData) {
    run_variant_path_count_on_index_filters_rows(true);
}

TEST_F(IndexStorageVariantPathIndexLifecycleTest,
       VariantPathEqualityChoosesStringIndexAfterCompaction) {
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
                    // Keep hit sets asymmetric so a phrase, sibling, or array index cannot satisfy
                    // the same row/filter-count assertions as the exact string path index.
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"string1": "hello", "string2": "side", "array_string": ["array-only"]})",
                                     R"({"string1": "hello world", "string2": "side", "array_string": ["hello"]})",
                                     R"({"string1": "world", "string2": "world", "array_string": ["world"]})",
                                     R"({"string1": "other", "string2": "world", "array_string": ["hello"]})",
                                     R"({"string1": "hello", "string2": "side", "array_string": ["other"]})"},
                                    0))
                    .rowset(1,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"string1": "hello", "string2": "side", "array_string": ["hello"]})",
                                     R"({"string1": "other", "string2": "world", "array_string": ["array-only"]})",
                                     R"({"string1": "world", "string2": "side", "array_string": ["hello"]})",
                                     R"({"string1": "other", "string2": "side", "array_string": ["world"]})",
                                     R"({"string1": "hello", "string2": "side", "array_string": ["other"]})"},
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
    const int32_t sibling_path_column_id = column_id_by_path("v.string2");
    ASSERT_GE(sibling_path_column_id, 0);
    const auto& sibling_path_column = tablet_schema()->column(sibling_path_column_id);

    auto expect_path_index_filtered_rows = [](const IndexReadResult& result,
                                              int64_t expected_filtered_rows) {
        expect_index_filter_stats(result, expected_filtered_rows);
    };

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "hello"));

    IndexReadOptions sibling_read_options;
    sibling_read_options.return_columns = {0, static_cast<uint32_t>(sibling_path_column_id)};
    sibling_read_options.target_cast_type_for_variants[sibling_path_column.name()] =
            nullable_string_target_type();
    sibling_read_options.predicates.push_back(
            string_equals(sibling_path_column_id, sibling_path_column.name(), "world"));

    auto before_compaction = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_compaction.has_value()) << before_compaction.error();
    EXPECT_EQ(before_compaction->rows_read, 4);
    expect_path_index_filtered_rows(before_compaction.value(), 6);

    auto sibling_before_compaction = read_rowsets(readable_rowsets.value(), sibling_read_options);
    ASSERT_TRUE(sibling_before_compaction.has_value()) << sibling_before_compaction.error();
    EXPECT_EQ(sibling_before_compaction->rows_read, 3);
    expect_path_index_filtered_rows(sibling_before_compaction.value(), 7);

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
    const int32_t compacted_sibling_path_column_id = column_id_by_path("v.string2");
    ASSERT_EQ(compacted_sibling_path_column_id, sibling_path_column_id);

    auto after_compaction = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(after_compaction.has_value()) << after_compaction.error();
    EXPECT_EQ(after_compaction->rows_read, 4);
    expect_path_index_filtered_rows(after_compaction.value(), 6);

    auto sibling_after_compaction = read_rowsets(readable_compacted.value(), sibling_read_options);
    ASSERT_TRUE(sibling_after_compaction.has_value()) << sibling_after_compaction.error();
    EXPECT_EQ(sibling_after_compaction->rows_read, 3);
    expect_path_index_filtered_rows(sibling_after_compaction.value(), 7);
}

} // namespace doris::index_storage_test
