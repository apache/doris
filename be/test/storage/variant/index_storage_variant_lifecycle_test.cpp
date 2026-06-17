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

#include "core/data_type/data_type_nullable.h"
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

DataTypePtr nullable_string_target_type() {
    return make_nullable(std::make_shared<DataTypeString>());
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

bool schema_has_variant_path(const TabletSchema& schema, int32_t parent_unique_id,
                             std::string_view relative_path) {
    if (!schema.has_column_unique_id(parent_unique_id)) {
        return false;
    }
    const auto& parent = schema.column_by_uid(parent_unique_id);
    for (const auto& subcolumn : parent.get_sub_columns()) {
        if (subcolumn == nullptr) {
            continue;
        }
        if (subcolumn->name() == relative_path ||
            (subcolumn->has_path_info() &&
             subcolumn->path_info_ptr()->copy_pop_front().get_path() == relative_path)) {
            return true;
        }
    }
    return false;
}

bool schema_has_extracted_variant_path(const TabletSchema& schema, int32_t parent_unique_id,
                                       std::string_view relative_path) {
    for (int32_t column_id = 0; column_id < schema.num_columns(); ++column_id) {
        const auto& column = schema.column(column_id);
        if (!column.is_extracted_column() || column.parent_unique_id() != parent_unique_id ||
            !column.has_path_info()) {
            continue;
        }
        if (column.path_info_ptr()->copy_pop_front().get_path() == relative_path) {
            return true;
        }
    }
    return false;
}

size_t sparse_stat_entry_count(const IndexRowsetProbe& probe) {
    size_t count = 0;
    for (const auto& segment : probe.segments) {
        for (const auto& column : segment.variant_columns) {
            count += column.sparse_non_null_size.size();
        }
    }
    return count;
}

} // namespace

class IndexStorageVariantLifecycleTest : public IndexStorageTestFixture {
protected:
    void run_deep_sparse_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_nested_group_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_variant_path_count_on_index_records_applied_event(bool verify_key_column_not_read);
    void run_multi_variant_columns_keep_shared_path_isolated(bool external_segment_meta,
                                                             int64_t tablet_id);
};

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsHonorsZeroLimitAndTieOrdering) {
    variant_util::PathToNoneNullValues stats {
            {"alpha", 3},
            {"beta", 3},
            {"gamma", 1},
    };

    TabletSchema::PathsSetInfo unlimited;
    variant_util::VariantCompactionUtil::get_subpaths(0, stats, unlimited);
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(unlimited.sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(unlimited.sparse_path_set.empty());

    TabletSchema::PathsSetInfo top_one;
    variant_util::VariantCompactionUtil::get_subpaths(1, stats, top_one);
    EXPECT_TRUE(top_one.sub_path_set.contains(StringRef("beta")));
    EXPECT_FALSE(top_one.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(top_one.sparse_path_set.contains(StringRef("gamma")));
}

TEST(IndexStorageVariantCompactionUtilTest, GetSubpathsKeepsAllPathsAtLimitAndHandlesEmptyStats) {
    variant_util::PathToNoneNullValues exact_limit {
            {"alpha", 2},
            {"beta", 1},
    };

    TabletSchema::PathsSetInfo at_limit;
    variant_util::VariantCompactionUtil::get_subpaths(2, exact_limit, at_limit);
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(at_limit.sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(at_limit.sparse_path_set.empty());

    TabletSchema::PathsSetInfo empty;
    variant_util::VariantCompactionUtil::get_subpaths(1, {}, empty);
    EXPECT_TRUE(empty.sub_path_set.empty());
    EXPECT_TRUE(empty.sparse_path_set.empty());
}

TEST(IndexStorageVariantCompactionUtilTest, EmptyInputsKeepVariantSchemaWithoutPathStats) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";

    IndexTabletOptions options;
    options.tablet_id = 110054;
    options.variant_columns = {std::move(variant)};
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    EXPECT_EQ(
            variant_util::VariantCompactionUtil::calculate_variant_extended_schema({}, base_schema),
            nullptr);

    auto compaction_schema = std::make_shared<TabletSchema>(*base_schema);
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(compaction_schema->has_column_unique_id(2));

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.empty());
    EXPECT_TRUE(path_set_info->sub_path_set.empty());
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
}

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

void IndexStorageVariantLifecycleTest::run_variant_path_count_on_index_records_applied_event(
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
        expect_applied_variant_path_index(read_result.value(), "b", 220401, 1);
        expect_index_probe_count(read_result.value(),
                                 IndexProbeExpectation {
                                         .source = IndexProbeSource::COLUMN_PREDICATE,
                                         .state = IndexProbeState::APPLIED,
                                         .reason = IndexFallbackReason::NONE,
                                         .column_uid = 2,
                                         .variant_path = "b",
                                         .index_id = 220401,
                                         .counts_toward_filter_stats = true,
                                         .filtered_rows = 1,
                                 },
                                 1);
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

TEST_F(IndexStorageVariantLifecycleTest, DISABLED_BuildAndDropVariantPathIndexAfterExistingRows) {
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

TEST_F(IndexStorageVariantLifecycleTest,
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

// DORIS-26466 is intentionally kept out for now; the product path is not being fixed yet.
#if 0
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
#endif

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

TEST_F(IndexStorageVariantLifecycleTest, VariantCompactionSchemaTopNRecordsSparseOverflowPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = 2;

    IndexTabletOptions options;
    options.tablet_id = 110045;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h0", "warm": "w0", "rare0": "r0"})",
             R"({"hot": "h1", "warm": "w1", "rare1": "r1"})", R"({"hot": "h2", "rare2": "r2"})"},
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h3", "warm": "w3", "rare3": "r3"})", R"({"hot": "h4", "rare4": "r4"})"},
            100));

    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            rowsets.value(), compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("hot")));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("warm")));
    EXPECT_TRUE(path_set_info->sparse_path_set.contains(StringRef("warm")));
    EXPECT_TRUE(path_set_info->sparse_path_set.contains(StringRef("rare0")));
}

TEST_F(IndexStorageVariantLifecycleTest,
       VariantMaxSubcolumnsCountZeroMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 0;

    IndexTabletOptions options;
    options.tablet_id = 110049;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "gamma": "g1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("gamma")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "gamma"));
}

TEST_F(IndexStorageVariantLifecycleTest, VariantPathCountAtLimitMaterializesAllObservedPaths) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110050;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"alpha": "a0", "beta": "b0"})", R"({"alpha": "a1", "beta": "b1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("alpha")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("beta")));
    EXPECT_TRUE(path_set_info->sparse_path_set.empty());
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "alpha"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "beta"));
}

TEST_F(IndexStorageVariantLifecycleTest, VariantSparseStatsLimitIsPreservedAfterCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = 2;

    IndexTabletOptions options;
    options.tablet_id = 110046;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h0", "warm": "w0", "rare0": "r0"})",
             R"({"hot": "h1", "warm": "w1", "rare1": "r1"})", R"({"hot": "h2", "rare2": "r2"})"},
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h3", "warm": "w3", "rare3": "r3"})", R"({"hot": "h4", "rare4": "r4"})"},
            100));

    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    ASSERT_EQ(compacted.value()->num_rows(), 5);

    auto probe = probe_rowset(compacted.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "hot"));
    EXPECT_FALSE(has_variant_layout(probe.value(), 2, "warm"));
    EXPECT_TRUE(has_sparse_path_stat(probe.value(), "warm"));
    EXPECT_LE(sparse_stat_entry_count(probe.value()), 2);
}

TEST_F(IndexStorageVariantLifecycleTest, VariantSparseStatsLimitOneIsPreservedAfterCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = 1;

    IndexTabletOptions options;
    options.tablet_id = 110051;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h0", "warm": "w0", "rare0": "r0"})",
             R"({"hot": "h1", "warm": "w1", "rare1": "r1"})", R"({"hot": "h2", "rare2": "r2"})"},
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h3", "warm": "w3", "rare3": "r3"})", R"({"hot": "h4", "rare4": "r4"})"},
            100));

    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    ASSERT_EQ(compacted.value()->num_rows(), 5);

    auto probe = probe_rowset(compacted.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "hot"));
    EXPECT_LE(sparse_stat_entry_count(probe.value()), 1);
}

TEST_F(IndexStorageVariantLifecycleTest,
       VariantTypedPathAndDynamicSubpathStaySeparatedInCompactionSchema) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "typed_i",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    IndexTabletOptions options;
    options.tablet_id = 110052;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_variant(
            {R"({"typed_i": 1, "hot": "h0"})", R"({"typed_i": 2, "cold": "c1"})"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto compaction_schema = std::make_shared<TabletSchema>(*tablet_schema());
    auto status = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            {rowset_result.value()}, compaction_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(2);
    ASSERT_NE(path_set_info, nullptr);
    EXPECT_TRUE(path_set_info->typed_path_set.contains("typed_i"));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("typed_i")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("hot")));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("cold")));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "typed_i"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "hot"));
    EXPECT_TRUE(schema_has_extracted_variant_path(*compaction_schema, 2, "cold"));
}

void IndexStorageVariantLifecycleTest::run_multi_variant_columns_keep_shared_path_isolated(
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

TEST_F(IndexStorageVariantLifecycleTest,
       MultiVariantColumnsKeepSharedPathIsolatedWithExternalSegmentMeta) {
    run_multi_variant_columns_keep_shared_path_isolated(true, 110047);
}

TEST_F(IndexStorageVariantLifecycleTest,
       MultiVariantColumnsKeepSharedPathIsolatedWithoutExternalSegmentMeta) {
    run_multi_variant_columns_keep_shared_path_isolated(false, 110053);
}

TEST_F(IndexStorageVariantLifecycleTest, SchemaPatchHelperKeepsVariantPathMetadataIsolated) {
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
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
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

TEST_F(IndexStorageVariantLifecycleTest, VariantPathCountOnIndexRecordsAppliedEvent) {
    run_variant_path_count_on_index_records_applied_event(false);
}

TEST_F(IndexStorageVariantLifecycleTest, VariantPathCountOnIndexSkipsReadingKeyData) {
    run_variant_path_count_on_index_records_applied_event(true);
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
    const int32_t sibling_path_column_id = column_id_by_path("v.string2");
    ASSERT_GE(sibling_path_column_id, 0);
    const auto& sibling_path_column = tablet_schema()->column(sibling_path_column_id);

    auto expect_only_path_string_index_applied =
            [](const IndexReadResult& result, std::string_view path, int64_t index_id,
               int64_t expected_filtered_rows, int64_t expected_probe_count) {
                expect_applied_variant_path_index(result, path, index_id, expected_filtered_rows);
                expect_index_probe_count(result,
                                         {.source = IndexProbeSource::COLUMN_PREDICATE,
                                          .state = IndexProbeState::APPLIED,
                                          .reason = IndexFallbackReason::NONE,
                                          .column_uid = 2,
                                          .variant_path = std::string(path),
                                          .index_id = index_id,
                                          .counts_toward_filter_stats = true},
                                         expected_probe_count);
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
    EXPECT_EQ(before_compaction->rows_read, 5);
    expect_only_path_string_index_applied(before_compaction.value(), "string1", 210102, 5, 2);
    expect_index_not_filtering(before_compaction.value(), 210100);
    expect_index_not_filtering(before_compaction.value(), 210101);
    expect_index_not_filtering(before_compaction.value(), 210103);
    expect_index_not_filtering(before_compaction.value(), 210104);
    expect_index_not_filtering(before_compaction.value(), 210105);

    auto sibling_before_compaction = read_rowsets(readable_rowsets.value(), sibling_read_options);
    ASSERT_TRUE(sibling_before_compaction.has_value()) << sibling_before_compaction.error();
    EXPECT_EQ(sibling_before_compaction->rows_read, 5);
    expect_only_path_string_index_applied(sibling_before_compaction.value(), "string2", 210104, 5,
                                          2);
    expect_index_not_filtering(sibling_before_compaction.value(), 210100);
    expect_index_not_filtering(sibling_before_compaction.value(), 210101);
    expect_index_not_filtering(sibling_before_compaction.value(), 210102);
    expect_index_not_filtering(sibling_before_compaction.value(), 210103);
    expect_index_not_filtering(sibling_before_compaction.value(), 210105);

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
    EXPECT_EQ(after_compaction->rows_read, 5);
    expect_only_path_string_index_applied(after_compaction.value(), "string1", 210102, 5, 1);
    expect_index_not_filtering(after_compaction.value(), 210100);
    expect_index_not_filtering(after_compaction.value(), 210101);
    expect_index_not_filtering(after_compaction.value(), 210103);
    expect_index_not_filtering(after_compaction.value(), 210104);
    expect_index_not_filtering(after_compaction.value(), 210105);

    auto sibling_after_compaction = read_rowsets(readable_compacted.value(), sibling_read_options);
    ASSERT_TRUE(sibling_after_compaction.has_value()) << sibling_after_compaction.error();
    EXPECT_EQ(sibling_after_compaction->rows_read, 5);
    expect_only_path_string_index_applied(sibling_after_compaction.value(), "string2", 210104, 5,
                                          1);
    expect_index_not_filtering(sibling_after_compaction.value(), 210100);
    expect_index_not_filtering(sibling_after_compaction.value(), 210101);
    expect_index_not_filtering(sibling_after_compaction.value(), 210102);
    expect_index_not_filtering(sibling_after_compaction.value(), 210103);
    expect_index_not_filtering(sibling_after_compaction.value(), 210105);
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
