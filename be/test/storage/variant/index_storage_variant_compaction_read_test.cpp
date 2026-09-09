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

#include <tuple>

#include "core/data_type/data_type_number.h"
#include "cpp/sync_point.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {

static bool nested_group_write_path_available() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

class IndexStorageVariantCompactionReadTest : public IndexStorageTestFixture {
protected:
    void run_numeric_index_compaction(bool add_index);
    void run_deep_sparse_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_nested_group_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
};

void IndexStorageVariantCompactionReadTest::run_deep_sparse_variant_lifecycle(
        bool external_segment_meta, int64_t tablet_id) {
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

void IndexStorageVariantCompactionReadTest::run_nested_group_variant_lifecycle(
        bool external_segment_meta, int64_t tablet_id) {
    if (!nested_group_write_path_available()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build";
    }

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

TEST_F(IndexStorageVariantCompactionReadTest, WriteReadProbeAndCumulativeCompact) {
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

TEST_F(IndexStorageVariantCompactionReadTest, DeepSparseVariantCompactsWithExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(true, 110023);
}

TEST_F(IndexStorageVariantCompactionReadTest, DeepSparseVariantCompactsWithoutExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(false, 110024);
}

TEST_F(IndexStorageVariantCompactionReadTest, NestedGroupVariantCompactsWithExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(true, 110025);
}

TEST_F(IndexStorageVariantCompactionReadTest,
       NestedGroupVariantCompactsWithoutExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(false, 110026);
}

TEST_F(IndexStorageVariantCompactionReadTest, VariantDocModeWritesDocValueColumnsAfterCompaction) {
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

TEST_F(IndexStorageVariantCompactionReadTest, CompactionAppliesLatestVariantProperties) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.predefined_paths = {VariantPathSpec {.path = "old",
                                                 .type = FieldType::OLAP_FIELD_TYPE_INT,
                                                 .nullable = true,
                                                 .pattern_type = PatternTypePB::MATCH_NAME,
                                                 .array_item_type = {},
                                                 .array_item_nullable = true}};
    IndexTabletOptions options;
    options.tablet_id = 110055;
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    std::vector<RowsetSharedPtr> inputs;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(
                IndexBatch::single_variant({R"({"old": 7, "new": "001", "hot": 1})"}, version));
        auto written = write_rowset(rowset);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
    }
    options.variant_columns[0].max_subcolumns_count = 0;
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "new",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true}};
    auto target = build_tablet_schema(options);
    target->set_schema_version(tablet_schema()->schema_version() + 1);
    // The latest schema is known to BE but none of the input rowsets use it yet.
    _tablet->update_max_version_schema(target);
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, inputs);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    EXPECT_EQ(compacted.value()->tablet_schema()->schema_version(), target->schema_version());
    IndexReadOptions read_options;
    read_options.collect_variant_values = true;
    auto result = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->variant_values_by_uid.at(2).size(), 2);
    for (const auto& value : result->variant_values_by_uid.at(2)) {
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, R"({"hot":1,"new":1,"old":7})");
    }
}

TEST_F(IndexStorageVariantCompactionReadTest, CompactionAppliesCountChangesOutsideInputRowsets) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    IndexTabletOptions options;
    options.tablet_id = 110056;
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    std::vector<RowsetSharedPtr> inputs;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(IndexBatch::single_variant({R"({"a":1,"b":2,"c":3})"}, version));
        auto written = write_rowset(rowset);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
    }
    int schema_version = tablet_schema()->schema_version();
    for (int count : {0, 3, 1, 0}) {
        SCOPED_TRACE(count);
        options.variant_columns[0].max_subcolumns_count = count;
        auto target = build_tablet_schema(options);
        target->set_schema_version(++schema_version);
        _tablet->update_max_version_schema(target);
        auto compacted = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE, inputs);
        ASSERT_TRUE(compacted.has_value()) << compacted.error();
        EXPECT_EQ(compacted.value()->tablet_schema()->schema_version(), schema_version);
        auto probe = probe_rowset(compacted.value());
        ASSERT_TRUE(probe.has_value()) << probe.error();
        int materialized_paths = 0;
        for (const auto* path : {"a", "b", "c"}) {
            materialized_paths += has_variant_layout(probe.value(), 2, path);
        }
        // Zero preserves the existing meaning: no limit on materialized dynamic paths.
        EXPECT_EQ(materialized_paths, count == 0 ? 3 : count);
        IndexReadOptions read_options;
        read_options.collect_variant_values = true;
        auto read = read_rowsets({compacted.value()}, read_options);
        ASSERT_TRUE(read.has_value()) << read.error();
        ASSERT_EQ(read->variant_values_by_uid.at(2).size(), 2);
        for (const auto& value : read->variant_values_by_uid.at(2)) {
            ASSERT_TRUE(value.has_value());
            EXPECT_EQ(*value, R"({"a":1,"b":2,"c":3})");
        }
        inputs = {compacted.value()};
    }
}

void IndexStorageVariantCompactionReadTest::run_numeric_index_compaction(bool add_index) {
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 1;
    IndexTabletOptions options;
    options.tablet_id = 110057;
    options.variant_columns = {variant};
    if (add_index) {
        options.variant_columns[0].predefined_paths = {
                VariantPathSpec {.path = "a",
                                 .type = FieldType::OLAP_FIELD_TYPE_INT,
                                 .nullable = true,
                                 .pattern_type = PatternTypePB::MATCH_NAME,
                                 .array_item_type = {},
                                 .array_item_nullable = true}};
    } else {
        options.inverted_indexes = {IndexSpec::field_pattern_index(1100571, "idx_a", 2, "a")};
    }
    ASSERT_TRUE(create_tablet(options).ok());
    std::vector<RowsetSharedPtr> inputs;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(IndexBatch::single_variant(
                {R"({"a":"001","keep":"yes"})", R"({"a":"002","keep":"no"})"}, version * 2));
        auto written = write_rowset(rowset);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
    }
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true}};
    options.inverted_indexes = {IndexSpec::field_pattern_index(1100571, "idx_a", 2, "a")};
    auto target = build_tablet_schema(options);
    target->set_schema_version(tablet_schema()->schema_version() + 1);
    _tablet->update_max_version_schema(target);
    auto compacted = compact_rowsets(IndexCompactionKind::FULL, inputs);
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_EQ(compacted.value()->tablet_schema()->inverted_indexes().size(), 1);
    auto readable = rowsets_with_variant_extended_schema({compacted.value()});
    ASSERT_TRUE(readable.has_value()) << readable.error();
    const auto path_id = column_id_by_path("v.a");
    ASSERT_GE(path_id, 0);
    const auto path_name = tablet_schema()->column(path_id).name();
    IndexReadOptions read_options;
    read_options.return_columns = {0, 1, static_cast<uint32_t>(path_id)};
    read_options.collect_variant_values = true;
    read_options.need_ordered_result = true;
    read_options.enable_fallback_on_missing_inverted_index = false;
    read_options.target_cast_type_for_variants[path_name] =
            make_nullable(std::make_shared<DataTypeInt32>());
    read_options.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            path_id, path_name, std::make_shared<DataTypeInt32>(), Field::create_field<TYPE_INT>(1),
            false));
    auto indexed = read_rowsets(readable.value(), read_options);
    ASSERT_TRUE(indexed.has_value()) << indexed.error();
    EXPECT_EQ(indexed->rows_read, 2);
    expect_index_filter_stats(indexed.value(), 2);
    read_options.enable_inverted_index_query = false;
    auto scanned = read_rowsets(readable.value(), read_options);
    ASSERT_TRUE(scanned.has_value()) << scanned.error();
    EXPECT_EQ(scanned->rows_read, 2);
    EXPECT_EQ(indexed->variant_values_by_uid, scanned->variant_values_by_uid);
    for (const auto& value : indexed->variant_values_by_uid.at(2)) {
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, R"({"a":1,"keep":"yes"})");
    }
}

TEST_F(IndexStorageVariantCompactionReadTest, ConvertedNumericIndexFiltersRows) {
    run_numeric_index_compaction(false);
}

TEST_F(IndexStorageVariantCompactionReadTest, NewFieldPatternIndexFromTabletSchema) {
    run_numeric_index_compaction(true);
}

// Exercise both persisted layouts with a newer tablet schema arriving after the
// compaction snapshot. The stage callback controls the interleaving without sleeps.
class VariantPolicySnapshotTest : public IndexStorageTestFixture,
                                  public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(VariantPolicySnapshotTest, SnapshotWithInterleavedWritePreservesCompleteValues) {
    const auto [doc_mode, external_meta] = GetParam();
    VariantColumnSpec variant;
    variant.max_subcolumns_count = 1;
    variant.enable_doc_mode = doc_mode;
    IndexTabletOptions options;
    options.tablet_id = 110060 + doc_mode * 2 + external_meta;
    options.external_segment_meta = external_meta;
    options.variant_columns = {variant};
    ASSERT_TRUE(create_tablet(options).ok());
    const std::string original = R"({"a":"001","arr":[1,null,3],"nested":{"keep":"x"},"z":true})";
    std::vector<RowsetSharedPtr> inputs;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(IndexBatch::single_variant({original}, version));
        auto written = write_rowset(rowset);
        ASSERT_TRUE(written.has_value()) << written.error();
        inputs.push_back(written.value());
    }
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true}};
    auto first = build_tablet_schema(options);
    first->set_schema_version(tablet_schema()->schema_version() + 1);
    options.variant_columns[0].predefined_paths[0].type = FieldType::OLAP_FIELD_TYPE_STRING;
    auto second = build_tablet_schema(options);
    second->set_schema_version(first->schema_version() + 1);
    _tablet->update_max_version_schema(first);
    RowsetSharedPtr late_write;
    const bool sync_points_enabled = SyncPoint::get_instance()->get_enable();
    SyncPoint::get_instance()->enable_processing();
    Defer restore_sync_points([&] {
        SyncPoint::get_instance()->clear_call_back("compaction::CompactionMixin::build_basic_info");
        if (!sync_points_enabled) {
            SyncPoint::get_instance()->disable_processing();
        }
    });
    SyncPoint::get_instance()->set_call_back(
            "compaction::CompactionMixin::build_basic_info", [&](auto&& args) {
                if (late_write != nullptr) {
                    return;
                }
                // prepare_compaction_schema has already captured the first version.
                _tablet->update_max_version_schema(second);
                IndexRowsetSpec rowset;
                rowset.version = 2;
                rowset.batches.push_back(IndexBatch::single_variant({original}, 2));
                auto written = write_rowset(rowset);
                ASSERT_TRUE(written.has_value()) << written.error();
                late_write = written.value();
                // An older load must not replace the latest write policy.
                _tablet->update_max_version_schema(tablet_schema());
                EXPECT_EQ(_tablet->tablet_schema()->schema_version(), second->schema_version());
            });
    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    read_options.need_ordered_result = true;
    auto before = read_rowsets(inputs, read_options);
    ASSERT_TRUE(before.has_value()) << before.error();
    ASSERT_EQ(before->variant_values_by_uid.at(2).size(), 2);
    for (const auto& value : before->variant_values_by_uid.at(2)) {
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, original);
    }
    for (int pass = 0; pass < 2; ++pass) {
        auto compacted = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE, inputs);
        ASSERT_TRUE(compacted.has_value()) << compacted.error();
        EXPECT_EQ(compacted.value()->tablet_schema()->schema_version(),
                  pass == 0 ? first->schema_version() : second->schema_version());
        auto read = read_rowsets({compacted.value()}, read_options);
        ASSERT_TRUE(read.has_value()) << read.error();
        ASSERT_EQ(read->variant_values_by_uid.at(2).size(), pass == 0 ? 2 : 3);
        for (const auto& value : read->variant_values_by_uid.at(2)) {
            ASSERT_TRUE(value.has_value());
            // Doc buckets retain source values; templates convert the materialized copy.
            // Sparse mode reconstructs the root from converted subcolumns instead.
            const bool late_row = pass == 1 && &value == &read->variant_values_by_uid.at(2).back();
            EXPECT_EQ(*value,
                      (doc_mode || late_row) ? original
                      : pass == 0 ? R"({"a":1,"arr":[1,null,3],"nested":{"keep":"x"},"z":true})"
                                  : R"({"a":"1","arr":[1,null,3],"nested":{"keep":"x"},"z":true})");
        }
        // Recreate both metadata and schema from serialized bytes, not shared pointers.
        std::string persisted;
        ASSERT_TRUE(compacted.value()->rowset_meta()->serialize(&persisted));
        auto restored_meta = std::make_shared<RowsetMeta>();
        ASSERT_TRUE(restored_meta->init(persisted));
        EXPECT_EQ(restored_meta->tablet_schema()->schema_version(),
                  compacted.value()->tablet_schema()->schema_version());
        RowsetSharedPtr restored_rowset;
        ASSERT_TRUE(RowsetFactory::create_rowset(restored_meta->tablet_schema(),
                                                 compacted.value()->tablet_path(), restored_meta,
                                                 &restored_rowset)
                            .ok());
        auto restored_read = read_rowsets({restored_rowset}, read_options);
        ASSERT_TRUE(restored_read.has_value()) << restored_read.error();
        EXPECT_EQ(restored_read->variant_values_by_uid, read->variant_values_by_uid);
        auto path_schema = build_schema_with_variant_path_column(
                *compacted.value()->tablet_schema(), 2, "a", FieldType::OLAP_FIELD_TYPE_VARIANT);
        auto readable = inject_reader_schema_for_rowsets({compacted.value()}, path_schema);
        ASSERT_TRUE(readable.has_value()) << readable.error();
        const auto path_id = column_id_by_path("v.a");
        ASSERT_GE(path_id, 0);
        IndexReadOptions path_options;
        path_options.return_columns = {static_cast<uint32_t>(path_id)};
        path_options.collect_variant_values = true;
        auto path_read = read_rowsets(readable.value(), path_options);
        ASSERT_TRUE(path_read.has_value()) << path_read.error();
        const auto& path_values = path_read->variant_values_by_uid.at(-1);
        ASSERT_EQ(path_values.size(), pass == 0 ? 2 : 3);
        for (const auto& value : path_values) {
            ASSERT_TRUE(value.has_value());
            const bool late_row = pass == 1 && &value == &path_values.back();
            EXPECT_EQ(*value, pass == 0 ? "1" : (doc_mode || late_row) ? R"("001")" : R"("1")");
        }
        inputs = {compacted.value()};
        if (pass == 0) {
            ASSERT_NE(late_write, nullptr);
            auto late_read = read_rowsets({late_write}, read_options);
            ASSERT_TRUE(late_read.has_value()) << late_read.error();
            ASSERT_EQ(late_read->variant_values_by_uid.at(2).size(), 1);
            EXPECT_EQ(late_read->variant_values_by_uid.at(2).front().value(), original);
            inputs.push_back(late_write);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(StorageModes, VariantPolicySnapshotTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

} // namespace doris::index_storage_test
