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
#include <fstream>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vliteral.h"
#include "exprs/vmatch_predicate.h"
#include "storage/compaction/full_compaction.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/index/inverted/snii/snii_query_oracle.h"
#include "storage/merger.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {

static bool nested_group_write_path_available() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

static Result<std::vector<uint32_t>> values_term_postings(const RowsetSharedPtr& rowset,
                                                          int32_t uid, std::string_view term) {
    DORIS_CHECK(rowset->num_segments() == 1);
    const auto segment = rowset->segment(0);
    const auto path = segment.path();
    if (!path.has_value()) {
        return ResultError(path.error());
    }
    const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(*path));
    IndexFileReader reader(rowset->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
                           segment.inverted_index_file_info(), rowset->rowset_meta()->tablet_id());
    RETURN_IF_ERROR_RESULT(reader.init());
    const auto indexes = rowset->tablet_schema()->inverted_indexs(uid);
    const auto meta = std::ranges::find_if(indexes, [](const TabletIndex* candidate) {
        return candidate->is_variant_values_index();
    });
    DORIS_CHECK(meta != indexes.end());
    auto index = reader.open_snii_index(*meta);
    if (!index.has_value()) {
        return ResultError(index.error());
    }
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR_RESULT(snii::query::term_query(**index, std::string(term), &docids));
    return docids;
}

class IndexStorageVariantCompactionReadTest : public IndexStorageTestFixture {
protected:
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

// NOLINTNEXTLINE(readability-function-cognitive-complexity): one mixed plan verifies native/rebuild selection, one eligibility pass, and final postings.
TEST_F(IndexStorageVariantCompactionReadTest, MixedNativeRebuildAndOrdinaryVariantPlan) {
    IndexTabletOptions options;
    options.tablet_id = 110086;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    for (int32_t uid : {2, 3, 4}) {
        VariantColumnSpec column;
        column.unique_id = uid;
        column.name = "v" + std::to_string(uid);
        column.max_subcolumns_count = 100;
        options.variant_columns.push_back(column);
    }
    const std::map<std::string, std::string> root_properties = {
            {"parser", "english"}, {"support_phrase", "false"}, {"variant_index_scope", "values"}};
    options.inverted_indexes = {
            IndexSpec::column_index(10102, "root2", 2, root_properties),
            IndexSpec::column_index(10103, "root3", 3, root_properties),
            IndexSpec::column_index(10104, "paths4", 4, {{"parser", "english"}})};
    ASSERT_TRUE(create_tablet(options).ok());
    std::vector<IndexRowsetSpec> specs;
    for (int version = 0; version < 2; ++version) {
        IndexRowsetSpec spec;
        spec.version = version;
        IndexBatch batch;
        batch.keys = {version};
        batch.variant_jsons_by_column = {
                {R"({"a":"hello","n":1})"},
                {version == 0 ? R"({"a":"hello","n":true})" : R"({"a":"world","n":1})"},
                {R"({"a":"ordinary","n":1})"}};
        spec.batches.push_back(std::move(batch));
        specs.push_back(std::move(spec));
    }
    auto rowsets = write_rowsets(specs);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();
    for (const auto& rowset : rowsets.value()) {
        rowset->set_skip_index_compaction(3);
    }
    const bool old_native = config::inverted_index_compaction_enable;
    const bool old_vertical = config::enable_vertical_compact_variant_subcolumns;
    const bool old_debug = config::enable_debug_points;
    config::enable_debug_points = true;
    const std::string reader_point = "Compaction::snii_eligibility_reader_initialized";
    size_t reader_opens = 0;
    std::function<void()> count_reader = [&reader_opens]() { ++reader_opens; };
    DebugPoints::instance()->add_with_callback(reader_point, count_reader);
    config::inverted_index_compaction_enable = true;
    config::enable_vertical_compact_variant_subcolumns = true;
    DEFER({
        config::inverted_index_compaction_enable = old_native;
        config::enable_vertical_compact_variant_subcolumns = old_vertical;
        config::enable_debug_points = old_debug;
        DebugPoints::instance()->remove(reader_point);
    });
    FullCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = rowsets.value();
    ASSERT_TRUE(compaction.build_basic_info().ok());
    const auto logical_indexes = compaction._cur_tablet_schema->inverted_indexes();
    Compaction::MergeInputRowsetsContext context;
    ASSERT_TRUE(compaction.prepare_merge_input_rowsets_execution(&context).ok());
    // Expansion stores inherited leaf indexes in the path plan, preserving the logical list.
    EXPECT_EQ(compaction._cur_tablet_schema->inverted_indexes(), logical_indexes);
    // The single logical-schema pass must open each source container once.
    EXPECT_EQ(reader_opens, rowsets->size());
    EXPECT_TRUE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.contains(
            {2, 10102}));
    EXPECT_FALSE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.contains(
            {3, 10103}));
    EXPECT_TRUE(compaction._cur_variant_compaction_paths->contains(2));
    EXPECT_FALSE(compaction._cur_variant_compaction_paths->contains(3));
    EXPECT_TRUE(compaction._cur_variant_compaction_paths->contains(4));
    ASSERT_TRUE(compaction.execute_merge_input_rowsets(&context).ok());
    ASSERT_TRUE(compaction.finish_merge_input_rowsets_execution(&context).ok());
    auto output = compaction._output_rowset;
    ASSERT_NE(output, nullptr);
    IndexReadOptions read_options;
    read_options.return_columns = {0, 1, 2, 3};
    read_options.collect_variant_values = true;
    auto actual = read_rowsets({output}, read_options);
    ASSERT_TRUE(actual.has_value()) << actual.error();
    config::inverted_index_compaction_enable = false;
    FullCompaction reference(*storage_engine(), tablet());
    reference._input_rowsets = rowsets.value();
    ASSERT_TRUE(reference.build_basic_info().ok());
    ASSERT_TRUE(reference.merge_input_rowsets().ok());
    auto expected = read_rowsets({reference._output_rowset}, read_options);
    ASSERT_TRUE(expected.has_value()) << expected.error();
    EXPECT_EQ(actual->variant_values_by_uid, expected->variant_values_by_uid);
    for (int32_t uid : {2, 3}) {
        for (std::string_view term : {"hello", "world", "true", "1"}) {
            auto actual_postings = values_term_postings(output, uid, term);
            auto reference_postings = values_term_postings(reference._output_rowset, uid, term);
            ASSERT_TRUE(actual_postings.has_value()) << actual_postings.error();
            ASSERT_TRUE(reference_postings.has_value()) << reference_postings.error();
            EXPECT_EQ(*actual_postings, *reference_postings) << "uid=" << uid << " term=" << term;
        }
    }
}

TEST_F(IndexStorageVariantCompactionReadTest,
       RebuildStripsHistoricalExtractedChildrenWithVerticalOff) {
    IndexTabletOptions options;
    options.tablet_id = 120300;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {VariantColumnSpec {}};
    options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                        {{"variant_index_scope", "values"},
                                                         {"parser", "english"},
                                                         {"support_phrase", "false"}})};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec spec;
    spec.version = 0;
    spec.batches.push_back(
            IndexBatch::single_variant({R"({"a":"hello","b":1})", R"({"a":"world","b":2})"}));
    auto source = write_rowset(spec);
    ASSERT_TRUE(source.has_value()) << source.error();
    auto historical = rowsets_with_variant_extended_schema({*source});
    ASSERT_TRUE(historical.has_value()) << historical.error();
    ASSERT_GT(historical->front()->tablet_schema()->num_columns(), 2);
    const bool old_vertical = config::enable_vertical_compact_variant_subcolumns;
    const bool old_native = config::inverted_index_compaction_enable;
    config::enable_vertical_compact_variant_subcolumns = false;
    config::inverted_index_compaction_enable = false;
    DEFER({
        config::enable_vertical_compact_variant_subcolumns = old_vertical;
        config::inverted_index_compaction_enable = old_native;
    });
    FullCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = *historical;
    ASSERT_TRUE(compaction.build_basic_info().ok());
    Compaction::MergeInputRowsetsContext context;
    ASSERT_TRUE(compaction.prepare_merge_input_rowsets_execution(&context).ok());
    EXPECT_EQ(compaction._cur_tablet_schema->num_columns(), 2);
    EXPECT_EQ(compaction._cur_variant_compaction_paths, nullptr);
    ASSERT_TRUE(compaction.execute_merge_input_rowsets(&context).ok());
    ASSERT_TRUE(compaction.finish_merge_input_rowsets_execution(&context).ok());
    auto hello = values_term_postings(compaction._output_rowset, 2, "hello");
    ASSERT_TRUE(hello.has_value()) << hello.error();
    EXPECT_EQ(*hello, (std::vector<uint32_t> {0}));
}

TEST_F(IndexStorageVariantCompactionReadTest, MowClusterKeyNativeValuesMergeRemovesDeletedRows) {
    IndexTabletOptions options;
    options.tablet_id = 120301;
    options.enable_unique_key_merge_on_write = true;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {VariantColumnSpec {}};
    options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                        {{"variant_index_scope", "values"},
                                                         {"parser", "english"},
                                                         {"support_phrase", "false"}})};
    ASSERT_TRUE(create_tablet(options).ok());
    // Cluster-key compaction consumes the bitmap while merging rows. Non-cluster MOW
    // instead retains physical rows and transfers their delete bitmap at publish time.
    tablet_schema()->_cluster_key_uids = {1};
    tablet()->tablet_meta()->set_tablet_state(TabletState::TABLET_RUNNING);
    IndexRowsetSpec spec;
    spec.version = 0;
    spec.batches.push_back(
            IndexBatch::single_variant({R"({"a":"deleted"})", R"({"a":"survivor"})"}));
    auto source = write_rowset(spec);
    ASSERT_TRUE(source.has_value()) << source.error();
    tablet()->tablet_meta()->delete_bitmap().add(
            {(*source)->rowset_id(), (*source)->segment(0).id(), 0}, 0);
    const bool old_native = config::inverted_index_compaction_enable;
    config::inverted_index_compaction_enable = true;
    DEFER({ config::inverted_index_compaction_enable = old_native; });
    FullCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = {*source};
    ASSERT_TRUE(compaction.build_basic_info().ok());
    ASSERT_TRUE(compaction.merge_input_rowsets().ok());
    const auto output = compaction._output_rowset;
    ASSERT_EQ(output->num_rows(), 1);
    EXPECT_TRUE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.contains(
            {2, 72}));
    auto deleted = values_term_postings(output, 2, "deleted");
    auto survivor = values_term_postings(output, 2, "survivor");
    ASSERT_TRUE(deleted.has_value()) << deleted.error();
    ASSERT_TRUE(survivor.has_value()) << survivor.error();
    EXPECT_TRUE(deleted->empty());
    EXPECT_EQ(*survivor, (std::vector<uint32_t> {0}));
}

TEST_F(IndexStorageVariantCompactionReadTest, UnreadableValuesIndexRebuildsFromWholeRoot) {
    IndexTabletOptions options;
    options.tablet_id = 120302;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {VariantColumnSpec {}};
    options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                        {{"variant_index_scope", "values"},
                                                         {"parser", "english"},
                                                         {"support_phrase", "false"}})};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec spec;
    spec.version = 0;
    spec.batches.push_back(IndexBatch::single_variant({R"({"a":"hello"})"}));
    auto source = write_rowset(spec);
    ASSERT_TRUE(source.has_value()) << source.error();
    auto path = (*source)->segment(0).path();
    ASSERT_TRUE(path.has_value());
    const auto prefix = std::string(InvertedIndexDescriptor::get_index_file_path_prefix(*path));
    {
        std::ofstream damaged(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                              std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(damaged.is_open());
        damaged << "invalid SNII container";
    }
    const bool old_native = config::inverted_index_compaction_enable;
    config::inverted_index_compaction_enable = true;
    DEFER({ config::inverted_index_compaction_enable = old_native; });
    FullCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = {*source};
    ASSERT_TRUE(compaction.build_basic_info().ok());
    const auto status = compaction.merge_input_rowsets();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.empty());
    auto hello = values_term_postings(compaction._output_rowset, 2, "hello");
    ASSERT_TRUE(hello.has_value()) << hello.error();
    EXPECT_EQ(*hello, (std::vector<uint32_t> {0}));
}

// The second segment has a valid SNII container (root B), but lacks root A's logical index.
// Injecting the final schema models a historical segment without corrupting unrelated indexes.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): keep the missing-index fixture, scan checks and selective rebuild assertions together.
TEST_F(IndexStorageVariantCompactionReadTest,
       MissingLogicalValuesIndexFallsBackAndRebuildsOneRoot) {
    IndexTabletOptions options;
    options.tablet_id = 120303;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    for (int32_t uid : {2, 3}) {
        VariantColumnSpec column;
        column.unique_id = uid;
        column.name = "v" + std::to_string(uid);
        column.nullable = true;
        options.variant_columns.push_back(column);
    }
    const std::map<std::string, std::string> properties = {
            {"parser", "english"}, {"variant_index_scope", "values"}, {"support_phrase", "false"}};
    options.inverted_indexes = {IndexSpec::column_index(73, "root_b", 3, properties)};
    ASSERT_TRUE(create_tablet(options).ok());
    IndexRowsetSpec historical;
    historical.version = 0;
    IndexBatch missing_batch;
    missing_batch.keys = {3, 4, 5};
    missing_batch.variant_jsons_by_column = {
            {R"(["hello",{"b":"world"},{"id":3}])", "null", R"({"a":"other","id":5})"},
            {R"({"b":"retained"})", R"({"b":"retained"})", R"({"b":"retained"})"}};
    // The middle row is SQL NULL, not a JSON null leaf.
    for (size_t column = 0; column < missing_batch.variant_jsons_by_column.size(); ++column) {
        JsonStringToVariantEncoder encoder;
        for (const auto& json : missing_batch.variant_jsons_by_column[column]) {
            encoder.add_json({json.data(), json.size()});
        }
        auto values = ColumnVariantV2::create();
        values->insert_encoded_batch(encoder.finish_batch());
        auto nulls = ColumnUInt8::create();
        nulls->insert_many_defaults(3);
        if (column == 0) {
            nulls->get_data()[1] = 1;
        }
        missing_batch.variant_columns_by_column.emplace_back(
                ColumnNullable::create(std::move(values), std::move(nulls)));
    }
    missing_batch.variant_jsons_by_column.clear();
    historical.batches.push_back(std::move(missing_batch));
    auto missing_source = write_rowset(historical);
    ASSERT_TRUE(missing_source.has_value()) << missing_source.error();
    options.inverted_indexes.insert(options.inverted_indexes.begin(),
                                    IndexSpec::column_index(72, "root_a", 2, properties));
    auto injected =
            inject_reader_schema_for_rowsets({*missing_source}, build_tablet_schema(options));
    ASSERT_TRUE(injected.has_value()) << injected.error();
    IndexRowsetSpec complete;
    complete.version = 1;
    IndexBatch complete_batch;
    complete_batch.keys = {1, 2};
    complete_batch.variant_jsons_by_column = {
            {R"({"a":"hello","id":1})", R"({"a":"other","id":2})"},
            {R"({"b":"retained"})", R"({"b":"retained"})"}};
    complete.batches.push_back(std::move(complete_batch));
    auto complete_source = write_rowset(complete);
    ASSERT_TRUE(complete_source.has_value()) << complete_source.error();
    const std::vector<RowsetSharedPtr> sources {*complete_source, injected->front()};
    auto make_read = [](const std::string& term, bool all = false, bool negate = false) {
        TMatchPredicate match;
        match.__set_parser_type("english");
        match.__set_parser_lowercase(true);
        TFunction fn;
        TFunctionName name;
        name.__set_function_name(all ? "match_all" : "match_any");
        fn.__set_name(name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_arg_types({create_type_desc(TYPE_VARIANT), create_type_desc(TYPE_STRING)});
        fn.__set_ret_type(create_type_desc(TYPE_BOOLEAN));
        TExprNode node;
        node.__set_node_type(TExprNodeType::MATCH_PRED);
        node.__set_type(create_type_desc(TYPE_BOOLEAN));
        node.__set_is_nullable(true);
        node.__set_num_children(2);
        node.__set_fn(fn);
        node.__set_match_predicate(match);
        auto expression = VMatchPredicate::create_shared(node);
        auto slot = std::make_shared<MockSlotRef>(
                1, make_nullable(std::make_shared<DataTypeVariantV2>()));
        slot->set_expr_name("v2");
        expression->add_child(slot);
        TExprNode literal;
        literal.__set_node_type(TExprNodeType::STRING_LITERAL);
        literal.__set_type(create_type_desc(TYPE_STRING));
        TStringLiteral text;
        text.__set_value(term);
        literal.__set_string_literal(text);
        expression->add_child(VLiteral::create_shared(literal));
        IndexReadOptions read;
        read.return_columns = {0, 1, 2};
        read.collect_variant_values = true;
        // This fixture returns the root for value assertions, rather than only counting matches.
        read.enable_no_need_read_data_opt = false;
        read.enable_match_without_inverted_index = true;
        VExprSPtr root = expression;
        if (negate) {
            TExprNode not_node;
            not_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
            not_node.__set_opcode(TExprOpcode::COMPOUND_NOT);
            not_node.__set_type(create_type_desc(TYPE_BOOLEAN));
            not_node.__set_is_nullable(true);
            not_node.__set_num_children(1);
            root = VCompoundPred::create_shared(not_node);
            root->add_child(expression);
        }
        read.common_expr_ctxs_push_down = {VExprContext::create_shared(root)};
        return read;
    };
    for (const bool all : {false, true}) {
        auto read = read_rowsets(sources, make_read(all ? "hello world" : "hello", all));
        ASSERT_TRUE(read.has_value()) << read.error();
        EXPECT_EQ(read->rows_read, all ? 1 : 2);
        EXPECT_GT(read->stats.inverted_index_downgrade_count, 0);
        EXPECT_EQ(canonical_variant_values(read->variant_values_by_uid.at(2)),
                  canonical_variant_values(
                          all ? std::vector<std::optional<
                                          std::string>> {R"(["hello",{"b":"world"},{"id":3}])"}
                              : std::vector<std::optional<std::string>> {
                                        R"({"a":"hello","id":1})",
                                        R"(["hello",{"b":"world"},{"id":3}])"}));
    }
    auto negated = read_rowsets(sources, make_read("hello", false, true));
    ASSERT_TRUE(negated.has_value()) << negated.error();
    EXPECT_EQ(negated->rows_read, 2);
    EXPECT_GT(negated->stats.inverted_index_downgrade_count, 0);
    EXPECT_EQ(canonical_variant_values(negated->variant_values_by_uid.at(2)),
              canonical_variant_values({R"({"a":"other","id":2})", R"({"a":"other","id":5})"}));
    auto no_fallback = make_read("hello");
    no_fallback.enable_fallback_on_missing_inverted_index = false;
    auto refused = read_rowsets(sources, no_fallback);
    ASSERT_FALSE(refused.has_value());
    EXPECT_TRUE(refused.error().is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>());
    auto no_residual = make_read("hello");
    no_residual.enable_match_without_inverted_index = false;
    EXPECT_FALSE(read_rowsets(sources, no_residual).has_value());

    const bool old_native = config::inverted_index_compaction_enable;
    config::inverted_index_compaction_enable = true;
    DEFER({ config::inverted_index_compaction_enable = old_native; });
    FullCompaction compaction(*storage_engine(), tablet());
    compaction._input_rowsets = {injected->front(), *complete_source};
    ASSERT_TRUE(compaction.build_basic_info().ok());
    Compaction::MergeInputRowsetsContext context;
    ASSERT_TRUE(compaction.prepare_merge_input_rowsets_execution(&context).ok());
    EXPECT_FALSE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.contains(
            {2, 72}));
    EXPECT_TRUE(compaction._output_rs_writer->context().snii_indexes_to_do_compaction.contains(
            {3, 73}));
    ASSERT_TRUE(compaction.execute_merge_input_rowsets(&context).ok());
    ASSERT_TRUE(compaction.finish_merge_input_rowsets_execution(&context).ok());
    auto hello = values_term_postings(compaction._output_rowset, 2, "hello");
    auto world = values_term_postings(compaction._output_rowset, 2, "world");
    auto retained = values_term_postings(compaction._output_rowset, 3, "retained");
    ASSERT_TRUE(hello.has_value()) << hello.error();
    ASSERT_TRUE(world.has_value()) << world.error();
    ASSERT_TRUE(retained.has_value()) << retained.error();
    EXPECT_EQ(*hello, (std::vector<uint32_t> {0, 2}));
    EXPECT_EQ(*world, (std::vector<uint32_t> {2}));
    EXPECT_EQ(*retained, (std::vector<uint32_t> {0, 1, 2, 3, 4}));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest assertions inside the mode matrix inflate complexity.
TEST_F(IndexStorageVariantCompactionReadTest, ValuesMergeChecksActualHorizontalAndVerticalModes) {
    const bool old_vertical = config::enable_vertical_compaction;
    const bool old_ordered = config::enable_ordered_data_compaction;
    const bool old_native = config::inverted_index_compaction_enable;
    config::enable_ordered_data_compaction = false;
    DEFER({
        config::enable_vertical_compaction = old_vertical;
        config::enable_ordered_data_compaction = old_ordered;
        config::inverted_index_compaction_enable = old_native;
    });
    int64_t tablet_id = 120310;
    for (bool mow : {false, true}) {
        for (bool doc : {false, true}) {
            for (bool vertical : {false, true}) {
                for (bool native : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << "mow=" << mow << " doc=" << doc << " vertical=" << vertical
                                 << " native=" << native);
                    IndexTabletOptions options;
                    options.tablet_id = tablet_id++;
                    options.enable_unique_key_merge_on_write = mow;
                    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
                    VariantColumnSpec column;
                    column.enable_doc_mode = doc;
                    column.doc_hash_shard_count = 3;
                    options.variant_columns = {column};
                    options.inverted_indexes = {
                            IndexSpec::column_index(72, "values", 2,
                                                    {{"variant_index_scope", "values"},
                                                     {"parser", "english"},
                                                     {"support_phrase", "false"}})};
                    ASSERT_TRUE(create_tablet(options).ok());
                    if (mow) {
                        // Real MOW schemas always include this hidden column; horizontal readers
                        // apply its visibility filter even when every row is live.
                        ColumnPB delete_sign;
                        delete_sign.set_unique_id(3);
                        delete_sign.set_name(DELETE_SIGN);
                        delete_sign.set_type("TINYINT");
                        delete_sign.set_aggregation("REPLACE");
                        delete_sign.set_is_nullable(false);
                        delete_sign.set_default_value("0");
                        TabletColumn column;
                        column.init_from_pb(delete_sign);
                        tablet_schema()->append_column(column);
                    }
                    std::vector<IndexRowsetSpec> specs;
                    for (int version = 0; version < 2; ++version) {
                        IndexRowsetSpec spec;
                        spec.version = version;
                        spec.batches.push_back(IndexBatch::single_variant(
                                {R"({"n":7,"s":"hello"})", R"({"n":9,"s":"world"})"}, version * 2));
                        specs.push_back(std::move(spec));
                    }
                    auto sources = write_rowsets(specs);
                    ASSERT_TRUE(sources.has_value()) << sources.error();
                    config::enable_vertical_compaction = vertical;
                    config::inverted_index_compaction_enable = native;
                    FullCompaction compaction(*storage_engine(), tablet());
                    compaction._input_rowsets = *sources;
                    ASSERT_TRUE(compaction.build_basic_info().ok());
                    Compaction::MergeInputRowsetsContext context;
                    ASSERT_TRUE(compaction.prepare_merge_input_rowsets_execution(&context).ok());
                    EXPECT_EQ(compaction._is_vertical, vertical);
                    EXPECT_EQ(compaction._output_rs_writer->context()
                                      .snii_indexes_to_do_compaction.contains({2, 72}),
                              native);
                    ASSERT_TRUE(compaction.execute_merge_input_rowsets(&context).ok());
                    ASSERT_TRUE(compaction.finish_merge_input_rowsets_execution(&context).ok());
                    auto hello = values_term_postings(compaction._output_rowset, 2, "hello");
                    auto number = values_term_postings(compaction._output_rowset, 2, "7");
                    ASSERT_TRUE(hello.has_value()) << hello.error();
                    ASSERT_TRUE(number.has_value()) << number.error();
                    EXPECT_EQ(*hello, (std::vector<uint32_t> {0, 2}));
                    EXPECT_EQ(*number, (std::vector<uint32_t> {0, 2}));
                    IndexReadOptions read;
                    read.collect_variant_values = true;
                    auto result = read_rowsets({compaction._output_rowset}, read);
                    ASSERT_TRUE(result.has_value()) << result.error();
                    EXPECT_EQ(canonical_variant_values(result->variant_values_by_uid.at(2)),
                              canonical_variant_values(
                                      {R"({"n":7,"s":"hello"})", R"({"n":9,"s":"world"})",
                                       R"({"n":7,"s":"hello"})", R"({"n":9,"s":"world"})"}));
                }
            }
        }
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size): one fixture checks the same physical index identities across layout and compaction modes.
TEST_F(IndexStorageVariantCompactionReadTest, SameRootMultiIndexesSurviveCompactionGroups) {
    const bool old_vertical = config::enable_vertical_compaction;
    const bool old_expansion = config::enable_vertical_compact_variant_subcolumns;
    const bool old_ordered = config::enable_ordered_data_compaction;
    const bool old_native = config::inverted_index_compaction_enable;
    const int old_group_size = config::vertical_compaction_num_columns_per_group;
    config::enable_vertical_compaction = true;
    config::enable_ordered_data_compaction = false;
    config::vertical_compaction_num_columns_per_group = 1;
    DEFER({
        config::enable_vertical_compaction = old_vertical;
        config::enable_vertical_compact_variant_subcolumns = old_expansion;
        config::enable_ordered_data_compaction = old_ordered;
        config::inverted_index_compaction_enable = old_native;
        config::vertical_compaction_num_columns_per_group = old_group_size;
    });
    struct Mode {
        bool native;
        bool expand;
        bool doc;
        bool missing;
        bool retry = false;
    };
    int64_t tablet_id = 120400;
    for (const auto mode :
         {Mode {.native = true, .expand = true, .doc = false, .missing = false},
          Mode {.native = false, .expand = true, .doc = false, .missing = false},
          Mode {.native = true, .expand = false, .doc = false, .missing = false},
          Mode {.native = false, .expand = false, .doc = false, .missing = false},
          Mode {.native = true, .expand = true, .doc = true, .missing = false},
          Mode {.native = false, .expand = true, .doc = true, .missing = false},
          Mode {.native = true, .expand = true, .doc = false, .missing = true},
          Mode {.native = true, .expand = true, .doc = false, .missing = false, .retry = true}}) {
        SCOPED_TRACE(testing::Message() << "native=" << mode.native << " expand=" << mode.expand
                                        << " doc=" << mode.doc << " missing=" << mode.missing);
        IndexTabletOptions options;
        options.tablet_id = tablet_id++;
        options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
        VariantColumnSpec column;
        column.nullable = true;
        column.max_subcolumns_count = 1;
        column.enable_doc_mode = mode.doc;
        column.doc_materialization_min_rows = 2;
        column.doc_hash_shard_count = 2;
        for (const auto& path : {"s", "exact", "tag*"}) {
            VariantPathSpec field;
            field.path = path;
            field.pattern_type = PatternTypePB::MATCH_NAME_GLOB;
            column.predefined_paths.push_back(field);
        }
        options.variant_columns = {column};
        options.inverted_indexes = {
                IndexSpec::column_index(72, "values", 2,
                                        {{"variant_index_scope", "values"},
                                         {"parser", "english"},
                                         {"support_phrase", "false"}}),
                IndexSpec::column_index(73, "english", 2,
                                        {{"parser", "english"}, {"lower_case", "false"}}),
                IndexSpec::column_index(74, "unicode", 2, {{"parser", "unicode"}}),
                IndexSpec::column_index(75, "exact", 2,
                                        {{"field_pattern", "exact"}, {"parser", "english"}}),
                IndexSpec::column_index(76, "glob", 2,
                                        {{"field_pattern", "tag*"}, {"parser", "unicode"}})};
        auto initial_options = options;
        if (mode.missing) {
            initial_options.inverted_indexes.erase(initial_options.inverted_indexes.begin());
        }
        ASSERT_TRUE(create_tablet(initial_options).ok());
        const std::vector<std::string> documents {
                R"({"exact":"EXACT","n":7,"rare":{"a":"kept"},"s":"HELLO","tagA":"alpha","tagB":"beta"})",
                R"({"exact":"OTHER","n":9,"rare":{"b":"kept"},"s":"other","tagA":"beta","tagB":"alpha"})",
                "null", "{}"};
        std::vector<RowsetSharedPtr> sources;
        for (int version = 0; version < 2; ++version) {
            JsonStringToVariantEncoder encoder;
            for (const auto& document : documents) {
                encoder.add_json({document.data(), document.size()});
            }
            auto values = ColumnVariantV2::create();
            values->insert_encoded_batch(encoder.finish_batch());
            auto nulls = ColumnUInt8::create();
            nulls->insert_many_defaults(4);
            nulls->get_data()[2] = 1;
            IndexRowsetSpec spec;
            spec.version = version;
            spec.batches.push_back(IndexBatch::single_variant_column(
                    ColumnNullable::create(std::move(values), std::move(nulls)), version * 4));
            auto source = write_rowset(spec);
            ASSERT_TRUE(source.has_value()) << source.error();
            auto layout = probe_rowset(*source);
            ASSERT_TRUE(layout.has_value()) << layout.error();
            if (mode.doc) {
                EXPECT_TRUE(has_doc_value_column(*layout));
            } else {
                EXPECT_TRUE(has_sparse_path_stat(*layout, "rare.a"));
            }
            if (mode.missing && version == 0) {
                // Only values is absent: the same root's ordinary/template indexes remain valid.
                auto injected =
                        inject_reader_schema_for_rowsets({*source}, build_tablet_schema(options));
                ASSERT_TRUE(injected.has_value()) << injected.error();
                sources.push_back(injected->front());
            } else {
                sources.push_back(*source);
            }
        }
        config::enable_vertical_compact_variant_subcolumns = mode.expand;
        config::inverted_index_compaction_enable = mode.native;
        if (mode.retry) {
            const bool old_debug = config::enable_debug_points;
            config::enable_debug_points = true;
            const std::string point = "Compaction::before_execute_snii_merge";
            DEFER({
                DebugPoints::instance()->remove(point);
                config::enable_debug_points = old_debug;
            });
            std::function<void(Status*)> fail_merge = [](Status* status) {
                *status = Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                        "injected VARIANT values merge failure");
            };
            DebugPoints::instance()->add_with_callback(point, fail_merge);
            FullCompaction failed(*storage_engine(), tablet());
            failed._input_rowsets = sources;
            ASSERT_TRUE(failed.build_basic_info().ok());
            Compaction::MergeInputRowsetsContext failed_context;
            ASSERT_TRUE(failed.prepare_merge_input_rowsets_execution(&failed_context).ok());
            auto status = failed.execute_merge_input_rowsets(&failed_context);
            ASSERT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
            EXPECT_EQ(failed._output_rowset, nullptr);
            for (const auto& source : sources) {
                EXPECT_TRUE(source->is_skip_index_compaction(2));
                auto hello = values_term_postings(source, 2, "hello");
                ASSERT_TRUE(hello.has_value()) << hello.error();
                EXPECT_EQ(*hello, (std::vector<uint32_t> {0}));
            }
        }
        // A real second compaction retries after the failure has armed the rebuild path.
        FullCompaction compaction(*storage_engine(), tablet());
        compaction._input_rowsets = sources;
        ASSERT_TRUE(compaction.build_basic_info().ok());
        Compaction::MergeInputRowsetsContext context;
        ASSERT_TRUE(compaction.prepare_merge_input_rowsets_execution(&context).ok());
        const auto& native_set =
                compaction._output_rs_writer->context().snii_indexes_to_do_compaction;
        EXPECT_EQ(native_set.contains({2, 72}), mode.native && !mode.missing && !mode.retry);
        for (int64_t id : {73, 74, 75, 76}) {
            EXPECT_FALSE(native_set.contains({2, id}));
        }
        ASSERT_TRUE(compaction._is_vertical);
        std::vector<std::vector<uint32_t>> groups;
        std::vector<uint32_t> cluster_keys;
        Merger::vertical_split_columns(*compaction._cur_tablet_schema, &groups, &cluster_keys, 1);
        // A rebuild deliberately reads one whole root; native expansion splits its paths across groups.
        ASSERT_GE(groups.size(),
                  mode.expand && mode.native && !mode.missing && !mode.retry ? 3 : 2);
        ASSERT_TRUE(compaction.execute_merge_input_rowsets(&context).ok());
        ASSERT_TRUE(compaction.finish_merge_input_rowsets_execution(&context).ok());
        auto reopened = reload_rowsets({compaction._output_rowset});
        ASSERT_TRUE(reopened.has_value()) << reopened.error();
        const auto output = reopened->front();
        IndexReadOptions read;
        read.collect_variant_values = true;
        auto result = read_rowsets({output}, read);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(canonical_variant_values(result->variant_values_by_uid.at(2)),
                  canonical_variant_values({documents[0], documents[1], std::nullopt, "{}",
                                            documents[0], documents[1], std::nullopt, "{}"}));
        ASSERT_EQ(output->num_segments(), 1);
        auto segment = output->segment(0);
        auto path = segment.path();
        ASSERT_TRUE(path.has_value());
        IndexFileReader reader(
                output->rowset_meta()->fs(),
                std::string(InvertedIndexDescriptor::get_index_file_path_prefix(*path)),
                InvertedIndexStorageFormatPB::SNII, segment.inverted_index_file_info());
        ASSERT_TRUE(reader.init().ok());
        auto entries = reader.snii_logical_indexes();
        ASSERT_TRUE(entries.has_value()) << entries.error();
        auto check = [&](IndexFileReader& file, uint32_t doc_count, uint64_t id,
                         const std::string& suffix, const std::string& term,
                         const std::vector<uint32_t>& expected) {
            auto index = file.open_snii_logical_index(id, suffix, nullptr,
                                                      snii::reader::LogicalIndexOpenMode::kQuery);
            ASSERT_TRUE(index.has_value()) << id << ":" << suffix << " " << index.error();
            EXPECT_EQ((*index)->stats().doc_count, doc_count);
            EXPECT_EQ((*index)->stats().null_count, id == 72 ? 2 : 4);
            std::vector<uint32_t> actual;
            ASSERT_TRUE(snii::query::term_query(**index, term, &actual).ok());
            EXPECT_EQ(actual, expected) << id << ":" << suffix << " term=" << term;
        };
        check(reader, 8, 72, "", "hello", {0, 4});
        check(reader, 8, 72, "", "world", {});
        check(reader, 8, 73, "v%2Es", "HELLO", {0, 4});
        check(reader, 8, 73, "v%2Es", "hello", {});
        check(reader, 8, 74, "v%2Es", "hello", {0, 4});
        check(reader, 8, 75, "v%2Eexact", "exact", {0, 4});
        check(reader, 8, 76, "v%2EtagA", "alpha", {0, 4});
        check(reader, 8, 76, "v%2EtagA", "beta", {1, 5});
        check(reader, 8, 76, "v%2EtagB", "alpha", {1, 5});
        check(reader, 8, 76, "v%2EtagB", "beta", {0, 4});
        if (mode.retry) {
            IndexRowsetSpec next;
            next.version = 2;
            next.batches.push_back(IndexBatch::single_variant(
                    {R"({"exact":"EXACT","s":"HELLO","tagA":"alpha","tagB":"beta"})"}, 8));
            auto next_source = write_rowset(next);
            ASSERT_TRUE(next_source.has_value()) << next_source.error();
            FullCompaction second(*storage_engine(), tablet());
            second._input_rowsets = {output, *next_source};
            ASSERT_TRUE(second.build_basic_info().ok());
            ASSERT_TRUE(second.merge_input_rowsets().ok());
            auto hello = values_term_postings(second._output_rowset, 2, "hello");
            ASSERT_TRUE(hello.has_value()) << hello.error();
            EXPECT_EQ(*hello, (std::vector<uint32_t> {0, 4, 8}));
            EXPECT_EQ(second._output_rowset->num_rows(), 9);
            const auto next_segment = second._output_rowset->segment(0);
            auto next_path = next_segment.path();
            ASSERT_TRUE(next_path.has_value());
            IndexFileReader next_reader(
                    second._output_rowset->rowset_meta()->fs(),
                    std::string(InvertedIndexDescriptor::get_index_file_path_prefix(*next_path)),
                    InvertedIndexStorageFormatPB::SNII, next_segment.inverted_index_file_info());
            ASSERT_TRUE(next_reader.init().ok());
            check(next_reader, 9, 72, "", "hello", {0, 4, 8});
            check(next_reader, 9, 73, "v%2Es", "HELLO", {0, 4, 8});
            check(next_reader, 9, 73, "v%2Es", "hello", {});
            check(next_reader, 9, 74, "v%2Es", "hello", {0, 4, 8});
            check(next_reader, 9, 75, "v%2Eexact", "exact", {0, 4, 8});
            check(next_reader, 9, 76, "v%2EtagA", "alpha", {0, 4, 8});
            check(next_reader, 9, 76, "v%2EtagA", "beta", {1, 5});
            check(next_reader, 9, 76, "v%2EtagB", "alpha", {1, 5});
            check(next_reader, 9, 76, "v%2EtagB", "beta", {0, 4, 8});
            auto next_result = read_rowsets({second._output_rowset}, read);
            ASSERT_TRUE(next_result.has_value()) << next_result.error();
            EXPECT_EQ(canonical_variant_values(next_result->variant_values_by_uid.at(2)),
                      canonical_variant_values(
                              {documents[0], documents[1], std::nullopt, "{}", documents[0],
                               documents[1], std::nullopt, "{}",
                               R"({"exact":"EXACT","s":"HELLO","tagA":"alpha","tagB":"beta"})"}));
        }
        for (const auto& entry : *entries) {
            if (entry.index_id == 72) {
                EXPECT_TRUE(entry.index_suffix.empty());
            }
        }
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size): one lifecycle follows rowid remapping, zero output, and refill for native/rebuild.
TEST_F(IndexStorageVariantCompactionReadTest, ValuesMultiSegmentMowRowidsAndEmptyOutput) {
    const bool old_native = config::inverted_index_compaction_enable;
    const bool old_vertical = config::enable_vertical_compaction;
    const bool old_ordered = config::enable_ordered_data_compaction;
    const auto old_batch_size = config::compaction_batch_size;
    config::compaction_batch_size = 1;
    const auto old_segment_size = config::vertical_compaction_max_segment_size;
    config::enable_vertical_compaction = true;
    config::enable_ordered_data_compaction = false;
    DEFER({
        config::inverted_index_compaction_enable = old_native;
        config::enable_vertical_compaction = old_vertical;
        config::enable_ordered_data_compaction = old_ordered;
        config::vertical_compaction_max_segment_size = old_segment_size;
        config::compaction_batch_size = old_batch_size;
    });
    int64_t tablet_id = 120500;
    for (bool native : {false, true}) {
        IndexTabletOptions options;
        options.tablet_id = tablet_id++;
        options.enable_unique_key_merge_on_write = true;
        options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
        options.variant_columns = {VariantColumnSpec {}};
        options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                            {{"variant_index_scope", "values"},
                                                             {"parser", "english"},
                                                             {"support_phrase", "false"}})};
        ASSERT_TRUE(create_tablet(options).ok());
        tablet_schema()->_cluster_key_uids = {1};
        tablet()->tablet_meta()->set_tablet_state(TabletState::TABLET_RUNNING);
        IndexRowsetSpec first;
        first.version = 0;
        first.max_rows_per_segment = 2;
        first.batches.push_back(
                IndexBatch::single_variant({R"({"n":1,"s":"hello"})", R"({"n":3,"s":"old"})",
                                            R"({"n":5,"s":"hello"})", R"({"n":7,"s":"deleted"})"}));
        first.batches.front().keys = {1, 3, 5, 7};
        IndexRowsetSpec second;
        second.version = 1;
        second.max_rows_per_segment = 2;
        second.batches.push_back(IndexBatch::single_variant(
                {R"({"n":2,"s":"other"})", R"({"n":3,"s":"replacement"})", R"({"n":4,"s":"other"})",
                 R"({"n":8,"s":"other"})"}));
        second.batches.front().keys = {2, 3, 4, 8};
        auto sources = write_rowsets({first, second});
        ASSERT_TRUE(sources.has_value()) << sources.error();
        for (const auto& source : *sources) {
            ASSERT_EQ(source->num_segments(), 2);
        }
        for (size_t pos : {0, 1}) {
            tablet()->tablet_meta()->delete_bitmap().add(
                    {sources->front()->rowset_id(), sources->front()->segment(pos).id(), 0}, 1);
        }
        config::inverted_index_compaction_enable = native;
        FullCompaction compaction(*storage_engine(), tablet());
        compaction._input_rowsets = *sources;
        ASSERT_TRUE(compaction.build_basic_info().ok());
        config::vertical_compaction_max_segment_size =
                (compaction._input_rowsets_data_size / (compaction._input_row_num + 1) + 1);
        ASSERT_EQ(compaction.get_avg_segment_rows(), 1);
        ASSERT_TRUE(compaction.merge_input_rowsets().ok());
        const auto output = compaction._output_rowset;
        ASSERT_EQ(output->num_rows(), 6);
        ASSERT_EQ(output->num_segments(), 3);
        IndexReadOptions read;
        read.collect_variant_values = true;
        read.need_ordered_result = true;
        auto actual = read_rowsets({output}, read);
        ASSERT_TRUE(actual.has_value()) << actual.error();
        EXPECT_EQ(
                canonical_variant_values(actual->variant_values_by_uid.at(2)),
                canonical_variant_values({R"({"n":1,"s":"hello"})", R"({"n":2,"s":"other"})",
                                          R"({"n":3,"s":"replacement"})", R"({"n":4,"s":"other"})",
                                          R"({"n":5,"s":"hello"})", R"({"n":8,"s":"other"})"}));
        for (const auto segment : output->segments()) {
            const auto pos = segment.pos();
            auto path = segment.path();
            ASSERT_TRUE(path.has_value());
            IndexFileReader reader(
                    output->rowset_meta()->fs(),
                    std::string(InvertedIndexDescriptor::get_index_file_path_prefix(*path)),
                    InvertedIndexStorageFormatPB::SNII, segment.inverted_index_file_info());
            ASSERT_TRUE(reader.init().ok());
            auto index =
                    reader.open_snii_index(output->tablet_schema()->inverted_indexs(2).front());
            ASSERT_TRUE(index.has_value()) << index.error();
            EXPECT_EQ((*index)->stats().doc_count, 2);
            for (const auto& term : {"hello", "replacement", "old", "deleted"}) {
                std::vector<uint32_t> postings;
                ASSERT_TRUE(snii::query::term_query(**index, term, &postings).ok());
                const bool hit = (std::string_view(term) == "hello" && pos != 1) ||
                                 (std::string_view(term) == "replacement" && pos == 1);
                EXPECT_EQ(postings, hit ? std::vector<uint32_t> {0} : std::vector<uint32_t> {});
            }
        }
        // Delete every surviving row. This produces a real zero-segment rowset, then reuse it.
        for (const auto segment : output->segments()) {
            for (uint32_t row : {0, 1}) {
                tablet()->tablet_meta()->delete_bitmap().add({output->rowset_id(), segment.id(), 1},
                                                             row);
            }
        }
        FullCompaction empty(*storage_engine(), tablet());
        empty._input_rowsets = {output};
        ASSERT_TRUE(empty.build_basic_info().ok());
        ASSERT_TRUE(empty.merge_input_rowsets().ok());
        ASSERT_EQ(empty._output_rowset->num_rows(), 0);
        ASSERT_EQ(empty._output_rowset->num_segments(), 0);
        IndexRowsetSpec next;
        next.version = 2;
        next.batches.push_back(IndexBatch::single_variant({R"({"n":9,"s":"fresh"})"}, 9));
        auto fresh = write_rowset(next);
        ASSERT_TRUE(fresh.has_value()) << fresh.error();
        config::vertical_compaction_max_segment_size = old_segment_size;
        FullCompaction refill(*storage_engine(), tablet());
        refill._input_rowsets = {empty._output_rowset, *fresh};
        ASSERT_TRUE(refill.build_basic_info().ok());
        ASSERT_TRUE(refill.merge_input_rowsets().ok());
        auto postings = values_term_postings(refill._output_rowset, 2, "fresh");
        ASSERT_TRUE(postings.has_value()) << postings.error();
        EXPECT_EQ(*postings, (std::vector<uint32_t> {0}));
        ASSERT_EQ(refill._output_rowset->num_rows(), 1);
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): keep real empty input and both NULL/empty-document expectations together.
TEST_F(IndexStorageVariantCompactionReadTest, EmptyRowsetAndAllNullOrEmptyDocuments) {
    const bool old_native = config::inverted_index_compaction_enable;
    const bool old_ordered = config::enable_ordered_data_compaction;
    config::enable_ordered_data_compaction = false;
    DEFER({
        config::inverted_index_compaction_enable = old_native;
        config::enable_ordered_data_compaction = old_ordered;
    });
    int64_t tablet_id = 120510;
    for (bool all_null : {true, false}) {
        IndexTabletOptions options;
        options.tablet_id = tablet_id++;
        options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
        VariantColumnSpec column;
        column.nullable = true;
        options.variant_columns = {column};
        options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                            {{"variant_index_scope", "values"},
                                                             {"parser", "english"},
                                                             {"support_phrase", "false"}})};
        ASSERT_TRUE(create_tablet(options).ok());
        // The high-level fixture deliberately rejects zero-row batches; build a real empty rowset.
        RowsetWriterContext empty_context;
        empty_context.rowset_id.init(1000);
        empty_context.rowset_type = BETA_ROWSET;
        empty_context.data_dir = _data_dir.get();
        empty_context.rowset_state = VISIBLE;
        empty_context.tablet_schema = tablet_schema();
        empty_context.tablet_path = tablet()->tablet_path();
        empty_context.tablet_id = tablet()->tablet_id();
        empty_context.tablet_uid = tablet()->tablet_uid();
        empty_context.tablet = tablet();
        empty_context.version = Version(0, 0);
        empty_context.segments_overlap = NONOVERLAPPING;
        empty_context.write_type = DataWriteType::TYPE_DIRECT;
        auto writer = RowsetFactory::create_rowset_writer(*storage_engine(), empty_context, false);
        ASSERT_TRUE(writer.has_value()) << writer.error();
        ASSERT_TRUE((*writer)->flush().ok());
        RowsetSharedPtr empty_source;
        ASSERT_TRUE((*writer)->build(empty_source).ok());
        ASSERT_TRUE(tablet()->add_rowset(empty_source).ok());
        ASSERT_EQ(empty_source->num_rows(), 0);
        ASSERT_EQ(empty_source->num_segments(), 0);
        std::vector<RowsetSharedPtr> sources {empty_source};
        for (int version = 1; version <= 2; ++version) {
            JsonStringToVariantEncoder encoder;
            for (int row = 0; row < 3; ++row) {
                encoder.add_json({"{}", 2});
            }
            auto values = ColumnVariantV2::create();
            values->insert_encoded_batch(encoder.finish_batch());
            auto nulls = ColumnUInt8::create();
            nulls->insert_many_vals(all_null ? 1 : 0, 3);
            IndexRowsetSpec spec;
            spec.version = version;
            spec.batches.push_back(IndexBatch::single_variant_column(
                    ColumnNullable::create(std::move(values), std::move(nulls)),
                    (version - 1) * 3));
            auto source = write_rowset(spec);
            ASSERT_TRUE(source.has_value()) << source.error();
            sources.push_back(*source);
        }
        config::inverted_index_compaction_enable = all_null;
        FullCompaction compaction(*storage_engine(), tablet());
        compaction._input_rowsets = sources;
        ASSERT_TRUE(compaction.build_basic_info().ok());
        ASSERT_TRUE(compaction.merge_input_rowsets().ok());
        ASSERT_EQ(compaction._output_rowset->num_rows(), 6);
        auto postings = values_term_postings(compaction._output_rowset, 2, "hello");
        ASSERT_TRUE(postings.has_value()) << postings.error();
        EXPECT_TRUE(postings->empty());
        IndexReadOptions read;
        read.collect_variant_values = true;
        auto actual = read_rowsets({compaction._output_rowset}, read);
        ASSERT_TRUE(actual.has_value()) << actual.error();
        EXPECT_EQ(actual->variant_values_by_uid.at(2),
                  (std::vector<std::optional<std::string>>(
                          6, all_null ? std::nullopt : std::optional<std::string>("{}"))));
    }
}

} // namespace doris::index_storage_test
