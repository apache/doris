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

#include <fstream>

#include "storage/compaction/full_compaction.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/index/inverted/snii/snii_query_oracle.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/variant/index_storage_variant_test_base.h"
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
    DORIS_CHECK(indexes.size() == 1);
    auto index = reader.open_snii_index(indexes.front());
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
} // namespace doris::index_storage_test
