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

} // namespace doris::index_storage_test
