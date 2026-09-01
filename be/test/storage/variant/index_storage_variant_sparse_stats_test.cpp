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
#include <optional>
#include <string>
#include <vector>

#include "exec/common/variant_util.h"
#include "storage/variant/index_storage_variant_test_base.h"

namespace doris::index_storage_test {
namespace {

std::string sparse_filler_for_bucket(uint32_t bucket_num, uint32_t bucket) {
    for (uint32_t candidate_id = 0; candidate_id < 10000; ++candidate_id) {
        std::string candidate =
                "aa_filler_" + std::to_string(bucket) + "_" + std::to_string(candidate_id);
        const StringRef path {candidate.data(), candidate.size()};
        if (variant_util::variant_binary_shard_of(path, bucket_num) == bucket) {
            return candidate;
        }
    }
    DORIS_CHECK(false) << "failed to find sparse filler for bucket " << bucket;
    return {};
}

std::string sparse_row_with_all_buckets_filled(uint32_t bucket_num) {
    std::string json = R"({"a":"hot-0")";
    for (uint32_t bucket = 0; bucket < bucket_num; ++bucket) {
        json += ",\"" + sparse_filler_for_bucket(bucket_num, bucket) +
                "\":" + std::to_string(bucket);
    }
    json += R"(,"b":"scalar-0","d":"dense-0"})";
    return json;
}

} // namespace

class IndexStorageVariantSparseStatsTest : public IndexStorageTestFixture {
protected:
    void run_sparse_stats_limit_boundary_case(int32_t sparse_stats_limit,
                                              size_t expected_sparse_stat_entries,
                                              bool expect_b_c_sparse_stat, int64_t tablet_id,
                                              int32_t sparse_hash_shard_count,
                                              bool external_segment_meta);
};

TEST_F(IndexStorageVariantSparseStatsTest, VariantCompactionSchemaTopNRecordsSparseOverflowPaths) {
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

TEST_F(IndexStorageVariantSparseStatsTest, VariantSparseStatsLimitIsPreservedAfterCompaction) {
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

TEST_F(IndexStorageVariantSparseStatsTest, VariantSparseStatsLimitOneIsPreservedAfterCompaction) {
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

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the matrix.
void IndexStorageVariantSparseStatsTest::run_sparse_stats_limit_boundary_case(
        int32_t sparse_stats_limit, size_t expected_sparse_stat_entries,
        bool expect_b_c_sparse_stat, int64_t tablet_id, int32_t sparse_hash_shard_count,
        bool external_segment_meta) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = sparse_stats_limit;
    variant.sparse_hash_shard_count = sparse_hash_shard_count;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    const bool force_truncation_in_all_shards =
            sparse_hash_shard_count > 1 && sparse_stats_limit == 1;
    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {force_truncation_in_all_shards
                     ? sparse_row_with_all_buckets_filled(sparse_hash_shard_count)
                     : R"({"a": "hot-0", "b": "scalar-0", "d": "dense-0"})",
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

    auto v2_schema = build_schema_with_variant_path_column(*tablet_schema(), 2, "v.b.c",
                                                           FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(v2_schema, nullptr);
    v2_schema = build_schema_with_variant_path_column(*v2_schema, 2, "v.b.missing",
                                                      FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(v2_schema, nullptr);

    auto readable = inject_reader_schema_for_rowsets(reloaded.value(), std::move(v2_schema));
    ASSERT_TRUE(readable.has_value()) << readable.error();

    auto probe = probe_rowset(readable->front());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "a"));
    EXPECT_EQ(has_sparse_path_stat(probe.value(), "b"), !force_truncation_in_all_shards);
    EXPECT_EQ(has_sparse_path_stat(probe.value(), "b.c"), expect_b_c_sparse_stat);
    EXPECT_EQ(sparse_stat_entry_count(probe.value()), expected_sparse_stat_entries);

    const int32_t child_column_id = column_id_by_path("v.b.c");
    ASSERT_GE(child_column_id, 0) << dump_schema_paths(*tablet_schema());
    const int32_t missing_column_id = column_id_by_path("v.b.missing");
    ASSERT_GE(missing_column_id, 0) << dump_schema_paths(*tablet_schema());

    auto read_path = [&](int32_t column_id) {
        IndexReadOptions read_options;
        read_options.need_ordered_result = true;
        read_options.return_columns = {0, static_cast<uint32_t>(column_id)};
        read_options.collect_variant_values = true;
        return read_rowsets(readable.value(), std::move(read_options));
    };

    const bool stats_below_limit =
            expected_sparse_stat_entries < static_cast<size_t>(sparse_stats_limit);
    const auto expected_child_values = canonical_variant_values(
            {std::nullopt, std::nullopt, std::nullopt, std::nullopt, R"("child-0")"});
    const auto expected_missing_values = canonical_variant_values(
            {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
    auto child = read_path(child_column_id);
    ASSERT_TRUE(child.has_value()) << child.error();
    ASSERT_EQ(child->rows_read, 5);
    EXPECT_TRUE(child->variant_v2_output_uids.contains(-1));
    ASSERT_TRUE(child->variant_values_by_uid.contains(-1));
    const auto& child_values = child->variant_values_by_uid.at(-1);
    ASSERT_EQ(child_values.size(), 5);
    EXPECT_EQ(canonical_variant_values(child_values), expected_child_values)
            << describe_optional_string_values(child_values);
    if (stats_below_limit) {
        EXPECT_GT(child->stats.variant_subtree_sparse_iter_count, 0);
    } else {
        EXPECT_GT(child->stats.variant_subtree_hierarchical_iter_count, 0);
    }

    auto missing = read_path(missing_column_id);
    ASSERT_TRUE(missing.has_value()) << missing.error();
    ASSERT_EQ(missing->rows_read, 5);
    EXPECT_TRUE(missing->variant_v2_output_uids.contains(-1));
    ASSERT_TRUE(missing->variant_values_by_uid.contains(-1));
    const auto& missing_values = missing->variant_values_by_uid.at(-1);
    ASSERT_EQ(missing_values.size(), 5);
    EXPECT_EQ(canonical_variant_values(missing_values), expected_missing_values)
            << describe_optional_string_values(missing_values);
    if (stats_below_limit) {
        EXPECT_GT(missing->stats.variant_subtree_default_iter_count, 0);
    } else {
        EXPECT_GT(missing->stats.variant_subtree_hierarchical_iter_count, 0);
    }
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsBeforeLimitReadsRecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/4,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110055,
                                         /*sparse_hash_shard_count=*/0,
                                         /*external_segment_meta=*/true);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsAtLimitReadsRecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/3,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110056,
                                         /*sparse_hash_shard_count=*/1,
                                         /*external_segment_meta=*/true);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsAfterLimitTruncationReadsUnrecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/2,
                                         /*expected_sparse_stat_entries=*/2,
                                         /*expect_b_c_sparse_stat=*/false, 110057,
                                         /*sparse_hash_shard_count=*/0,
                                         /*external_segment_meta=*/true);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       V3FormatShardNReadsExactLeafAfterPerShardStatsTruncation) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/1,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/false, 110061,
                                         /*sparse_hash_shard_count=*/3,
                                         /*external_segment_meta=*/true);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       V2FormatShardZeroReadsSparseBeforeStatsLimitAndDefaultFill) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/4,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110058,
                                         /*sparse_hash_shard_count=*/0,
                                         /*external_segment_meta=*/false);
}

TEST_F(IndexStorageVariantSparseStatsTest, V2FormatShardOneReadsSparseAtStatsLimitAndMissingPath) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/3,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110059,
                                         /*sparse_hash_shard_count=*/1,
                                         /*external_segment_meta=*/false);
}

TEST_F(IndexStorageVariantSparseStatsTest, V2FormatShardNReadsExactLeafAfterStatsTruncation) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/1,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/false, 110060,
                                         /*sparse_hash_shard_count=*/3,
                                         /*external_segment_meta=*/false);
}

TEST_F(IndexStorageVariantSparseStatsTest,
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

} // namespace doris::index_storage_test
