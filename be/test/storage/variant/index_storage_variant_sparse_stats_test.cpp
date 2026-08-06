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

class IndexStorageVariantSparseStatsTest : public IndexStorageTestFixture {
protected:
    void run_sparse_stats_limit_boundary_case(int32_t sparse_stats_limit,
                                              size_t expected_sparse_stat_entries,
                                              bool expect_b_c_sparse_stat, int64_t tablet_id);
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

void IndexStorageVariantSparseStatsTest::run_sparse_stats_limit_boundary_case(
        int32_t sparse_stats_limit, size_t expected_sparse_stat_entries,
        bool expect_b_c_sparse_stat, int64_t tablet_id) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = sparse_stats_limit;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
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

    auto child_schema = build_schema_with_variant_path_column(*tablet_schema(), 2, "v.b.c",
                                                              FieldType::OLAP_FIELD_TYPE_STRING);
    ASSERT_NE(child_schema, nullptr);
    auto child_readable =
            inject_reader_schema_for_rowsets(reloaded.value(), std::move(child_schema));
    ASSERT_TRUE(child_readable.has_value()) << child_readable.error();

    auto probe = probe_rowset(child_readable->front());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_TRUE(has_variant_layout(probe.value(), 2, "a"));
    EXPECT_TRUE(has_sparse_path_stat(probe.value(), "b"));
    EXPECT_EQ(has_sparse_path_stat(probe.value(), "b.c"), expect_b_c_sparse_stat);
    EXPECT_EQ(sparse_stat_entry_count(probe.value()), expected_sparse_stat_entries);

    const int32_t child_column_id = column_id_by_path("v.b.c");
    ASSERT_GE(child_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& child_column = tablet_schema()->column(child_column_id);

    IndexReadOptions child_read_options;
    child_read_options.need_ordered_result = true;
    child_read_options.return_columns = {0, static_cast<uint32_t>(child_column_id)};
    child_read_options.collect_string_values = true;
    auto child_read_result = read_rowsets(child_readable.value(), child_read_options);
    ASSERT_TRUE(child_read_result.has_value()) << child_read_result.error();
    ASSERT_EQ(child_read_result->rows_read, 5);
    ASSERT_TRUE(child_read_result->string_values_by_uid.contains(child_column.unique_id()));

    const auto& child_values = child_read_result->string_values_by_uid.at(child_column.unique_id());
    ASSERT_EQ(child_values.size(), 5);
    EXPECT_EQ(std::count(child_values.begin(), child_values.end(),
                         std::optional<std::string> {"child-0"}),
              1)
            << describe_optional_string_values(child_values);
    EXPECT_EQ(std::count(child_values.begin(), child_values.end(), std::nullopt), 4)
            << describe_optional_string_values(child_values);

    auto missing_schema = build_schema_with_variant_path_column(*tablet_schema(), 2, "v.b.missing",
                                                                FieldType::OLAP_FIELD_TYPE_STRING);
    ASSERT_NE(missing_schema, nullptr);
    auto missing_readable =
            inject_reader_schema_for_rowsets(child_readable.value(), std::move(missing_schema));
    ASSERT_TRUE(missing_readable.has_value()) << missing_readable.error();

    const int32_t missing_column_id = column_id_by_path("v.b.missing");
    ASSERT_GE(missing_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& missing_column = tablet_schema()->column(missing_column_id);

    IndexReadOptions missing_read_options;
    missing_read_options.need_ordered_result = true;
    missing_read_options.return_columns = {0, static_cast<uint32_t>(missing_column_id)};
    missing_read_options.collect_string_values = true;
    auto missing_read_result = read_rowsets(missing_readable.value(), missing_read_options);
    ASSERT_TRUE(missing_read_result.has_value()) << missing_read_result.error();
    ASSERT_EQ(missing_read_result->rows_read, 5);
    ASSERT_TRUE(missing_read_result->string_values_by_uid.contains(missing_column.unique_id()));

    const auto& missing_values =
            missing_read_result->string_values_by_uid.at(missing_column.unique_id());
    ASSERT_EQ(missing_values.size(), 5);
    EXPECT_TRUE(std::all_of(missing_values.begin(), missing_values.end(), [](const auto& value) {
        return !value.has_value();
    })) << describe_optional_string_values(missing_values);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsBeforeLimitReadsRecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/4,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110055);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsAtLimitReadsRecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/3,
                                         /*expected_sparse_stat_entries=*/3,
                                         /*expect_b_c_sparse_stat=*/true, 110056);
}

TEST_F(IndexStorageVariantSparseStatsTest,
       SparseStatsAfterLimitTruncationReadsUnrecordedExactChildAndMissingChildNull) {
    run_sparse_stats_limit_boundary_case(/*sparse_stats_limit=*/2,
                                         /*expected_sparse_stat_entries=*/2,
                                         /*expect_b_c_sparse_stat=*/false, 110057);
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
