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
#include <map>

#include "core/data_type/data_type_string.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

std::shared_ptr<ColumnPredicate> title_equals(std::string value) {
    return create_comparison_predicate<PredicateType::EQ>(
            1, "title", std::make_shared<DataTypeString>(),
            Field::create_field<TYPE_STRING>(std::move(value)), false);
}

void expect_applied_title_index(const IndexReadResult& result, int64_t expected_filtered_rows,
                                int64_t index_id = 20001) {
    expect_inverted_index_used(result);
    expect_index_filter_stats(result, expected_filtered_rows);

    int64_t filtered_rows = 0;
    int64_t applied_events = 0;
    for (const auto& event : result.stats.index_probe_events) {
        if (event.state != IndexProbeState::APPLIED || event.column_uid != 2 ||
            event.index_id != index_id) {
            continue;
        }
        if (event.counts_toward_filter_stats) {
            ++applied_events;
            filtered_rows += event.filtered_rows;
        }
        EXPECT_FALSE(event.variant_path.has_value());
    }
    EXPECT_GT(applied_events, 0);
    EXPECT_EQ(filtered_rows, expected_filtered_rows);
}

} // namespace

class IndexStorageLifecycleTest : public IndexStorageTestFixture {};

TEST_F(IndexStorageLifecycleTest, TextIndexHitAfterCumulativeCompaction) {
    const auto index_case =
            IndexStorageCaseBuilder("text_index_hit_after_cumulative_compaction")
                    .tablet_id(110001)
                    .text_column(TextColumnSpec {.unique_id = 2, .name = "title"})
                    .inverted_index(IndexSpec::column_index(20001, "idx_title", 2))
                    .rowset(0, IndexDataSourceSpec::inline_text({"hello", "other"}, 0))
                    .rowset(1, IndexDataSourceSpec::inline_text({"hello", "other"}, 100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets(rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_applied_title_index(read_result.value(), 2);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    IndexReadOptions compacted_read_options;
    compacted_read_options.predicates.push_back(title_equals("hello"));
    auto compacted_read = read_rowsets(reloaded.value(), compacted_read_options);
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 2);
    expect_applied_title_index(compacted_read.value(), 2);
}

TEST_F(IndexStorageLifecycleTest, TextIndexHitAfterFullCompaction) {
    IndexTabletOptions options;
    options.tablet_id = 110007;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_text({"other", "hello"}, 100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto compacted = compact_rowsets(IndexCompactionKind::FULL,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_applied_title_index(read_result.value(), 2);
}

TEST_F(IndexStorageLifecycleTest, CountOnIndexPredicateRecordsAppliedEvent) {
    IndexTabletOptions options;
    options.tablet_id = 110008;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other", "hello"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexReadOptions read_options;
    read_options.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_applied_title_index(read_result.value(), 1);
}

TEST_F(IndexStorageLifecycleTest, CountOnIndexSkipsReadingKeyDataWhenIndexApplied) {
    IndexTabletOptions options;
    options.tablet_id = 110027;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other", "hello"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    ScopedDebugPoint fail_if_key_column_is_read("segment_iterator._read_columns_by_index",
                                                {{"column_name", "k"}});
    IndexReadOptions read_options;
    read_options.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    EXPECT_EQ(read_result->stats.raw_rows_read, 2);
    expect_applied_title_index(read_result.value(), 1);
}

TEST_F(IndexStorageLifecycleTest, TextPredicateRecordsQueryIndexIdAfterNullBitmapCheck) {
    IndexTabletOptions options;
    options.tablet_id = 110010;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(
            IndexSpec::column_index(20001, "idx_title_fulltext", 2, {{"parser", "english"}}));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_title_string", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.data_sources.push_back(IndexDataSourceSpec::inline_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    expect_applied_title_index(read_result.value(), 1, 20002);
}

TEST_F(IndexStorageLifecycleTest, V1IndexStorageWritesOneFilePerIndex) {
    IndexTabletOptions options;
    options.tablet_id = 110019;
    options.index_storage_format = InvertedIndexStorageFormatPB::V1;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(
            IndexSpec::column_index(20001, "idx_title_fulltext", 2, {{"parser", "english"}}));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_title_string", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto probe = probe_rowset(rowset_result.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_EQ(probe->num_segments, 1);
    EXPECT_EQ(probe->index_files.expected_files, 2);
    expect_index_files(probe.value(), true);

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    expect_applied_title_index(read_result.value(), 1, 20002);
}

TEST_F(IndexStorageLifecycleTest, V2IndexStorageWritesCompoundIndexFile) {
    IndexTabletOptions options;
    options.tablet_id = 110020;
    options.index_storage_format = InvertedIndexStorageFormatPB::V2;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(
            IndexSpec::column_index(20001, "idx_title_fulltext", 2, {{"parser", "english"}}));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_title_string", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto probe = probe_rowset(rowset_result.value());
    ASSERT_TRUE(probe.has_value()) << probe.error();
    EXPECT_EQ(probe->num_segments, 1);
    EXPECT_EQ(probe->index_files.expected_files, 1);
    EXPECT_GT(probe->index_files.rowset_meta_entries, 0);
    expect_index_files(probe.value(), true);

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    expect_applied_title_index(read_result.value(), 1, 20002);
}

TEST_F(IndexStorageLifecycleTest, TextIndexDisabledRecordsNotAttempted) {
    IndexTabletOptions options;
    options.tablet_id = 110013;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other", "hello"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    EXPECT_EQ(read_result->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageLifecycleTest, TextPredicateWithoutIndexRecordsNotAttempted) {
    IndexTabletOptions options;
    options.tablet_id = 110004;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    EXPECT_EQ(read_result->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageLifecycleTest, DropTextIndexRecordsNotAttempted) {
    const auto index_case =
            IndexStorageCaseBuilder("drop_text_index_records_not_attempted")
                    .tablet_id(110005)
                    .text_column(TextColumnSpec {.unique_id = 2, .name = "title"})
                    .inverted_index(IndexSpec::column_index(20001, "idx_title", 2))
                    .rowset(0, IndexDataSourceSpec::inline_text({"hello", "other"}, 0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    IndexReadOptions before_drop_options;
    before_drop_options.predicates.push_back(title_equals("hello"));
    auto before_drop = read_rowsets(rowsets.value(), before_drop_options);
    ASSERT_TRUE(before_drop.has_value()) << before_drop.error();
    EXPECT_EQ(before_drop->rows_read, 1);
    expect_applied_title_index(before_drop.value(), 1);

    auto dropped_rowsets = drop_inverted_indexes({IndexSpec::column_index(20001, "idx_title", 2)});
    ASSERT_TRUE(dropped_rowsets.has_value()) << dropped_rowsets.error();
    ASSERT_EQ(dropped_rowsets->size(), 1);

    auto dropped_probe = probe_rowset(dropped_rowsets->front());
    ASSERT_TRUE(dropped_probe.has_value()) << dropped_probe.error();
    expect_index_files(dropped_probe.value(), false);

    auto reloaded_dropped_rowsets = reload_rowsets(dropped_rowsets.value());
    ASSERT_TRUE(reloaded_dropped_rowsets.has_value()) << reloaded_dropped_rowsets.error();
    IndexReadOptions after_drop_options;
    after_drop_options.predicates.push_back(title_equals("hello"));
    auto after_drop = read_rowsets(reloaded_dropped_rowsets.value(), after_drop_options);
    ASSERT_TRUE(after_drop.has_value()) << after_drop.error();
    EXPECT_EQ(after_drop->rows_read, 1);
    EXPECT_EQ(after_drop->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(after_drop.value());
}

TEST_F(IndexStorageLifecycleTest, BuildTextIndexAfterExistingRowsUsesNewIndex) {
    const auto index_case =
            IndexStorageCaseBuilder("build_text_index_after_existing_rows_uses_new_index")
                    .tablet_id(110011)
                    .text_column(TextColumnSpec {.unique_id = 2, .name = "title"})
                    .rowset(0, IndexDataSourceSpec::inline_text({"hello", "other"}, 0))
                    .rowset(1, IndexDataSourceSpec::inline_text({"other", "hello"}, 100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    IndexReadOptions before_build_options;
    before_build_options.predicates.push_back(title_equals("hello"));
    auto before_build = read_rowsets(rowsets.value(), before_build_options);
    ASSERT_TRUE(before_build.has_value()) << before_build.error();
    EXPECT_EQ(before_build->rows_read, 2);
    EXPECT_EQ(before_build->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(before_build.value());

    const auto built_index = IndexSpec::column_index(20003, "idx_title_built", 2);
    auto built_rowsets = build_inverted_indexes({built_index});
    ASSERT_TRUE(built_rowsets.has_value()) << built_rowsets.error();
    ASSERT_EQ(built_rowsets->size(), 2);
    for (const auto& rowset : built_rowsets.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), true);
    }

    auto reloaded_built_rowsets = reload_rowsets(built_rowsets.value());
    ASSERT_TRUE(reloaded_built_rowsets.has_value()) << reloaded_built_rowsets.error();
    IndexReadOptions after_build_options;
    after_build_options.predicates.push_back(title_equals("hello"));
    auto after_build = read_rowsets(reloaded_built_rowsets.value(), after_build_options);
    ASSERT_TRUE(after_build.has_value()) << after_build.error();
    EXPECT_EQ(after_build->rows_read, 2);
    expect_applied_title_index(after_build.value(), 2, 20003);
}

TEST_F(IndexStorageLifecycleTest, PatchedSchemaKeepsExistingTextIndexUsable) {
    IndexTabletOptions options;
    options.tablet_id = 110015;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexSchemaPatch patch;
    patch.add_text_columns.push_back(
            TextColumnSpec {.unique_id = 3, .name = "body", .nullable = true});
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_column_unique_id(3));

    auto patched_rowsets =
            inject_reader_schema_for_rowsets({rowset_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets(patched_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    expect_applied_title_index(read_result.value(), 1);
}

TEST_F(IndexStorageLifecycleTest, InjectReaderSchemaUpdatesTabletRowsetMapForIndexDrop) {
    IndexTabletOptions options;
    options.tablet_id = 110019;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(IndexBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto index = IndexSpec::column_index(20008, "idx_title_patch_drop", 2);
    IndexSchemaPatch patch;
    patch.add_inverted_indexes.push_back(index);
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_inverted_index_with_index_id(20008));

    auto patched_rowsets =
            inject_reader_schema_for_rowsets({rowset_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();
    ASSERT_EQ(patched_rowsets->size(), 1);
    ASSERT_TRUE(patched_rowsets->front()->tablet_schema()->has_inverted_index_with_index_id(20008));

    auto dropped_rowsets = drop_inverted_indexes({index});
    ASSERT_TRUE(dropped_rowsets.has_value()) << dropped_rowsets.error();
    ASSERT_EQ(dropped_rowsets->size(), 1);
    EXPECT_FALSE(
            dropped_rowsets->front()->tablet_schema()->has_inverted_index_with_index_id(20008));
}

TEST_F(IndexStorageLifecycleTest, PatchedSchemaAddDropTextColumnReadsDefaultsAndCompacts) {
    IndexTabletOptions options;
    options.tablet_id = 110016;
    options.text_columns = {
            TextColumnSpec {.unique_id = 2, .name = "title"},
            TextColumnSpec {.unique_id = 3, .name = "obsolete", .nullable = true},
    };
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_obsolete", 3));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    IndexBatch batch0;
    batch0.keys = {0, 1};
    batch0.text_values_by_column = {{"hello", "other"}, {"drop-a", "drop-b"}};
    rowset0.batches.push_back(std::move(batch0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    IndexBatch batch1;
    batch1.keys = {2, 3};
    batch1.text_values_by_column = {{"hello", "later"}, {"drop-c", "drop-d"}};
    rowset1.batches.push_back(std::move(batch1));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    IndexSchemaPatch patch;
    patch.drop_column_uids.insert(3);
    patch.add_text_columns.push_back(
            TextColumnSpec {.unique_id = 4, .name = "body", .nullable = true});
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    EXPECT_FALSE(patched_schema->has_column_unique_id(3));
    ASSERT_TRUE(patched_schema->has_column_unique_id(4));

    auto patched_rowsets = inject_reader_schema_for_rowsets(
            {rowset0_result.value(), rowset1_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();

    IndexReadOptions read_options;
    read_options.collect_string_values = true;
    read_options.need_ordered_result = true;
    read_options.return_columns = {1, 2};
    auto read_result = read_rowsets(patched_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);
    ASSERT_TRUE(read_result->string_values_by_uid.contains(2));
    ASSERT_TRUE(read_result->string_values_by_uid.contains(4));
    EXPECT_EQ(read_result->string_values_by_uid.at(2),
              (std::vector<std::optional<std::string>> {"hello", "other", "hello", "later"}));
    EXPECT_EQ(read_result->string_values_by_uid.at(4),
              (std::vector<std::optional<std::string>> {std::nullopt, std::nullopt, std::nullopt,
                                                        std::nullopt}));

    IndexReadOptions index_read_options;
    index_read_options.predicates.push_back(title_equals("hello"));
    auto index_read = read_rowsets(patched_rowsets.value(), index_read_options);
    ASSERT_TRUE(index_read.has_value()) << index_read.error();
    EXPECT_EQ(index_read->rows_read, 2);
    expect_applied_title_index(index_read.value(), 2);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, patched_rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    IndexReadOptions compacted_read_options;
    compacted_read_options.collect_string_values = true;
    compacted_read_options.need_ordered_result = true;
    compacted_read_options.return_columns = {1, 2};
    auto compacted_read = read_rowsets({compacted.value()}, compacted_read_options);
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
    ASSERT_TRUE(compacted_read->string_values_by_uid.contains(2));
    ASSERT_TRUE(compacted_read->string_values_by_uid.contains(4));
    EXPECT_EQ(compacted_read->string_values_by_uid.at(2),
              (std::vector<std::optional<std::string>> {"hello", "other", "hello", "later"}));
    EXPECT_EQ(compacted_read->string_values_by_uid.at(4),
              (std::vector<std::optional<std::string>> {std::nullopt, std::nullopt, std::nullopt,
                                                        std::nullopt}));
}

} // namespace doris::index_storage_test
