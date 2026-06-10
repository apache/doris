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
#include <sstream>
#include <string_view>

#include "common/config.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"
#include "util/debug_points.h"

namespace doris::index_storage_test {
namespace {

std::shared_ptr<ColumnPredicate> title_equals(std::string value) {
    return create_comparison_predicate<PredicateType::EQ>(
            1, "title", std::make_shared<DataTypeString>(),
            Field::create_field<TYPE_STRING>(std::move(value)), false);
}

std::shared_ptr<ColumnPredicate> string_equals(int32_t column_id, std::string column_name,
                                               std::string value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeString>(),
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
        ++applied_events;
        filtered_rows += event.filtered_rows;
        EXPECT_FALSE(event.variant_path.has_value());
    }
    EXPECT_GT(applied_events, 0);
    EXPECT_EQ(filtered_rows, expected_filtered_rows);
}

void expect_applied_variant_path_index(const IndexReadResult& result, std::string_view path,
                                       int64_t index_id, int64_t expected_filtered_rows) {
    expect_inverted_index_used(result);
    expect_index_filter_stats(result, expected_filtered_rows);

    int64_t filtered_rows = 0;
    int64_t applied_events = 0;
    for (const auto& event : result.stats.index_probe_events) {
        if (event.state != IndexProbeState::APPLIED || event.column_uid != 2 ||
            event.index_id != index_id || !event.variant_path.has_value() ||
            event.variant_path.value() != path) {
            continue;
        }
        ++applied_events;
        filtered_rows += event.filtered_rows;
    }
    EXPECT_GT(applied_events, 0);
    EXPECT_EQ(filtered_rows, expected_filtered_rows);
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

std::string dump_schema_paths(const TabletSchema& schema) {
    std::ostringstream out;
    for (int32_t i = 0; i < schema.num_columns(); ++i) {
        const auto& column = schema.column(i);
        out << i << ": uid=" << column.unique_id() << " name=" << column.name()
            << " type=" << TabletColumn::get_string_by_field_type(column.type());
        if (column.has_path_info()) {
            out << " path=" << column.path_info_ptr()->get_path();
        }
        out << '\n';
    }
    return out.str();
}

class ScopedDebugPoint {
public:
    ScopedDebugPoint(std::string name, std::map<std::string, std::string> params)
            : _name(std::move(name)), _enable_debug_points(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->remove(_name);
        DebugPoints::instance()->add_with_params(_name, params);
    }

    ~ScopedDebugPoint() {
        DebugPoints::instance()->remove(_name);
        config::enable_debug_points = _enable_debug_points;
    }

private:
    std::string _name;
    bool _enable_debug_points = false;
};

} // namespace

class IndexStorageLifecycleTest : public IndexStorageTestFixture {
protected:
    void run_deep_sparse_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_nested_group_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
};

void IndexStorageLifecycleTest::run_deep_sparse_variant_lifecycle(bool external_segment_meta,
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
    rowset0.batches.push_back(VariantJsonBatch::single_variant(
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
    rowset1.batches.push_back(VariantJsonBatch::single_variant(
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

void IndexStorageLifecycleTest::run_nested_group_variant_lifecycle(bool external_segment_meta,
                                                                   int64_t tablet_id) {
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
    rowset0.batches.push_back(VariantJsonBatch::single_variant(
            {R"({"owner": "alice", "profile": {"region": "us"}, "items": [{"sku": "a", "qty": 1}]})",
             R"({"owner": "bob", "profile": {"region": "eu"}, "items": [{"sku": "b", "qty": 2}]})"},
            0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(VariantJsonBatch::single_variant(
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
    rowset0.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(VariantJsonBatch::single_text({"other", "hello"}, 100));
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
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other", "hello"}, 0));
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
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other", "hello"}, 0));
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
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
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
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
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
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other", "hello"}, 0));
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

    auto event = std::find_if(read_result->stats.index_probe_events.begin(),
                              read_result->stats.index_probe_events.end(), [](const auto& e) {
                                  return e.state == IndexProbeState::NOT_ATTEMPTED &&
                                         e.reason == IndexFallbackReason::QUERY_DISABLED &&
                                         e.column_uid == 2 && e.index_id == -1;
                              });
    ASSERT_NE(event, read_result->stats.index_probe_events.end());
}

TEST_F(IndexStorageLifecycleTest, TextPredicateWithoutIndexRecordsNotAttempted) {
    IndexTabletOptions options;
    options.tablet_id = 110004;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexReadOptions read_options;
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets({rowset_result.value()}, read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    EXPECT_EQ(read_result->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(read_result.value());

    auto event = std::find_if(read_result->stats.index_probe_events.begin(),
                              read_result->stats.index_probe_events.end(), [](const auto& e) {
                                  return e.state == IndexProbeState::NOT_ATTEMPTED &&
                                         e.reason == IndexFallbackReason::MISSING_INDEX &&
                                         e.column_uid == 2;
                              });
    ASSERT_NE(event, read_result->stats.index_probe_events.end());
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

    auto event = std::find_if(after_drop->stats.index_probe_events.begin(),
                              after_drop->stats.index_probe_events.end(), [](const auto& e) {
                                  return e.state == IndexProbeState::NOT_ATTEMPTED &&
                                         e.reason == IndexFallbackReason::MISSING_INDEX &&
                                         e.column_uid == 2;
                              });
    ASSERT_NE(event, after_drop->stats.index_probe_events.end());
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

TEST_F(IndexStorageLifecycleTest, BuildAndDropVariantPathIndexAfterExistingRows) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b", .type = FieldType::OLAP_FIELD_TYPE_STRING},
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

    auto missing_before = std::find_if(
            before_build->stats.index_probe_events.begin(),
            before_build->stats.index_probe_events.end(), [](const auto& e) {
                return e.state == IndexProbeState::NOT_ATTEMPTED &&
                       e.reason == IndexFallbackReason::MISSING_INDEX && e.column_uid == 2 &&
                       e.variant_path.has_value() && e.variant_path.value() == "b";
            });
    ASSERT_NE(missing_before, before_build->stats.index_probe_events.end());

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

    auto missing_after = std::find_if(
            after_drop->stats.index_probe_events.begin(),
            after_drop->stats.index_probe_events.end(), [](const auto& e) {
                return e.state == IndexProbeState::NOT_ATTEMPTED &&
                       e.reason == IndexFallbackReason::MISSING_INDEX && e.column_uid == 2 &&
                       e.variant_path.has_value() && e.variant_path.value() == "b";
            });
    ASSERT_NE(missing_after, after_drop->stats.index_probe_events.end());
}

TEST_F(IndexStorageLifecycleTest, DropVariantPathIndexWithoutMaterializedPathRemovesMetadata) {
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
            VariantJsonBatch::single_variant({R"({"b": "one"})", R"({"b": "other"})"}, 0));
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

TEST_F(IndexStorageLifecycleTest, DropOneVariantPathIndexKeepsSiblingPathIndexUsable) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b", .type = FieldType::OLAP_FIELD_TYPE_STRING},
            VariantPathSpec {.path = "c", .type = FieldType::OLAP_FIELD_TYPE_STRING},
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

    auto missing_b = std::find_if(b_after_drop->stats.index_probe_events.begin(),
                                  b_after_drop->stats.index_probe_events.end(), [](const auto& e) {
                                      return e.state == IndexProbeState::NOT_ATTEMPTED &&
                                             e.reason == IndexFallbackReason::MISSING_INDEX &&
                                             e.column_uid == 2 && e.variant_path.has_value() &&
                                             e.variant_path.value() == "b";
                                  });
    ASSERT_NE(missing_b, b_after_drop->stats.index_probe_events.end());

    auto c_after_drop = read_rowsets(reloaded_dropped_rowsets.value(), c_read_options);
    ASSERT_TRUE(c_after_drop.has_value()) << c_after_drop.error();
    EXPECT_EQ(c_after_drop->rows_read, 2);
    expect_applied_variant_path_index(c_after_drop.value(), "c", 20007, 2);
}

TEST_F(IndexStorageLifecycleTest, PatchedSchemaKeepsExistingTextIndexUsable) {
    IndexTabletOptions options;
    options.tablet_id = 110015;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    IndexSchemaPatch patch;
    patch.add_text_columns.push_back(
            TextColumnSpec {.unique_id = 3, .name = "body", .nullable = true});
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_column_unique_id(3));

    auto patched_rowsets = rowsets_with_schema({rowset_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.predicates.push_back(title_equals("hello"));
    auto read_result = read_rowsets(patched_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    expect_applied_title_index(read_result.value(), 1);
}

TEST_F(IndexStorageLifecycleTest, RowsetsWithSchemaUpdatesTabletRowsetMapForIndexDrop) {
    IndexTabletOptions options;
    options.tablet_id = 110019;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
    auto rowset_result = write_rowset(rowset);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();

    auto index = IndexSpec::column_index(20008, "idx_title_patch_drop", 2);
    IndexSchemaPatch patch;
    patch.add_inverted_indexes.push_back(index);
    auto patched_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(patched_schema, nullptr);
    ASSERT_TRUE(patched_schema->has_inverted_index_with_index_id(20008));

    auto patched_rowsets = rowsets_with_schema({rowset_result.value()}, std::move(patched_schema));
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
    VariantJsonBatch batch0;
    batch0.keys = {0, 1};
    batch0.text_values_by_column = {{"hello", "other"}, {"drop-a", "drop-b"}};
    rowset0.batches.push_back(std::move(batch0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    VariantJsonBatch batch1;
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

    auto patched_rowsets = rowsets_with_schema({rowset0_result.value(), rowset1_result.value()},
                                               std::move(patched_schema));
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

TEST_F(IndexStorageLifecycleTest, PatchedSchemaAddDropVariantColumnCompactsNewRows) {
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
    VariantJsonBatch batch0;
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
            VariantPathSpec {.path = "x", .type = FieldType::OLAP_FIELD_TYPE_STRING},
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
            rowsets_with_schema({rowset0_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_old_rowsets.has_value()) << patched_old_rowsets.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    VariantJsonBatch batch1;
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

TEST_F(IndexStorageLifecycleTest, MissingRequiredVariantColumnFailsExtendedInfoAggregation) {
    IndexTabletOptions options;
    options.tablet_id = 110018;
    options.text_columns = {TextColumnSpec {.unique_id = 2, .name = "title"}};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(VariantJsonBatch::single_text({"hello", "other"}, 0));
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

    auto patched_rowsets = rowsets_with_schema({rowset_result.value()}, std::move(patched_schema));
    ASSERT_TRUE(patched_rowsets.has_value()) << patched_rowsets.error();

    std::unordered_map<int32_t, variant_util::VariantExtendedInfo> variant_extended_info;
    auto status = variant_util::VariantCompactionUtil::aggregate_variant_extended_info(
            patched_rowsets->front(), &variant_extended_info);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("column not found in segment"), std::string::npos)
            << status.to_string();
}

TEST_F(IndexStorageLifecycleTest, VariantPathIndexHitAfterCumulativeCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.predefined_paths = {
            VariantPathSpec {.path = "b", .type = FieldType::OLAP_FIELD_TYPE_STRING},
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

TEST_F(IndexStorageLifecycleTest, WriteReadProbeAndCumulativeCompact) {
    IndexTabletOptions options;
    options.tablet_id = 110002;
    options.external_segment_meta = true;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.variant_columns[0].max_subcolumns_count = 4;
    options.variant_columns[0].sparse_hash_shard_count = 2;
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "a", .type = FieldType::OLAP_FIELD_TYPE_INT},
            VariantPathSpec {.path = "b", .type = FieldType::OLAP_FIELD_TYPE_STRING},
    };
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(VariantJsonBatch::single_variant(
            {R"({"a": 1, "b": "one"})", R"({"a": 2, "c": 20})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(VariantJsonBatch::single_variant(
            {R"({"a": 3, "b": "three"})", R"({"a": 4, "d": 40})"}, 100));
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

TEST_F(IndexStorageLifecycleTest, DeepSparseVariantCompactsWithExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(true, 110023);
}

TEST_F(IndexStorageLifecycleTest, DeepSparseVariantCompactsWithoutExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(false, 110024);
}

TEST_F(IndexStorageLifecycleTest, ExactSparsePathReadsHiddenChildAfterSparseStatsLimitTruncated) {
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
            VariantJsonBatch::single_variant({R"({"a": "hot-0", "b": "scalar-0", "d": "dense-0"})",
                                              R"({"a": "hot-1", "b": "scalar-1", "d": "dense-1"})",
                                              R"({"a": "hot-2", "b": "scalar-2", "d": "dense-2"})"},
                                             0));

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(
            VariantJsonBatch::single_variant({R"({"a": "hot-3", "b": "scalar-3", "d": "dense-3"})",
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
    auto readable_compacted = rowsets_with_schema(reloaded.value(), std::move(read_schema));
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

TEST_F(IndexStorageLifecycleTest, NestedGroupVariantCompactsWithExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(true, 110025);
}

TEST_F(IndexStorageLifecycleTest, NestedGroupVariantCompactsWithoutExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(false, 110026);
}

TEST_F(IndexStorageLifecycleTest, VariantDocModeWritesDocValueColumnsAfterCompaction) {
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
    rowset0.batches.push_back(VariantJsonBatch::single_variant(
            {R"({"a": "one", "b": 1})", R"({"a": "two", "c": 2})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(VariantJsonBatch::single_variant(
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

TEST(IndexStorageSchemaPatchTest, AddDropIndexAndModifyVariantProperties) {
    IndexTabletOptions options;
    options.tablet_id = 110003;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.inverted_indexes.push_back(
            IndexSpec::field_pattern_index(20001, "idx_v_old", 2, "old"));
    auto base_schema = build_tablet_schema(options);

    VariantColumnSpec modified = options.variant_columns[0];
    modified.max_subcolumns_count = 16;
    modified.enable_doc_mode = true;
    modified.doc_materialization_min_rows = 128;

    VariantColumnSpec added;
    added.unique_id = 3;
    added.name = "v2";
    added.max_subcolumns_count = 2;

    IndexSchemaPatch patch;
    patch.modify_variant_columns.emplace(2, modified);
    patch.add_variant_columns.push_back(added);
    patch.drop_index_names.insert("idx_v_old");
    patch.add_inverted_indexes.push_back(
            IndexSpec::field_pattern_index(20002, "idx_v_new", 2, "a"));

    auto patched_schema = build_patched_tablet_schema(*base_schema, patch);
    ASSERT_NE(patched_schema, nullptr);

    ASSERT_TRUE(patched_schema->has_column_unique_id(2));
    ASSERT_TRUE(patched_schema->has_column_unique_id(3));
    const auto& variant = patched_schema->column_by_uid(2);
    EXPECT_EQ(variant.variant_max_subcolumns_count(), 16);
    EXPECT_TRUE(variant.variant_enable_doc_mode());
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(2, "old").empty());
    auto new_indexes = patched_schema->inverted_index_by_field_pattern(2, "a");
    ASSERT_EQ(new_indexes.size(), 1);
    EXPECT_EQ(new_indexes[0]->index_name(), "idx_v_new");
}

TEST(IndexStorageSchemaPatchTest, VariantDocAndNestedGroupOptionsArePreserved) {
    IndexTabletOptions options;
    options.tablet_id = 110009;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.variant_columns[0].enable_doc_mode = true;
    options.variant_columns[0].doc_materialization_min_rows = 64;
    options.variant_columns[0].doc_hash_shard_count = 8;
    options.variant_columns[0].enable_nested_group = true;

    auto schema = build_tablet_schema(options);
    ASSERT_NE(schema, nullptr);
    ASSERT_TRUE(schema->has_column_unique_id(2));
    const auto& variant = schema->column_by_uid(2);
    EXPECT_TRUE(variant.variant_enable_doc_mode());
    EXPECT_EQ(variant.variant_doc_materialization_min_rows(), 64);
    EXPECT_EQ(variant.variant_doc_hash_shard_count(), 8);
    EXPECT_TRUE(variant.variant_enable_nested_group());
}

TEST(IndexStorageSchemaPatchTest, AddAndDropTextColumns) {
    IndexTabletOptions options;
    options.tablet_id = 110014;
    options.text_columns = {
            TextColumnSpec {.unique_id = 2, .name = "title"},
            TextColumnSpec {.unique_id = 3, .name = "obsolete"},
    };
    options.inverted_indexes.push_back(IndexSpec::column_index(20001, "idx_title", 2));
    options.inverted_indexes.push_back(IndexSpec::column_index(20002, "idx_obsolete", 3));
    auto base_schema = build_tablet_schema(options);
    ASSERT_NE(base_schema, nullptr);

    IndexSchemaPatch patch;
    patch.drop_column_uids.insert(3);
    patch.add_text_columns.push_back(TextColumnSpec {.unique_id = 4, .name = "body"});

    auto patched_schema = build_patched_tablet_schema(*base_schema, patch);
    ASSERT_NE(patched_schema, nullptr);

    ASSERT_TRUE(patched_schema->has_column_unique_id(2));
    EXPECT_FALSE(patched_schema->has_column_unique_id(3));
    ASSERT_TRUE(patched_schema->has_column_unique_id(4));
    const auto& body = patched_schema->column_by_uid(4);
    EXPECT_EQ(body.name(), "body");
    EXPECT_EQ(body.type(), FieldType::OLAP_FIELD_TYPE_STRING);
    EXPECT_EQ(patched_schema->next_column_unique_id(), 5);
    EXPECT_TRUE(patched_schema->inverted_index_by_field_pattern(3, "").empty());
}

} // namespace doris::index_storage_test
