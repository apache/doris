// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_scan.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/native/common.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "storage/utils.h"

namespace doris::format::parquet {
namespace {

ParquetColumnSchema int64_schema() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "mock";
    schema.type = std::make_shared<DataTypeInt64>();
    return schema;
}

class CursorColumnReader final : public ParquetColumnReader {
public:
    CursorColumnReader() : ParquetColumnReader(int64_schema(), std::make_shared<DataTypeInt64>()) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        DORIS_CHECK(column);
        DORIS_CHECK(rows_read != nullptr);
        auto& values = assert_cast<ColumnInt64&>(*column);
        for (int64_t row = 0; row < rows; ++row) {
            values.insert_value(_cursor + row);
        }
        _read_lengths.push_back(rows);
        _cursor += rows;
        *rows_read = rows;
        return Status::OK();
    }

    Status skip(int64_t rows) override {
        DORIS_CHECK(rows >= 0);
        _skip_lengths.push_back(rows);
        _cursor += rows;
        return Status::OK();
    }

    void flush_profile() override { ++_profile_flushes; }
    bool crossed_page_since_last_batch() override {
        ++_page_crossing_checks;
        return _crossed_page;
    }

    void set_crossed_page(bool crossed_page) { _crossed_page = crossed_page; }

    int64_t cursor() const { return _cursor; }
    const std::vector<int64_t>& skip_lengths() const { return _skip_lengths; }
    const std::vector<int64_t>& read_lengths() const { return _read_lengths; }
    int profile_flushes() const { return _profile_flushes; }
    int page_crossing_checks() const { return _page_crossing_checks; }

private:
    int64_t _cursor = 0;
    std::vector<int64_t> _skip_lengths;
    std::vector<int64_t> _read_lengths;
    int _profile_flushes = 0;
    bool _crossed_page = false;
    int _page_crossing_checks = 0;
};

GlobalRowLoacationV2 decode_rowid(const ColumnString& column, size_t row) {
    const auto ref = column.get_data_at(row);
    EXPECT_EQ(ref.size, sizeof(GlobalRowLoacationV2));
    GlobalRowLoacationV2 location(0, 0, 0, 0);
    std::memcpy(&location, ref.data, sizeof(GlobalRowLoacationV2));
    return location;
}

} // namespace

TEST(SelectionVectorTest, IdentitySelectionToRanges) {
    SelectionVector selection;
    const auto ranges = selection_to_ranges(selection, 5);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 5);
    EXPECT_TRUE(selection.verify(5, 5).ok());
}

TEST(SelectionVectorTest, ExternalBufferSelectionToRanges) {
    SelectionVector::Index indices[] = {0, 1, 4, 6, 7};
    SelectionVector selection(indices, std::size(indices));
    const auto ranges = selection_to_ranges(selection, std::size(indices));
    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 2);
    EXPECT_EQ(ranges[1].start, 4);
    EXPECT_EQ(ranges[1].length, 1);
    EXPECT_EQ(ranges[2].start, 6);
    EXPECT_EQ(ranges[2].length, 2);
    EXPECT_TRUE(selection.verify(std::size(indices), 8).ok());
}

TEST(SelectionVectorTest, OutputRangesReuseCapacity) {
    SelectionVector::Index indices[] = {1, 2, 5};
    SelectionVector selection(indices, std::size(indices));
    std::vector<RowRange> ranges;
    ranges.reserve(8);
    const auto retained_capacity = ranges.capacity();

    selection_to_ranges(selection, std::size(indices), &ranges);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges.capacity(), retained_capacity);
    selection_to_ranges(selection, 1, &ranges);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges.capacity(), retained_capacity);
}

TEST(SelectionVectorTest, VerifyRejectsInvalidSelection) {
    SelectionVector selection(2);
    EXPECT_FALSE(selection.verify(3, 3).ok());
    EXPECT_FALSE(selection.verify(1, -1).ok());

    selection.set_index(0, 2);
    selection.set_index(1, 1);
    EXPECT_FALSE(selection.verify(2, 3).ok());

    selection.set_index(0, 0);
    selection.set_index(1, 3);
    EXPECT_FALSE(selection.verify(2, 3).ok());
}

TEST(SelectionVectorTest, MaterializedFilterIsReusedUntilSelectionChanges) {
    SelectionVector selection(4);
    selection.set_index(0, 1);
    selection.set_index(1, 3);
    const uint8_t* first_filter = nullptr;
    ASSERT_TRUE(selection.materialize_filter(2, 4, &first_filter).ok());
    ASSERT_NE(first_filter, nullptr);
    EXPECT_EQ(std::vector<uint8_t>(first_filter, first_filter + 4),
              std::vector<uint8_t>({0, 1, 0, 1}));

    const uint8_t* reused_filter = nullptr;
    ASSERT_TRUE(selection.materialize_filter(2, 4, &reused_filter).ok());
    EXPECT_EQ(reused_filter, first_filter);

    selection.set_index(1, 2);
    const uint8_t* updated_filter = nullptr;
    ASSERT_TRUE(selection.materialize_filter(2, 4, &updated_filter).ok());
    EXPECT_EQ(updated_filter, first_filter);
    EXPECT_EQ(std::vector<uint8_t>(updated_filter, updated_filter + 4),
              std::vector<uint8_t>({0, 1, 1, 0}));
}

TEST(SelectionVectorTest, IdentitySelectionDoesNotMaterializeFilter) {
    SelectionVector selection(4);
    EXPECT_FALSE(selection.is_set());
    const uint8_t* filter = reinterpret_cast<const uint8_t*>(1);
    ASSERT_TRUE(selection.materialize_filter(4, 4, &filter).ok());
    EXPECT_EQ(filter, nullptr);
}

TEST(NativeNullableSelectionTest, BuildsPhysicalRangesAndSelectedNullsInOnePass) {
    using native::FilterMap;

    const std::vector<uint16_t> null_runs {2, 1, 3, 2, 2};
    const std::vector<uint8_t> filter_data {1, 0, 1, 1, 0, 1, 1, 1, 0, 1};
    FilterMap filter;
    ASSERT_TRUE(filter.init(filter_data.data(), filter_data.size(), false).ok());
    ParquetSelection selection;
    NullMap output_nulls {1};
    NullMap selected_nulls;
    size_t num_filtered = 0;

    ASSERT_TRUE(native::build_filtered_nullable_selection(null_runs, filter_data.size(), 3,
                                                          &output_nulls, &filter, 0, &selection,
                                                          &selected_nulls, &num_filtered)
                        .ok());

    EXPECT_EQ(selection.total_values, 7);
    EXPECT_EQ(selection.selected_values, 4);
    ASSERT_EQ(selection.ranges.size(), 4);
    EXPECT_EQ(selection.ranges[0].first, 0);
    EXPECT_EQ(selection.ranges[0].count, 1);
    EXPECT_EQ(selection.ranges[1].first, 2);
    EXPECT_EQ(selection.ranges[1].count, 1);
    EXPECT_EQ(selection.ranges[2].first, 4);
    EXPECT_EQ(selection.ranges[2].count, 1);
    EXPECT_EQ(selection.ranges[3].first, 6);
    EXPECT_EQ(selection.ranges[3].count, 1);
    EXPECT_EQ(selected_nulls, (NullMap {0, 1, 0, 0, 1, 1, 0}));
    EXPECT_EQ(output_nulls, (NullMap {1, 0, 1, 0, 0, 1, 1, 0}));
    EXPECT_EQ(num_filtered, 3);
}

TEST(NativeNullableSelectionTest, UsesDirectPhysicalCoordinatesWithoutNulls) {
    using native::FilterMap;

    const std::vector<uint16_t> no_nulls {10};
    const std::vector<uint8_t> filter_data {1, 1, 0, 1, 0, 0, 1, 1, 1, 0};
    FilterMap filter;
    ASSERT_TRUE(filter.init(filter_data.data(), filter_data.size(), false).ok());
    ParquetSelection selection;
    NullMap output_nulls;
    NullMap selected_nulls;
    size_t num_filtered = 0;

    ASSERT_TRUE(native::build_filtered_nullable_selection(no_nulls, filter_data.size(), 0,
                                                          &output_nulls, &filter, 0, &selection,
                                                          &selected_nulls, &num_filtered)
                        .ok());

    EXPECT_EQ(selection.total_values, 10);
    EXPECT_EQ(selection.selected_values, 6);
    ASSERT_EQ(selection.ranges.size(), 3);
    EXPECT_EQ(selection.ranges[0].first, 0);
    EXPECT_EQ(selection.ranges[0].count, 2);
    EXPECT_EQ(selection.ranges[1].first, 3);
    EXPECT_EQ(selection.ranges[1].count, 1);
    EXPECT_EQ(selection.ranges[2].first, 6);
    EXPECT_EQ(selection.ranges[2].count, 3);
    EXPECT_EQ(selected_nulls, (NullMap {0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(output_nulls, selected_nulls);
    EXPECT_EQ(num_filtered, 4);
}

TEST(NativeNullableSelectionTest, BuildsDictionaryPlanWithoutMaterializationNullMap) {
    using native::FilterMap;

    const std::vector<uint16_t> null_runs {2, 1, 3, 2, 2};
    const std::vector<uint8_t> filter_data {1, 0, 1, 1, 0, 1, 1, 1, 0, 1};
    FilterMap filter;
    ASSERT_TRUE(filter.init(filter_data.data(), filter_data.size(), false).ok());
    ParquetSelection selection;
    NullMap selected_nulls;
    size_t num_filtered = 0;

    ASSERT_TRUE(native::build_filtered_nullable_selection(null_runs, filter_data.size(), 3, nullptr,
                                                          &filter, 0, &selection, &selected_nulls,
                                                          &num_filtered)
                        .ok());

    EXPECT_EQ(selection.total_values, 7);
    EXPECT_EQ(selection.selected_values, 4);
    EXPECT_EQ(selected_nulls, (NullMap {0, 1, 0, 0, 1, 1, 0}));
    EXPECT_EQ(num_filtered, 3);
}

TEST(NativeNullableSelectionTest, GatesDictionaryFusionByBatchAndInputSelection) {
    using native::FilterMap;

    std::vector<uint8_t> partial_data(4096, 0);
    partial_data[1] = 1;
    FilterMap partial;
    ASSERT_TRUE(partial.init(partial_data.data(), partial_data.size(), false).ok());
    FilterMap identity;
    ASSERT_TRUE(identity.init(nullptr, 0, false).ok());
    std::vector<uint8_t> dense_data(4096, 1);
    dense_data[1] = 0;
    FilterMap dense;
    ASSERT_TRUE(dense.init(dense_data.data(), dense_data.size(), false).ok());

    EXPECT_TRUE(native::should_use_fused_dictionary_selection(4096, 0, partial, 0));
    EXPECT_FALSE(native::should_use_fused_dictionary_selection(4096, 8, partial, 0));
    EXPECT_FALSE(native::should_use_fused_dictionary_selection(4096, 8, dense, 0));
    EXPECT_FALSE(native::should_use_fused_dictionary_selection(512, 0, partial, 0));
    EXPECT_FALSE(native::should_use_fused_dictionary_selection(4096, 0, identity, 0));
}

TEST(NativeNullableSelectionTest, EnablesFusionOnlyForMateriallyFragmentedNullableBatches) {
    EXPECT_FALSE(native::should_use_fused_nullable_selection(65536, 0, 3));
    EXPECT_FALSE(native::should_use_fused_nullable_selection(65536, 655, 1311));
    EXPECT_FALSE(native::should_use_fused_nullable_selection(65536, 32768, 3));
    EXPECT_FALSE(native::should_use_fused_nullable_selection(512, 256, 512));
    EXPECT_TRUE(native::should_use_fused_nullable_selection(65536, 6553, 13107));
    EXPECT_TRUE(native::should_use_fused_nullable_selection(65536, 32768, 65536));
}

TEST(NativeNestedSelectionTest, BuildsSelectionAndCompactsSurvivingParentLevels) {
    using native::ColumnSelectVector;
    using native::FilterMap;
    using native::level_t;

    std::vector<level_t> repetition_levels {0, 1, 1, 0, 0, 1};
    std::vector<level_t> definition_levels {3, 2, 1, 3, 0, 3};
    std::vector<uint8_t> parent_filter_data {1, 0, 1};
    FilterMap parent_filter;
    ASSERT_TRUE(
            parent_filter.init(parent_filter_data.data(), parent_filter_data.size(), false).ok());

    ColumnSelectVector selection;
    NullMap selected_nulls;
    size_t ancestor_null_count = 0;
    ASSERT_TRUE(selection
                        .init_nested(&repetition_levels, &definition_levels, 0,
                                     /*repeated_parent_def_level=*/2,
                                     /*definition_level=*/3, &selected_nulls, &parent_filter, 0,
                                     &ancestor_null_count)
                        .ok());

    EXPECT_EQ(ancestor_null_count, 2);
    EXPECT_EQ(selection.num_values(), 4);
    EXPECT_EQ(selection.num_nulls(), 1);
    EXPECT_EQ(selection.num_filtered(), 1);
    EXPECT_EQ(selected_nulls, NullMap({0, 1, 0}));
    EXPECT_EQ(repetition_levels, (std::vector<level_t> {0, 1, 1, 0, 1}));
    EXPECT_EQ(definition_levels, (std::vector<level_t> {3, 2, 1, 0, 3}));

    ColumnSelectVector::DataReadType type;
    EXPECT_EQ(selection.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);
    EXPECT_EQ(selection.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::NULL_DATA);
    EXPECT_EQ(selection.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_CONTENT);
    EXPECT_EQ(selection.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);
    EXPECT_EQ(selection.get_next_run<true>(&type), 0);
}

TEST(NativeNestedSelectionTest, PreservesPriorLevelsAcrossPageContinuation) {
    using native::ColumnSelectVector;
    using native::FilterMap;
    using native::level_t;

    std::vector<level_t> repetition_levels {0, 1, 1, 0, 1};
    std::vector<level_t> definition_levels {3, 3, 2, 3, 1};
    std::vector<uint8_t> parent_filter_data {1, 0};
    FilterMap parent_filter;
    ASSERT_TRUE(
            parent_filter.init(parent_filter_data.data(), parent_filter_data.size(), false).ok());

    ColumnSelectVector selection;
    NullMap selected_nulls;
    size_t ancestor_null_count = 0;
    ASSERT_TRUE(selection
                        .init_nested(&repetition_levels, &definition_levels,
                                     /*level_start_index=*/2,
                                     /*repeated_parent_def_level=*/2,
                                     /*definition_level=*/3, &selected_nulls, &parent_filter, 0,
                                     &ancestor_null_count)
                        .ok());

    EXPECT_EQ(ancestor_null_count, 1);
    EXPECT_EQ(selection.num_values(), 2);
    EXPECT_EQ(selection.num_nulls(), 1);
    EXPECT_EQ(selection.num_filtered(), 1);
    EXPECT_EQ(selected_nulls, NullMap({1}));
    EXPECT_EQ(repetition_levels, (std::vector<level_t> {0, 1, 1}));
    EXPECT_EQ(definition_levels, (std::vector<level_t> {3, 3, 2}));
}

TEST(SelectionVectorTest, BulkCompactionSupportsBothFilterCoordinates) {
    SelectionVector selection(6);
    const uint8_t row_filter[] = {0, 1, 1, 0, 1, 0};
    ASSERT_EQ(selection.compact_with_row_filter(row_filter, 6), 3);
    EXPECT_EQ(selection.get_index(0), 1);
    EXPECT_EQ(selection.get_index(1), 2);
    EXPECT_EQ(selection.get_index(2), 4);

    const uint8_t compact_filter[] = {1, 0, 1};
    ASSERT_EQ(selection.compact_with_selection_filter(compact_filter, 3), 2);
    EXPECT_EQ(selection.get_index(0), 1);
    EXPECT_EQ(selection.get_index(1), 4);
    EXPECT_TRUE(selection.verify(2, 6).ok());
}

TEST(SelectionVectorTest, BatchResetRetainsMaterializedScratchHighWaterMark) {
    SelectionVector selection(6);
    ASSERT_NE(selection.data(), nullptr);
    const uint8_t first_filter[] = {0, 1, 1, 0, 1, 0};
    ASSERT_EQ(selection.compact_with_row_filter(first_filter, 6), 3);

    selection.resize(6);
    const uint8_t second_filter[] = {0, 0, 0, 0, 0, 1};
    ASSERT_EQ(selection.compact_with_row_filter(second_filter, 6), 1);
    EXPECT_EQ(selection.get_index(0), 5);
    // Positions beyond the logical result remain reusable scratch. Clearing and resizing the
    // owned vector would value-initialize this slot on every scanner batch.
    EXPECT_EQ(selection.get_index(5), 5);
}

TEST(ParquetColumnReaderControlTest, BaseSelectUsesSkipReadRanges) {
    CursorColumnReader reader;
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);

    auto column = std::make_shared<DataTypeInt64>()->create_column();
    ASSERT_TRUE(reader.select(selection, 3, 6, column).ok());

    const auto& values = assert_cast<const ColumnInt64&>(*column);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_element(0), 0);
    EXPECT_EQ(values.get_element(1), 2);
    EXPECT_EQ(values.get_element(2), 4);
    EXPECT_EQ(reader.cursor(), 6);
    EXPECT_EQ(reader.read_lengths(), std::vector<int64_t>({1, 1, 1}));
    EXPECT_EQ(reader.skip_lengths(), std::vector<int64_t>({0, 1, 1, 1}));
}

TEST(ParquetColumnReaderControlTest, BaseSelectZeroRowsConsumesBatch) {
    CursorColumnReader reader;
    SelectionVector selection;
    auto column = std::make_shared<DataTypeInt64>()->create_column();
    ASSERT_TRUE(reader.select(selection, 0, 4, column).ok());
    EXPECT_TRUE(column->empty());
    EXPECT_EQ(reader.cursor(), 4);
    EXPECT_TRUE(reader.read_lengths().empty());
    EXPECT_EQ(reader.skip_lengths(), std::vector<int64_t>({4}));
}

TEST(ParquetColumnReaderControlTest, SchedulerFlushesReaderProfilesAtBatchBoundary) {
    ParquetScanScheduler scheduler;
    auto reader = std::make_unique<CursorColumnReader>();
    auto* reader_ptr = reader.get();
    scheduler._current_predicate_columns.emplace(0, std::move(reader));

    scheduler.flush_current_reader_profiles();
    EXPECT_EQ(reader_ptr->profile_flushes(), 1);
}

TEST(ParquetColumnReaderControlTest, SchedulerOrsPageCrossingOncePerBatch) {
    ParquetScanScheduler scheduler;
    auto predicate_reader = std::make_unique<CursorColumnReader>();
    auto* predicate_ptr = predicate_reader.get();
    predicate_ptr->set_crossed_page(true);
    scheduler._current_predicate_columns.emplace(0, std::move(predicate_reader));

    auto lazy_reader = std::make_unique<CursorColumnReader>();
    auto* lazy_ptr = lazy_reader.get();
    lazy_ptr->set_crossed_page(true);
    scheduler._current_non_predicate_columns.emplace(1, std::move(lazy_reader));

    // Both readers are sampled even after the OR becomes true so their next batch starts cleanly.
    EXPECT_TRUE(scheduler.finish_current_reader_batch_profiles());
    EXPECT_EQ(predicate_ptr->page_crossing_checks(), 1);
    EXPECT_EQ(lazy_ptr->page_crossing_checks(), 1);
}

TEST(ParquetColumnReaderControlTest, PendingRequestActivatesOnlyAtRowGroupBoundary) {
    ParquetScanScheduler scheduler;
    auto initial = std::make_shared<format::FileScanRequest>();
    auto refreshed = std::make_shared<format::FileScanRequest>();
    refreshed->predicate_only_columns.push_back(format::LocalColumnId(7));

    scheduler.set_scan_request(initial);
    scheduler._has_current_row_group = true;
    scheduler.queue_scan_request(refreshed);
    scheduler.activate_pending_scan_request_at_row_group_boundary();
    EXPECT_EQ(scheduler._active_request, initial);

    scheduler._has_current_row_group = false;
    scheduler._predicate_survival_ratio = 0.5;
    scheduler._predicate_batch_sequence = 3;
    scheduler._predicate_runtime_stats.emplace(1, detail::AdaptivePredicateStats {});
    scheduler.activate_pending_scan_request_at_row_group_boundary();
    EXPECT_EQ(scheduler._active_request, refreshed);
    EXPECT_TRUE(scheduler._remaining_plans_need_replanning);
    EXPECT_EQ(scheduler._predicate_survival_ratio, -1);
    EXPECT_EQ(scheduler._predicate_batch_sequence, 0);
    EXPECT_TRUE(scheduler._predicate_runtime_stats.empty());
}

TEST(ParquetColumnReaderControlTest, PendingOutputDrainsBeforePageCrossingSample) {
    ParquetScanScheduler scheduler;
    scheduler._batch_size = 1;
    auto lazy_reader = std::make_unique<CursorColumnReader>();
    auto* lazy_ptr = lazy_reader.get();
    scheduler._current_non_predicate_columns.emplace(format::LocalColumnId(0),
                                                     std::move(lazy_reader));
    scheduler._pending_predicate_batch_rows = 2;
    scheduler._pending_predicate_selection = {0, 1};

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    Block block;
    auto type = std::make_shared<DataTypeInt64>();
    block.insert({type->create_column(), type, "mock"});
    size_t rows = 0;

    ASSERT_TRUE(scheduler.materialize_pending_predicate_batch(request, &block, &rows).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_EQ(lazy_ptr->page_crossing_checks(), 0);
    lazy_ptr->set_crossed_page(true);

    block.get_by_position(0).column = type->create_column();
    ASSERT_TRUE(scheduler.materialize_pending_predicate_batch(request, &block, &rows).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_TRUE(scheduler._pending_predicate_selection.empty());
    EXPECT_EQ(lazy_ptr->page_crossing_checks(), 1);
}

TEST(ParquetVirtualColumnReaderTest, RowPositionReadSkipAndInvalidArgs) {
    RowPositionColumnReader reader(100);
    EXPECT_EQ(reader.file_column_id(), format::ROW_POSITION_COLUMN_ID);
    EXPECT_EQ(reader.parquet_leaf_column_id(), -1);
    EXPECT_EQ(reader.name(), format::ROW_POSITION_COLUMN_NAME);

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 2);
    ASSERT_TRUE(reader.skip(3).ok());
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());

    const auto& values = assert_cast<const ColumnInt64&>(*column);
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), 100);
    EXPECT_EQ(values.get_element(1), 101);
    EXPECT_EQ(values.get_element(2), 105);
    EXPECT_EQ(values.get_element(3), 106);

    MutableColumnPtr null_column;
    EXPECT_FALSE(reader.read(1, null_column, &rows_read).ok());
    EXPECT_FALSE(reader.read(-1, column, &rows_read).ok());
    EXPECT_FALSE(reader.read(1, column, nullptr).ok());
}

TEST(ParquetVirtualColumnReaderTest, GlobalRowIdReadSkipSelectAndInvalidArgs) {
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    GlobalRowIdColumnReader reader(context, 10);
    EXPECT_EQ(reader.file_column_id(), format::GLOBAL_ROWID_COLUMN_ID);
    EXPECT_EQ(reader.parquet_leaf_column_id(), -1);
    EXPECT_EQ(reader.name(), BeConsts::GLOBAL_ROWID_COL);

    auto column = reader.type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader.read(2, column, &rows_read).ok());
    ASSERT_TRUE(reader.skip(2).ok());
    ASSERT_TRUE(reader.read(1, column, &rows_read).ok());

    const auto& strings = assert_cast<const ColumnString&>(*column);
    ASSERT_EQ(strings.size(), 3);
    const auto first = decode_rowid(strings, 0);
    EXPECT_EQ(first.version, context.version);
    EXPECT_EQ(first.backend_id, context.backend_id);
    EXPECT_EQ(first.file_id, context.file_id);
    EXPECT_EQ(first.row_id, 10);
    EXPECT_EQ(decode_rowid(strings, 1).row_id, 11);
    EXPECT_EQ(decode_rowid(strings, 2).row_id, 14);

    GlobalRowIdColumnReader select_reader(context, 20);
    SelectionVector selection(2);
    selection.set_index(0, 1);
    selection.set_index(1, 3);
    auto selected_column = select_reader.type()->create_column();
    ASSERT_TRUE(select_reader.select(selection, 2, 5, selected_column).ok());
    const auto& selected_strings = assert_cast<const ColumnString&>(*selected_column);
    ASSERT_EQ(selected_strings.size(), 2);
    EXPECT_EQ(decode_rowid(selected_strings, 0).row_id, 21);
    EXPECT_EQ(decode_rowid(selected_strings, 1).row_id, 23);

    MutableColumnPtr null_column;
    EXPECT_FALSE(reader.read(1, null_column, &rows_read).ok());
    EXPECT_FALSE(reader.read(-1, column, &rows_read).ok());
    EXPECT_FALSE(reader.read(1, column, nullptr).ok());
}

} // namespace doris::format::parquet
