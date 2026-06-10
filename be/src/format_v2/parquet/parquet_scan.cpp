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

#include "format_v2/parquet/parquet_scan.h"

#include <parquet/api/reader.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "exprs/vexpr_context.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_statistics.h"

namespace doris::parquet {

namespace {

int64_t column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? cast_set<int64_t>(column_metadata.dictionary_page_offset())
                   : cast_set<int64_t>(column_metadata.data_page_offset());
}

bool is_row_group_outside_range(const ::parquet::FileMetaData& metadata,
                                const ParquetScanRange& scan_range, int row_group_idx) {
    if (scan_range.size < 0) {
        return false;
    }
    const int64_t range_start_offset = scan_range.start_offset;
    const int64_t range_end_offset = range_start_offset + scan_range.size;
    DORIS_CHECK(range_start_offset >= 0);
    DORIS_CHECK(range_end_offset >= range_start_offset);
    if (range_start_offset == 0 &&
        (scan_range.file_size < 0 || range_end_offset >= scan_range.file_size)) {
        return false;
    }

    auto row_group_metadata = metadata.RowGroup(row_group_idx);
    DORIS_CHECK(row_group_metadata != nullptr);
    DORIS_CHECK(row_group_metadata->num_columns() > 0);
    const auto first_column = row_group_metadata->ColumnChunk(0);
    const auto last_column = row_group_metadata->ColumnChunk(row_group_metadata->num_columns() - 1);
    DORIS_CHECK(first_column != nullptr);
    DORIS_CHECK(last_column != nullptr);
    const int64_t row_group_start_offset = column_start_offset(*first_column);
    const int64_t row_group_end_offset =
            column_start_offset(*last_column) + last_column->total_compressed_size();
    const int64_t row_group_mid_offset =
            row_group_start_offset + (row_group_end_offset - row_group_start_offset) / 2;
    return row_group_mid_offset < range_start_offset || row_group_mid_offset >= range_end_offset;
}

} // namespace

Status plan_parquet_row_groups(const ::parquet::FileMetaData& metadata,
                               ::parquet::ParquetFileReader* file_reader,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan) {
    DORIS_CHECK(plan != nullptr);
    plan->row_groups.clear();
    plan->pruning_stats = ParquetPruningStats {};

    std::vector<int> statistics_selected_row_groups;
    RETURN_IF_ERROR(select_row_groups_by_statistics(metadata, file_reader, file_schema, request,
                                                    &statistics_selected_row_groups,
                                                    enable_bloom_filter, &plan->pruning_stats));

    std::vector<int64_t> row_group_first_rows(metadata.num_row_groups());
    int64_t next_row_group_first_row = 0;
    for (int row_group_idx = 0; row_group_idx < metadata.num_row_groups(); ++row_group_idx) {
        row_group_first_rows[row_group_idx] = next_row_group_first_row;
        auto row_group_metadata = metadata.RowGroup(row_group_idx);
        DORIS_CHECK(row_group_metadata != nullptr);
        const int64_t row_group_rows = row_group_metadata->num_rows();
        if (row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        }
        next_row_group_first_row += row_group_rows;
    }

    plan->row_groups.reserve(statistics_selected_row_groups.size());
    for (const auto row_group_idx : statistics_selected_row_groups) {
        if (is_row_group_outside_range(metadata, scan_range, row_group_idx)) {
            continue;
        }

        auto row_group_metadata = metadata.RowGroup(row_group_idx);
        DORIS_CHECK(row_group_metadata != nullptr);
        const int64_t row_group_rows = row_group_metadata->num_rows();
        if (row_group_rows == 0) {
            continue;
        }

        RowGroupReadPlan row_group_plan;
        row_group_plan.row_group_id = row_group_idx;
        row_group_plan.first_file_row = row_group_first_rows[row_group_idx];
        row_group_plan.row_group_rows = row_group_rows;
        RETURN_IF_ERROR(select_row_group_ranges_by_page_index(
                file_reader, file_schema, request, row_group_idx, row_group_rows,
                &row_group_plan.selected_ranges, &row_group_plan.page_skip_plans,
                &plan->pruning_stats));
        if (row_group_plan.selected_ranges.empty()) {
            continue;
        }
        plan->pruning_stats.selected_row_ranges += row_group_plan.selected_ranges.size();
        plan->row_groups.push_back(std::move(row_group_plan));
    }
    plan->pruning_stats.selected_row_groups = plan->row_groups.size();
    return Status::OK();
}

namespace {

uint16_t apply_filter_to_selection(const IColumn::Filter& filter, SelectionVector* selection,
                                   uint16_t selected_rows) {
    uint16_t new_selected_rows = 0;
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        const auto row_idx = selection->get_index(selection_idx);
        if (filter[row_idx] != 0) {
            selection->set_index(new_selected_rows++, static_cast<SelectionVector::Index>(row_idx));
        }
    }
    return new_selected_rows;
}

Status execute_filter_conjuncts(const format::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& conjunct : request.conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(conjunct != nullptr);
        IColumn::Filter filter(static_cast<size_t>(batch_rows), 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(conjunct->execute_filter(file_block, filter.data(),
                                                 static_cast<size_t>(batch_rows), false,
                                                 &can_filter_all));
        *selected_rows =
                can_filter_all ? 0 : apply_filter_to_selection(filter, selection, *selected_rows);
    }
    return Status::OK();
}

Status execute_delete_conjuncts(const format::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& delete_conjunct : request.delete_conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(delete_conjunct != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR(delete_conjunct->root()->execute(delete_conjunct.get(), file_block,
                                                         &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        const auto& delete_filter = assert_cast<const ColumnUInt8&>(
                                            *file_block->get_by_position(result_column_id).column)
                                            .get_data();
        DORIS_CHECK(delete_filter.size() == static_cast<size_t>(batch_rows));
        IColumn::Filter keep_filter(static_cast<size_t>(batch_rows), 1);
        bool has_kept_row = false;
        for (size_t row = 0; row < static_cast<size_t>(batch_rows); ++row) {
            keep_filter[row] = !delete_filter[row];
            has_kept_row |= keep_filter[row] != 0;
        }
        file_block->erase(result_column_id);
        *selected_rows =
                !has_kept_row ? 0
                              : apply_filter_to_selection(keep_filter, selection, *selected_rows);
    }
    return Status::OK();
}

} // namespace

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows) {
    IColumn::Filter filter(static_cast<size_t>(batch_rows), 0);
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        filter[selection.get_index(selection_idx)] = 1;
    }
    return filter;
}

Status execute_batch_filters(const format::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection,
                             uint16_t* selected_rows) {
    if (request.conjuncts.empty() && request.delete_conjuncts.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(
            execute_filter_conjuncts(request, batch_rows, file_block, selection, selected_rows));
    if (*selected_rows == 0) {
        return Status::OK();
    }
    return execute_delete_conjuncts(request, batch_rows, file_block, selection, selected_rows);
}

namespace {
// TODO: batch size in SessionVariable
constexpr int64_t DEFAULT_PARQUET_READ_BATCH_SIZE = 4096;

} // namespace

void ParquetScanScheduler::set_plan(RowGroupScanPlan plan) {
    _row_group_plans = std::move(plan.row_groups);
    reset();
}

void ParquetScanScheduler::reset() {
    _next_row_group_plan_idx = 0;
    reset_current_row_group();
}

void ParquetScanScheduler::reset_current_row_group() {
    _current_row_group.reset();
    _current_predicate_columns.clear();
    _current_non_predicate_columns.clear();
    _current_row_group_rows = 0;
    _current_row_group_rows_read = 0;
    _current_row_group_first_row = 0;
    _current_selected_ranges.clear();
    _current_range_idx = 0;
    _current_range_rows_read = 0;
}

Status ParquetScanScheduler::open_next_row_group(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, bool* has_row_group) {
    *has_row_group = false;
    if (_next_row_group_plan_idx >= _row_group_plans.size()) {
        return Status::OK();
    }
    const RowGroupReadPlan& row_group_plan = _row_group_plans[_next_row_group_plan_idx++];
    const int row_group_idx = row_group_plan.row_group_id;
    try {
        _current_row_group = file_context.file_reader->RowGroup(row_group_idx);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to open parquet row group {}: {}", row_group_idx,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open parquet row group {}: {}", row_group_idx,
                                     e.what());
    }

    auto row_group_metadata = file_context.metadata->RowGroup(row_group_idx);
    DORIS_CHECK(row_group_metadata != nullptr);
    _current_row_group_rows = row_group_metadata->num_rows();
    DORIS_CHECK(_current_row_group_rows == row_group_plan.row_group_rows);
    DORIS_CHECK(_current_row_group_rows > 0);
    DORIS_CHECK(!row_group_plan.selected_ranges.empty());
    _current_row_group_first_row = row_group_plan.first_file_row;
    _current_row_group_rows_read = 0;
    _current_selected_ranges = row_group_plan.selected_ranges;
    _current_range_idx = 0;
    _current_range_rows_read = 0;
    _current_predicate_columns.clear();
    _current_non_predicate_columns.clear();

    ParquetColumnReaderFactory column_reader_factory(
            _current_row_group, file_context.schema->num_columns(), &row_group_plan.page_skip_plans,
            _page_skip_profile, _timezone, _scan_profile.column_reader_profile);
    for (const auto& col : request.predicate_columns) {
        const auto local_id = col.field_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID) {
            _current_predicate_columns[local_id] =
                    column_reader_factory.create_row_position_column_reader(
                            _current_row_group_first_row);
            continue;
        }
        if (local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            DORIS_CHECK(_global_rowid_context.has_value());
            _current_predicate_columns[local_id] =
                    column_reader_factory.create_global_rowid_column_reader(
                            *_global_rowid_context, _current_row_group_first_row);
            continue;
        }

        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        const auto& column_schema = file_schema[local_id];
        DORIS_CHECK(column_schema != nullptr);
        std::unique_ptr<ParquetColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_factory.create(*column_schema, &col, &column_reader));
        _current_predicate_columns[local_id] = std::move(column_reader);
    }
    for (const auto& col : request.non_predicate_columns) {
        const auto local_id = col.field_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID) {
            _current_non_predicate_columns[local_id] =
                    column_reader_factory.create_row_position_column_reader(
                            _current_row_group_first_row);
            continue;
        }
        if (local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            DORIS_CHECK(_global_rowid_context.has_value());
            _current_non_predicate_columns[local_id] =
                    column_reader_factory.create_global_rowid_column_reader(
                            *_global_rowid_context, _current_row_group_first_row);
            continue;
        }
        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        const auto& column_schema = file_schema[local_id];
        DORIS_CHECK(column_schema != nullptr);
        std::unique_ptr<ParquetColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_factory.create(*column_schema, &col, &column_reader));
        _current_non_predicate_columns[local_id] = std::move(column_reader);
    }
    *has_row_group = true;
    return Status::OK();
}

Status ParquetScanScheduler::skip_current_row_group_rows(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    if (rows == 0) {
        return Status::OK();
    }
    if (_scan_profile.range_gap_skipped_rows != nullptr) {
        COUNTER_UPDATE(_scan_profile.range_gap_skipped_rows, rows);
    }
    for (const auto& column_reader : _current_predicate_columns | std::views::values) {
        RETURN_IF_ERROR(column_reader->skip(rows));
    }
    for (const auto& column_reader : _current_non_predicate_columns | std::views::values) {
        RETURN_IF_ERROR(column_reader->skip(rows));
    }
    _current_row_group_rows_read += rows;
    return Status::OK();
}

Status ParquetScanScheduler::read_filter_columns(int64_t batch_rows,
                                                 const format::FileScanRequest& request,
                                                 Block* file_block, SelectionVector* selection,
                                                 uint16_t* selected_rows) {
    if (!request.conjuncts.empty() || !request.delete_conjuncts.empty()) {
        selection->resize(static_cast<size_t>(batch_rows));
    }
    for (const auto& [fid, column_reader] : _current_predicate_columns) {
        auto position_it = request.local_positions.find(format::LocalColumnId(fid));
        DORIS_CHECK(position_it != request.local_positions.end());
        const auto block_position = position_it->second.value();
        DCHECK(remove_nullable(column_reader->type())
                       ->equals(*remove_nullable(file_block->get_by_position(block_position).type)))
                << column_reader->type()->get_name() << " "
                << file_block->get_by_position(block_position).type->get_name() << " "
                << column_reader->name() << " " << file_block->get_by_position(block_position).name;
        auto column = file_block->get_by_position(block_position).column->assert_mutable();
        int64_t column_rows = 0;
        {
            SCOPED_TIMER(_scan_profile.column_read_time);
            RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
        }
        if (column_rows != batch_rows) {
            return Status::Corruption("Parquet filter column {} returned {} rows, expected {} rows",
                                      column_reader->name(), column_rows, batch_rows);
        }
        file_block->replace_by_position(block_position, std::move(column));
    }
    if (_scan_profile.predicate_filter_time == nullptr) {
        return execute_batch_filters(request, batch_rows, file_block, selection, selected_rows);
    }
    SCOPED_TIMER(_scan_profile.predicate_filter_time);
    return execute_batch_filters(request, batch_rows, file_block, selection, selected_rows);
}

Status ParquetScanScheduler::read_current_row_group_batch(int64_t batch_rows,
                                                          const format::FileScanRequest& request,
                                                          Block* file_block, size_t* rows) {
    if (_scan_profile.total_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.total_batches, 1);
    }
    if (_scan_profile.raw_rows_read != nullptr) {
        COUNTER_UPDATE(_scan_profile.raw_rows_read, batch_rows);
    }
    if (_current_predicate_columns.empty() && _current_non_predicate_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        if (_scan_profile.selected_rows != nullptr) {
            COUNTER_UPDATE(_scan_profile.selected_rows, batch_rows);
        }
        return Status::OK();
    }
    SelectionVector selection;
    DORIS_CHECK(batch_rows <= std::numeric_limits<uint16_t>::max());
    uint16_t selected_rows = static_cast<uint16_t>(batch_rows);
    RETURN_IF_ERROR(
            read_filter_columns(batch_rows, request, file_block, &selection, &selected_rows));

    const bool need_filter_output = selected_rows != batch_rows;
    if (_scan_profile.selected_rows != nullptr) {
        COUNTER_UPDATE(_scan_profile.selected_rows, selected_rows);
    }
    if (_scan_profile.rows_filtered_by_conjunct != nullptr) {
        COUNTER_UPDATE(_scan_profile.rows_filtered_by_conjunct, batch_rows - selected_rows);
    }
    if (selected_rows == 0 && _scan_profile.empty_selection_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.empty_selection_batches, 1);
    }
    if (need_filter_output) {
        IColumn::Filter output_filter = selection_to_filter(selection, selected_rows, batch_rows);
        for (const auto& col : request.predicate_columns) {
            auto position_it = request.local_positions.find(col.column_id());
            DORIS_CHECK(position_it != request.local_positions.end());
            const auto block_position = position_it->second.value();
            RETURN_IF_CATCH_EXCEPTION(file_block->replace_by_position(
                    block_position, file_block->get_by_position(block_position)
                                            .column->filter(output_filter, selected_rows)));
        }
    }

    {
        SCOPED_TIMER(_scan_profile.column_read_time);
        for (const auto& [fid, column_reader] : _current_non_predicate_columns) {
            auto position_it = request.local_positions.find(format::LocalColumnId(fid));
            DORIS_CHECK(position_it != request.local_positions.end());
            const auto block_position = position_it->second.value();
            auto column = file_block->get_by_position(block_position).column->assert_mutable();
            DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                      column_reader->type()->get_primitive_type())
                    << type_to_string(file_block->get_by_position(block_position)
                                              .type->get_primitive_type())
                    << " " << type_to_string(column_reader->type()->get_primitive_type()) << " "
                    << column_reader->name() << " " << fid << " " << block_position;
            if (need_filter_output) {
                [[maybe_unused]] auto old_size = column->size();
                RETURN_IF_ERROR(
                        column_reader->select(selection, selected_rows, batch_rows, column));
                if (column->size() != old_size + selected_rows) {
                    return Status::Corruption(
                            "Parquet selected output column {} returned {} rows, expected {} rows",
                            column_reader->name(), column->size(), old_size + selected_rows);
                }
            } else {
                int64_t column_rows = 0;
                RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
                if (column_rows != batch_rows) {
                    return Status::Corruption(
                            "Parquet output column {} returned {} rows, expected {} rows",
                            column_reader->name(), column_rows, batch_rows);
                }
            }
            file_block->replace_by_position(block_position, std::move(column));
        }
    }
    *rows = static_cast<size_t>(selected_rows);
    return Status::OK();
}

Status ParquetScanScheduler::read_next_batch(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, Block* file_block, size_t* rows, bool* eof) {
    *rows = 0;
    while (true) {
        if (_current_row_group == nullptr) {
            bool has_row_group = false;
            RETURN_IF_ERROR(
                    open_next_row_group(file_context, file_schema, request, &has_row_group));
            if (!has_row_group) {
                *eof = true;
                return Status::OK();
            }
        }

        if (_current_range_idx >= _current_selected_ranges.size()) {
            // Current row group finished, try next row group.
            reset_current_row_group();
            continue;
        }

        const RowRange& current_range = _current_selected_ranges[_current_range_idx];
        DORIS_CHECK(current_range.start >= 0);
        DORIS_CHECK(current_range.length > 0);
        DORIS_CHECK(current_range.start + current_range.length <= _current_row_group_rows);

        if (_current_row_group_rows_read < current_range.start) {
            // Skip filtered rows according to row group level pruning.
            RETURN_IF_ERROR(skip_current_row_group_rows(current_range.start -
                                                        _current_row_group_rows_read));
        }
        DORIS_CHECK(_current_row_group_rows_read == current_range.start + _current_range_rows_read);
        const int64_t remaining_rows = current_range.length - _current_range_rows_read;
        if (remaining_rows <= 0) {
            // Current range finished, try next range in the same row group.
            ++_current_range_idx;
            _current_range_rows_read = 0;
            continue;
        }

        const int64_t batch_rows =
                std::min<int64_t>(DEFAULT_PARQUET_READ_BATCH_SIZE, remaining_rows);
        const int64_t physical_rows_read = batch_rows;
        RETURN_IF_ERROR(read_current_row_group_batch(batch_rows, request, file_block, rows));
        _current_row_group_rows_read += physical_rows_read;
        _current_range_rows_read += physical_rows_read;
        if (_current_range_rows_read >= current_range.length) {
            ++_current_range_idx;
            _current_range_rows_read = 0;
        }
        if (*rows == 0) {
            continue;
        }
        *eof = false;
        return Status::OK();
    }
}

} // namespace doris::parquet
