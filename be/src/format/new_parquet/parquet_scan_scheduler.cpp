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

#include "format/new_parquet/parquet_scan_scheduler.h"

#include <parquet/api/reader.h>

#include <algorithm>
#include <limits>
#include <utility>

#include "common/exception.h"
#include "core/block/block.h"
#include "format/new_parquet/parquet_batch_filter.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_file_context.h"
#include "format/new_parquet/selection_vector.h"

namespace doris::parquet {

namespace {

constexpr int64_t DEFAULT_PARQUET_READ_BATCH_SIZE = 4096;

} // namespace

void ParquetScanScheduler::set_plan(RowGroupScanPlan plan) {
    _selected_row_groups = std::move(plan.selected_row_groups);
    _row_group_first_rows = std::move(plan.row_group_first_rows);
    reset();
}

void ParquetScanScheduler::reset() {
    _next_row_group_idx = 0;
    reset_current_row_group();
}

void ParquetScanScheduler::reset_current_row_group() {
    _current_row_group.reset();
    _current_predicate_columns.clear();
    _current_non_predicate_columns.clear();
    _current_row_group_rows = 0;
    _current_row_group_rows_read = 0;
    _current_row_group_first_row = 0;
}

Status ParquetScanScheduler::open_next_row_group(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, bool* has_row_group) {
    *has_row_group = false;
    while (_next_row_group_idx < _selected_row_groups.size()) {
        const int row_group_idx = _selected_row_groups[_next_row_group_idx++];
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
        _current_row_group_rows =
                row_group_metadata == nullptr ? 0 : row_group_metadata->num_rows();
        if (_current_row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        } else if (_current_row_group_rows == 0) {
            reset_current_row_group();
            continue;
        }
        DORIS_CHECK(row_group_idx >= 0 &&
                    row_group_idx < static_cast<int>(_row_group_first_rows.size()));
        _current_row_group_first_row = _row_group_first_rows[row_group_idx];
        _current_row_group_rows_read = 0;
        _current_predicate_columns.clear();
        _current_non_predicate_columns.clear();

        ParquetColumnReaderFactory column_reader_factory(_current_row_group,
                                                         file_context.schema->num_columns());
        for (const auto file_column_id : request.predicate_columns) {
            if (file_column_id == ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID) {
                _current_predicate_columns.push_back(
                        column_reader_factory.create_row_position_column_reader(
                                _current_row_group_first_row));
                continue;
            }
            const auto& column_schema = file_schema[file_column_id];
            const auto projection_it = request.complex_projections.find(file_column_id);
            const auto* projection = projection_it == request.complex_projections.end()
                                             ? nullptr
                                             : &projection_it->second;
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(
                    column_reader_factory.create(*column_schema, projection, &column_reader));
            _current_predicate_columns.push_back(std::move(column_reader));
        }
        for (const auto file_column_id : request.non_predicate_columns) {
            if (file_column_id == ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID) {
                _current_non_predicate_columns.push_back(
                        column_reader_factory.create_row_position_column_reader(
                                _current_row_group_first_row));
                continue;
            }
            const auto& column_schema = file_schema[file_column_id];
            const auto projection_it = request.complex_projections.find(file_column_id);
            const auto* projection = projection_it == request.complex_projections.end()
                                             ? nullptr
                                             : &projection_it->second;
            std::unique_ptr<ParquetColumnReader> column_reader;
            RETURN_IF_ERROR(
                    column_reader_factory.create(*column_schema, projection, &column_reader));
            _current_non_predicate_columns.push_back(std::move(column_reader));
        }
        *has_row_group = true;
        break;
    }
    return Status::OK();
}

Status ParquetScanScheduler::read_filter_columns(int64_t batch_rows,
                                                 const reader::FileScanRequest& request,
                                                 Block* file_block, SelectionVector* selection,
                                                 uint16_t* selected_rows) {
    selection->resize(static_cast<size_t>(batch_rows));
    for (size_t filter_idx = 0; filter_idx < request.predicate_columns.size(); ++filter_idx) {
        const int file_field_id = request.predicate_columns[filter_idx];
        auto& column_reader = _current_predicate_columns[filter_idx];
        auto position_it = request.column_positions.find(file_field_id);
        DORIS_CHECK(position_it != request.column_positions.end());
        const auto block_position = position_it->second;
        auto column = file_block->get_by_position(block_position).column->assume_mutable();
        DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                  column_reader->type()->get_primitive_type());
        int64_t column_rows = 0;
        RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
        if (column_rows != batch_rows) {
            return Status::Corruption("Parquet filter column {} returned {} rows, expected {} rows",
                                      column_reader->name(), column_rows, batch_rows);
        }
        file_block->replace_by_position(block_position, std::move(column));
    }
    RETURN_IF_ERROR(execute_reader_expression_map(request, file_block, request.predicate_columns));
    return execute_batch_filters(request, batch_rows, file_block, selection, selected_rows);
}

Status ParquetScanScheduler::read_current_row_group_batch(int64_t batch_rows,
                                                          const reader::FileScanRequest& request,
                                                          Block* file_block, size_t* rows) {
    if (_current_predicate_columns.empty() && _current_non_predicate_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        return Status::OK();
    }
    SelectionVector selection;
    DORIS_CHECK(batch_rows <= std::numeric_limits<uint16_t>::max());
    uint16_t selected_rows = static_cast<uint16_t>(batch_rows);
    RETURN_IF_ERROR(
            read_filter_columns(batch_rows, request, file_block, &selection, &selected_rows));

    const bool need_filter_output = selected_rows != batch_rows;
    if (need_filter_output) {
        IColumn::Filter output_filter = selection_to_filter(selection, selected_rows, batch_rows);
        for (const auto file_field_id : request.predicate_columns) {
            auto position_it = request.column_positions.find(file_field_id);
            DORIS_CHECK(position_it != request.column_positions.end());
            const auto block_position = position_it->second;
            RETURN_IF_CATCH_EXCEPTION(file_block->replace_by_position(
                    block_position, file_block->get_by_position(block_position)
                                            .column->filter(output_filter, selected_rows)));
        }
    }

    for (size_t output_idx = 0; output_idx < _current_non_predicate_columns.size(); ++output_idx) {
        auto& column_reader = _current_non_predicate_columns[output_idx];
        auto position_it = request.column_positions.find(request.non_predicate_columns[output_idx]);
        DORIS_CHECK(position_it != request.column_positions.end());
        const auto block_position = position_it->second;
        auto col = file_block->get_columns()[block_position]->assume_mutable();
        DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                  column_reader->type()->get_primitive_type());
        if (need_filter_output) {
            [[maybe_unused]] auto old_size = col->size();
            RETURN_IF_ERROR(column_reader->select(selection, selected_rows, batch_rows, col));
            if (col->size() != old_size + selected_rows) {
                return Status::Corruption(
                        "Parquet selected output column {} returned {} rows, expected {} rows",
                        column_reader->name(), col->size(), old_size + selected_rows);
            }
        } else {
            int64_t column_rows = 0;
            RETURN_IF_ERROR(column_reader->read(batch_rows, col, &column_rows));
            if (column_rows != batch_rows) {
                return Status::Corruption(
                        "Parquet output column {} returned {} rows, expected {} rows",
                        column_reader->name(), column_rows, batch_rows);
            }
        }
    }
    RETURN_IF_ERROR(
            execute_reader_expression_map(request, file_block, request.non_predicate_columns));

    *rows = static_cast<size_t>(selected_rows);
    return Status::OK();
}

Status ParquetScanScheduler::read_next_batch(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileScanRequest& request, Block* file_block, size_t* rows, bool* eof) {
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

        const int64_t remaining_rows = _current_row_group_rows - _current_row_group_rows_read;
        if (remaining_rows <= 0) {
            reset_current_row_group();
            continue;
        }

        const int64_t batch_rows =
                std::min<int64_t>(DEFAULT_PARQUET_READ_BATCH_SIZE, remaining_rows);
        const int64_t physical_rows_read = batch_rows;
        RETURN_IF_ERROR(read_current_row_group_batch(batch_rows, request, file_block, rows));
        _current_row_group_rows_read += physical_rows_read;
        if (_current_row_group_rows_read >= _current_row_group_rows) {
            reset_current_row_group();
        }
        if (*rows == 0) {
            continue;
        }
        *eof = false;
        return Status::OK();
    }
}

} // namespace doris::parquet
