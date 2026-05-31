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

#include "format/new_parquet/parquet_scan_planner.h"

#include <parquet/api/reader.h>

#include <utility>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "format/new_parquet/parquet_statistics.h"

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
                               const reader::FileScanRequest& request,
                               const ParquetScanRange& scan_range, RowGroupScanPlan* plan) {
    DORIS_CHECK(plan != nullptr);
    plan->row_groups.clear();

    std::vector<int> statistics_selected_row_groups;
    RETURN_IF_ERROR(select_row_groups_by_statistics(metadata, file_reader, file_schema, request,
                                                    &statistics_selected_row_groups));

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
        row_group_plan.selected_ranges.push_back(RowRange {0, row_group_rows});
        plan->row_groups.push_back(std::move(row_group_plan));
    }
    return Status::OK();
}

} // namespace doris::parquet
