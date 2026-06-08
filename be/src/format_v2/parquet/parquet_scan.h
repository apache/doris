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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "format_v2/file_reader.h"
#include "runtime/runtime_profile.h"

namespace parquet {
class FileMetaData;
class ParquetFileReader;
class RowGroupReader;
} // namespace parquet

namespace doris {
class Block;

namespace format {
struct FileScanRequest;
} // namespace format
} // namespace doris

namespace doris::parquet {

struct ParquetFileContext;
struct ParquetColumnSchema;

struct ParquetScanRange {
    int64_t start_offset = 0;
    int64_t size = -1;
    int64_t file_size = -1;
};

struct RowGroupReadPlan {
    int row_group_id = -1;
    int64_t first_file_row = 0;
    int64_t row_group_rows = 0;
    std::vector<RowRange> selected_ranges;
    std::map<int, ParquetPageSkipPlan> page_skip_plans;
};

struct RowGroupScanPlan {
    std::vector<RowGroupReadPlan> row_groups;
    ParquetPruningStats pruning_stats;
};

struct ParquetScanProfile {
    RuntimeProfile::Counter* raw_rows_read = nullptr;
    RuntimeProfile::Counter* selected_rows = nullptr;
    RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
    RuntimeProfile::Counter* total_batches = nullptr;
    RuntimeProfile::Counter* empty_selection_batches = nullptr;
    RuntimeProfile::Counter* range_gap_skipped_rows = nullptr;
    RuntimeProfile::Counter* column_read_time = nullptr;
    RuntimeProfile::Counter* predicate_filter_time = nullptr;
};

Status plan_parquet_row_groups(const ::parquet::FileMetaData& metadata,
                               ::parquet::ParquetFileReader* file_reader,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan);

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows);

Status execute_batch_filters(const format::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection,
                             uint16_t* selected_rows);

class ParquetScanScheduler {
public:
    void set_plan(RowGroupScanPlan plan);
    void set_page_skip_profile(ParquetPageSkipProfile page_skip_profile) {
        _page_skip_profile = page_skip_profile;
    }
    void set_scan_profile(ParquetScanProfile scan_profile) { _scan_profile = scan_profile; }
    void reset();
    bool empty() const { return _row_group_plans.empty(); }

    Status read_next_batch(ParquetFileContext& file_context,
                           const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                           const format::FileScanRequest& request, Block* file_block, size_t* rows,
                           bool* eof);

private:
    void reset_current_row_group();

    Status open_next_row_group(ParquetFileContext& file_context,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request, bool* has_row_group);

    Status skip_current_row_group_rows(int64_t rows);

    Status read_filter_columns(int64_t batch_rows, const format::FileScanRequest& request,
                               Block* file_block, SelectionVector* selection,
                               uint16_t* selected_rows);

    Status read_current_row_group_batch(int64_t batch_rows, const format::FileScanRequest& request,
                                        Block* file_block, size_t* rows);

    std::vector<RowGroupReadPlan> _row_group_plans;
    size_t _next_row_group_plan_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> _current_row_group;
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>> _current_predicate_columns;
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>> _current_non_predicate_columns;
    int64_t _current_row_group_rows = 0;
    int64_t _current_row_group_rows_read = 0;
    int64_t _current_row_group_first_row = 0;
    std::vector<RowRange> _current_selected_ranges;
    size_t _current_range_idx = 0;
    int64_t _current_range_rows_read = 0;
    ParquetPageSkipProfile _page_skip_profile;
    ParquetScanProfile _scan_profile;
};

} // namespace doris::parquet
