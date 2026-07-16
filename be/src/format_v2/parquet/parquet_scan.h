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

#pragma once

#include <gen_cpp/parquet_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/selection_vector.h"
#include "runtime/runtime_profile.h"
#include "storage/segment/condition_cache.h"

namespace parquet {
class FileMetaData;
class ParquetFileReader;
class RowGroupMetaData;
class RowGroupReader;
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class Block;
class RuntimeState;

namespace format {
struct FileScanRequest;
} // namespace format
} // namespace doris

namespace doris::format::parquet {

struct ParquetFileContext;
struct ParquetColumnSchema;

// ============================================================================
// ============================================================================

struct ParquetScanRange {
    int64_t start_offset = 0;
    int64_t size = -1;      // -1 means read the whole file
    int64_t file_size = -1; // -1 means unknown
};

struct RowGroupReadPlan {
    int row_group_id = -1;                 // row group id
    int64_t first_file_row = 0;            // first file row for this row group (0-based)
    int64_t row_group_rows = 0;            // row count of this row group
    std::vector<RowRange> selected_ranges; // row ranges to read after page-index pruning
    std::map<int, ParquetPageSkipPlan>
            page_skip_plans; // leaf_column_id -> data pages that can be skipped completely
};

struct RowGroupScanPlan {
    std::vector<RowGroupReadPlan> row_groups; // row groups selected after pruning
    ParquetPruningStats pruning_stats;        // pruning statistics
};

// ============================================================================
// ============================================================================

Status plan_parquet_row_groups(const ::parquet::FileMetaData& metadata,
                               ::parquet::ParquetFileReader* file_reader,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan, const cctz::time_zone* timezone = nullptr,
                               const RuntimeState* runtime_state = nullptr);

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows);

uint16_t apply_compact_filter_to_selection(const IColumn::Filter& filter,
                                           SelectionVector* selection, uint16_t selected_rows);

Status execute_batch_filters(const format::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection, uint16_t* selected_rows,
                             int64_t* conjunct_filtered_rows = nullptr);

// ============================================================================
// ============================================================================
//   while true:
//     3. read_current_row_group_batch(batch_rows)
// ============================================================================
class ParquetScanScheduler {
public:
    static constexpr int64_t DEFAULT_READ_BATCH_SIZE = 4096;

    void set_plan(RowGroupScanPlan plan);
    void set_page_skip_profile(ParquetPageSkipProfile page_skip_profile) {
        _page_skip_profile = page_skip_profile;
    }
    void set_scan_profile(ParquetScanProfile scan_profile) { _scan_profile = scan_profile; }
    void set_merge_read_options(RuntimeProfile* profile, int64_t merge_read_slice_size) {
        _profile = profile;
        _merge_read_slice_size = merge_read_slice_size;
    }
    void set_global_rowid_context(std::optional<format::GlobalRowIdContext> context) {
        _global_rowid_context = context;
    }
    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx);
    void set_timezone(const cctz::time_zone* timezone) { _timezone = timezone; }
    void set_enable_strict_mode(bool enable_strict_mode) {
        _enable_strict_mode = enable_strict_mode;
    }
    void set_runtime_state(RuntimeState* runtime_state) { _runtime_state = runtime_state; }
    // Release row-group readers before the owning RuntimeProfile is reported. Native readers
    // publish their accumulated page/decode statistics from their destructor.
    void close() { reset_current_row_group(); }
    // Upper scanner owns adaptive memory feedback; scheduler only applies the current row cap when
    // splitting selected row ranges into physical read batches.
    void set_batch_size(size_t batch_size) {
        _batch_size = batch_size == 0 ? 1 : static_cast<int64_t>(batch_size);
    }
    void reset();
    bool empty() const { return _row_group_plans.empty(); }
    int64_t condition_cache_filtered_rows() const { return _condition_cache_filtered_rows; }
    int64_t predicate_filtered_rows() const { return _predicate_filtered_rows; }
    int64_t raw_rows_read() const { return _raw_rows_read; }

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
    Status flush_pending_non_predicate_skip_rows();

    Status read_filter_columns(int64_t batch_rows, const format::FileScanRequest& request,
                               Block* file_block, SelectionVector* selection,
                               uint16_t* selected_rows, int64_t* conjunct_filtered_rows,
                               bool* predicate_columns_filtered);

    Status prepare_current_dictionary_filters(
            ParquetFileContext& file_context,
            const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
            const format::FileScanRequest& request, int row_group_idx,
            const ::parquet::RowGroupMetaData& row_group_metadata);

    void prefetch_current_row_group_columns(
            ParquetFileContext& file_context,
            const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
            const std::vector<format::LocalColumnIndex>& scan_columns, bool* prefetched);

    Status read_current_row_group_batch(
            ParquetFileContext& file_context,
            const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
            int64_t batch_rows, const format::FileScanRequest& request,
            int64_t batch_first_file_row, Block* file_block, size_t* rows);

    void mark_condition_cache_granules(const SelectionVector& selection, uint16_t selected_rows,
                                       int64_t batch_first_file_row);

    std::vector<RowGroupReadPlan> _row_group_plans; // row group queue to scan
    size_t _next_row_group_plan_idx = 0;            // index of the next row group to process

    bool _has_current_row_group = false;
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>>
            _current_predicate_columns; // predicate ColumnReaders
    std::map<ColumnId, std::unique_ptr<ParquetColumnReader>>
            _current_non_predicate_columns; // non-predicate ColumnReaders
    std::map<ColumnId, IColumn::Filter>
            _current_dictionary_filters; // local id -> dict entry bitmap
    std::map<ColumnId, std::vector<std::pair<VExprContextSPtr, VExprSPtr>>>
            _current_dictionary_residual_conjuncts; // local id -> row-level residual conjuncts
    int64_t _current_row_group_rows = 0;            // current row group row count
    int _current_row_group_id = -1;                 // current row group id in parquet metadata
    int64_t _current_row_group_rows_read = 0;       // rows read in the current row group (cursor)
    int64_t _current_row_group_first_row = 0;       // first file row of the current row group
    std::vector<RowRange>
            _current_selected_ranges; // selected ranges for the current row group after page-index pruning
    std::unordered_map<int, tparquet::OffsetIndex> _current_offset_indexes;
    size_t _current_range_idx = 0;        // current selected_range index
    int64_t _current_range_rows_read = 0; // rows read in the current range
    // Predicate readers move immediately because they decide which rows survive. Non-predicate
    // readers can lag behind across fully filtered batches and range gaps; the lag is flushed once
    // before the next surviving batch is materialized, or discarded with the row group.
    int64_t _pending_non_predicate_skip_rows = 0;

    bool _current_predicate_prefetched = false;
    bool _current_non_predicate_prefetched = false;
    bool _current_merge_range_active = false;
    ParquetPageSkipProfile _page_skip_profile;
    ParquetScanProfile _scan_profile;
    RuntimeProfile* _profile = nullptr;
    int64_t _merge_read_slice_size = -1;
    std::optional<format::GlobalRowIdContext> _global_rowid_context;
    const cctz::time_zone* _timezone = nullptr;
    bool _enable_strict_mode = false;
    RuntimeState* _runtime_state = nullptr;
    int64_t _batch_size = DEFAULT_READ_BATCH_SIZE;
    std::shared_ptr<ConditionCacheContext> _condition_cache_ctx;
    int64_t _condition_cache_filtered_rows = 0;
    int64_t _predicate_filtered_rows = 0;
    int64_t _raw_rows_read = 0;
};

} // namespace doris::format::parquet
