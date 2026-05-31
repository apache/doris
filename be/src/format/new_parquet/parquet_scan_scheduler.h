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

#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "format/new_parquet/column_reader.h"
#include "format/new_parquet/parquet_scan_planner.h"
#include "format/new_parquet/selection_vector.h"

namespace parquet {
class RowGroupReader;
} // namespace parquet

namespace doris {
class Block;

namespace reader {
struct FileScanRequest;
} // namespace reader
} // namespace doris

namespace doris::parquet {

struct ParquetFileContext;
struct ParquetColumnSchema;

class ParquetScanScheduler {
public:
    void set_plan(RowGroupScanPlan plan);
    void reset();
    bool empty() const { return _selected_row_groups.empty(); }

    Status read_next_batch(ParquetFileContext& file_context,
                           const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                           const reader::FileScanRequest& request, Block* file_block, size_t* rows,
                           bool* eof);

private:
    void reset_current_row_group();

    Status open_next_row_group(ParquetFileContext& file_context,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const reader::FileScanRequest& request, bool* has_row_group);

    Status read_filter_columns(int64_t batch_rows, const reader::FileScanRequest& request,
                               Block* file_block, SelectionVector* selection,
                               uint16_t* selected_rows);

    Status read_current_row_group_batch(int64_t batch_rows, const reader::FileScanRequest& request,
                                        Block* file_block, size_t* rows);

    std::vector<int> _selected_row_groups;
    std::vector<int64_t> _row_group_first_rows;
    size_t _next_row_group_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> _current_row_group;
    std::vector<std::unique_ptr<ParquetColumnReader>> _current_predicate_columns;
    std::vector<std::unique_ptr<ParquetColumnReader>> _current_non_predicate_columns;
    int64_t _current_row_group_rows = 0;
    int64_t _current_row_group_rows_read = 0;
    int64_t _current_row_group_first_row = 0;
};

} // namespace doris::parquet
