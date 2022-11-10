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
#include <common/status.h>

#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vparquet_column_reader.h"

namespace doris::vectorized {

class RowGroupReader {
public:
    struct LazyReadContext {
        VExprContext* vconjunct_ctx = nullptr;
        bool can_lazy_read = false;
        bool resize_first_column = true;
        std::vector<std::string> all_read_columns;
        std::vector<std::string> predicate_columns;
        std::vector<uint32_t> predicate_col_ids;
        std::vector<std::string> lazy_read_columns;
    };

    RowGroupReader(doris::FileReader* file_reader,
                   const std::vector<ParquetReadColumn>& read_columns, const int32_t row_group_id,
                   const tparquet::RowGroup& row_group, cctz::time_zone* ctz,
                   const LazyReadContext& lazy_read_ctx);

    ~RowGroupReader();
    Status init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                std::unordered_map<int, tparquet::OffsetIndex>& col_offsets);
    Status next_batch(Block* block, size_t batch_size, size_t* read_rows, bool* _batch_eof);
    int64_t lazy_read_filtered_rows() { return _lazy_read_filtered_rows; }

    ParquetColumnReader::Statistics statistics();

private:
    Status _read_empty_batch(size_t batch_size, size_t* read_rows, bool* _batch_eof);
    Status _read_column_data(Block* block, const std::vector<std::string>& columns,
                             size_t batch_size, size_t* read_rows, bool* _batch_eof,
                             ColumnSelectVector& select_vector);
    Status _do_lazy_read(Block* block, size_t batch_size, size_t* read_rows, bool* batch_eof);
    const uint8_t* _build_filter_map(ColumnPtr& sv, size_t num_rows, bool* can_filter_all);
    void _rebuild_select_vector(ColumnSelectVector& select_vector,
                                std::unique_ptr<uint8_t[]>& filter_map, size_t pre_read_rows);

    doris::FileReader* _file_reader;
    std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> _column_readers;
    const std::vector<ParquetReadColumn>& _read_columns;
    const int32_t _row_group_id;
    const tparquet::RowGroup& _row_group_meta;
    int64_t _remaining_rows;
    int64_t _read_rows = 0;
    cctz::time_zone* _ctz;

    VExprContext* _vconjunct_ctx;
    bool _can_lazy_read;
    // block->rows() returns the number of rows of the first column,
    // so we should check and resize the first column
    const bool _resize_first_column;
    const std::vector<std::string>& _all_read_columns;
    const std::vector<std::string>& _predicate_columns;
    const std::vector<uint32_t>& _predicate_col_ids;
    const std::vector<std::string>& _lazy_read_columns;
    int64_t _lazy_read_filtered_rows = 0;
    // If continuous batches are skipped, we can cache them to skip a whole page
    size_t _cached_filtered_rows = 0;
};
} // namespace doris::vectorized
