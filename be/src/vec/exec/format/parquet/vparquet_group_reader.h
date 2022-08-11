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

#include "exprs/expr_context.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vparquet_column_reader.h"
#include "vparquet_file_metadata.h"
#include "vparquet_reader.h"

#define MAX_PARQUET_BLOCK_SIZE 1024

namespace doris::vectorized {
class ParquetReadColumn;
class ParquetColumnReader;
class RowGroupReader {
public:
    RowGroupReader(doris::FileReader* file_reader,
                   const std::shared_ptr<FileMetaData>& file_metadata,
                   const std::vector<ParquetReadColumn>& read_columns,
                   const std::map<std::string, int>& map_column,
                   const std::vector<ExprContext*>& conjunct_ctxs);
    ~RowGroupReader();
    Status init(const TupleDescriptor* tuple_desc, int64_t split_start_offset, int64_t split_size);
    Status get_next_row_group(const int32_t* group_id);
    Status fill_columns_data(Block* block, const int32_t group_id);

private:
    bool _is_misaligned_range_group(const tparquet::RowGroup& row_group);

    Status _process_column_stat_filter(const tparquet::RowGroup& row_group,
                                       const std::vector<ExprContext*>& conjunct_ctxs,
                                       bool* filter_group);

    void _init_conjuncts(const TupleDescriptor* tuple_desc,
                         const std::vector<ExprContext*>& conjunct_ctxs);

    Status _init_column_readers();

    Status _process_row_group_filter(const tparquet::RowGroup& row_group,
                                     const std::vector<ExprContext*>& conjunct_ctxs,
                                     bool* filter_group);

    void _init_chunk_dicts();

    Status _process_dict_filter(bool* filter_group);

    void _init_bloom_filter();

    Status _process_bloom_filter(bool* filter_group);

    int64_t _get_row_group_start_offset(const tparquet::RowGroup& row_group);
    int64_t _get_column_start_offset(const tparquet::ColumnMetaData& column_init_column_readers);

    bool _determine_filter_row_group(const std::vector<ExprContext*>& conjuncts,
                                     const std::string& encoded_min,
                                     const std::string& encoded_max);

    void _eval_binary_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                                bool& need_filter);

    void _eval_in_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                            bool& need_filter);

    bool _eval_in_val(PrimitiveType conjunct_type, std::vector<void*> in_pred_values,
                      const char* min_bytes, const char* max_bytes);

    bool _eval_eq(PrimitiveType conjunct_type, void* value, const char* min_bytes,
                  const char* max_bytes);

    bool _eval_gt(PrimitiveType conjunct_type, void* value, const char* max_bytes);

    bool _eval_ge(PrimitiveType conjunct_type, void* value, const char* max_bytes);

    bool _eval_lt(PrimitiveType conjunct_type, void* value, const char* min_bytes);

    bool _eval_le(PrimitiveType conjunct_type, void* value, const char* min_bytes);

private:
    doris::FileReader* _file_reader;
    const std::shared_ptr<FileMetaData>& _file_metadata;
    std::unordered_map<int32_t, ParquetColumnReader*> _column_readers;
    const TupleDescriptor* _tuple_desc; // get all slot info
    const std::vector<ParquetReadColumn>& _read_columns;
    const std::map<std::string, int>& _map_column;
    std::unordered_set<int> _parquet_column_ids;
    const std::vector<ExprContext*>& _conjunct_ctxs;
    std::unordered_map<int, std::vector<ExprContext*>> _slot_conjuncts;
    int64_t _split_start_offset;
    int64_t _split_size;
    int32_t _current_row_group;
    int32_t _filtered_num_row_groups = 0;
};
} // namespace doris::vectorized
