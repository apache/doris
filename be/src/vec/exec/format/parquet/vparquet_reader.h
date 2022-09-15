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

#include <stdint.h>

#include <queue>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/expr_context.h"
#include "gen_cpp/parquet_types.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vparquet_file_metadata.h"
#include "vparquet_group_reader.h"
#include "vparquet_page_index.h"

namespace doris::vectorized {

//    struct Statistics {
//        int32_t filtered_row_groups = 0;
//        int32_t total_groups = 0;
//        int64_t filtered_rows = 0;
//        int64_t total_rows = 0;
//        int64_t filtered_total_bytes = 0;
//        int64_t total_bytes = 0;
//    };
class RowGroupReader;
class PageIndex;

struct RowRange {
    int64_t first_row;
    int64_t last_row;
};

class ParquetReadColumn {
public:
    friend class ParquetReader;
    friend class RowGroupReader;
    ParquetReadColumn(int parquet_col_id, SlotDescriptor* slot_desc) : _parquet_col_id(parquet_col_id),
                                                                       _slot_desc(slot_desc) {};
    ~ParquetReadColumn() = default;

private:
    int _parquet_col_id;
    SlotDescriptor* _slot_desc;
    //    int64_t start_offset;
    //    int64_t chunk_size;
};

class ParquetReader {
public:
    ParquetReader(FileReader* file_reader, int32_t num_of_columns_from_file, size_t batch_size,
                  int64_t range_start_offset, int64_t range_size, cctz::time_zone* ctz);

    ~ParquetReader();

    Status init_reader(const TupleDescriptor* tuple_desc,
                       const std::vector<SlotDescriptor*>& tuple_slot_descs,
                       std::vector<ExprContext*>& conjunct_ctxs, const std::string& timezone);

    Status read_next_batch(Block* block, bool* eof);

    // std::shared_ptr<Statistics>& statistics() { return _statistics; }
    void close();

    int64_t size() const { return _file_reader->size(); }

private:
    bool _next_row_group_reader();
    Status _init_read_columns(const std::vector<SlotDescriptor*>& tuple_slot_descs);
    Status _init_row_group_readers(const std::vector<ExprContext*>& conjunct_ctxs);
    void _init_conjuncts(const std::vector<ExprContext*>& conjunct_ctxs);
    // Page Index Filter
    bool _has_page_index(std::vector<tparquet::ColumnChunk>& columns);
    Status _process_page_index(tparquet::RowGroup& row_group);

    // Row Group Filter
    bool _is_misaligned_range_group(const tparquet::RowGroup& row_group);
    Status _process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& column_meta,
                                       bool* filter_group);
    Status _process_row_group_filter(const tparquet::RowGroup& row_group, bool* filter_group);
    void _init_chunk_dicts();
    Status _process_dict_filter(bool* filter_group);
    void _init_bloom_filter();
    Status _process_bloom_filter(bool* filter_group);
    Status _filter_row_groups();
    int64_t _get_column_start_offset(const tparquet::ColumnMetaData& column_init_column_readers);
    bool _determine_filter_min_max(const std::vector<ExprContext*>& conjuncts,
                                   const std::string& encoded_min, const std::string& encoded_max);
    void _eval_binary_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                                bool& need_filter);
    void _eval_in_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                            bool& need_filter);

private:
    FileReader* _file_reader;
    std::shared_ptr<FileMetaData> _file_metadata;
    tparquet::FileMetaData* _t_metadata;
    std::unique_ptr<PageIndex> _page_index;
    std::list<std::shared_ptr<RowGroupReader>> _row_group_readers;
    std::shared_ptr<RowGroupReader> _current_group_reader;
    int32_t _total_groups; // num of groups(stripes) of a parquet(orc) file
    int32_t _current_row_group_id;
    //        std::shared_ptr<Statistics> _statistics;
    const int32_t _num_of_columns_from_file;
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::unordered_map<int, std::vector<ExprContext*>> _slot_conjuncts;
    std::vector<int> _include_column_ids; // columns that need to get from file
    std::vector<ParquetReadColumn> _read_columns;
    std::list<int32_t> _read_row_groups;
    // parquet file reader object
    size_t _batch_size;
    int64_t _range_start_offset;
    int64_t _range_size;
    cctz::time_zone* _ctz;
    std::vector<RowRange> _skipped_row_ranges;
    std::unordered_map<int, tparquet::OffsetIndex> _col_offsets;

    const TupleDescriptor* _tuple_desc; // get all slot info
};
} // namespace doris::vectorized
