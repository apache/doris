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

class ParquetReader {
public:
    ParquetReader(FileReader* file_reader, int32_t num_of_columns_from_file,
                  int64_t range_start_offset, int64_t range_size);

    ~ParquetReader();

    Status init_reader(const TupleDescriptor* tuple_desc,
                       const std::vector<SlotDescriptor*>& tuple_slot_descs,
                       const std::vector<ExprContext*>& conjunct_ctxs, const std::string& timezone);

    Status read_next_batch(Block* block);

    bool has_next() const { return !_batch_eof; };

    //        std::shared_ptr<Statistics>& statistics() { return _statistics; }
    void close();

    int64_t size() const { return _file_reader->size(); }

private:
    Status _column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs);
    void _init_row_group_reader();
    void _fill_block_data(std::vector<tparquet::ColumnChunk> columns);
    bool _has_page_index(std::vector<tparquet::ColumnChunk> columns);
    Status _process_page_index(std::vector<tparquet::ColumnChunk> columns);

private:
    FileReader* _file_reader;
    std::shared_ptr<FileMetaData> _file_metadata;
    std::shared_ptr<RowGroupReader> _row_group_reader;
    std::shared_ptr<PageIndex> _page_index;
    int _total_groups; // num of groups(stripes) of a parquet(orc) file
    //    int _current_group;                     // current group(stripe)
    //        std::shared_ptr<Statistics> _statistics;
    const int32_t _num_of_columns_from_file;
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::vector<int> _include_column_ids;   // columns that need to get from file
    // parquet file reader object
    Block* _batch;
    bool* _batch_eof;
    //    int64_t _range_start_offset;
    //    int64_t _range_size;
};
} // namespace doris::vectorized
