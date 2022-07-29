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

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/parquet_types.h"
#include "io/file_reader.h"
#include "vec/core/block.h"
#include "vparquet_file_metadata.h"

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
    ParquetReader(FileReader* file_reader, int64_t batch_size, int32_t num_of_columns_from_file,
                  int64_t range_start_offset, int64_t range_size);
    ~ParquetReader();
    virtual Status init_reader(const TupleDescriptor* tuple_desc,
                               const std::vector<SlotDescriptor*>& tuple_slot_descs,
                               const std::vector<ExprContext*>& conjunct_ctxs,
                               const std::string& timezone) = 0;
    virtual Status next_batch(bool* eof) = 0;
    //        std::shared_ptr<Statistics>& statistics() { return _statistics; }
    void close() {};
    int64_t size(int64_t* size) { return _file_reader->size(); }

private:
    int64_t _get_row_group_start_offset(const tparquet::RowGroup& row_group);

private:
    FileReader* _file_reader;
    std::shared_ptr<FileMetaData> _file_metadata;
    //    const int64_t _batch_size;
    //    const int32_t _num_of_columns_from_file;
    int _total_groups; // num of groups(stripes) of a parquet(orc) file
    //    int _current_group;                     // current group(stripe)
    //    std::map<std::string, int> _map_column; // column-name <---> column-index
    //    std::vector<int> _include_column_ids;   // columns that need to get from file
    //        std::shared_ptr<Statistics> _statistics;

    // parquet file reader object
    //    std::vector<Block*> _batch;
    //    std::string _timezone;
    //    int64_t _range_start_offset;
    //    int64_t _range_size;

private:
    std::atomic<bool> _closed = false;
};

} // namespace doris::vectorized
