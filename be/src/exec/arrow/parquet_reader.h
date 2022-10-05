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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/status.h"
#include "exec/arrow/arrow_reader.h"
#include "exec/arrow/parquet_row_group_reader.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

class ExecEnv;
class TBrokerRangeDesc;
class TNetworkAddress;
class RuntimeState;
class Tuple;
class SlotDescriptor;
class MemPool;
class FileReader;
class RowGroupReader;

// Reader of parquet file
class ParquetReaderWrap final : public ArrowReaderWrap {
public:
    // batch_size is not use here
    ParquetReaderWrap(RuntimeState* state, const std::vector<SlotDescriptor*>& file_slot_descs,
                      FileReader* file_reader, int32_t num_of_columns_from_file,
                      int64_t range_start_offset, int64_t range_size, bool case_sensitive = true);
    ~ParquetReaderWrap() override = default;

    // Read
    Status read(Tuple* tuple, MemPool* mem_pool, bool* eof) override;
    Status size(int64_t* size) override;
    Status init_reader(const TupleDescriptor* tuple_desc,
                       const std::vector<ExprContext*>& conjunct_ctxs,
                       const std::string& timezone) override;
    Status init_parquet_type();

private:
    void fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value,
                   int32_t len);
    Status set_field_null(Tuple* tuple, const SlotDescriptor* slot_desc);
    Status read_record_batch(bool* eof);
    Status handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array, uint8_t* buf,
                            int32_t* wbtyes);

private:
    Status read_next_batch();
    void read_batches(arrow::RecordBatchVector& batches, int current_group) override;
    bool filter_row_group(int current_group) override;

private:
    // parquet file reader object
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::unique_ptr<parquet::arrow::FileReader> _reader;
    std::shared_ptr<parquet::FileMetaData> _file_metadata;
    std::vector<arrow::Type::type> _parquet_column_type;

    int _rows_of_group; // rows in a group.
    int _current_line_of_group;
    int _current_line_of_batch;
    std::string _timezone;
    int64_t _range_start_offset;
    int64_t _range_size;
    bool _need_filter_row_group = false;

private:
    std::unique_ptr<doris::RowGroupReader> _row_group_reader;
};

} // namespace doris
