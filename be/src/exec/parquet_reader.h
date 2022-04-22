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

#include "common/status.h"
#include "common/config.h"
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

class ParquetFile : public arrow::io::RandomAccessFile {
public:
    ParquetFile(FileReader* file);
    ~ParquetFile() override;
    arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override;

private:
    FileReader* _file;
    int64_t _pos = 0;
};

// Reader of broker parquet file
class ParquetReaderWrap {
public:
    ParquetReaderWrap(FileReader* file_reader, int32_t num_of_columns_from_file);
    virtual ~ParquetReaderWrap();

    // Read
    Status read(Tuple* tuple, const std::vector<SlotDescriptor*>& tuple_slot_descs,
                MemPool* mem_pool, bool* eof);
    void close();
    Status size(int64_t* size);
    Status init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                               const std::string& timezone);

private:
    void fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value,
                   int32_t len);
    Status column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs);
    Status set_field_null(Tuple* tuple, const SlotDescriptor* slot_desc);
    Status read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof);
    Status handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array, uint8_t* buf,
                            int32_t* wbtyes);

private:
    void prefetch_batch();
    Status read_next_batch();

private:
    const int32_t _num_of_columns_from_file;
    std::shared_ptr<ParquetFile> _parquet;

    // parquet file reader object
    std::unique_ptr<::arrow::RecordBatchReader> _rb_batch;
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::unique_ptr<parquet::arrow::FileReader> _reader;
    std::shared_ptr<parquet::FileMetaData> _file_metadata;
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::vector<int> _parquet_column_ids;
    std::vector<arrow::Type::type> _parquet_column_type;
    int _total_groups; // groups in a parquet file
    int _current_group;

    int _rows_of_group; // rows in a group.
    int _current_line_of_group;
    int _current_line_of_batch;

    std::string _timezone;

private:
    std::atomic<bool> _closed = false;
    arrow::Status _status;
    std::mutex _mtx;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::list<std::shared_ptr<arrow::RecordBatch>> _queue;
    const size_t _max_queue_size = config::parquet_reader_max_buffer_size;
};

} // namespace doris
