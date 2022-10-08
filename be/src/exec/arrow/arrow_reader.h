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
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {

class ExecEnv;
class TBrokerRangeDesc;
class TNetworkAddress;
class RuntimeState;
class Tuple;
class SlotDescriptor;
class MemPool;
class FileReader;

struct Statistics {
    int32_t filtered_row_groups = 0;
    int32_t total_groups = 0;
    int64_t filtered_rows = 0;
    int64_t total_rows = 0;
    int64_t filtered_total_bytes = 0;
    int64_t total_bytes = 0;
};

class ArrowFile : public arrow::io::RandomAccessFile {
public:
    ArrowFile(FileReader* file);
    virtual ~ArrowFile();
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

// base of arrow reader
class ArrowReaderWrap : public vectorized::GenericReader {
public:
    ArrowReaderWrap(RuntimeState* state, const std::vector<SlotDescriptor*>& file_slot_descs,
                    FileReader* file_reader, int32_t num_of_columns_from_file, bool caseSensitive);
    virtual ~ArrowReaderWrap();

    virtual Status init_reader(const TupleDescriptor* tuple_desc,
                               const std::vector<ExprContext*>& conjunct_ctxs,
                               const std::string& timezone) = 0;
    // for row
    virtual Status read(Tuple* tuple, MemPool* mem_pool, bool* eof) {
        return Status::NotSupported("Not Implemented read");
    }
    // for vec
    Status get_next_block(vectorized::Block* block, size_t* read_row, bool* eof) override;
    // This method should be deprecated once the old scanner is removed.
    // And user should use "get_next_block" instead.
    Status next_batch(std::shared_ptr<arrow::RecordBatch>* batch, bool* eof);

    std::shared_ptr<Statistics>& statistics() { return _statistics; }
    void close();
    virtual Status size(int64_t* size) { return Status::NotSupported("Not Implemented size"); }
    int get_column_index(std::string column_name);

    void prefetch_batch();
    bool is_case_sensitive() { return _case_sensitive; }

protected:
    virtual Status column_indices();
    virtual void read_batches(arrow::RecordBatchVector& batches, int current_group) = 0;
    virtual bool filter_row_group(int current_group) = 0;

protected:
    RuntimeState* _state;
    std::vector<SlotDescriptor*> _file_slot_descs;

    const int32_t _num_of_columns_from_file;
    std::shared_ptr<ArrowFile> _arrow_file;
    std::shared_ptr<::arrow::RecordBatchReader> _rb_reader;
    int _total_groups;                      // num of groups(stripes) of a parquet(orc) file
    int _current_group;                     // current group(stripe)
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::vector<int> _include_column_ids;   // columns that need to get from file
    std::shared_ptr<Statistics> _statistics;

    std::atomic<bool> _closed = false;
    std::atomic<bool> _batch_eof = false;
    arrow::Status _status;
    std::mutex _mtx;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::list<std::shared_ptr<arrow::RecordBatch>> _queue;
    const size_t _max_queue_size = config::parquet_reader_max_buffer_size;
    std::thread _thread;
    bool _case_sensitive;

    // The following fields are only valid when using "get_block()" interface.
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx = 0;
    // Save col names which need to be read but does not exist in file
    std::vector<std::string> _missing_cols;
};

} // namespace doris
