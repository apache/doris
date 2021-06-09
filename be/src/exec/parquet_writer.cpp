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

#include "exec/parquet_writer.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "exec/file_writer.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "util/thrift_util.h"

namespace doris {

/// ParquetOutputStream
ParquetOutputStream::ParquetOutputStream(FileWriter* file_writer) : _file_writer(file_writer) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    Close();
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    size_t written_len = 0;
    Status st = _file_writer->write(reinterpret_cast<const uint8_t*>(data), nbytes, &written_len);
    if (!st.ok()) {
        return arrow::Status::IOError(st.get_error_msg());
    }
    _cur_pos += written_len;
    return arrow::Status::OK();
}

arrow::Status ParquetOutputStream::Tell(int64_t* position) const {
    *position = _cur_pos;
    return arrow::Status::OK();
}

arrow::Status ParquetOutputStream::Close() {
    Status st = _file_writer->close();
    if (!st.ok()) {
        return arrow::Status::IOError(st.get_error_msg());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

/// ParquetWriterWrapper
ParquetWriterWrapper::ParquetWriterWrapper(FileWriter* file_writer,
                                           const std::vector<ExprContext*>& output_expr_ctxs)
        : _output_expr_ctxs(output_expr_ctxs) {
    // TODO(cmy): implement
    _outstream = new ParquetOutputStream(file_writer);
}

Status ParquetWriterWrapper::write(const RowBatch& row_batch) {
    // TODO(cmy): implement
    return Status::OK();
}

void ParquetWriterWrapper::close() {
    // TODO(cmy): implement
}

ParquetWriterWrapper::~ParquetWriterWrapper() {
    close();
}

} // namespace doris
