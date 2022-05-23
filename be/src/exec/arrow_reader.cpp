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
#include "exec/arrow_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "exec/file_reader.h"
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

// Broker

ArrowReaderWrap::ArrowReaderWrap(FileReader* file_reader, int64_t batch_size,
                                 int32_t num_of_columns_from_file)
        : _batch_size(batch_size), _num_of_columns_from_file(num_of_columns_from_file) {
    _arrow_file = std::shared_ptr<ArrowFile>(new ArrowFile(file_reader));
}

ArrowReaderWrap::~ArrowReaderWrap() {
    close();
}

void ArrowReaderWrap::close() {
    arrow::Status st = _arrow_file->Close();
    if (!st.ok()) {
        LOG(WARNING) << "close file error: " << st.ToString();
    }
}

ArrowFile::ArrowFile(FileReader* file) : _file(file) {}

ArrowFile::~ArrowFile() {
    arrow::Status st = Close();
    if (!st.ok()) {
        LOG(WARNING) << "close file error: " << st.ToString();
    }
}

arrow::Status ArrowFile::Close() {
    if (_file != nullptr) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
    return arrow::Status::OK();
}

bool ArrowFile::closed() const {
    if (_file != nullptr) {
        return _file->closed();
    } else {
        return true;
    }
}

arrow::Result<int64_t> ArrowFile::Read(int64_t nbytes, void* buffer) {
    return ReadAt(_pos, nbytes, buffer);
}

arrow::Result<int64_t> ArrowFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
    int64_t reads = 0;
    int64_t bytes_read = 0;
    _pos = position;
    while (nbytes > 0) {
        Status result = _file->readat(_pos, nbytes, &reads, out);
        if (!result.ok()) {
            bytes_read = 0;
            return arrow::Status::IOError("Readat failed.");
        }
        if (reads == 0) {
            break;
        }
        bytes_read += reads; // total read bytes
        nbytes -= reads;     // remained bytes
        _pos += reads;
        out = (char*)out + reads;
    }
    return bytes_read;
}

arrow::Result<int64_t> ArrowFile::GetSize() {
    return _file->size();
}

arrow::Status ArrowFile::Seek(int64_t position) {
    _pos = position;
    // NOTE: Only readat operation is used, so _file seek is not called here.
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowFile::Tell() const {
    return _pos;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowFile::Read(int64_t nbytes) {
    auto buffer = arrow::AllocateBuffer(nbytes, arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(buffer);
    std::shared_ptr<arrow::Buffer> read_buf = std::move(buffer.ValueOrDie());
    auto bytes_read = ReadAt(_pos, nbytes, read_buf->mutable_data());
    ARROW_RETURN_NOT_OK(bytes_read);
    // If bytes_read is equal with read_buf's capacity, we just assign
    if (bytes_read.ValueOrDie() == nbytes) {
        return std::move(read_buf);
    } else {
        return arrow::SliceBuffer(read_buf, 0, bytes_read.ValueOrDie());
    }
}

} // namespace doris
