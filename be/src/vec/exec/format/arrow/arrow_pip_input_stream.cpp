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

#include "arrow_pip_input_stream.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/buffered.h"
#include "arrow/io/stdio.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/logging.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {

ArrowPipInputStream::ArrowPipInputStream(io::FileReaderSPtr file_reader)
        : _file_reader(file_reader), _pos(0), _begin(true), _read_buf(new uint8_t[4]) {
    set_mode(arrow::io::FileMode::READ);
}

arrow::Status ArrowPipInputStream::Close() {
    return arrow::Status::OK();
}

bool ArrowPipInputStream::closed() const {
    return false;
}

arrow::Result<int64_t> ArrowPipInputStream::Tell() const {
    return _pos;
}

Status ArrowPipInputStream::HasNext(bool* get) {
    // 1. Arrow's serialization uses a 4-byte data to specify the length of the data that follows,
    //    so there must be 4-byte data here.
    // 2. If it is not determined whether there is a next batch of data (the data has already been transmitted),
    //    then the `_file_reader->read_at` will return a buff with a read length of 0,
    //    and the `RecordBatchStreamReader::Open` function will directly report an error when it gets this buff
    Slice file_slice(_read_buf, 4);
    size_t read_length = 0;
    RETURN_IF_ERROR(_file_reader->read_at(0, file_slice, &read_length, NULL));
    if (read_length == 0) {
        *get = false;
    } else {
        *get = true;
    }
    return Status::OK();
}

arrow::Result<int64_t> ArrowPipInputStream::Read(int64_t nbytes, void* out) {
    // RecordBatchStreamReader::Open will create a new reader that will stream a batch of arrow data.
    // But the first four bytes of this batch of data were taken by the HasNext function, so they need to be copied back here.
    uint8_t* out_ptr = (uint8_t*)out;
    if (_begin) {
        memmove(out_ptr, _read_buf, 4);
        out_ptr += 4;
        nbytes -= 4;
    }

    Slice file_slice(out_ptr, nbytes);
    size_t read_length = 0;
    Status status = _file_reader->read_at(0, file_slice, &read_length, NULL);
    if (UNLIKELY(!status.ok())) {
        return arrow::Status::IOError("Error to read data from pip");
    }

    if (_begin) {
        read_length += 4;
        _begin = false;
    }
    return (int64_t)read_length;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowPipInputStream::Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return buffer;
}

} // namespace doris::vectorized
