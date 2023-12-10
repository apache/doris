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

#include "new_plain_binary_line_reader.h"

#include <gen_cpp/internal_service.pb.h>

#include "io/fs/file_reader.h"
#include "io/fs/stream_load_pipe.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io

NewPlainBinaryLineReader::NewPlainBinaryLineReader(io::FileReaderSPtr file_reader)
        : _file_reader(file_reader) {}

NewPlainBinaryLineReader::~NewPlainBinaryLineReader() {
    close();
}

void NewPlainBinaryLineReader::close() {}

Status NewPlainBinaryLineReader::read_line(const uint8_t** ptr, size_t* size, bool* eof,
                                           const io::IOContext* /*io_ctx*/) {
    size_t read_size = 0;
    RETURN_IF_ERROR((dynamic_cast<io::StreamLoadPipe*>(_file_reader.get()))
                            ->read_one_message(&_file_buf, &read_size));
    *ptr = _file_buf.get();
    *size = read_size;
    if (read_size == 0) {
        *eof = true;
    } else {
        _cur_row.reset(*reinterpret_cast<PDataRow**>(_file_buf.get()));
    }
    return Status::OK();
}

} // namespace doris
