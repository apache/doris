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

#include <gen_cpp/Types_types.h>

#include "io/fs/file_reader.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/iterators.h"

namespace doris {

NewPlainBinaryLineReader::NewPlainBinaryLineReader(io::FileReaderSPtr file_reader,
                                                   TFileType::type file_type)
        : _file_reader(file_reader), _file_type(file_type) {}

NewPlainBinaryLineReader::~NewPlainBinaryLineReader() {
    close();
}

void NewPlainBinaryLineReader::close() {}

Status NewPlainBinaryLineReader::read_line(const uint8_t** ptr, size_t* size, bool* eof) {
    std::unique_ptr<uint8_t[]> file_buf;
    size_t read_size = 0;
    IOContext io_ctx;
    io_ctx.reader_type = READER_QUERY;
    switch (_file_type) {
    case TFileType::FILE_LOCAL:
    case TFileType::FILE_HDFS:
    case TFileType::FILE_S3: {
        size_t file_size = _file_reader->size();
        file_buf.reset(new uint8_t[file_size]);
        Slice result(file_buf.get(), file_size);
        RETURN_IF_ERROR(_file_reader->read_at(0, result, io_ctx, &read_size));
        break;
    }
    case TFileType::FILE_STREAM: {
        RETURN_IF_ERROR((dynamic_cast<io::StreamLoadPipe*>(_file_reader.get()))
                                ->read_one_message(&file_buf, &read_size));

        break;
    }
    default: {
        return Status::NotSupported("no supported file reader type: {}", _file_type);
    }
    }
    *ptr = file_buf.release();
    *size = read_size;
    if (read_size == 0) {
        *eof = true;
    }
    return Status::OK();
}

} // namespace doris
