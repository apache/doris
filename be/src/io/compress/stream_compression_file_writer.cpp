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

#include "io/compress/stream_compression_file_writer.h"
#include "io/compress/compression_file_writer.h"

namespace doris::io {

Status StreamCompressionFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_compressor->finished()) {
        return Status::InternalError("write beyond end of stream.");
    }

    for (size_t i = 0; i < data_cnt; ++i) {
        if (data[i].get_data() == nullptr) {
            return Status::InternalError("null data.");
        } else if (data[i].size < 0) {
            return Status::InternalError("negative data size.");
        } else if (data[i].size == 0) {
            break;
        }

        RETURN_IF_ERROR(_compressor->set_input(data[i].get_data(), data[i].size));
        while (!_compressor->need_input()) {
            RETURN_IF_ERROR(_compress());
        }
    }

    return Status::OK();
}

Status StreamCompressionFileWriter::init() {
    RETURN_IF_ERROR(CompressionFileWriter::init());
    if (_buffer_size <= 0) {
        return Status::InternalError("buffer size is invalid.");
    }
    _buffer = new char[_buffer_size];
    return Status::OK();
}

Status StreamCompressionFileWriter::finish() {
    if (!_compressor->finished()) {
        _compressor->finish();
        while (!_compressor->finished()) {
            RETURN_IF_ERROR(_compress());
        }
    }
    return Status::OK();
}

Status StreamCompressionFileWriter::_compress() {
    size_t len;
    RETURN_IF_ERROR(_compressor->compress(_buffer, _buffer_size, len));
    if (len > 0) {
        RETURN_IF_ERROR(_file_writer->append(Slice(_buffer, len)));
    }
    return Status::OK();
}

} // namespace doris::io
