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

#include "io/compress/block_compression_file_writer.h"

namespace doris::io {

// TODO: split thie function
Status BlockCompressionFileWriter::appendv(const Slice* data, size_t data_cnt) {
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

        size_t len = data[i].get_size();
        const char* buf = data[i].get_data();
        size_t limit_len = _compressor->get_bytes_read();
        if (len + limit_len > _max_input_size && limit_len > 0) {
            // Adding this segment would exceed the maximum size.
            // Flush data if we have it.
            RETURN_IF_ERROR(finish());
            RETURN_IF_ERROR(_compressor->reset());
        }

        if (len > _max_input_size) {
            // The data we're given exceeds the maximum size. Any data
            // we had have been flushed, so we write out this chunk in segments
            // not exceeding the maximum size until it is exhausted.
            RETURN_IF_ERROR(_write_length(len));
            do {
                size_t buf_len = std::min(len, _max_input_size);

                RETURN_IF_ERROR(_compressor->set_input(buf, buf_len));
                _compressor->finish();
                while (!_compressor->finished()) {
                    RETURN_IF_ERROR(_compress());
                }
                RETURN_IF_ERROR(_compressor->reset());
                buf += buf_len;
                len -= buf_len;
            } while (len > 0);
        } else {
            RETURN_IF_ERROR(_compressor->set_input(buf, len));
            if (!_compressor->need_input()) {
                // compressor buffer size might be smaller than the maximum
                // size, so we permit it to flush if required.
                RETURN_IF_ERROR(_write_length(_compressor->get_bytes_read()));
                do {
                    RETURN_IF_ERROR(_compress());
                } while (!_compressor->need_input());
            }
        }
    }
    return Status::OK();
}

Status BlockCompressionFileWriter::finish() {
    if (!_compressor->finished()) {
        RETURN_IF_ERROR(_write_length(_compressor->get_bytes_read()));
        _compressor->finish();
        while (!_compressor->finished()) {
            RETURN_IF_ERROR(_compress());
        }
    }
    return Status::OK();
}

Status BlockCompressionFileWriter::_compress() {
    size_t len;
    RETURN_IF_ERROR(_compressor->compress(_buffer, _buffer_size, len));
    if (len > 0) {
        // Write out the compressed data size.
        RETURN_IF_ERROR(_write_length(len));
        RETURN_IF_ERROR(_file_writer->append(Slice(_buffer, len)));
    }
    return Status::OK();
}

Status BlockCompressionFileWriter::_write_length(size_t len) {
    // To ensure endianness.
    char data[4];
    data[0] = (len >> 24) & 0xff;
    data[1] = (len >> 16) & 0xff;
    data[2] = (len >> 8) & 0xff;
    data[3] = len & 0xff;
    return _file_writer->append(Slice(data, sizeof(data)));
}

} // namespace doris::io
