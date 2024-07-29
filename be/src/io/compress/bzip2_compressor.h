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

#include "bzlib.h"
#include "io/compress/compressor.h"
#include "util/byte_buffer.h"
namespace doris::io {

class Bzip2Compressor : public Compressor {
public:
    Bzip2Compressor(int block_size, int work_factor, size_t direct_bufffer_size)
            : _block_size(block_size),
              _work_factor(work_factor),
              _direct_bufffer_size(direct_bufffer_size) {}

    Bzip2Compressor() : _block_size(9), _work_factor(30), _direct_bufffer_size(64 * 1024) {}

    // TODO: clean the resource
    ~Bzip2Compressor() override = default;
    Status init() override;
    Status set_input(const char* data, size_t length) override;
    bool need_input() override;
    Status compress(char* buffer, size_t length, size_t& compressed_length) override;
    size_t get_bytes_read() override;
    size_t get_bytes_written() override;
    void finish() override;
    bool finished() override;
    Status reset() override;

private:
    // try to copy data from input_buffer to uncompressed_buffer as much as possible
    void _copy_from_input_buffer();
    // try to compress uncompressed_buffer as much as possible
    Status _compress_buffer();
    // bzlib struct for compressing data
    bz_stream _stream;
    const int _block_size;
    const int _work_factor;
    // the max size of _uncompressed_buffer and _compressed_buffer
    const size_t _direct_bufffer_size;
    // input data
    const char* _input_buffer = nullptr;
    size_t _input_buffer_length = 0;
    ByteBufferPtr _uncompressed_buffer = nullptr;
    // TODO: introducing a new buffer abstraction to remove offset and length
    size_t _uncompressed_buffer_offset = 0;
    size_t _uncompressed_buffer_length = 0;
    ByteBufferPtr _compressed_buffer = nullptr;
    // if finish is true, force to flush data when deflating z_stream
    bool _finish = false;
    bool _finished = false;
};
} // namespace doris::io
