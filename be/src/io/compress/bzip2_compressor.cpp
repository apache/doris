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

#include "bzip2_compressor.h"

#include "common/logging.h"
#include "common/status.h"

namespace doris::io {
Status Bzip2Compressor::init() {
    bzero(&_stream, sizeof(_stream));
    int ret = BZ2_bzCompressInit(&_stream, _block_size, 0, _work_factor);
    if (ret != BZ_OK) {
        return Status::InternalError("BZ2_bzCompressInit failed, error: {}", ret);
    }

    _uncompressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
    _compressed_buffer = ByteBuffer::allocate(_direct_bufffer_size);
    // the compressed_buffer should be empty when inited
    _compressed_buffer->pos = _direct_bufffer_size;
    return Status::OK();
}

Status Bzip2Compressor::set_input(const char* data, size_t length) {
    _input_buffer = data;
    _input_buffer_length = length;
    // before calling set_input, will first check need_input,
    // so the unconpressed_buffer and compressed_buffer is clear now
    DCHECK(_uncompressed_buffer_offset == 0);
    DCHECK(_uncompressed_buffer_length == 0);
    DCHECK(!_compressed_buffer->has_remaining());
    _copy_from_input_buffer();
    return Status::OK();
}

void Bzip2Compressor::_copy_from_input_buffer() {
    size_t len = std::min(_input_buffer_length, _uncompressed_buffer->remaining());
    _uncompressed_buffer->put_bytes(_input_buffer, len);
    _uncompressed_buffer_length += len;
    _input_buffer += len;
    _input_buffer_length -= len;
}

bool Bzip2Compressor::need_input() {
    // is there compressed data left?
    if (_compressed_buffer->has_remaining()) {
        return false;
    }

    // is there _uncompressed data left?
    if (_uncompressed_buffer_length > 0) {
        return false;
    }

    // can copy more data to uncompressed_buffer
    if (_uncompressed_buffer->remaining() > 0) {
        // is there input data left?
        if (_input_buffer_length <= 0) {
            return true;
        } else {
            _copy_from_input_buffer();
            return _uncompressed_buffer->has_remaining();
        }
    }
    return false;
}
Status Bzip2Compressor::compress(char* buffer, size_t length, size_t& compressed_length) {
    // 1. try to return the left data in compressed_buffer
    compressed_length = _compressed_buffer->remaining();
    if (compressed_length > 0) {
        compressed_length = std::min(length, compressed_length);
        _compressed_buffer->get_bytes(buffer, compressed_length);
        return Status::OK();
    }

    // 2. try to compress data from uncompressed_buffer to compressed_buffer
    RETURN_IF_ERROR(_compress_buffer());

    if (_uncompressed_buffer_length <= 0) {
        // zlib consumed all input buffer, reset uncompressed_buffer
        _uncompressed_buffer->pos = 0;
        _uncompressed_buffer->limit = _direct_bufffer_size;
        _uncompressed_buffer_offset = 0;
    }

    // 3. return the data of compressed_buffer
    compressed_length = std::min(length, _compressed_buffer->remaining());
    _compressed_buffer->get_bytes(buffer, compressed_length);
    return Status::OK();
}

Status Bzip2Compressor::_compress_buffer() {
    _stream.next_in = _uncompressed_buffer->ptr + _uncompressed_buffer_offset;
    _stream.avail_in = _uncompressed_buffer_length;
    _stream.next_out = _compressed_buffer->ptr;
    _stream.avail_out = _compressed_buffer->capacity;

    _compressed_buffer->pos = 0;

    int ret = BZ2_bzCompress(&_stream, _finish ? BZ_FINISH : BZ_RUN);
    switch (ret) {
    case BZ_STREAM_END:
        _finished = true;
        break;
    case BZ_RUN_OK:
    case BZ_FINISH_OK:
        // update uncompressed_buffer and compressed_buffer info
        _uncompressed_buffer_offset += _uncompressed_buffer_length - _stream.avail_in;
        _uncompressed_buffer_length = _stream.avail_in;
        _compressed_buffer->limit = _compressed_buffer->capacity - _stream.avail_out;
        break;
    default:
        return Status::InternalError("BZ2_bzCompress failed, error: {}", ret);
    }
    return Status::OK();
}

size_t Bzip2Compressor::get_bytes_read() {
    return (size_t)_stream.total_in_hi32 << 32 | _stream.total_in_lo32;
}
size_t Bzip2Compressor::get_bytes_written() {
    return (size_t)_stream.total_out_hi32 << 32 | _stream.total_out_lo32;
}
void Bzip2Compressor::finish() {
    _finished = true;
}
bool Bzip2Compressor::finished() {
    return _finished && !_compressed_buffer->has_remaining();
}
Status Bzip2Compressor::reset() {
    int ret = BZ2_bzCompressEnd(&_stream);
    if (ret != BZ_OK) {
        return Status::InternalError("BZ2_bzDecompressEnd failed, error: {}", ret);
    }
    ret = BZ2_bzCompressInit(&_stream, _block_size, _work_factor, 0);
    if (ret != BZ_OK) {
        return Status::InternalError("BZ2_bzCompressInit failed, error: {}", ret);
    }
    _uncompressed_buffer->pos = 0;
    _uncompressed_buffer->limit = _direct_bufffer_size;
    _uncompressed_buffer_offset = 0;
    _uncompressed_buffer_length = 0;
    _compressed_buffer->pos = _direct_bufffer_size;
    _compressed_buffer->limit = _direct_bufffer_size;
    _finish = false;
    _finished = false;
    return Status::OK();
}

}; // namespace doris::io
