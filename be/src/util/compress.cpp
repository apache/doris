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

#include "util/compress.h"
#include "exec/read_write_util.h"
#include "runtime/runtime_state.h"

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy/snappy.h>

namespace doris {

GzipCompressor::GzipCompressor(Format format, MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer),
        _format(format) {
    bzero(&_stream, sizeof(_stream));
    init();
}

GzipCompressor::~GzipCompressor() {
    (void)deflateEnd(&_stream);
}

Status GzipCompressor::init() {
    int ret = 0;
    // Initialize to run specified format
    int window_bits = WINDOW_BITS;

    if (_format == DEFLATE) {
        window_bits = -window_bits;
    } else if (_format == GZIP) {
        window_bits += GZIP_CODEC;
    }

    if ((ret = deflateInit2(&_stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                            window_bits, 9, Z_DEFAULT_STRATEGY)) != Z_OK) {
        return Status::InternalError("zlib deflateInit failed: " +  std::string(_stream.msg));
    }

    return Status::OK();
}

int GzipCompressor::max_compressed_len(int input_length) {
    return deflateBound(&_stream, input_length);
}

Status GzipCompressor::compress(
        int input_length,
        uint8_t* input,
        int* output_length,
        uint8_t* output) {
    DCHECK_GE(*output_length, max_compressed_len(input_length));
    _stream.next_in = reinterpret_cast<Bytef*>(input);
    _stream.avail_in = input_length;
    _stream.next_out = reinterpret_cast<Bytef*>(output);
    _stream.avail_out = *output_length;

    int ret = 0;

    if ((ret = deflate(&_stream, Z_FINISH)) != Z_STREAM_END) {
        std::stringstream ss;
        ss << "zlib deflate failed: " << _stream.msg;
        return Status::InternalError(ss.str());
    }

    *output_length = *output_length - _stream.avail_out;

    if (deflateReset(&_stream) != Z_OK) {
        return Status::InternalError("zlib deflateReset failed: " + std::string(_stream.msg));
    }

    return Status::OK();
}

Status GzipCompressor::process_block(
        int input_length,
        uint8_t* input,
        int* output_length,
        uint8_t** output) {
    // If length is set then the output has been allocated.
    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else {
        int len = max_compressed_len(input_length);

        if (!_reuse_buffer || _buffer_length < len || _out_buffer == NULL) {
            DCHECK(_memory_pool != NULL) << "Can't allocate without passing in a mem pool";
            _buffer_length = len;
            _out_buffer = _memory_pool->allocate(_buffer_length);
            *output_length = _buffer_length;
        }
    }

    RETURN_IF_ERROR(compress(input_length, input, output_length, _out_buffer));
    *output = _out_buffer;
    return Status::OK();
}

BzipCompressor::BzipCompressor(MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer) {
}

Status BzipCompressor::process_block(
        int input_length,
        uint8_t* input,
        int* output_length,
        uint8_t** output) {
    // If length is set then the output has been allocated.
    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else if (!_reuse_buffer || _out_buffer == NULL) {
        // guess that we will need no more the input length.
        _buffer_length = input_length;
        _out_buffer = _temp_memory_pool.allocate(_buffer_length);
    }

    unsigned int outlen = 0;
    int ret = BZ_OUTBUFF_FULL;

    while (ret == BZ_OUTBUFF_FULL) {
        if (_out_buffer == NULL) {
            DCHECK_EQ(*output_length, 0);
            _temp_memory_pool.clear();
            _buffer_length = _buffer_length * 2;
            _out_buffer = _temp_memory_pool.allocate(_buffer_length);
        }

        outlen = static_cast<unsigned int>(_buffer_length);

        if ((ret = BZ2_bzBuffToBuffCompress(
                reinterpret_cast<char*>(_out_buffer), &outlen,
                reinterpret_cast<char*>(input),
                static_cast<unsigned int>(input_length), 5, 2, 0)) == BZ_OUTBUFF_FULL) {
            // If the output_length was passed we must have enough room.
            DCHECK_EQ(*output_length, 0);

            if (*output_length != 0) {
                return Status::InternalError("Too small buffer passed to BzipCompressor");
            }

            _out_buffer = NULL;
        }
    }

    if (ret !=  BZ_OK) {
        std::stringstream ss;
        ss << "bzlib BZ2_bzBuffToBuffCompressor failed: " << ret;
        return Status::InternalError(ss.str());

    }

    *output = _out_buffer;
    *output_length = outlen;
    _memory_pool->acquire_data(&_temp_memory_pool, false);
    return Status::OK();
}

// Currently this is only use for testing of the decompressor.
SnappyBlockCompressor::SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer) {
}

Status SnappyBlockCompressor::process_block(
        int input_length,
        uint8_t* input,
        int* output_length,
        uint8_t** output) {

    // Hadoop uses a block compression scheme on top of snappy.  First there is
    // an integer which is the size of the decompressed data followed by a
    // sequence of compressed blocks each preceded with an integer size.
    // For testing purposes we are going to generate two blocks.
    int block_size = input_length / 2;
    size_t length = snappy::MaxCompressedLength(block_size) * 2;
    length += 3 * sizeof(int32_t);
    DCHECK(*output_length == 0 || length <= *output_length);

    // If length is non-zero then the output has been allocated.
    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else if (!_reuse_buffer || _out_buffer == NULL || _buffer_length < length) {
        _buffer_length = length;
        _out_buffer = _memory_pool->allocate(_buffer_length);
    }

    uint8_t* outp = _out_buffer;
    uint8_t* sizep = NULL;
    ReadWriteUtil::put_int(outp, input_length);
    outp += sizeof(int32_t);

    do {
        // Point at the spot to store the compressed size.
        sizep = outp;
        outp += sizeof(int32_t);
        size_t size = 0;
        snappy::RawCompress(reinterpret_cast<const char*>(input),
                            static_cast<size_t>(block_size), reinterpret_cast<char*>(outp), &size);

        ReadWriteUtil::put_int(sizep, size);
        input += block_size;
        input_length -= block_size;
        outp += size;
    } while (input_length > 0);

    *output = _out_buffer;
    *output_length = outp - _out_buffer;
    return Status::OK();
}

SnappyCompressor::SnappyCompressor(MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer) {
}

int SnappyCompressor::max_compressed_len(int input_length) {
    return snappy::MaxCompressedLength(input_length);
}

Status SnappyCompressor::compress(int input_len, uint8_t* input,
                                  int* output_len, uint8_t* output) {
    DCHECK_GE(*output_len, max_compressed_len(input_len));
    size_t out_len = 0;
    snappy::RawCompress(reinterpret_cast<const char*>(input),
                        static_cast<size_t>(input_len),
                        reinterpret_cast<char*>(output), &out_len);
    *output_len = out_len;
    return Status::OK();
}

Status SnappyCompressor::process_block(int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
    int max_compressed_len = this->max_compressed_len(input_length);

    if (*output_length != 0 && *output_length < max_compressed_len) {
        return Status::InternalError("process_block: output length too small");
    }

    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else if (!_reuse_buffer ||
               _out_buffer == NULL || _buffer_length < max_compressed_len) {
        DCHECK(_memory_pool != NULL) << "Can't allocate without passing in a mem pool";
        _buffer_length = max_compressed_len;
        _out_buffer = _memory_pool->allocate(_buffer_length);
        *output = _out_buffer;
        *output_length = max_compressed_len;
    }

    return compress(input_length, input, output_length, _out_buffer);
}

}
