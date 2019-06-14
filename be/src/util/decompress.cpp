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

#include <boost/assign/list_of.hpp>
#include "util/decompress.h"
#include "exec/read_write_util.h"
#include "runtime/runtime_state.h"
#include "gen_cpp/Descriptors_types.h"

// Codec libraries
#include <zlib.h>
#include <bzlib.h>
#include <snappy/snappy.h>

namespace doris {

GzipDecompressor::GzipDecompressor(MemPool* mem_pool, bool reuse_buffer, bool is_deflate) : 
        Codec(mem_pool, reuse_buffer),
        _is_deflate(is_deflate) {
    bzero(&_stream, sizeof(_stream));
}

GzipDecompressor::~GzipDecompressor() {
    (void)inflateEnd(&_stream);
}

Status GzipDecompressor::init() {
    int ret = 0;
    // Initialize to run either deflate or zlib/gzip format
    int window_bits = _is_deflate ? -WINDOW_BITS : WINDOW_BITS | DETECT_CODEC;

    if ((ret = inflateInit2(&_stream, window_bits)) != Z_OK) {
        return Status::InternalError("zlib inflateInit failed: " +  std::string(_stream.msg));
    }

    return Status::OK();
}

Status GzipDecompressor::process_block(int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
    bool use_temp = false;

    // If length is set then the output has been allocated.
    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else if (!_reuse_buffer || _out_buffer == NULL) {
        // guess that we will need 2x the input length.
        _buffer_length = input_length * 2;

        if (_buffer_length > MAX_BLOCK_SIZE) {
            return Status::InternalError("Decompressor: block size is too big");
        }

        _out_buffer = _temp_memory_pool.allocate(_buffer_length);
        use_temp = true;
    }

    int ret = 0;

    while (ret != Z_STREAM_END) {
        _stream.next_in = reinterpret_cast<Bytef*>(input);
        _stream.avail_in = input_length;
        _stream.next_out = reinterpret_cast<Bytef*>(_out_buffer);
        _stream.avail_out = _buffer_length;

        ret = inflate(&_stream, 1);

        if (ret != Z_STREAM_END) {
            if (ret == Z_OK) {
                // Not enough output space.
                DCHECK_EQ(*output_length, 0);

                if (*output_length != 0) {
                    return Status::InternalError("Too small a buffer passed to GzipDecompressor");
                }

                _temp_memory_pool.clear();
                _buffer_length *= 2;

                if (_buffer_length > MAX_BLOCK_SIZE) {
                    return Status::InternalError("Decompressor: block size is too big");
                }

                _out_buffer = _temp_memory_pool.allocate(_buffer_length);

                if (inflateReset(&_stream) != Z_OK) {
                    return Status::InternalError("zlib inflateEnd failed: " + std::string(_stream.msg));
                }

                continue;
            }

            return Status::InternalError("zlib inflate failed: " + std::string(_stream.msg));
        }
    }

    if (inflateReset(&_stream) != Z_OK) {
        return Status::InternalError("zlib inflateEnd failed: " + std::string(_stream.msg));
    }

    *output = _out_buffer;

    // _stream.avail_out is the number of bytes *left* in the out buffer, but
    // we're interested in the number of bytes used.
    if (*output_length == 0) {
        *output_length = _buffer_length - _stream.avail_out;
    }

    if (use_temp) {
        _memory_pool->acquire_data(&_temp_memory_pool, _reuse_buffer);
    }

    return Status::OK();
}

BzipDecompressor::BzipDecompressor(MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer) {
}

Status BzipDecompressor::process_block(int input_length, uint8_t* input,
                                      int* output_length, uint8_t** output) {
    bool use_temp = false;

    // If length is set then the output has been allocated.
    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else if (!_reuse_buffer || _out_buffer == NULL) {
        // guess that we will need 2x the input length.
        _buffer_length = input_length * 2;

        if (_buffer_length > MAX_BLOCK_SIZE) {
            return Status::InternalError("Decompressor: block size is too big");
        }

        _out_buffer = _temp_memory_pool.allocate(_buffer_length);
        use_temp = true;
    }

    int ret = BZ_OUTBUFF_FULL;
    unsigned int outlen = 0;

    while (ret == BZ_OUTBUFF_FULL) {
        if (_out_buffer == NULL) {
            DCHECK_EQ(*output_length, 0);
            _temp_memory_pool.clear();
            _buffer_length = _buffer_length * 2;

            if (_buffer_length > MAX_BLOCK_SIZE) {
                return Status::InternalError("Decompressor: block size is too big");
            }

            _out_buffer = _temp_memory_pool.allocate(_buffer_length);
        }

        outlen = static_cast<unsigned int>(_buffer_length);

        if ((ret = BZ2_bzBuffToBuffDecompress(
                reinterpret_cast<char*>(_out_buffer), &outlen,
                reinterpret_cast<char*>(input),
                static_cast<unsigned int>(input_length), 0, 0)) == BZ_OUTBUFF_FULL) {
            // If the output_length was passed we must have enough room.
            DCHECK_EQ(*output_length, 0);

            if (*output_length != 0) {
                return Status::InternalError("Too small a buffer passed to BzipDecompressor");
            }

            _out_buffer = NULL;
        }
    }

    if (ret !=  BZ_OK) {
        std::stringstream ss;
        ss << "bzlib BZ2_bzBuffToBuffDecompressor failed: " << ret;
        return Status::InternalError(ss.str());

    }

    *output = _out_buffer;

    if (*output_length == 0) {
        *output_length = outlen;
    }

    if (use_temp) {
        _memory_pool->acquire_data(&_temp_memory_pool, _reuse_buffer);
    }

    return Status::OK();
}

SnappyDecompressor::SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer)
    : Codec(mem_pool, reuse_buffer) {
}

Status SnappyDecompressor::process_block(int input_length, uint8_t* input,
                                        int* output_length, uint8_t** output) {
    // If length is set then the output has been allocated.
    size_t uncompressed_length = 0;

    if (*output_length != 0) {
        _buffer_length = *output_length;
        _out_buffer = *output;
    } else {
        // Snappy saves the uncompressed length so we never have to retry.
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
                                           input_length, &uncompressed_length)) {
            return Status::InternalError("Snappy: GetUncompressedLength failed");
        }

        if (!_reuse_buffer || _out_buffer == NULL || _buffer_length < uncompressed_length) {
            _buffer_length = uncompressed_length;

            if (_buffer_length > MAX_BLOCK_SIZE) {
                return Status::InternalError("Decompressor: block size is too big");
            }

            _out_buffer = _memory_pool->allocate(_buffer_length);
        }
    }

    if (!snappy::RawUncompress(
            reinterpret_cast<const char*>(input),
            static_cast<size_t>(input_length), reinterpret_cast<char*>(_out_buffer))) {
        return Status::InternalError("Snappy: RawUncompress failed");
    }

    if (*output_length == 0) {
        *output_length = uncompressed_length;
        *output = _out_buffer;
    }

    return Status::OK();
}

SnappyBlockDecompressor::SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer) : 
        Codec(mem_pool, reuse_buffer) {
}

// Hadoop uses a block compression scheme on top of snappy.  As per the hadoop docs
// the input is split into blocks.  Each block "contains the uncompressed length for
// the block followed by one of more length-prefixed blocks of compressed data."
// This is essentially blocks of blocks.
// The outer block consists of:
//   - 4 byte little endian uncompressed_size
//   < inner blocks >
//   ... repeated until input_len is consumed ..
// The inner blocks have:
//   - 4-byte little endian compressed_size
//   < snappy compressed block >
//   - 4-byte little endian compressed_size
//   < snappy compressed block >
//   ... repeated until uncompressed_size from outer block is consumed ...

// Utility function to decompress snappy block compressed data.  If size_only is true,
// this function does not decompress but only computes the output size and writes
// the result to *output_len.
// If size_only is false, output must be preallocated to output_len and this needs to
// be exactly big enough to hold the decompressed output.
// size_only is a O(1) operations (just reads a single varint for each snappy block).
static Status snappy_block_decompress(int input_len, uint8_t* input, bool size_only,
                                    int* output_len, char* output) {

    int uncompressed_total_len = 0;

    while (input_len > 0) {
        size_t uncompressed_block_len = ReadWriteUtil::get_int(input);
        input += sizeof(int32_t);
        input_len -= sizeof(int32_t);

        if (uncompressed_block_len > Codec::MAX_BLOCK_SIZE || uncompressed_block_len == 0) {
            if (uncompressed_total_len == 0) {
                // TODO: is this check really robust?
                std::stringstream ss;
                ss << "Decompressor: block size is too big.  Data is likely corrupt. "
                   << "Size: " << uncompressed_block_len;
                return Status::InternalError(ss.str());
            }

            break;
        }

        if (!size_only) {
            int remaining_output_size = *output_len - uncompressed_total_len;
            DCHECK_GE(remaining_output_size, uncompressed_block_len);
        }

        while (uncompressed_block_len > 0) {
            // Read the length of the next snappy compressed block.
            size_t compressed_len = ReadWriteUtil::get_int(input);
            input += sizeof(int32_t);
            input_len -= sizeof(int32_t);

            if (compressed_len == 0 || compressed_len > input_len) {
                if (uncompressed_total_len == 0) {
                    return Status::InternalError(
                               "Decompressor: invalid compressed length.  Data is likely corrupt.");
                }

                input_len = 0;
                break;
            }

            // Read how big the output will be.
            size_t uncompressed_len = 0;

            if (!snappy::GetUncompressedLength(reinterpret_cast<char*>(input),
                                               input_len, &uncompressed_len)) {
                if (uncompressed_total_len == 0) {
                    return Status::InternalError("Snappy: GetUncompressedLength failed");
                }

                input_len = 0;
                break;
            }

            DCHECK_GT(uncompressed_len, 0);

            if (!size_only) {
                // Decompress this snappy block
                if (!snappy::RawUncompress(reinterpret_cast<char*>(input),
                                           compressed_len, output)) {
                    return Status::InternalError("Snappy: RawUncompress failed");
                }

                output += uncompressed_len;
            }

            input += compressed_len;
            input_len -= compressed_len;
            uncompressed_block_len -= uncompressed_len;
            uncompressed_total_len += uncompressed_len;
        }
    }

    if (size_only) {
        *output_len = uncompressed_total_len;
    } else if (*output_len != uncompressed_total_len) {
        return Status::InternalError("Snappy: Decompressed size is not correct.");
    }

    return Status::OK();
}

Status SnappyBlockDecompressor::process_block(int input_len, uint8_t* input,
        int* output_len, uint8_t** output) {
    if (*output_len == 0) {
        // If we don't know the size beforehand, compute it.
        RETURN_IF_ERROR(snappy_block_decompress(input_len, input, true, output_len, NULL));
        DCHECK_NE(*output_len, 0);

        if (!_reuse_buffer || _out_buffer == NULL || _buffer_length < *output_len) {
            // Need to allocate a new buffer
            _buffer_length = *output_len;
            _out_buffer = _memory_pool->allocate(_buffer_length);
        }

        *output = _out_buffer;
    }

    DCHECK(*output != NULL);

    if (*output_len > MAX_BLOCK_SIZE) {
        // TODO: is this check really robust?
        std::stringstream ss;
        ss << "Decompressor: block size is too big.  Data is likely corrupt. "
           << "Size: " << *output_len;
        return Status::InternalError(ss.str());
    }

    char* out_ptr = reinterpret_cast<char*>(*output);
    RETURN_IF_ERROR(snappy_block_decompress(input_len, input, false, output_len, out_ptr));
    return Status::OK();
}

}
