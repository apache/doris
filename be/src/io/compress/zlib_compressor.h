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

#include "io/compress/compressor.h"
#include "util/byte_buffer.h"
#include "zlib.h"
namespace doris::io {

class ZlibCompressor : public Compressor {
public:
    // The compression level for zlib library
    enum CompressionLevel {
        // Compression level for no compression.
        NO_COMPRESSION = 0,

        // Compression level for fastest compression.
        BEST_SPEED = 1,

        TWO = 2,

        THREE = 3,

        FOUR = 4,

        FIVE = 5,

        SIX = 6,

        SEVEN = 7,

        EIGHT = 8,

        // Compression level for best compression.
        BEST_COMPRESSION = 9,
        
        // Default compression level.
        DEFAULT_COMPRESSION = -1,

    };

    // The compression level for zlib library.
    enum CompressionStrategy {
        // Compression strategy best used for data consisting mostly of small
        // values with a somewhat random distribution. Forces more Huffman coding
        // and less string matching.
        FILTERED = 2,

        // Compression strategy for Huffman coding only.
        HUFFMAN_ONLY = 2,

        // Compression strategy to limit match distances to one
        // (run-length encoding).
        RLE = 3,

        // Compression strategy to prevent the use of dynamic Huffman codes,
        // allowing for a simpler decoder for special applications.
        FIXED = 4,

        // Default compression strategy.
        DEFAULT_STRATEGY = 0,
    };

    // The type of header for compressed data.
    enum CompressionHeader {
        // No headers/trailers/checksums.
        NO_HEADER = -15,

        // Default headers/trailers/checksums.
        DEFAULT_HEADER = 15,

        // Simple gzip headers/trailers.
        GZIP_FORMAT = 31,
    };

    ZlibCompressor(CompressionLevel level, CompressionStrategy strategy, CompressionHeader header,
                   size_t direct_buffer_size)
            : _level(level),
              _strategy(strategy),
              _window_bits(header),
              _direct_bufffer_size(direct_buffer_size) {}
    // TODO: clean the resource
    ~ZlibCompressor() override = default;
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
    // zlib struct for compressing data
    z_stream _stream;
    const CompressionLevel _level;
    const CompressionStrategy _strategy;
    const CompressionHeader _window_bits;
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
