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

#include "exec/Compressor.h"

#include <bzlib.h>
#include <lz4/lz4frame.h>
#include <strings.h>
#include <zlib.h>

#include "common/logging.h"
#include "common/status.h"

namespace doris {

std::string Compressor::debug_info() {
    return "Compressor";
}

GzipCompressor::GzipCompressor(bool is_deflate)
        : Compressor(is_deflate ? CompressType::DEFLATE : CompressType::GZIP),
          _is_deflate(is_deflate) {};

GzipCompressor::~GzipCompressor() {
    deflateEnd(&_z_strm);
}

Status GzipCompressor::init() {
    _z_strm = {};
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;

    int window_bits = _is_deflate ? WINDOW_BITS : (WINDOW_BITS | DETECT_CODEC);
    // TODO: config CompressionLevel and strategy from hive hadoop conf
    int ret = deflateInit2(&_z_strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits, 8,
                           Z_DEFAULT_STRATEGY);
    if (ret < 0) {
        return Status::InternalError("Failed to init deflate. status code: {}", ret);
    }

    return Status::OK();
}

Status GzipCompressor::compress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                uint8_t* output, size_t output_max_len, size_t* compressed_len,
                                bool* stream_end, size_t* more_input_bytes,
                                size_t* more_output_bytes) {
    // 1. set input and output
    _z_strm.next_in = input;
    _z_strm.avail_in = input_len;
    _z_strm.next_out = output;
    _z_strm.avail_out = output_max_len;

    while (_z_strm.avail_out > 0 && _z_strm.avail_in > 0) {
        *stream_end = false;
        int ret = deflate(&_z_strm, Z_NO_FLUSH);
        *input_bytes_read = input_len - _z_strm.avail_in;
        *compressed_len = output_max_len - _z_strm.avail_out;

        VLOG_TRACE << "gzip dec ret: " << ret << " input_bytes_read: " << *input_bytes_read
                   << " compressed_len: " << *compressed_len;

        if (ret == Z_BUF_ERROR) {
            // Z_BUF_ERROR indicates that deflate() could not consume more input or
            // produce more output. deflate() can be called again with more output space
            // or more available input
            // ATTN: even if ret == Z_OK, compressed_len may also be zero
            return Status::OK();
        } else if (ret == Z_STREAM_END) {
            *stream_end = true;
            // reset _z_strm to continue coding a subsequent gzip stream
            ret = deflateReset(&_z_strm);
            if (ret != Z_OK) {
                return Status::InternalError("Failed to deflateReset. return code: {}", ret);
            }
        } else if (ret != Z_OK) {
            return Status::InternalError("Failed to deflate. return code: {}", ret);
        }
    }

    return Status::OK();
}

std::string GzipCompressor::debug_info() {
    std::stringstream ss;
    ss << "GzipCompressor."
       << " is_deflate: " << _is_deflate;
    return ss.str();
}

Bzip2Compressor::~Bzip2Compressor() {
    BZ2_bzCompressEnd(&_bz_strm);
}

Status Bzip2Compressor::init() {
    bzero(&_bz_strm, sizeof(_bz_strm));
    // TODO: conf blockSize100k
    int ret = BZ2_bzCompressInit(&_bz_strm, 9, 0, 0);
    if (ret != BZ_OK) {
        return Status::InternalError("Failed to init bzip2. status code: {}", ret);
    }

    return Status::OK();
}

Bzip2Compressor::Bzip2Compressor() : Compressor(CompressType::BZIP2) {}

Status Bzip2Compressor::compress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                 uint8_t* output, size_t output_max_len, size_t* compressed_len,
                                 bool* stream_end, size_t* more_input_bytes,
                                 size_t* more_output_bytes) {
    // 1. set input and output
    // 1. set input and output
    _bz_strm.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    _bz_strm.avail_in = input_len;
    _bz_strm.next_out = reinterpret_cast<char*>(output);
    _bz_strm.avail_out = output_max_len;

    while (_bz_strm.avail_out > 0 && _bz_strm.avail_in > 0) {
        *stream_end = false;
        int ret = BZ2_bzCompress(&_bz_strm, BZ_RUN);
        *input_bytes_read = input_len - _bz_strm.avail_in;
        *compressed_len = output_max_len - _bz_strm.avail_out;

        VLOG_TRACE << "bzip2 dec ret: " << ret << " input_bytes_read: " << *input_bytes_read
                   << " compressed_len: " << *compressed_len;

        if (ret == BZ_OUTBUFF_FULL) {
            // BZ_OUTBUFF_FULL indicates that bzip2() could not consume more input or
            // produce more output. bzip2() can be called again with more output space
            // or more available input
            // ATTN: even if ret == BZ_OK, compressed_len may also be zero
            return Status::OK();
        } else if (ret == BZ_STREAM_END) {
            *stream_end = true;
            // reset _bz_strm to continue coding a subsequent bzip2 stream
            ret = BZ2_bzCompressEnd(&_bz_strm);
            if (ret != BZ_OK) {
                return Status::InternalError("Failed to BZ2_bzCompressEnd. return code: {}", ret);
            }

            ret = BZ2_bzCompressInit(&_bz_strm, 9, 0, 0);
            if (ret != BZ_OK) {
                return Status::InternalError("Failed to init bzip2. status code: {}", ret);
            }
        } else if (ret != BZ_OK) {
            return Status::InternalError("Failed to bzip2. return code: {}", ret);
        }
    }

    return Status::OK();
}

std::string Bzip2Compressor::debug_info() {
    std::stringstream ss;
    ss << "Bzip2Compressor.";
    return ss.str();
}

// Lz4Frame
// Lz4 version: 1.7.5
// define LZ4F_VERSION = 100
const unsigned Lz4FrameCompressor::DORIS_LZ4F_VERSION = 100;

Lz4FrameCompressor::~Lz4FrameCompressor() {
    LZ4F_freeCompressionContext(_cctx);
}

Status Lz4FrameCompressor::init() {
    size_t ret = LZ4F_createCompressionContext(&_cctx, DORIS_LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "LZ4F_dctx creation error: " << std::string(LZ4F_getErrorName(ret));
        return Status::InternalError(ss.str());
    }
}

Status Lz4FrameCompressor::compress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                    uint8_t* output, size_t output_max_len, size_t* compressed_len,
                                    bool* stream_end, size_t* more_input_bytes,
                                    size_t* more_output_bytes) {
    // compress
    size_t output_len = output_max_len;
    size_t ret = LZ4F_compressFrame(void* dstBuffer, size_t dstCapacity, const void* srcBuffer,
                                    size_t srcSize, const LZ4F_preferences_t* preferencesPtr);
                                    }

} // namespace doris