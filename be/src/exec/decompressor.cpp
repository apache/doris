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

#include "exec/decompressor.h"

namespace doris {

Status Decompressor::create_decompressor(CompressType type, Decompressor** decompressor) {
    switch (type) {
    case CompressType::UNCOMPRESSED:
        *decompressor = nullptr;
        break;
    case CompressType::GZIP:
        *decompressor = new GzipDecompressor(false);
        break;
    case CompressType::DEFLATE:
        *decompressor = new GzipDecompressor(true);
        break;
    case CompressType::BZIP2:
        *decompressor = new Bzip2Decompressor();
        break;
    case CompressType::LZ4FRAME:
        *decompressor = new Lz4FrameDecompressor();
        break;
#ifdef DORIS_WITH_LZO
    case CompressType::LZOP:
        *decompressor = new LzopDecompressor();
        break;
#endif
    default:
        std::stringstream ss;
        ss << "Unknown compress type: " << type;
        return Status::InternalError(ss.str());
    }

    Status st = Status::OK();
    if (*decompressor != nullptr) {
        st = (*decompressor)->init();
    }

    return st;
}

std::string Decompressor::debug_info() {
    return "Decompressor";
}

// Gzip
GzipDecompressor::GzipDecompressor(bool is_deflate)
        : Decompressor(is_deflate ? CompressType::DEFLATE : CompressType::GZIP),
          _is_deflate(is_deflate) {}

GzipDecompressor::~GzipDecompressor() {
    (void)inflateEnd(&_z_strm);
}

Status GzipDecompressor::init() {
    _z_strm = {nullptr};
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;

    int window_bits = _is_deflate ? WINDOW_BITS : (WINDOW_BITS | DETECT_CODEC);
    int ret = inflateInit2(&_z_strm, window_bits);
    if (ret < 0) {
        std::stringstream ss;
        ss << "Failed to init inflate. status code: " << ret;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status GzipDecompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                    uint8_t* output, size_t output_max_len,
                                    size_t* decompressed_len, bool* stream_end,
                                    size_t* more_input_bytes, size_t* more_output_bytes) {
    // 1. set input and output
    _z_strm.next_in = input;
    _z_strm.avail_in = input_len;
    _z_strm.next_out = output;
    _z_strm.avail_out = output_max_len;

    while (_z_strm.avail_out > 0 && _z_strm.avail_in > 0) {
        *stream_end = false;
        // inflate() performs one or both of the following actions:
        //   Decompress more input starting at next_in and update next_in and avail_in
        //       accordingly.
        //   Provide more output starting at next_out and update next_out and avail_out
        //       accordingly.
        // inflate() returns Z_OK if some progress has been made (more input processed
        // or more output produced)

        int ret = inflate(&_z_strm, Z_NO_FLUSH);
        *input_bytes_read = input_len - _z_strm.avail_in;
        *decompressed_len = output_max_len - _z_strm.avail_out;

        VLOG_TRACE << "gzip dec ret: " << ret << " input_bytes_read: " << *input_bytes_read
                   << " decompressed_len: " << *decompressed_len;

        if (ret == Z_BUF_ERROR) {
            // Z_BUF_ERROR indicates that inflate() could not consume more input or
            // produce more output. inflate() can be called again with more output space
            // or more available input
            // ATTN: even if ret == Z_OK, decompressed_len may also be zero
            return Status::OK();
        } else if (ret == Z_STREAM_END) {
            *stream_end = true;
            // reset _z_strm to continue decoding a subsequent gzip stream
            ret = inflateReset(&_z_strm);
            if (ret != Z_OK) {
                std::stringstream ss;
                ss << "Failed to inflateReset. return code: " << ret;
                return Status::InternalError(ss.str());
            }
        } else if (ret != Z_OK) {
            std::stringstream ss;
            ss << "Failed to inflate. return code: " << ret;
            return Status::InternalError(ss.str());
        } else {
            // here ret must be Z_OK.
            // we continue if avail_out and avail_in > 0.
            // this means 'inflate' is not done yet.
        }
    }

    return Status::OK();
}

std::string GzipDecompressor::debug_info() {
    std::stringstream ss;
    ss << "GzipDecompressor."
       << " is_deflate: " << _is_deflate;
    return ss.str();
}

// Bzip2
Bzip2Decompressor::~Bzip2Decompressor() {
    BZ2_bzDecompressEnd(&_bz_strm);
}

Status Bzip2Decompressor::init() {
    bzero(&_bz_strm, sizeof(_bz_strm));
    int ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
    if (ret != BZ_OK) {
        std::stringstream ss;
        ss << "Failed to init bz2. status code: " << ret;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status Bzip2Decompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                     uint8_t* output, size_t output_max_len,
                                     size_t* decompressed_len, bool* stream_end,
                                     size_t* more_input_bytes, size_t* more_output_bytes) {
    // 1. set input and output
    _bz_strm.next_in = const_cast<char*>(reinterpret_cast<const char*>(input));
    _bz_strm.avail_in = input_len;
    _bz_strm.next_out = reinterpret_cast<char*>(output);
    _bz_strm.avail_out = output_max_len;

    while (_bz_strm.avail_out > 0 && _bz_strm.avail_in > 0) {
        *stream_end = false;
        // decompress
        int ret = BZ2_bzDecompress(&_bz_strm);
        *input_bytes_read = input_len - _bz_strm.avail_in;
        *decompressed_len = output_max_len - _bz_strm.avail_out;

        if (ret == BZ_DATA_ERROR || ret == BZ_DATA_ERROR_MAGIC) {
            LOG(INFO) << "input_bytes_read: " << *input_bytes_read
                      << " decompressed_len: " << *decompressed_len;
            std::stringstream ss;
            ss << "Failed to bz2 decompress. status code: " << ret;
            return Status::InternalError(ss.str());
        } else if (ret == BZ_STREAM_END) {
            *stream_end = true;
            ret = BZ2_bzDecompressEnd(&_bz_strm);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to end bz2 after meet BZ_STREAM_END. status code: " << ret;
                return Status::InternalError(ss.str());
            }

            ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to init bz2 after meet BZ_STREAM_END. status code: " << ret;
                return Status::InternalError(ss.str());
            }
        } else if (ret != BZ_OK) {
            std::stringstream ss;
            ss << "Failed to bz2 decompress. status code: " << ret;
            return Status::InternalError(ss.str());
        } else {
            // continue
        }
    }

    return Status::OK();
}

std::string Bzip2Decompressor::debug_info() {
    std::stringstream ss;
    ss << "Bzip2Decompressor.";
    return ss.str();
}

// Lz4Frame
// Lz4 version: 1.7.5
// define LZ4F_VERSION = 100
const unsigned Lz4FrameDecompressor::DORIS_LZ4F_VERSION = 100;

Lz4FrameDecompressor::~Lz4FrameDecompressor() {
    LZ4F_freeDecompressionContext(_dctx);
}

Status Lz4FrameDecompressor::init() {
    size_t ret = LZ4F_createDecompressionContext(&_dctx, DORIS_LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "LZ4F_dctx creation error: " << std::string(LZ4F_getErrorName(ret));
        return Status::InternalError(ss.str());
    }

    // init as -1
    _expect_dec_buf_size = -1;

    return Status::OK();
}

Status Lz4FrameDecompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                        uint8_t* output, size_t output_max_len,
                                        size_t* decompressed_len, bool* stream_end,
                                        size_t* more_input_bytes, size_t* more_output_bytes) {
    uint8_t* src = input;
    size_t src_size = input_len;
    size_t ret = 1;
    *input_bytes_read = 0;

    if (_expect_dec_buf_size == -1) {
        // init expected decompress buf size, and check if output_max_len is large enough
        // ATTN: _expect_dec_buf_size is uninit, which means this is the first time to call
        //       decompress(), so *input* should point to the head of the compressed file,
        //       where lz4 header section is there.

        if (input_len < 15) {
            std::stringstream ss;
            ss << "Lz4 header size is between 7 and 15 bytes. "
               << "but input size is only: " << input_len;
            return Status::InternalError(ss.str());
        }

        LZ4F_frameInfo_t info;
        ret = LZ4F_getFrameInfo(_dctx, &info, (void*)src, &src_size);
        if (LZ4F_isError(ret)) {
            std::stringstream ss;
            ss << "LZ4F_getFrameInfo error: " << std::string(LZ4F_getErrorName(ret));
            return Status::InternalError(ss.str());
        }

        _expect_dec_buf_size = get_block_size(&info);
        if (_expect_dec_buf_size == -1) {
            std::stringstream ss;
            ss << "Impossible lz4 block size unless more block sizes are allowed"
               << std::string(LZ4F_getErrorName(ret));
            return Status::InternalError(ss.str());
        }

        *input_bytes_read = src_size;

        src += src_size;
        src_size = input_len - src_size;

        LOG(INFO) << "lz4 block size: " << _expect_dec_buf_size;
    }

    // decompress
    size_t output_len = output_max_len;
    ret = LZ4F_decompress(_dctx, (void*)output, &output_len, (void*)src, &src_size,
                          /* LZ4F_decompressOptions_t */ nullptr);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "Decompression error: " << std::string(LZ4F_getErrorName(ret));
        return Status::InternalError(ss.str());
    }

    // update
    *input_bytes_read += src_size;
    *decompressed_len = output_len;
    if (ret == 0) {
        *stream_end = true;
    } else {
        *stream_end = false;
    }

    return Status::OK();
}

std::string Lz4FrameDecompressor::debug_info() {
    std::stringstream ss;
    ss << "Lz4FrameDecompressor."
       << " expect dec buf size: " << _expect_dec_buf_size
       << " Lz4 Frame Version: " << DORIS_LZ4F_VERSION;
    return ss.str();
}

size_t Lz4FrameDecompressor::get_block_size(const LZ4F_frameInfo_t* info) {
    switch (info->blockSizeID) {
    case LZ4F_default:
    case LZ4F_max64KB:
        return 1 << 16;
    case LZ4F_max256KB:
        return 1 << 18;
    case LZ4F_max1MB:
        return 1 << 20;
    case LZ4F_max4MB:
        return 1 << 22;
    default:
        // error
        return -1;
    }
}

} // namespace doris
