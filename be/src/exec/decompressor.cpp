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

#include <strings.h>

#include <memory>
#include <ostream>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"

namespace doris {

Status Decompressor::create_decompressor(CompressType type,
                                         std::unique_ptr<Decompressor>* decompressor) {
    switch (type) {
    case CompressType::UNCOMPRESSED:
        decompressor->reset(nullptr);
        break;
    case CompressType::GZIP:
        decompressor->reset(new GzipDecompressor(false));
        break;
    case CompressType::DEFLATE:
        decompressor->reset(new GzipDecompressor(true));
        break;
    case CompressType::BZIP2:
        decompressor->reset(new Bzip2Decompressor());
        break;
    case CompressType::ZSTD:
        decompressor->reset(new ZstdDecompressor());
        break;
    case CompressType::LZ4FRAME:
        decompressor->reset(new Lz4FrameDecompressor());
        break;
    case CompressType::LZ4BLOCK:
        decompressor->reset(new Lz4BlockDecompressor());
        break;
    case CompressType::SNAPPYBLOCK:
        decompressor->reset(new SnappyBlockDecompressor());
        break;
    case CompressType::LZOP:
        decompressor->reset(new LzopDecompressor());
        break;
    default:
        return Status::InternalError("Unknown compress type: {}", type);
    }

    Status st = Status::OK();
    if (*decompressor != nullptr) {
        st = (*decompressor)->init();
    }

    return st;
}

Status Decompressor::create_decompressor(TFileCompressType::type type,
                                         std::unique_ptr<Decompressor>* decompressor) {
    CompressType compress_type;
    switch (type) {
    case TFileCompressType::PLAIN:
    case TFileCompressType::UNKNOWN:
        compress_type = CompressType::UNCOMPRESSED;
        break;
    case TFileCompressType::GZ:
        compress_type = CompressType::GZIP;
        break;
    case TFileCompressType::LZO:
    case TFileCompressType::LZOP:
        compress_type = CompressType::LZOP;
        break;
    case TFileCompressType::BZ2:
        compress_type = CompressType::BZIP2;
        break;
    case TFileCompressType::ZSTD:
        compress_type = CompressType::ZSTD;
        break;
    case TFileCompressType::LZ4FRAME:
        compress_type = CompressType::LZ4FRAME;
        break;
    case TFileCompressType::LZ4BLOCK:
        compress_type = CompressType::LZ4BLOCK;
        break;
    case TFileCompressType::DEFLATE:
        compress_type = CompressType::DEFLATE;
        break;
    case TFileCompressType::SNAPPYBLOCK:
        compress_type = CompressType::SNAPPYBLOCK;
        break;
    default:
        return Status::InternalError<false>("unknown compress type: {}", type);
    }
    RETURN_IF_ERROR(Decompressor::create_decompressor(compress_type, decompressor));

    return Status::OK();
}

Status Decompressor::create_decompressor(TFileFormatType::type type,
                                         std::unique_ptr<Decompressor>* decompressor) {
    CompressType compress_type;
    switch (type) {
    case TFileFormatType::FORMAT_PROTO:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_PLAIN:
        compress_type = CompressType::UNCOMPRESSED;
        break;
    case TFileFormatType::FORMAT_CSV_GZ:
        compress_type = CompressType::GZIP;
        break;
    case TFileFormatType::FORMAT_CSV_BZ2:
        compress_type = CompressType::BZIP2;
        break;
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        compress_type = CompressType::LZ4FRAME;
        break;
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
        compress_type = CompressType::LZ4BLOCK;
        break;
    case TFileFormatType::FORMAT_CSV_LZOP:
        compress_type = CompressType::LZOP;
        break;
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        compress_type = CompressType::DEFLATE;
        break;
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
        compress_type = CompressType::SNAPPYBLOCK;
        break;
    default:
        return Status::InternalError<false>("unknown compress type: {}", type);
    }
    RETURN_IF_ERROR(Decompressor::create_decompressor(compress_type, decompressor));

    return Status::OK();
}

uint32_t Decompressor::_read_int32(uint8_t* buf) {
    return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
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
    _z_strm = {};
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;

    int window_bits = _is_deflate ? WINDOW_BITS : (WINDOW_BITS | DETECT_CODEC);
    int ret = inflateInit2(&_z_strm, window_bits);
    if (ret < 0) {
        return Status::InternalError("Failed to init inflate. status code: {}", ret);
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
                return Status::InternalError("Failed to inflateReset. return code: {}", ret);
            }
        } else if (ret != Z_OK) {
            return Status::InternalError("Failed to inflate. return code: {}", ret);
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
        return Status::InternalError("Failed to init bz2. status code: {}", ret);
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
            return Status::InternalError("Failed to bz2 decompress. status code: {}", ret);
        } else if (ret == BZ_STREAM_END) {
            *stream_end = true;
            ret = BZ2_bzDecompressEnd(&_bz_strm);
            if (ret != BZ_OK) {
                return Status::InternalError(
                        "Failed to end bz2 after meet BZ_STREAM_END. status code: {}", ret);
            }

            ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
            if (ret != BZ_OK) {
                return Status::InternalError(
                        "Failed to init bz2 after meet BZ_STREAM_END. status code: {}", ret);
            }
        } else if (ret != BZ_OK) {
            return Status::InternalError("Failed to bz2 decompress. status code: {}", ret);
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

ZstdDecompressor::~ZstdDecompressor() {
    ZSTD_freeDStream(_zstd_strm);
}

Status ZstdDecompressor::init() {
    _zstd_strm = ZSTD_createDStream();
    if (!_zstd_strm) {
        std::stringstream ss;
        return Status::InternalError("ZSTD_dctx creation error");
    }
    auto ret = ZSTD_initDStream(_zstd_strm);
    if (ZSTD_isError(ret)) {
        return Status::InternalError("ZSTD_initDStream error: {}", ZSTD_getErrorName(ret));
    }
    return Status::OK();
}

Status ZstdDecompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                    uint8_t* output, size_t output_max_len,
                                    size_t* decompressed_len, bool* stream_end,
                                    size_t* more_input_bytes, size_t* more_output_bytes) {
    // 1. set input and output
    ZSTD_inBuffer inputBuffer = {input, input_len, 0};
    ZSTD_outBuffer outputBuffer = {output, output_max_len, 0};

    // decompress
    int ret = ZSTD_decompressStream(_zstd_strm, &outputBuffer, &inputBuffer);
    *input_bytes_read = inputBuffer.pos;
    *decompressed_len = outputBuffer.pos;

    if (ZSTD_isError(ret)) {
        return Status::InternalError("Failed to zstd decompress: {}", ZSTD_getErrorName(ret));
    }

    *stream_end = ret == 0;
    return Status::OK();
}

std::string ZstdDecompressor::debug_info() {
    std::stringstream ss;
    ss << "ZstdDecompressor.";
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
    size_t remaining_input_size = input_len;
    size_t ret = 1;
    *input_bytes_read = 0;

    if (_expect_dec_buf_size == -1) {
        // init expected decompress buf size, and check if output_max_len is large enough
        // ATTN: _expect_dec_buf_size is uninit, which means this is the first time to call
        //       decompress(), so *input* should point to the head of the compressed file,
        //       where lz4 header section is there.

        if (input_len < 15) {
            return Status::InternalError(
                    "Lz4 header size is between 7 and 15 bytes. "
                    "but input size is only: {}",
                    input_len);
        }

        LZ4F_frameInfo_t info;
        ret = LZ4F_getFrameInfo(_dctx, &info, (void*)src, &remaining_input_size);
        if (LZ4F_isError(ret)) {
            return Status::InternalError("LZ4F_getFrameInfo error: {}",
                                         std::string(LZ4F_getErrorName(ret)));
        }

        _expect_dec_buf_size = get_block_size(&info);
        if (_expect_dec_buf_size == -1) {
            return Status::InternalError(
                    "Impossible lz4 block size unless more block sizes are allowed {}",
                    std::string(LZ4F_getErrorName(ret)));
        }

        *input_bytes_read = remaining_input_size;

        src += remaining_input_size;
        remaining_input_size = input_len - remaining_input_size;

        LOG(INFO) << "lz4 block size: " << _expect_dec_buf_size;
    }

    // decompress
    size_t output_len = output_max_len;
    ret = LZ4F_decompress(_dctx, (void*)output, &output_len, (void*)src, &remaining_input_size,
                          /* LZ4F_decompressOptions_t */ nullptr);
    if (LZ4F_isError(ret)) {
        return Status::InternalError("Decompression error: {}",
                                     std::string(LZ4F_getErrorName(ret)));
    }

    // update
    *input_bytes_read += remaining_input_size;
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

/// Lz4BlockDecompressor
Status Lz4BlockDecompressor::init() {
    return Status::OK();
}

// Hadoop lz4codec source :
// https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
// Example:
// OriginData(The original data will be divided into several large data block.) :
//      large data block1 | large data block2 | large data block3 | ....
// The large data block will be divided into several small data block.
// Suppose a large data block is divided into three small blocks:
// large data block1:            | small block1 | small block2 | small block3 |
// CompressData:   <A [B1 compress(small block1) ] [B2 compress(small block1) ] [B3 compress(small block1)]>
//
// A : original length of the current block of large data block.
// sizeof(A) = 4 bytes.
// A = length(small block1) + length(small block2) + length(small block3)
// Bx : length of  small data block bx.
// sizeof(Bx) = 4 bytes.
// Bx = length(compress(small blockx))
Status Lz4BlockDecompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                        uint8_t* output, size_t output_max_len,
                                        size_t* decompressed_len, bool* stream_end,
                                        size_t* more_input_bytes, size_t* more_output_bytes) {
    auto* input_ptr = input;
    auto* output_ptr = output;

    while (input_len > 0) {
        //if faild ,  fall back to large block begin
        auto* large_block_input_ptr = input_ptr;
        auto* large_block_output_ptr = output_ptr;

        if (input_len < sizeof(uint32_t)) {
            return Status::InvalidArgument(strings::Substitute(
                    "fail to do hadoop-lz4 decompress, input_len=$0", input_len));
        }

        uint32_t remaining_decompressed_large_block_len = BigEndian::Load32(input_ptr);

        input_ptr += sizeof(uint32_t);
        input_len -= sizeof(uint32_t);

        std::size_t remaining_output_len = output_max_len - *decompressed_len;

        if (remaining_output_len < remaining_decompressed_large_block_len) {
            // Need more output buffer
            *more_output_bytes = remaining_decompressed_large_block_len - remaining_output_len;
            input_ptr = large_block_input_ptr;
            output_ptr = large_block_output_ptr;

            break;
        }

        std::size_t decompressed_large_block_len = 0;
        while (remaining_decompressed_large_block_len > 0) {
            // Check that input length should not be negative.
            if (input_len < sizeof(uint32_t)) {
                *more_input_bytes = sizeof(uint32_t) - input_len;
                break;
            }

            // Read the length of the next lz4 compressed block.
            size_t compressed_small_block_len = BigEndian::Load32(input_ptr);

            input_ptr += sizeof(uint32_t);
            input_len -= sizeof(uint32_t);

            if (compressed_small_block_len == 0) {
                continue;
            }

            if (compressed_small_block_len > input_len) {
                // Need more input buffer
                *more_input_bytes = compressed_small_block_len - input_len;
                break;
            }

            // Decompress this block.
            auto decompressed_small_block_len = LZ4_decompress_safe(
                    reinterpret_cast<const char*>(input_ptr), reinterpret_cast<char*>(output_ptr),
                    compressed_small_block_len, remaining_output_len);
            if (decompressed_small_block_len < 0) {
                return Status::InvalidArgument("fail to do LZ4 decompress, error = {}",
                                               LZ4F_getErrorName(decompressed_small_block_len));
            }
            input_ptr += compressed_small_block_len;
            input_len -= compressed_small_block_len;

            output_ptr += decompressed_small_block_len;
            remaining_decompressed_large_block_len -= decompressed_small_block_len;
            decompressed_large_block_len += decompressed_small_block_len;
        };

        if (*more_input_bytes != 0) {
            // Need more input buffer
            input_ptr = large_block_input_ptr;
            output_ptr = large_block_output_ptr;
            break;
        }

        *decompressed_len += decompressed_large_block_len;
    }
    *input_bytes_read += (input_ptr - input);
    // If no more input and output need, means this is the end of a compressed block
    *stream_end = (*more_input_bytes == 0 && *more_output_bytes == 0);

    return Status::OK();
}

std::string Lz4BlockDecompressor::debug_info() {
    std::stringstream ss;
    ss << "Lz4BlockDecompressor.";
    return ss.str();
}

/// SnappyBlockDecompressor
Status SnappyBlockDecompressor::init() {
    return Status::OK();
}

// Hadoop snappycodec source :
// https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/SnappyCodec.cc
// Example:
// OriginData(The original data will be divided into several large data block.) :
//      large data block1 | large data block2 | large data block3 | ....
// The large data block will be divided into several small data block.
// Suppose a large data block is divided into three small blocks:
// large data block1:            | small block1 | small block2 | small block3 |
// CompressData:   <A [B1 compress(small block1) ] [B2 compress(small block1) ] [B3 compress(small block1)]>
//
// A : original length of the current block of large data block.
// sizeof(A) = 4 bytes.
// A = length(small block1) + length(small block2) + length(small block3)
// Bx : length of  small data block bx.
// sizeof(Bx) = 4 bytes.
// Bx = length(compress(small blockx))
Status SnappyBlockDecompressor::decompress(uint8_t* input, size_t input_len,
                                           size_t* input_bytes_read, uint8_t* output,
                                           size_t output_max_len, size_t* decompressed_len,
                                           bool* stream_end, size_t* more_input_bytes,
                                           size_t* more_output_bytes) {
    auto* input_ptr = input;
    auto* output_ptr = output;

    while (input_len > 0) {
        //if faild ,  fall back to large block begin
        auto* large_block_input_ptr = input_ptr;
        auto* large_block_output_ptr = output_ptr;

        if (input_len < sizeof(uint32_t)) {
            return Status::InvalidArgument(strings::Substitute(
                    "fail to do hadoop-snappy decompress, input_len=$0", input_len));
        }

        uint32_t remaining_decompressed_large_block_len = BigEndian::Load32(input_ptr);

        input_ptr += sizeof(uint32_t);
        input_len -= sizeof(uint32_t);

        std::size_t remaining_output_len = output_max_len - *decompressed_len;

        if (remaining_output_len < remaining_decompressed_large_block_len) {
            // Need more output buffer
            *more_output_bytes = remaining_decompressed_large_block_len - remaining_output_len;
            input_ptr = large_block_input_ptr;
            output_ptr = large_block_output_ptr;

            break;
        }

        std::size_t decompressed_large_block_len = 0;
        while (remaining_decompressed_large_block_len > 0) {
            // Check that input length should not be negative.
            if (input_len < sizeof(uint32_t)) {
                *more_input_bytes = sizeof(uint32_t) - input_len;
                break;
            }

            // Read the length of the next snappy compressed block.
            size_t compressed_small_block_len = BigEndian::Load32(input_ptr);

            input_ptr += sizeof(uint32_t);
            input_len -= sizeof(uint32_t);

            if (compressed_small_block_len == 0) {
                continue;
            }

            if (compressed_small_block_len > input_len) {
                // Need more input buffer
                *more_input_bytes = compressed_small_block_len - input_len;
                break;
            }

            // Decompress this block.
            size_t decompressed_small_block_len;
            if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input_ptr),
                                               compressed_small_block_len,
                                               &decompressed_small_block_len)) {
                return Status::InternalError(
                        "snappy block decompress failed to get uncompressed len");
            }
            if (!snappy::RawUncompress(reinterpret_cast<const char*>(input_ptr),
                                       compressed_small_block_len,
                                       reinterpret_cast<char*>(output_ptr))) {
                return Status::InternalError(
                        "snappy block decompress failed. uncompressed_len: {}, compressed_len: {}",
                        decompressed_small_block_len, compressed_small_block_len);
            }
            input_ptr += compressed_small_block_len;
            input_len -= compressed_small_block_len;

            output_ptr += decompressed_small_block_len;
            remaining_decompressed_large_block_len -= decompressed_small_block_len;
            decompressed_large_block_len += decompressed_small_block_len;
        };

        if (*more_input_bytes != 0) {
            // Need more input buffer
            input_ptr = large_block_input_ptr;
            output_ptr = large_block_output_ptr;
            break;
        }

        *decompressed_len += decompressed_large_block_len;
    }
    *input_bytes_read += (input_ptr - input);
    // If no more input and output need, means this is the end of a compressed block
    *stream_end = (*more_input_bytes == 0 && *more_output_bytes == 0);

    return Status::OK();
}

std::string SnappyBlockDecompressor::debug_info() {
    std::stringstream ss;
    ss << "SnappyBlockDecompressor.";
    return ss.str();
}

} // namespace doris
