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

namespace palo {

Status Decompressor::create_decompressor(CompressType type,
                                         Decompressor** decompressor) {
    switch(type) {
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
    case CompressType::LZOP:
        *decompressor = new LzopDecompressor();
        break;
    default:
        std::stringstream ss;
        ss << "Unknown compress type: " << type;
        return Status(ss.str());
    }

    Status st = Status::OK;
    if (*decompressor != nullptr) {
        st = (*decompressor)->init();
    }
    
    return st;
}

Decompressor::~Decompressor() {
}

std::string Decompressor::debug_info() {
    return "Decompressor";
}

// Gzip
GzipDecompressor::GzipDecompressor(bool is_deflate):
    Decompressor(_is_deflate ? CompressType::DEFLATE : CompressType::GZIP),
    _is_deflate(is_deflate) {
}

GzipDecompressor::~GzipDecompressor() {
    (void) inflateEnd(&_z_strm);
}

Status GzipDecompressor::init() {
    _z_strm = {0};
    _z_strm.zalloc = Z_NULL;
    _z_strm.zfree = Z_NULL;
    _z_strm.opaque = Z_NULL;

    int window_bits = _is_deflate ? WINDOW_BITS : (WINDOW_BITS | DETECT_CODEC);
    int ret = inflateInit2(&_z_strm, window_bits);
    if (ret < 0) {
        std::stringstream ss;
        ss << "Failed to init inflate. status code: " << ret;
        return Status(ss.str());
    }

    return Status::OK;
}

Status GzipDecompressor::decompress(
        uint8_t* input, size_t input_len, size_t* input_bytes_read,
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

        LOG(INFO) << "gzip dec ret: " << ret
                  << " input_bytes_read: " << *input_bytes_read
                  << " decompressed_len: " << *decompressed_len;

        if (ret == Z_BUF_ERROR) {
            // Z_BUF_ERROR indicates that inflate() could not consume more input or
            // produce more output. inflate() can be called again with more output space
            // or more available input
            // ATTN: even if ret == Z_OK, decompressed_len may also be zero
            return Status::OK;
        } else if (ret == Z_STREAM_END) {
            *stream_end = true;
            // reset _z_strm to continue decoding a subsequent gzip stream
            ret = inflateReset(&_z_strm);
            if (ret != Z_OK) {
                std::stringstream ss;
                ss << "Failed to inflateRset. return code: " << ret;
                return Status(ss.str());
            }
        } else if (ret != Z_OK) {
            std::stringstream ss;
            ss << "Failed to inflate. return code: " << ret;
            return Status(ss.str());
        } else {
            // here ret must be Z_OK.
            // we continue if avail_out and avail_in > 0.
            // this means 'inflate' is not done yet.
        }
    }

    return Status::OK;
}

std::string GzipDecompressor::debug_info() {
    std::stringstream ss;
    ss << "GzipDecompressor." << " is_deflate: " << _is_deflate;
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
        return Status(ss.str());
    }

    return Status::OK;
}

Status Bzip2Decompressor::decompress(
        uint8_t* input, size_t input_len, size_t* input_bytes_read,
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
            return Status(ss.str());
        } else if (ret == BZ_STREAM_END) {
            *stream_end = true;
            ret = BZ2_bzDecompressEnd(&_bz_strm);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to end bz2 after meet BZ_STREAM_END. status code: " << ret;
                return Status(ss.str());
            }

            ret = BZ2_bzDecompressInit(&_bz_strm, 0, 0);
            if (ret != BZ_OK) {
                std::stringstream ss;
                ss << "Failed to init bz2 after meet BZ_STREAM_END. status code: " << ret;
                return Status(ss.str());
            }
        } else if (ret != BZ_OK) {
            std::stringstream ss;
            ss << "Failed to bz2 decompress. status code: " << ret;
            return Status(ss.str());
        } else {
            // continue
        }
    }

    return Status::OK;
}

std::string Bzip2Decompressor::debug_info() {
    std::stringstream ss;
    ss << "Bzip2Decompressor.";
    return ss.str();
}

// Lz4Frame
// Lz4 version: 1.7.5
// define LZ4F_VERSION = 100
const unsigned Lz4FrameDecompressor::PALO_LZ4F_VERSION = 100;

Lz4FrameDecompressor::~Lz4FrameDecompressor() {
    LZ4F_freeDecompressionContext(_dctx);
}

Status Lz4FrameDecompressor::init() {
    size_t ret = LZ4F_createDecompressionContext(&_dctx, PALO_LZ4F_VERSION);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "LZ4F_dctx creation error: " << std::string(LZ4F_getErrorName(ret));
        return Status(ss.str());
    }

    // init as -1
    _expect_dec_buf_size = -1;

    return Status::OK;
}

Status Lz4FrameDecompressor::decompress(
        uint8_t* input, size_t input_len, size_t* input_bytes_read,
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
            return Status(ss.str());
        }

        LZ4F_frameInfo_t info;
        ret = LZ4F_getFrameInfo(_dctx, &info, (void*) src, &src_size);
        if (LZ4F_isError(ret)) {
            std::stringstream ss;
            ss << "LZ4F_getFrameInfo error: " << std::string(LZ4F_getErrorName(ret));
            return Status(ss.str());
        }

        _expect_dec_buf_size = get_block_size(&info);
        if (_expect_dec_buf_size == -1) {
            std::stringstream ss;
            ss << "Impossible lz4 block size unless more block sizes are allowed"
               << std::string(LZ4F_getErrorName(ret));
            return Status(ss.str());
        }

        *input_bytes_read = src_size;

        src += src_size;
        src_size = input_len - src_size;

        LOG(INFO) << "lz4 block size: " << _expect_dec_buf_size;
    } 
    
    // decompress
    size_t output_len = output_max_len;
    ret = LZ4F_decompress(_dctx, (void*) output, &output_len, (void*) src, &src_size,
                          /* LZ4F_decompressOptions_t */ NULL);
    if (LZ4F_isError(ret)) {
        std::stringstream ss;
        ss << "Decompression error: " << std::string(LZ4F_getErrorName(ret));
        return Status(ss.str());
    }

    // update
    *input_bytes_read += src_size;
    *decompressed_len = output_len;
    if (ret == 0) {
        *stream_end = true;
    } else {
        *stream_end = false;
    }

    return Status::OK;
}

std::string Lz4FrameDecompressor::debug_info() {
    std::stringstream ss;
    ss << "Lz4FrameDecompressor."
       << " expect dec buf size: " << _expect_dec_buf_size
       << " Lz4 Frame Version: " << PALO_LZ4F_VERSION;
    return ss.str();
}

size_t Lz4FrameDecompressor::get_block_size(const LZ4F_frameInfo_t* info) {
    switch (info->blockSizeID) {
        case LZ4F_default:
        case LZ4F_max64KB:  return 1 << 16;
        case LZ4F_max256KB: return 1 << 18;
        case LZ4F_max1MB:   return 1 << 20;
        case LZ4F_max4MB:   return 1 << 22;
        default:
            // error
            return -1;
    }
}

// Lzop
const uint8_t LzopDecompressor::LZOP_MAGIC[9] =
    { 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a };

const uint64_t LzopDecompressor::LZOP_VERSION = 0x1030;
const uint64_t LzopDecompressor::MIN_LZO_VERSION = 0x0100;
// magic(9) + ver(2) + lib_ver(2) + ver_needed(2) + method(1)
// + lvl(1) + flags(4) + mode/mtime(12) + filename_len(1)
// without the real file name, extra field and checksum
const uint32_t LzopDecompressor::MIN_HEADER_SIZE = 34;
const uint32_t LzopDecompressor::LZO_MAX_BLOCK_SIZE = (64*1024l*1024l);

const uint32_t LzopDecompressor::CRC32_INIT_VALUE = 0;
const uint32_t LzopDecompressor::ADLER32_INIT_VALUE = 1;

const uint64_t LzopDecompressor::F_H_CRC32        = 0x00001000L;
const uint64_t LzopDecompressor::F_MASK           = 0x00003FFFL;
const uint64_t LzopDecompressor::F_OS_MASK        = 0xff000000L;
const uint64_t LzopDecompressor::F_CS_MASK        = 0x00f00000L;
const uint64_t LzopDecompressor::F_RESERVED       = ((F_MASK | F_OS_MASK | F_CS_MASK) ^ 0xffffffffL);
const uint64_t LzopDecompressor::F_MULTIPART      = 0x00000400L;
const uint64_t LzopDecompressor::F_H_FILTER       = 0x00000800L;
const uint64_t LzopDecompressor::F_H_EXTRA_FIELD  = 0x00000040L;
const uint64_t LzopDecompressor::F_CRC32_C        = 0x00000200L;
const uint64_t LzopDecompressor::F_ADLER32_C      = 0x00000002L;
const uint64_t LzopDecompressor::F_CRC32_D        = 0x00000100L;
const uint64_t LzopDecompressor::F_ADLER32_D      = 0x00000001L;

LzopDecompressor::~LzopDecompressor() {
}

Status LzopDecompressor::init() {
    return Status::OK;
}

Status LzopDecompressor::decompress(
        uint8_t* input, size_t input_len, size_t* input_bytes_read,
        uint8_t* output, size_t output_max_len,
        size_t* decompressed_len, bool* stream_end,
        size_t* more_input_bytes, size_t* more_output_bytes) {
    
    if (!_is_header_loaded) {
        // this is the first time to call lzo decompress, parse the header info first
        RETURN_IF_ERROR(parse_header_info(input, input_len, input_bytes_read, more_input_bytes));
        if (*more_input_bytes > 0) {
            return Status::OK;
        }
    }

    // LOG(INFO) << "after load header: " << *input_bytes_read;

    // read compressed block
    // compressed-block ::=
    //   <uncompressed-size>
    //   <compressed-size>
    //   <uncompressed-checksums>
    //   <compressed-checksums>
    //   <compressed-data>
    int left_input_len = input_len - *input_bytes_read;
    if (left_input_len < sizeof(uint32_t)) {
        // block is at least have uncompressed_size
        *more_input_bytes = sizeof(uint32_t) - left_input_len;
        return Status::OK;
    }

    uint8_t* block_start = input + *input_bytes_read;
    uint8_t* ptr = block_start;
    // 1. uncompressed size
    uint32_t uncompressed_size;
    ptr = get_uint32(ptr, &uncompressed_size);
    left_input_len -= sizeof(uint32_t);
    if (uncompressed_size == 0) {
        *stream_end = true;
        return Status::OK;
    }

    // 2. compressed size
    if (left_input_len < sizeof(uint32_t)) {
        *more_input_bytes = sizeof(uint32_t) - left_input_len;
        return Status::OK;
    }

    uint32_t compressed_size;
    ptr = get_uint32(ptr, &compressed_size);
    left_input_len -= sizeof(uint32_t);
    if (compressed_size > LZO_MAX_BLOCK_SIZE) {
        std::stringstream ss;
        ss << "lzo block size: " << compressed_size << " is greater than LZO_MAX_BLOCK_SIZE: "
           << LZO_MAX_BLOCK_SIZE;
        return Status(ss.str());
    }

    // 3. out checksum
    uint32_t out_checksum = 0;
    if (_header_info.output_checksum_type != CHECK_NONE) {
        if (left_input_len < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left_input_len;
            return Status::OK;
        }

        ptr = get_uint32(ptr, &out_checksum);
        left_input_len -= sizeof(uint32_t);
    }

    // 4. in checksum
    uint32_t in_checksum = 0;
    if (compressed_size < uncompressed_size && _header_info.input_checksum_type != CHECK_NONE) {
        if (left_input_len < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left_input_len;
            return Status::OK;
        }

        ptr = get_uint32(ptr, &out_checksum);
        left_input_len -= sizeof(uint32_t);
    } else {
        // If the compressed data size is equal to the uncompressed data size, then
        // the uncompressed data is stored and there is no compressed checksum.
        in_checksum = out_checksum;
    }

    // 5. checksum compressed data
    if (left_input_len < compressed_size) {
        *more_input_bytes = compressed_size - left_input_len;    
        return Status::OK;
    }
    RETURN_IF_ERROR(checksum(_header_info.input_checksum_type,
                             "compressed", in_checksum, ptr, compressed_size));

    // 6. decompress
    if (output_max_len < uncompressed_size) {
        *more_output_bytes = uncompressed_size - output_max_len;
        return Status::OK;
    }
    if (compressed_size == uncompressed_size) {
        // the data is uncompressed, just copy to the output buf
        memmove(output, ptr, compressed_size);
        ptr += compressed_size;
    } else {
        // decompress
        *decompressed_len = uncompressed_size;
        int ret = lzo1x_decompress_safe(ptr, compressed_size,
                output, reinterpret_cast<lzo_uint*>(&uncompressed_size), nullptr);
        if (ret != LZO_E_OK || uncompressed_size != *decompressed_len) {
            std::stringstream ss;
            ss << "Lzo decompression failed with ret: " << ret
               << " decompressed len: " << uncompressed_size
               << " expected: " << *decompressed_len;
            return Status(ss.str());
        }

        RETURN_IF_ERROR(checksum(_header_info.output_checksum_type, "decompressed",
                                 out_checksum, output, uncompressed_size));
        ptr += compressed_size;
    }

    // 7. peek next block's uncompressed size
    uint32_t next_uncompressed_size;
    get_uint32(ptr, &next_uncompressed_size);
    if (next_uncompressed_size == 0) {
        // 0 means current block is the last block.
        // consume this uncompressed_size to finish reading.
        ptr += sizeof(uint32_t);
    }

    // 8. done
    *stream_end = true;
    *decompressed_len = uncompressed_size;
    *input_bytes_read += ptr - block_start;

    LOG(INFO) << "finished decompress lzo block."
              << " compressed_size: " << compressed_size
              << " decompressed_len: " << *decompressed_len
              << " input_bytes_read: " << *input_bytes_read
              << " next_uncompressed_size: " << next_uncompressed_size;

    return Status::OK;
}

// file-header ::=  -- most of this information is not used.
//   <magic>
//   <version>
//   <lib-version>
//   [<version-needed>] -- present for all modern files.
//   <method>
//   <level>
//   <flags>
//   <mode>
//   <mtime>
//   <file-name>
//   <header-checksum>
//   <extra-field> -- presence indicated in flags, not currently used. 
Status LzopDecompressor::parse_header_info(uint8_t* input, size_t input_len,
                                           size_t* input_bytes_read,
                                           size_t* more_input_bytes) {
    if (input_len < MIN_HEADER_SIZE) {
        LOG(INFO) << "highly recommanded that Lzo header size is larger than " << MIN_HEADER_SIZE
                  << ", or parsing header info may failed."
                  << " only given: " << input_len;
        *more_input_bytes = MIN_HEADER_SIZE - input_len;
        return Status::OK;
    }

    uint8_t* ptr = input;
    // 1. magic
    if (memcmp(ptr, LZOP_MAGIC, sizeof(LZOP_MAGIC))) {
        std::stringstream ss;
        ss << "invalid lzo magic number";
        return Status(ss.str());
    }
    ptr += sizeof(LZOP_MAGIC);
    uint8_t* header = ptr;

    // 2. version
    ptr = get_uint16(ptr, &_header_info.version);
    if (_header_info.version > LZOP_VERSION) {
        std::stringstream ss;
        ss << "compressed with later version of lzop: " << &_header_info.version
           << " must be less than: " << LZOP_VERSION;
        return Status(ss.str());
    }

    // 3. lib version
    ptr = get_uint16(ptr, &_header_info.lib_version);
    if (_header_info.lib_version < MIN_LZO_VERSION) {
        std::stringstream ss;
        ss << "compressed with incompatible lzo version: " << &_header_info.lib_version
           << "must be at least: " << MIN_LZO_VERSION;
        return Status(ss.str());
    }

    // 4. version needed
    ptr = get_uint16(ptr, &_header_info.version_needed);
    if (_header_info.version_needed > LZOP_VERSION) {
        std::stringstream ss;
        ss << "compressed with imp incompatible lzo version: " << &_header_info.version
           << " must be at no more than: " << LZOP_VERSION;
        return Status(ss.str());
    }

    // 5. method
    ptr = get_uint8(ptr, &_header_info.method);
    if (_header_info.method < 1 || _header_info.method > 3) {
        std::stringstream ss;
        ss << "invalid compression method: " << _header_info.method;
        return Status(ss.str());
    }

    // 6. skip level
    ++ptr;

    // 7. flags
    uint32_t flags;
    ptr = get_uint32(ptr, &flags);
    if (flags & (F_RESERVED | F_MULTIPART | F_H_FILTER)) {
        std::stringstream ss;
        ss << "unsupported lzo flags: " << flags;
        return Status(ss.str());
    }
    _header_info.header_checksum_type = header_type(flags);
    _header_info.input_checksum_type = input_type(flags);
    _header_info.output_checksum_type = output_type(flags);

    // 8. skip mode and mtime
    ptr += 3 * sizeof(int32_t);

    // 9. filename
    uint8_t filename_len;
    ptr = get_uint8(ptr, &filename_len);
    
    // here we already consume (MIN_HEADER_SIZE)
    // from now we have to check left input is enough for each step
    size_t left = input_len - (ptr - input);
    if (left < filename_len) {
        *more_input_bytes = filename_len - left;
        return Status::OK;
    }

    _header_info.filename = std::string((char*) ptr, (size_t) filename_len);
    ptr += filename_len;
    left -= filename_len;

    // 10. checksum
    if (left < sizeof(uint32_t)) {
        *more_input_bytes = sizeof(uint32_t) - left;
        return Status::OK;
    }
    uint32_t expected_checksum;
    uint8_t* cur = ptr;
    ptr = get_uint32(ptr, &expected_checksum);
    uint32_t computed_checksum;
    if (_header_info.header_checksum_type == CHECK_CRC32) {
        computed_checksum = CRC32_INIT_VALUE;
        computed_checksum = lzo_crc32(computed_checksum, header, cur - header);
    } else {
        computed_checksum = ADLER32_INIT_VALUE;
        computed_checksum = lzo_adler32(computed_checksum, header, cur - header);
    }

    if (computed_checksum != expected_checksum) {
        std::stringstream ss;
        ss << "invalid header checksum: " << computed_checksum
           << " expected: " << expected_checksum;
        return Status(ss.str());
    }
    left -= sizeof(uint32_t);

    // 11. skip extra
    if (flags & F_H_EXTRA_FIELD) {
        if (left < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left;
            return Status::OK;
        }
        uint32_t extra_len;
        ptr = get_uint32(ptr, &extra_len);
        left -= sizeof(uint32_t);

        // add the checksum and the len to the total ptr size.
        if (left < sizeof(int32_t) + extra_len) {
            *more_input_bytes =  sizeof(int32_t) + extra_len - left;
            return Status::OK;
        }
        left -= sizeof(int32_t) + extra_len;
        ptr += sizeof(int32_t) + extra_len;
    }

    _header_info.header_size = ptr - input;
    *input_bytes_read = _header_info.header_size;

    _is_header_loaded = true;
    LOG(INFO) << debug_info();

    return Status::OK;
}

Status LzopDecompressor::checksum(LzoChecksum type, const std::string& source,
                                  uint32_t expected,
                                  uint8_t* ptr, size_t len) {
    uint32_t computed_checksum;
    switch (type) {
    case CHECK_NONE:
        return Status::OK;
    case CHECK_CRC32:
        computed_checksum = lzo_crc32(CRC32_INIT_VALUE, ptr, len);
        break;
    case CHECK_ADLER:
        computed_checksum = lzo_adler32(ADLER32_INIT_VALUE, ptr, len);
        break;
    default:
        std::stringstream ss;
        ss << "Invalid checksum type: " << type;
        return Status(ss.str());
    }

    if (computed_checksum != expected) {
        std::stringstream ss;
        ss << "checksum of " << source << " block failed."
           << " computed checksum: " << computed_checksum
           << " expected: " << expected;
        return Status(ss.str());
    }

    return Status::OK;
}

std::string LzopDecompressor::debug_info() {
    std::stringstream ss;
    ss << "LzopDecompressor."
       << " version: " << _header_info.version
       << " lib version: " << _header_info.lib_version
       << " version needed: " << _header_info.version_needed
       << " method: " << (uint16_t) _header_info.method
       << " filename: " << _header_info.filename
       << " header size: " << _header_info.header_size
       << " header checksum type: " << _header_info.header_checksum_type
       << " input checksum type: " << _header_info.input_checksum_type
       << " ouput checksum type: " << _header_info.output_checksum_type;
    return ss.str();
}

} // namespace
