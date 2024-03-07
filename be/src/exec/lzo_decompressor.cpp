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
#include "olap/utils.h"
#include "orc/Exceptions.hh"
#include "util/crc32c.h"

namespace orc {
/**
 * Decompress the bytes in to the output buffer.
 * @param inputAddress the start of the input
 * @param inputLimit one past the last byte of the input
 * @param outputAddress the start of the output buffer
 * @param outputLimit one past the last byte of the output buffer
 * @result the number of bytes decompressed
 */
uint64_t lzoDecompress(const char* inputAddress, const char* inputLimit, char* outputAddress,
                       char* outputLimit);
} // namespace orc

namespace doris {

// Lzop
const uint8_t LzopDecompressor::LZOP_MAGIC[9] = {0x89, 0x4c, 0x5a, 0x4f, 0x00,
                                                 0x0d, 0x0a, 0x1a, 0x0a};

const uint64_t LzopDecompressor::LZOP_VERSION = 0x1040;
const uint64_t LzopDecompressor::MIN_LZO_VERSION = 0x0100;
// magic(9) + ver(2) + lib_ver(2) + ver_needed(2) + method(1)
// + lvl(1) + flags(4) + mode/mtime(12) + filename_len(1)
// without the real file name, extra field and checksum
const uint32_t LzopDecompressor::MIN_HEADER_SIZE = 34;
const uint32_t LzopDecompressor::LZO_MAX_BLOCK_SIZE = (64 * 1024l * 1024l);

const uint32_t LzopDecompressor::CRC32_INIT_VALUE = 0;
const uint32_t LzopDecompressor::ADLER32_INIT_VALUE = 1;

const uint64_t LzopDecompressor::F_H_CRC32 = 0x00001000L;
const uint64_t LzopDecompressor::F_MASK = 0x00003FFFL;
const uint64_t LzopDecompressor::F_OS_MASK = 0xff000000L;
const uint64_t LzopDecompressor::F_CS_MASK = 0x00f00000L;
const uint64_t LzopDecompressor::F_RESERVED = ((F_MASK | F_OS_MASK | F_CS_MASK) ^ 0xffffffffL);
const uint64_t LzopDecompressor::F_MULTIPART = 0x00000400L;
const uint64_t LzopDecompressor::F_H_FILTER = 0x00000800L;
const uint64_t LzopDecompressor::F_H_EXTRA_FIELD = 0x00000040L;
const uint64_t LzopDecompressor::F_CRC32_C = 0x00000200L;
const uint64_t LzopDecompressor::F_ADLER32_C = 0x00000002L;
const uint64_t LzopDecompressor::F_CRC32_D = 0x00000100L;
const uint64_t LzopDecompressor::F_ADLER32_D = 0x00000001L;

Status LzopDecompressor::init() {
    return Status::OK();
}

Status LzopDecompressor::decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                                    uint8_t* output, size_t output_max_len,
                                    size_t* decompressed_len, bool* stream_end,
                                    size_t* more_input_bytes, size_t* more_output_bytes) {
    if (!_is_header_loaded) {
        // this is the first time to call lzo decompress, parse the header info first
        RETURN_IF_ERROR(parse_header_info(input, input_len, input_bytes_read, more_input_bytes));
        if (*more_input_bytes > 0) {
            return Status::OK();
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
        return Status::OK();
    }

    uint8_t* block_start = input + *input_bytes_read;
    uint8_t* ptr = block_start;
    // 1. uncompressed size
    uint32_t uncompressed_size;
    ptr = get_uint32(ptr, &uncompressed_size);
    left_input_len -= sizeof(uint32_t);
    if (uncompressed_size == 0) {
        *stream_end = true;
        return Status::OK();
    }

    // 2. compressed size
    if (left_input_len < sizeof(uint32_t)) {
        *more_input_bytes = sizeof(uint32_t) - left_input_len;
        return Status::OK();
    }

    uint32_t compressed_size;
    ptr = get_uint32(ptr, &compressed_size);
    left_input_len -= sizeof(uint32_t);
    if (compressed_size > LZO_MAX_BLOCK_SIZE) {
        std::stringstream ss;
        ss << "lzo block size: " << compressed_size
           << " is greater than LZO_MAX_BLOCK_SIZE: " << LZO_MAX_BLOCK_SIZE;
        return Status::InternalError(ss.str());
    }

    // 3. out checksum
    uint32_t out_checksum = 0;
    if (_header_info.output_checksum_type != CHECK_NONE) {
        if (left_input_len < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left_input_len;
            return Status::OK();
        }

        ptr = get_uint32(ptr, &out_checksum);
        left_input_len -= sizeof(uint32_t);
    }

    // 4. in checksum
    uint32_t in_checksum = 0;
    if (compressed_size < uncompressed_size && _header_info.input_checksum_type != CHECK_NONE) {
        if (left_input_len < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left_input_len;
            return Status::OK();
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
        return Status::OK();
    }
    RETURN_IF_ERROR(checksum(_header_info.input_checksum_type, "compressed", in_checksum, ptr,
                             compressed_size));

    // 6. decompress
    if (output_max_len < uncompressed_size) {
        *more_output_bytes = uncompressed_size - output_max_len;
        return Status::OK();
    }
    if (compressed_size == uncompressed_size) {
        // the data is uncompressed, just copy to the output buf
        memmove(output, ptr, compressed_size);
        ptr += compressed_size;
    } else {
        try {
            *decompressed_len =
                    orc::lzoDecompress((const char*)ptr, (const char*)(ptr + compressed_size),
                                       (char*)output, (char*)(output + uncompressed_size));
        } catch (const orc::ParseError& err) {
            std::stringstream ss;
            ss << "Lzo decompression failed: " << err.what();
            return Status::InternalError(ss.str());
        }

        RETURN_IF_ERROR(checksum(_header_info.output_checksum_type, "decompressed", out_checksum,
                                 output, *decompressed_len));
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

    return Status::OK();
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
                                           size_t* input_bytes_read, size_t* more_input_bytes) {
    if (input_len < MIN_HEADER_SIZE) {
        LOG(INFO) << "highly recommanded that Lzo header size is larger than " << MIN_HEADER_SIZE
                  << ", or parsing header info may failed."
                  << " only given: " << input_len;
        *more_input_bytes = MIN_HEADER_SIZE - input_len;
        return Status::OK();
    }

    uint8_t* ptr = input;
    // 1. magic
    if (memcmp(ptr, LZOP_MAGIC, sizeof(LZOP_MAGIC))) {
        std::stringstream ss;
        ss << "invalid lzo magic number";
        return Status::InternalError(ss.str());
    }
    ptr += sizeof(LZOP_MAGIC);
    uint8_t* header = ptr;

    // 2. version
    ptr = get_uint16(ptr, &_header_info.version);
    if (_header_info.version > LZOP_VERSION) {
        std::stringstream ss;
        ss << "compressed with later version of lzop: " << &_header_info.version
           << " must be less than: " << LZOP_VERSION;
        return Status::InternalError(ss.str());
    }

    // 3. lib version
    ptr = get_uint16(ptr, &_header_info.lib_version);
    if (_header_info.lib_version < MIN_LZO_VERSION) {
        std::stringstream ss;
        ss << "compressed with incompatible lzo version: " << &_header_info.lib_version
           << "must be at least: " << MIN_LZO_VERSION;
        return Status::InternalError(ss.str());
    }

    // 4. version needed
    ptr = get_uint16(ptr, &_header_info.version_needed);
    if (_header_info.version_needed > LZOP_VERSION) {
        std::stringstream ss;
        ss << "compressed with imp incompatible lzo version: " << &_header_info.version
           << " must be at no more than: " << LZOP_VERSION;
        return Status::InternalError(ss.str());
    }

    // 5. method
    ptr = get_uint8(ptr, &_header_info.method);
    if (_header_info.method < 1 || _header_info.method > 3) {
        std::stringstream ss;
        ss << "invalid compression method: " << _header_info.method;
        return Status::InternalError(ss.str());
    }

    // 6. unsupported level: 7, 8, 9
    uint8_t level;
    ptr = get_uint8(ptr, &level);
    if (level > 6) {
        std::stringstream ss;
        ss << "unsupported lzo level: " << level;
        return Status::InternalError(ss.str());
    }

    // 7. flags
    uint32_t flags;
    ptr = get_uint32(ptr, &flags);
    if (flags & (F_RESERVED | F_MULTIPART | F_H_FILTER)) {
        std::stringstream ss;
        ss << "unsupported lzo flags: " << flags;
        return Status::InternalError(ss.str());
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
        return Status::OK();
    }

    _header_info.filename = std::string((char*)ptr, (size_t)filename_len);
    ptr += filename_len;
    left -= filename_len;

    // 10. checksum
    if (left < sizeof(uint32_t)) {
        *more_input_bytes = sizeof(uint32_t) - left;
        return Status::OK();
    }
    uint32_t expected_checksum;
    uint8_t* cur = ptr;
    ptr = get_uint32(ptr, &expected_checksum);
    uint32_t computed_checksum;
    if (_header_info.header_checksum_type == CHECK_CRC32) {
        computed_checksum = CRC32_INIT_VALUE;
        computed_checksum = crc32c::Extend(computed_checksum, (const char*)header, cur - header);
    } else {
        computed_checksum = ADLER32_INIT_VALUE;
        computed_checksum = olap_adler32(computed_checksum, (const char*)header, cur - header);
    }

    if (computed_checksum != expected_checksum) {
        std::stringstream ss;
        ss << "invalid header checksum: " << computed_checksum
           << " expected: " << expected_checksum;
        return Status::InternalError(ss.str());
    }
    left -= sizeof(uint32_t);

    // 11. skip extra
    if (flags & F_H_EXTRA_FIELD) {
        if (left < sizeof(uint32_t)) {
            *more_input_bytes = sizeof(uint32_t) - left;
            return Status::OK();
        }
        uint32_t extra_len;
        ptr = get_uint32(ptr, &extra_len);
        left -= sizeof(uint32_t);

        // add the checksum and the len to the total ptr size.
        if (left < sizeof(int32_t) + extra_len) {
            *more_input_bytes = sizeof(int32_t) + extra_len - left;
            return Status::OK();
        }
        left -= sizeof(int32_t) + extra_len;
        ptr += sizeof(int32_t) + extra_len;
    }

    _header_info.header_size = ptr - input;
    *input_bytes_read = _header_info.header_size;

    _is_header_loaded = true;
    LOG(INFO) << debug_info();

    return Status::OK();
}

Status LzopDecompressor::checksum(LzoChecksum type, const std::string& source, uint32_t expected,
                                  uint8_t* ptr, size_t len) {
    uint32_t computed_checksum;
    switch (type) {
    case CHECK_NONE:
        return Status::OK();
    case CHECK_CRC32:
        computed_checksum = crc32c::Extend(CRC32_INIT_VALUE, (const char*)ptr, len);
        break;
    case CHECK_ADLER:
        computed_checksum = olap_adler32(ADLER32_INIT_VALUE, (const char*)ptr, len);
        break;
    default:
        std::stringstream ss;
        ss << "Invalid checksum type: " << type;
        return Status::InternalError(ss.str());
    }

    if (computed_checksum != expected) {
        std::stringstream ss;
        ss << "checksum of " << source << " block failed."
           << " computed checksum: " << computed_checksum << " expected: " << expected;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

std::string LzopDecompressor::debug_info() {
    std::stringstream ss;
    ss << "LzopDecompressor."
       << " version: " << _header_info.version << " lib version: " << _header_info.lib_version
       << " version needed: " << _header_info.version_needed
       << " method: " << (uint16_t)_header_info.method << " filename: " << _header_info.filename
       << " header size: " << _header_info.header_size
       << " header checksum type: " << _header_info.header_checksum_type
       << " input checksum type: " << _header_info.input_checksum_type
       << " output checksum type: " << _header_info.output_checksum_type;
    return ss.str();
}

} // namespace doris
