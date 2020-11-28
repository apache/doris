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

#include <bzlib.h>
#include <lz4/lz4frame.h>
#include <zlib.h>

#ifdef DORIS_WITH_LZO
#include <lzo/lzo1x.h>
#include <lzo/lzoconf.h>
#endif

#include "common/status.h"

namespace doris {

enum CompressType { UNCOMPRESSED, GZIP, DEFLATE, BZIP2, LZ4FRAME, LZOP };

class Decompressor {
public:
    virtual ~Decompressor();

    // implement in derived class
    // input(in):               buf where decompress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by decompressor
    // output(out):             buf where to save decompressed data
    // output_max_len(in):      max length of output buf
    // decompressed_len(out):   decompressed data size in output buf
    // stream_end(out):         true if reach the and of stream,
    //                          or normally finished decompressing entire block
    // more_input_bytes(out):   decompressor need more bytes to consume
    // more_output_bytes(out):  decompressor need more space to save decompressed data
    //
    // input and output buf should be allocated and released outside
    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                              uint8_t* output, size_t output_max_len, size_t* decompressed_len,
                              bool* stream_end, size_t* more_input_bytes,
                              size_t* more_output_bytes) = 0;

public:
    static Status create_decompressor(CompressType type, Decompressor** decompressor);

    virtual std::string debug_info();

    CompressType get_type() { return _ctype; }

protected:
    virtual Status init() = 0;

    Decompressor(CompressType ctype) : _ctype(ctype) {}

    CompressType _ctype;
};

class GzipDecompressor : public Decompressor {
public:
    virtual ~GzipDecompressor();

    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                              uint8_t* output, size_t output_max_len, size_t* decompressed_len,
                              bool* stream_end, size_t* more_input_bytes,
                              size_t* more_output_bytes) override;

    virtual std::string debug_info() override;

private:
    friend class Decompressor;
    GzipDecompressor(bool is_deflate);
    virtual Status init() override;

private:
    bool _is_deflate;

    z_stream _z_strm;

    // These are magic numbers from zlib.h.  Not clear why they are not defined there.
    const static int WINDOW_BITS = 15;  // Maximum window size
    const static int DETECT_CODEC = 32; // Determine if this is libz or gzip from header.
};

class Bzip2Decompressor : public Decompressor {
public:
    virtual ~Bzip2Decompressor();

    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                              uint8_t* output, size_t output_max_len, size_t* decompressed_len,
                              bool* stream_end, size_t* more_input_bytes,
                              size_t* more_output_bytes) override;

    virtual std::string debug_info() override;

private:
    friend class Decompressor;
    Bzip2Decompressor() : Decompressor(CompressType::BZIP2) {}
    virtual Status init() override;

private:
    bz_stream _bz_strm;
};

class Lz4FrameDecompressor : public Decompressor {
public:
    virtual ~Lz4FrameDecompressor();

    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                              uint8_t* output, size_t output_max_len, size_t* decompressed_len,
                              bool* stream_end, size_t* more_input_bytes,
                              size_t* more_output_bytes) override;

    virtual std::string debug_info() override;

private:
    friend class Decompressor;
    Lz4FrameDecompressor() : Decompressor(CompressType::LZ4FRAME) {}
    virtual Status init() override;

    size_t get_block_size(const LZ4F_frameInfo_t* info);

private:
    LZ4F_dctx* _dctx;
    size_t _expect_dec_buf_size;
    const static unsigned DORIS_LZ4F_VERSION;
};

#ifdef DORIS_WITH_LZO
class LzopDecompressor : public Decompressor {
public:
    virtual ~LzopDecompressor();

    virtual Status decompress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                              uint8_t* output, size_t output_max_len, size_t* decompressed_len,
                              bool* stream_end, size_t* more_input_bytes,
                              size_t* more_output_bytes) override;

    virtual std::string debug_info() override;

private:
    friend class Decompressor;
    LzopDecompressor()
            : Decompressor(CompressType::LZOP), _header_info({0}), _is_header_loaded(false) {}
    virtual Status init() override;

private:
    enum LzoChecksum { CHECK_NONE, CHECK_CRC32, CHECK_ADLER };

private:
    inline uint8_t* get_uint8(uint8_t* ptr, uint8_t* value) {
        *value = *ptr;
        return ptr + sizeof(uint8_t);
    }

    inline uint8_t* get_uint16(uint8_t* ptr, uint16_t* value) {
        *value = *ptr << 8 | *(ptr + 1);
        return ptr + sizeof(uint16_t);
    }

    inline uint8_t* get_uint32(uint8_t* ptr, uint32_t* value) {
        *value = (*ptr << 24) | (*(ptr + 1) << 16) | (*(ptr + 2) << 8) | *(ptr + 3);
        return ptr + sizeof(uint32_t);
    }

    inline LzoChecksum header_type(int flags) {
        return (flags & F_H_CRC32) ? CHECK_CRC32 : CHECK_ADLER;
    }

    inline LzoChecksum input_type(int flags) {
        return (flags & F_CRC32_C) ? CHECK_CRC32 : (flags & F_ADLER32_C) ? CHECK_ADLER : CHECK_NONE;
    }

    inline LzoChecksum output_type(int flags) {
        return (flags & F_CRC32_D) ? CHECK_CRC32 : (flags & F_ADLER32_D) ? CHECK_ADLER : CHECK_NONE;
    }

    Status parse_header_info(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                             size_t* more_bytes_needed);

    Status checksum(LzoChecksum type, const std::string& source, uint32_t expected, uint8_t* ptr,
                    size_t len);

private:
    // lzop header info
    struct HeaderInfo {
        uint16_t version;
        uint16_t lib_version;
        uint16_t version_needed;
        uint8_t method;
        std::string filename;
        uint32_t header_size;
        LzoChecksum header_checksum_type;
        LzoChecksum input_checksum_type;
        LzoChecksum output_checksum_type;
    };

    struct HeaderInfo _header_info;

    // true if header is decompressed and loaded
    bool _is_header_loaded;

private:
    const static uint8_t LZOP_MAGIC[9];
    const static uint64_t LZOP_VERSION;
    const static uint64_t MIN_LZO_VERSION;
    const static uint32_t MIN_HEADER_SIZE;
    const static uint32_t LZO_MAX_BLOCK_SIZE;

    const static uint32_t CRC32_INIT_VALUE;
    const static uint32_t ADLER32_INIT_VALUE;

    const static uint64_t F_H_CRC32;
    const static uint64_t F_MASK;
    const static uint64_t F_OS_MASK;
    const static uint64_t F_CS_MASK;
    const static uint64_t F_RESERVED;
    const static uint64_t F_MULTIPART;
    const static uint64_t F_H_FILTER;
    const static uint64_t F_H_EXTRA_FIELD;
    const static uint64_t F_CRC32_C;
    const static uint64_t F_ADLER32_C;
    const static uint64_t F_CRC32_D;
    const static uint64_t F_ADLER32_D;
};
#endif // DORIS_WITH_LZO

} // namespace doris
