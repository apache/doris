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
#include <lz4/lz4.h>
#include <lz4/lz4frame.h>
#include <lz4/lz4hc.h>
#include <snappy.h>
#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// TODO: move to common header
enum CompressType { UNCOMPRESSED, GZIP, DEFLATE, BZIP2, LZ4FRAME, LZOP, LZ4BLOCK, SNAPPYBLOCK };

class Compressor {
public:
    virtual ~Compressor() = default;
    // implement in derived class
    // input(in):               buf where compress begin
    // input_len(in):           max length of input buf
    // input_bytes_read(out):   bytes which is consumed by compressor
    // output(out):             buf where to save compressed data
    // output_max_len(in):      max length of output buf
    // compressed_len(out):   compressed data size in output buf
    // stream_end(out):         true if reach the and of stream,
    //                          or normally finished compressing entire block
    // more_input_bytes(out):   compressor need more bytes to consume
    // more_output_bytes(out):  compressor need more space to save compressed data
    //
    // input and output buf should be allocated and released outside
    virtual Status compress(uint8_t* input, size_t input_len, size_t* input_bytes_read,
                            uint8_t* output, size_t output_max_len, size_t* compressed_len,
                            bool* stream_end, size_t* more_input_bytes,
                            size_t* more_output_bytes) = 0;

    static Status create_compressor(CompressType type, std::unique_ptr<Compressor>* compressor);

    static Status create_compressor(TFileCompressType::type type,
                                    std::unique_ptr<Compressor>* compressor);

    static Status create_compressor(TFileFormatType::type type,
                                    std::unique_ptr<Compressor>* compressor);

    virtual std::string debug_info();

    CompressType get_type() { return _ctype; }

protected:
    virtual Status init() = 0;

    // static uint32_t _read_int32(uint8_t* buf);

    Compressor(CompressType ctype) : _ctype(ctype) {}

    CompressType _ctype;
};

class GzipCompressor : public Compressor {
public:
    ~GzipCompressor() override;

    Status compress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_max_len, size_t* compressed_len, bool* stream_end,
                    size_t* more_input_bytes, size_t* more_output_bytes) override;

    std::string debug_info() override;

private:
    friend class Compressor;
    GzipCompressor(bool is_deflate);
    Status init() override;

    bool _is_deflate;

    z_stream _z_strm;

    // These are magic numbers from zlib.h.  Not clear why they are not defined there.
    const static int WINDOW_BITS = 15;  // Maximum window size
    const static int DETECT_CODEC = 32; // Determine if this is libz or gzip from header.
};

class Bzip2Compressor : public Compressor {
public:
    ~Bzip2Compressor() override;

    Status compress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_max_len, size_t* compressed_len, bool* stream_end,
                    size_t* more_input_bytes, size_t* more_output_bytes) override;

    std::string debug_info() override;

private:
    friend class Compressor;
    Bzip2Compressor();
    Status init() override;

    bz_stream _bz_strm;
};

class Lz4FrameCompressor : public Compressor {
public:
    ~Lz4FrameCompressor() override;

    Status compress(uint8_t* input, size_t input_len, size_t* input_bytes_read, uint8_t* output,
                    size_t output_max_len, size_t* compressed_len, bool* stream_end,
                    size_t* more_input_bytes, size_t* more_output_bytes) override;

    std::string debug_info() override;

private:
    friend class Compressor;
    Lz4FrameCompressor();
    Status init() override;

    LZ4F_cctx* _cctx = nullptr;
    size_t _expect_dec_buf_size;
    const static unsigned DORIS_LZ4F_VERSION;
};

} // namespace doris