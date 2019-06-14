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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DECOMPRESS_H
#define DORIS_BE_SRC_COMMON_UTIL_DECOMPRESS_H

// We need zlib.h here to declare _stream below.
#include <zlib.h>

#include "util/codec.h"
#include "runtime/mem_pool.h"

namespace doris {

class GzipDecompressor : public Codec {
public:
    GzipDecompressor(MemPool* mem_pool, bool reuse_buffer, bool is_deflate);
    virtual ~GzipDecompressor();

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

protected:
    // Initialize the decompressor.
    virtual Status init();

private:
    // If set assume deflate format, otherwise zlib or gzip
    bool _is_deflate;

    z_stream _stream;

    // These are magic numbers from zlib.h.  Not clear why they are not defined there.
    const static int WINDOW_BITS = 15;    // Maximum window size
    const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
};

class BzipDecompressor : public Codec {
public:
    BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~BzipDecompressor() { }

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);
protected:
    // Bzip does not need initialization
    virtual Status init() {
        return Status::OK();
    }
};

class SnappyDecompressor : public Codec {
public:
    SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~SnappyDecompressor() { }

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

protected:
    // Snappy does not need initialization
    virtual Status init() {
        return Status::OK();
    }

};

class SnappyBlockDecompressor : public Codec {
public:
    SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~SnappyBlockDecompressor() { }

    //Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

protected:
    // Snappy does not need initialization
    virtual Status init() {
        return Status::OK();
    }
};

}
#endif
