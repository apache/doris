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

#ifndef DORIS_BE_SRC_COMMON_UTIL_COMPRESS_H
#define DORIS_BE_SRC_COMMON_UTIL_COMPRESS_H

// We need zlib.h here to declare _stream below.
#include <zlib.h>

#include "util/codec.h"
#include "runtime/mem_pool.h"

namespace doris {

// Different compression classes.  The classes all expose the same API and
// abstracts the underlying calls to the compression libraries.
// TODO: reconsider the abstracted API

class GzipCompressor : public Codec {
public:
    // Compression formats supported by the zlib library
    enum Format {
        ZLIB,
        DEFLATE,
        GZIP,
    };

    // If gzip is set then we create gzip otherwise lzip.
    GzipCompressor(Format format, MemPool* mem_pool, bool reuse_buffer);

    virtual ~GzipCompressor();

    // Returns an upper bound on the max compressed length.
    int max_compressed_len(int input_len);

    // Compresses 'input' into 'output'.  Output must be preallocated and
    // at least big enough.
    // *output_length should be called with the length of the output buffer and on return
    // is the length of the output.
    Status compress(int input_length, uint8_t* input,
                    int* output_length, uint8_t* output);

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

protected:
    // Initialize the compressor.
    virtual Status init();

private:
    Format _format;

    // Structure used to communicate with the library.
    z_stream _stream;

    // These are magic numbers from zlib.h.  Not clear why they are not defined there.
    const static int WINDOW_BITS = 15;    // Maximum window size
    const static int GZIP_CODEC = 16;     // Output Gzip.
};

class BzipCompressor : public Codec {
public:
    BzipCompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~BzipCompressor() { }

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);
    // Initialize the compressor.
    virtual Status init() {
        return Status::OK();
    }
};

class SnappyBlockCompressor : public Codec {
public:
    SnappyBlockCompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~SnappyBlockCompressor() { }

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

protected:
    // Snappy does not need initialization
    virtual Status init() {
        return Status::OK();
    }
};

class SnappyCompressor : public Codec {
public:
    SnappyCompressor(MemPool* mem_pool, bool reuse_buffer);
    virtual ~SnappyCompressor() { }

    // Process a block of data.
    virtual Status process_block(int input_length, uint8_t* input,
                                int* output_length, uint8_t** output);

    // Returns an upper bound on the max compressed length.
    int max_compressed_len(int input_len);

    // Compresses 'input' into 'output'.  Output must be preallocated and
    // at least big enough.
    // *output_length should be called with the length of the output buffer and on return
    // is the length of the output.
    Status compress(int input_length, uint8_t* input,
                    int* output_length, uint8_t* output);

protected:
    // Snappy does not need initialization
    virtual Status init() {
        return Status::OK();
    }
};

}
#endif
