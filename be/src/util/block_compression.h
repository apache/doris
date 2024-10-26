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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>

#include <cstddef>
#include <vector>

#include "common/status.h"
#include "util/slice.h"

namespace doris {
class faststring;

namespace segment_v2 {
enum CompressionTypePB : int;
} // namespace segment_v2

// This class is used to encapsulate Compression/Decompression algorithm.
// This class only used to compress a block data, which means all data
// should given when call compress or decompress. This class don't handle
// stream compression.
//
// NOTICE!! BlockCompressionCodec is NOT thread safe, it should NOT be shared by threads
//

// max compression reuse buffer size
// if max_compress_len is bigger than this, don't use faststring in context
const static int MAX_COMPRESSION_BUFFER_SIZE_FOR_REUSE = 1024 * 1024 * 8;
class BlockCompressionCodec {
public:
    virtual ~BlockCompressionCodec() {}

    virtual Status init() { return Status::OK(); }

    // This function will compress input data into output.
    // output should be preallocated, and its capacity must be large enough
    // for compressed input, which can be get through max_compressed_len function.
    // Size of compressed data will be set in output's size.
    virtual Status compress(const Slice& input, faststring* output) = 0;

    // Default implementation will merge input list into a big buffer and call
    // compress(Slice) to finish compression. If compression type support digesting
    // slice one by one, it should reimplement this function.
    virtual Status compress(const std::vector<Slice>& input, size_t uncompressed_size,
                            faststring* output);

    // Decompress input data into output, output's capacity should be large enough
    // for decompressed data.
    // Size of decompressed data will be set in output's size.
    virtual Status decompress(const Slice& input, Slice* output) = 0;

    // Returns an upper bound on the max compressed length.
    virtual size_t max_compressed_len(size_t len) = 0;

    virtual bool exceed_max_compress_len(size_t uncompressed_size);
};

// Get a BlockCompressionCodec through type.
// Return Status::OK if a valid codec is found. If codec is null, it means it is
// NO_COMPRESSION. If codec is not null, user can use it to compress/decompress
// data.
//
// NOTICE!! BlockCompressionCodec is NOT thread safe, it should NOT be shared by threads
//
// Return not OK, if error happens.
Status get_block_compression_codec(segment_v2::CompressionTypePB type,
                                   BlockCompressionCodec** codec);

Status get_block_compression_codec(tparquet::CompressionCodec::type parquet_codec,
                                   BlockCompressionCodec** codec);

// TODO: refactor code as CompressionOutputStream and CompressionInputStream
Status get_block_compression_codec(TFileCompressType::type type, BlockCompressionCodec** codec);

} // namespace doris
