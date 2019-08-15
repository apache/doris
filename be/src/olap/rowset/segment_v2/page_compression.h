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

#include <cstddef>
#include <vector>

#include "common/status.h"
#include "util/slice.h"
#include "util/faststring.h"

namespace doris {

class BlockCompressionCodec;

namespace segment_v2 {

// Helper to decompress a compressed page.
// Compressed page is composed of
// Header | CompressedData
// Header : uncompressed data length(fixed32)
// CompressedData: binary
// Usage: 
//      Slice compressed_data;
//      PageDecompressor decompressor(compressed_data, uncomrcodec);
//      RETURN_IF_ERROR(decompressor.init());
//      std::string buf;
//      buf.resize(decompressor.uncompressed_bytes());
//      RETURN_IF_ERROR(decompress_to(buf.data()));
class PageDecompressor {
public:
    PageDecompressor(const Slice& data, const BlockCompressionCodec* codec)
        : _data(data), _codec(codec) {
    }

    // Parse and validate compressed page's header.
    // Only this funciton is executed successfully, uncompressed_bytes
    // and decompress_to can be called.
    // Return error if this page is corrupt.
    Status init();

    // Get uncompressed size in bytes of this page
    size_t uncompressed_bytes() const { return _uncompressed_bytes; }
    
    // Decmopress compressed data into buf whose capacity must be greater than
    // uncompressed_bytes()
    Status decompress_to(void* buf);
private:
    Slice _data;
    const BlockCompressionCodec* _codec;
    size_t _uncompressed_bytes;
};

// Helper to build a compress page.
// Usage:
//      std::<Slice> raw_data;
//      PageCompressor compressor(codec, 0.9);
//      std::<Slice> compressed_data;
//      compressor.compress(raw_data, &compressed_data)
class PageCompressor {
public:
    PageCompressor(const BlockCompressionCodec* codec, double min_compression_ratio = 1)
        : _codec(codec), _min_compression_ratio(min_compression_ratio) {
    }

    // Try to compress input raw data into compressed page
    // according given BlockCompressionCodec. If compressed page is not
    // smaller enough than raw data, this class will return uncompressed data.
    Status compress(const std::vector<Slice>& raw_data,
                    std::vector<Slice>* compressed_data);
private:
    const BlockCompressionCodec* _codec;

    // If ratio of compressed_data to raw_data is greater than min_compression_ratio,
    // compress will return orgin data
    double _min_compression_ratio;

    // used to store compressed data
    faststring _buf;
};

}
}
