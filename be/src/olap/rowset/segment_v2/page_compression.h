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

// Utility class for parsing and decompressing compressed page.
// Format of compressed page := Data, UncompressedSize(fixed32)
// When sizeof(Data) == UncompressedSize, it means Data is stored in uncompressed
// form, thus decompression is not needed.
// Otherwise Data is in compressed form and should be decompressed.
// The type of compression codec for Data is stored elsewhere and should
// be passed into the constructor.
// Usage example:
//     // page_size refers to page read from storage
//     PageDecompressor decompressor(page_slice, codec);
//     // points to decompressed Data of the page (without footer)
//     Slice uncompressed_slice;
//     RETURN_IF_ERROR(decompressor.decompress_to(&uncompressed_slice));
//     // use uncompressed_slice
//     // we have a new buffer for decompressed page
//     if (uncompressed_slice.data != page_slice.data) {
//         delete[] uncompressed_bytes.data;
//     }
class PageDecompressor {
public:
    PageDecompressor(const Slice& data, const BlockCompressionCodec* codec)
        : _data(data), _codec(codec) {
    }
    
    // This client will set uncompress content to input param.
    // In normal case(content.data != input_data.data) client should
    // call delete[] content.data to free heap memory. However
    // when the data is not compressed, this function will return input data
    // directly. In this case content.data == input_data.data,
    // client should not free content.
    Status decompress_to(Slice* content);
private:
    Slice _data;
    const BlockCompressionCodec* _codec;
};

// Helper to build a compress page.
// Usage:
//      std::vector<Slice> raw_data;
//      PageCompressor compressor(codec, 0.1);
//      std::vector<Slice> compressed_data;
//      compressor.compress(raw_data, &compressed_data)
class PageCompressor {
public:
    PageCompressor(const BlockCompressionCodec* codec, double min_space_saving = 0.1)
        : _codec(codec), _min_space_saving(min_space_saving) {
    }

    // Try to compress input raw data into compressed page
    // according given BlockCompressionCodec. If compressed page is not
    // smaller enough than raw data, this class will return uncompressed data.
    Status compress(const std::vector<Slice>& raw_data,
                    std::vector<Slice>* compressed_data);
private:
    const BlockCompressionCodec* _codec;

    // If space saving is lower than _min_space_saving, compress will return origin data
    double _min_space_saving;

    // used to store compressed data
    faststring _buf;
};

}
}
