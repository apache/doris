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

#include "olap/rowset/segment_v2/page_compression.h"

#include "gutil/strings/substitute.h"
#include "util/block_compression.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status PageDecompressor::decompress_to(Slice* content) {
    if (_data.size < 4) {
        return Status::Corruption(
            Substitute("Compressed page's size is too small, size=$0, needed=$1",
                       _data.size, 4));
    }
    // decode uncompressed_bytes from footer
    uint32_t uncompressed_bytes = decode_fixed32_le((uint8_t*)_data.data + _data.size - 4);

    Slice compressed_slice(_data.data, _data.size - 4);
    if (compressed_slice.size == uncompressed_bytes) {
        // If compressed_slice's size is equal with _uncompressed_bytes, it means
        // compressor store this directly without compression. So we just copy
        // this to buf and return.
        *content = compressed_slice;
        return Status::OK();
    }
    std::unique_ptr<char[]> buf(new char[uncompressed_bytes]);
    
    Slice uncompressed_slice(buf.get(), uncompressed_bytes);
    RETURN_IF_ERROR(_codec->decompress(compressed_slice, &uncompressed_slice));
    if (uncompressed_slice.size != uncompressed_bytes) {
        // If size after decompress didn't match recorded size, we think this
        // page is corrupt.
        return Status::Corruption(
            Substitute("Uncompressed size not match, record=$0 vs decompress=$1",
                       uncompressed_bytes, uncompressed_slice.size));
    }
    *content = Slice(buf.release(), uncompressed_bytes);
    return Status::OK();
}

Status PageCompressor::compress(const std::vector<Slice>& raw_data,
                                std::vector<Slice>* compressed_slices) {
    size_t uncompressed_bytes = Slice::compute_total_size(raw_data);
    size_t max_compressed_bytes = _codec->max_compressed_len(uncompressed_bytes);
    _buf.resize(max_compressed_bytes + 4);
    Slice compressed_slice(_buf.data(), max_compressed_bytes);
    RETURN_IF_ERROR(_codec->compress(raw_data, &compressed_slice));

    double space_saving = 1.0 - (double)compressed_slice.size / uncompressed_bytes;
    if (compressed_slice.size >= uncompressed_bytes || // use integer to make definite
            space_saving < _min_space_saving) {
        // If space saving is not higher enough we just copy uncompressed
        // data to avoid decompression CPU cost
        for (auto& slice : raw_data) {
            compressed_slices->push_back(slice);
        }

        // encode uncompressed_bytes into footer of compressed value
        encode_fixed32_le((uint8_t*)_buf.data(), uncompressed_bytes);
        compressed_slices->emplace_back(_buf.data(), 4);
        return Status::OK();
    }
    // encode uncompressed_bytes into footer of compressed value
    encode_fixed32_le((uint8_t*)_buf.data() + compressed_slice.size, uncompressed_bytes);
    // return compressed data to client
    compressed_slices->emplace_back(_buf.data(), 4 + compressed_slice.size);

    return Status::OK();
}

}
}
