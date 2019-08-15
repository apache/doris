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

Status PageDecompressor::init() {
    if (_data.size < 4) {
        return Status::Corruption(
            Substitute("Compressed page's size is too small, size=$0, needed=$1",
                       _data.size, 4));
    }
    _uncompressed_bytes = decode_fixed32_le((uint8_t*)_data.data);
    return Status::OK();
}

Status PageDecompressor::decompress_to(void* buf) {
    Slice compressed_slice(_data.data + 4, _data.size - 4);
    if (compressed_slice.size == _uncompressed_bytes) {
        // If compressed_slice's size is equal with _uncompressed_bytes, it means
        // compressor store this directly without compression. So we just copy
        // this to buf and return.
        memcpy(buf, compressed_slice.data, _uncompressed_bytes);
        return Status::OK();
    }
    
    Slice uncompressed_data((char*)buf, _uncompressed_bytes);
    RETURN_IF_ERROR(_codec->decompress(compressed_slice, &uncompressed_data));
    if (uncompressed_data.size != _uncompressed_bytes) {
        // If size after decompress didn't match recorded size, we think this
        // page is corrupt.
        return Status::Corruption(
            Substitute("Uncompressed size not match, record=$0 vs decompress=$1",
                       _uncompressed_bytes, uncompressed_data.size));
    }
    return Status::OK();
}

Status PageCompressor::compress(const std::vector<Slice>& raw_data,
                                std::vector<Slice>* compressed_data) {
    size_t uncompressed_bytes = Slice::compute_total_size(raw_data);
    size_t max_compressed_bytes = _codec->max_compressed_len(uncompressed_bytes);
    _buf.resize(max_compressed_bytes + 4);
    Slice compressed_slice(_buf.data() + 4, max_compressed_bytes);
    RETURN_IF_ERROR(_codec->compress(raw_data, &compressed_slice));
    double compression_ratio = (double)compressed_slice.size / uncompressed_bytes;
    if (compressed_slice.size >= uncompressed_bytes ||
        compression_ratio > _min_compression_ratio) {
        // If compression ration is not lower enough we just copy uncompressed
        // data to avoid decompression CPU cost
        encode_fixed32_le((uint8_t*)_buf.data(), uncompressed_bytes);
        compressed_data->emplace_back(_buf.data(), 4);
        for (auto& slice : raw_data) {
            compressed_data->push_back(slice);
        }
        return Status::OK();
    }
    // return compressed data to client
    encode_fixed32_le((uint8_t*)_buf.data(), uncompressed_bytes);
    compressed_data->emplace_back(_buf.data(), 4 + compressed_slice.size);

    return Status::OK();
}

}
}
