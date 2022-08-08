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

#include "level_decoder.h"

#include "util/bit_util.h"
#include "util/coding.h"

doris::Status doris::vectorized::LevelDecoder::init(doris::Slice* slice,
                                                    tparquet::Encoding::type encoding,
                                                    doris::vectorized::level_t max_level,
                                                    uint32_t num_levels) {
    _encoding = encoding;
    _bit_width = BitUtil::log2(max_level + 1);
    _max_level = max_level;
    _num_levels = num_levels;
    switch (encoding) {
    case tparquet::Encoding::RLE: {
        if (slice->size < 4) {
            return Status::Corruption("Wrong parquet level format");
        }

        uint8_t* data = (uint8_t*)slice->data;
        uint32_t num_bytes = decode_fixed32_le(data);
        if (num_bytes > slice->size - 4) {
            return Status::Corruption("Wrong parquet level format");
        }
        _rle_decoder = RleDecoder<level_t>(data + 4, num_bytes, _bit_width);

        slice->data += 4 + num_bytes;
        slice->size -= 4 + num_bytes;
        break;
    }
    case tparquet::Encoding::BIT_PACKED: {
        uint32_t num_bits = num_levels * _bit_width;
        uint32_t num_bytes = BitUtil::RoundUpNumBytes(num_bits);
        if (num_bytes > slice->size) {
            return Status::Corruption("Wrong parquet level format");
        }
        _bit_packed_decoder = BitReader((uint8_t*)slice->data, num_bytes);

        slice->data += num_bytes;
        slice->size -= num_bytes;
        break;
    }
    default:
        return Status::IOError("Unsupported encoding for parquet level");
    }
    return Status::OK();
}

size_t doris::vectorized::LevelDecoder::get_levels(doris::vectorized::level_t* levels, size_t n) {
    if (_encoding == tparquet::Encoding::RLE) {
        n = std::min((size_t)_num_levels, n);
        auto num_decoded = _rle_decoder.get_values(levels, n);
        _num_levels -= num_decoded;
        return num_decoded;
    } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
        // TODO(gaoxin): BIT_PACKED encoding
    }
    return 0;
}
