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

#include "olap/rowset/segment_v2/bitshuffle_page.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {
namespace segment_v2 {

void abort_with_bitshuffle_error(int64_t val) {
    switch (val) {
    case -1:
      LOG(FATAL) << "Failed to allocate memory";
      break;
    case -11:
      LOG(FATAL) << "Missing SSE";
      break;
    case -12:
      LOG(FATAL) << "Missing AVX";
      break;
    case -80:
      LOG(FATAL) << "Input size not a multiple of 8";
      break;
    case -81:
      LOG(FATAL) << "block_size not multiple of 8";
      break;
    case -91:
      LOG(FATAL) << "Decompression error, wrong number of bytes processed";
      break;
    default:
      LOG(FATAL) << "Error internal to compression routine";
    }
}

template<>
Slice BitshufflePageBuilder<OLAP_FIELD_TYPE_UNSIGNED_INT>::finish(rowid_t page_first_rowid) {
    uint32_t max_value = 0;
    for (int i = 0; i < _count; i++) {
        max_value = std::max(max_value, cell(i));
    }

    // Shrink the block of UINT32 to block of UINT8 or UINT16 whenever possible and
    // set the header information accordingly, so that the decoder can recover the
    // encoded data.
    Slice ret;
    if (max_value <= std::numeric_limits<uint8_t>::max()) {
        for (int i = 0; i < _count; i++) {
            uint32_t value = cell(i);
            uint8_t converted_value = static_cast<uint8_t>(value);
            memcpy(&_data[i * sizeof(converted_value)], &converted_value, sizeof(converted_value));
        }
        ret = _finish(page_first_rowid, sizeof(uint8_t));
        encode_fixed32_le((uint8_t*)ret.mutable_data() + 16, sizeof(uint8_t));
    } else if (max_value <= std::numeric_limits<uint16_t>::max()) {
        for (int i = 0; i < _count; i++) {
            uint32_t value = cell(i);
            uint16_t converted_value = static_cast<uint16_t>(value);
            memcpy(&_data[i * sizeof(converted_value)], &converted_value, sizeof(converted_value));
        }
        ret = _finish(page_first_rowid, sizeof(uint16_t));
        encode_fixed32_le((uint8_t*)ret.mutable_data() + 16, sizeof(uint16_t));
    } else {
        ret = _finish(page_first_rowid, sizeof(uint32_t));
        encode_fixed32_le((uint8_t*)ret.mutable_data() + 16, sizeof(uint32_t));
    }
    return ret;
}

} // namespace segment_v2
} // namespace doris
