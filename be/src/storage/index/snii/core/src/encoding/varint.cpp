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

#include "snii/encoding/varint.h"

namespace snii {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

size_t varint_len(uint64_t v) {
    size_t n = 1;
    while (v >= 0x80) {
        v >>= 7;
        ++n;
    }
    return n;
}

size_t encode_varint64(uint64_t v, uint8_t* out) {
    size_t i = 0;
    while (v >= 0x80) {
        out[i++] = static_cast<uint8_t>(v) | 0x80;
        v >>= 7;
    }
    out[i++] = static_cast<uint8_t>(v);
    return i;
}

size_t encode_varint32(uint32_t v, uint8_t* out) {
    return encode_varint64(v, out);
}

doris::Status decode_varint64(const uint8_t* p, const uint8_t* end, uint64_t* v, const uint8_t** next) {
    uint64_t result = 0;
    int shift = 0;
    while (p < end) {
        uint8_t b = *p++;
        result |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            *v = result;
            *next = p;
            return doris::Status::OK();
        }
        shift += 7;
        if (shift >= 64) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("varint64 overflow");
    }
    return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("varint truncated");
}

doris::Status decode_varint32(const uint8_t* p, const uint8_t* end, uint32_t* v, const uint8_t** next) {
    uint64_t tmp;
    RETURN_IF_ERROR(decode_varint64(p, end, &tmp, next));
    if (tmp > 0xFFFFFFFFu) return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("varint32 overflow");
    *v = static_cast<uint32_t>(tmp);
    return doris::Status::OK();
}

} // namespace snii
