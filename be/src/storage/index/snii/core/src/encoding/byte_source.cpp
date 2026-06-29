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

#include "snii/encoding/byte_source.h"

#include "snii/encoding/varint.h"

namespace snii {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

doris::Status ByteSource::get_u8(uint8_t* v) {
    if (remaining() < 1)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_u8 overrun");
    *v = s_[pos_++];
    return doris::Status::OK();
}

doris::Status ByteSource::get_fixed16(uint16_t* v) {
    if (remaining() < 2)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed16 overrun");
    uint16_t r = 0;
    for (int i = 0; i < 2; ++i) r |= static_cast<uint16_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 2;
    *v = r;
    return doris::Status::OK();
}

doris::Status ByteSource::get_fixed32(uint32_t* v) {
    if (remaining() < 4)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed32 overrun");
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i) r |= static_cast<uint32_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 4;
    *v = r;
    return doris::Status::OK();
}

doris::Status ByteSource::get_fixed64(uint64_t* v) {
    if (remaining() < 8)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed64 overrun");
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) r |= static_cast<uint64_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 8;
    *v = r;
    return doris::Status::OK();
}

doris::Status ByteSource::get_varint64(uint64_t* v) {
    const uint8_t* p = s_.data() + pos_;
    const uint8_t* next = nullptr;
    RETURN_IF_ERROR(decode_varint64(p, s_.data() + s_.size(), v, &next));
    pos_ = static_cast<size_t>(next - s_.data());
    return doris::Status::OK();
}

doris::Status ByteSource::get_varint32(uint32_t* v) {
    uint64_t tmp;
    RETURN_IF_ERROR(get_varint64(&tmp));
    if (tmp > 0xFFFFFFFFu)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "varint32 overflow");
    *v = static_cast<uint32_t>(tmp);
    return doris::Status::OK();
}

doris::Status ByteSource::get_zigzag(int64_t* v) {
    uint64_t tmp;
    RETURN_IF_ERROR(get_varint64(&tmp));
    *v = zigzag_decode(tmp);
    return doris::Status::OK();
}

doris::Status ByteSource::get_bytes(size_t n, Slice* out) {
    if (remaining() < n)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_bytes overrun");
    *out = s_.subslice(pos_, n);
    pos_ += n;
    return doris::Status::OK();
}

} // namespace snii
