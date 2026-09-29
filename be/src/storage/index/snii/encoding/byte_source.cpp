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

#include "storage/index/snii/encoding/byte_source.h"

#include <algorithm>
#include <array>
#include <limits>

#include "storage/index/snii/encoding/varint.h"

namespace doris::snii {

namespace {

Status decode_delta_value(const uint8_t** cursor, const uint8_t* end, uint32_t* previous,
                          bool* first_position, uint32_t* value) {
    const uint8_t* p = *cursor;
    if (p >= end) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "byte_source: delta run past end");
    }
    uint32_t byte = *p++;
    uint32_t delta = byte & 0x7FU;
    if (byte >= 0x80) {
        uint32_t shift = 7;
        for (;;) {
            if (p >= end) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "byte_source: delta run past end");
            }
            byte = *p++;
            if (shift == 28 && (byte & 0xF0U) != 0) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "byte_source: delta varint32 overflow");
            }
            delta |= (byte & 0x7FU) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            if (shift == 28) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "byte_source: delta varint32 overflow");
            }
            shift += 7;
        }
    }
    if (!*first_position && delta > std::numeric_limits<uint32_t>::max() - *previous) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "byte_source: delta prefix sum overflow");
    }
    *value = *first_position ? delta : *previous + delta;
    *previous = *value;
    *first_position = false;
    *cursor = p;
    return Status::OK();
}

} // namespace

Status ByteSource::get_u8(uint8_t* v) {
    if (remaining() < 1)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("get_u8 overrun");
    *v = s_[pos_++];
    return Status::OK();
}

Status ByteSource::get_fixed16(uint16_t* v) {
    if (remaining() < 2)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed16 overrun");
    uint16_t r = 0;
    for (int i = 0; i < 2; ++i) r |= static_cast<uint16_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 2;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_fixed32(uint32_t* v) {
    if (remaining() < 4)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed32 overrun");
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i) r |= static_cast<uint32_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 4;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_fixed64(uint64_t* v) {
    if (remaining() < 8)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "get_fixed64 overrun");
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) r |= static_cast<uint64_t>(s_[pos_ + i]) << (8 * i);
    pos_ += 8;
    *v = r;
    return Status::OK();
}

Status ByteSource::get_varint64(uint64_t* v) {
    const uint8_t* p = s_.data() + pos_;
    const uint8_t* next = nullptr;
    RETURN_IF_ERROR(decode_varint64(p, s_.data() + s_.size(), v, &next));
    pos_ = static_cast<size_t>(next - s_.data());
    return Status::OK();
}

Status ByteSource::get_varint32(uint32_t* v) {
    uint64_t tmp;
    RETURN_IF_ERROR(get_varint64(&tmp));
    if (tmp > 0xFFFFFFFFu)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("varint32 overflow");
    *v = static_cast<uint32_t>(tmp);
    return Status::OK();
}

// NOLINTNEXTLINE(readability-non-const-parameter): out is the decoded position output buffer.
Status ByteSource::decode_delta_run(size_t count, std::vector<uint32_t>* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "byte_source: null delta run output");
    }
    if (count > std::numeric_limits<size_t>::max() - out->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "byte_source: delta run size overflow");
    }
    const uint8_t* const begin = s_.data();
    const uint8_t* const end = begin + s_.size();
    const uint8_t* p = begin + pos_;
    const size_t original_size = out->size();
    out->reserve(out->size() + count);
    uint32_t previous = 0;
    bool first_position = true;
    for (size_t i = 0; i < count; ++i) {
        uint32_t value = 0;
        const Status status = decode_delta_value(&p, end, &previous, &first_position, &value);
        if (!status.ok()) {
            out->resize(original_size);
            return status;
        }
        out->push_back(value);
    }
    pos_ = static_cast<size_t>(p - begin);
    return Status::OK();
}

Status ByteSource::decode_delta_batch(std::span<uint32_t> out, uint32_t* previous,
                                      bool* first_position) {
    constexpr size_t kBatchCapacity = 16;
    if (out.size() > kBatchCapacity) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "byte_source: delta batch exceeds fixed capacity");
    }
    std::array<uint32_t, kBatchCapacity> scratch {};
    const uint8_t* const begin = s_.data();
    const uint8_t* const end = begin + s_.size();
    const uint8_t* p = begin + pos_;
    uint32_t local_previous = *previous;
    bool local_first_position = *first_position;
    for (size_t i = 0; i < out.size(); ++i) {
        RETURN_IF_ERROR(
                decode_delta_value(&p, end, &local_previous, &local_first_position, &scratch[i]));
    }
    std::copy_n(scratch.begin(), out.size(), out.begin());
    pos_ = static_cast<size_t>(p - begin);
    *previous = local_previous;
    *first_position = local_first_position;
    return Status::OK();
}

Status ByteSource::skip_varints(size_t count) {
    const uint8_t* const begin = s_.data();
    const uint8_t* const end = begin + s_.size();
    const uint8_t* p = begin + pos_;
    // Each varint ends at the first byte whose continuation bit (0x80) is clear.
    // Scanning for `count` such terminators skips the values with one branch per
    // byte -- no shift/accumulate/store and no per-value bounds Status. (A SIMD
    // bulk terminator-count was tried and reverted: the skipped position runs
    // between selected docs are almost always 1-3 varints -- far below a 16-byte
    // block -- so the vector path never amortized, and the larger body stopped
    // this function from inlining into the CSR reader, a net CPU regression on
    // the httplogs/agentlogs phrase-prefix profiles.)
    for (size_t k = 0; k < count; ++k) {
        while (p < end && (*p & 0x80) != 0) {
            ++p;
        }
        if (p >= end) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "byte_source: varint skip past end");
        }
        ++p; // consume the terminator byte
    }
    pos_ = static_cast<size_t>(p - begin);
    return Status::OK();
}

Status ByteSource::get_zigzag(int64_t* v) {
    uint64_t tmp;
    RETURN_IF_ERROR(get_varint64(&tmp));
    *v = zigzag_decode(tmp);
    return Status::OK();
}

Status ByteSource::get_bytes(size_t n, Slice* out) {
    if (remaining() < n)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("get_bytes overrun");
    *out = s_.subslice(pos_, n);
    pos_ += n;
    return Status::OK();
}

} // namespace doris::snii
