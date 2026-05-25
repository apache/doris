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

#include "storage/index/inverted/spimi/pfor_encoder.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted PFOR
// sub-blocks. StdHeader.h sets up CLUCENE_EXPORT / CL_NS_DEF
// macros that debug/error.h depends on.
#include <arrow/util/bit_stream_utils.h>

#include <algorithm>
#include <cstring>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Bits needed to represent the largest value in [0, max]. For max = 0
// we still return 1 — the format always reserves at least 1 bit per
// value so the bitpacker has a non-empty payload, matching how
// Arrow's BitWriter / Parquet PLAIN bitpack handle all-zero columns.
inline uint8_t bit_width_for(uint32_t max_value) {
    if (max_value == 0) {
        return 1;
    }
    // __builtin_clz on uint32_t is well-defined for non-zero input.
    return static_cast<uint8_t>(32U - static_cast<uint32_t>(__builtin_clz(max_value)));
}

} // namespace

size_t SpimiPforEncoder::EncodeBlock(uint32_t* values, size_t count, ByteOutput* out) {
    DCHECK(out != nullptr);
    DCHECK_LE(count, kBlockSize);
    DCHECK_GT(count, 0U);

    const int64_t start_offset = out->FilePointer();
    out->WriteVInt(static_cast<int32_t>(count));

    uint32_t max_value = 0;
    for (size_t i = 0; i < count; ++i) {
        max_value = std::max(max_value, values[i]);
    }
    const uint8_t width = bit_width_for(max_value);
    // Reserve the high bit (0x80) for a future patched-PFOR extension;
    // 1..32 fits in 6 bits, so 0x80 is currently always clear. A
    // future "patched PFOR" can set 0x80 to signal a trailing
    // exception list.
    DCHECK_LE(width, 32U);
    out->WriteByte(width);

    // Buffer the bitpacked payload via Arrow's BitWriter, then copy
    // into the ByteOutput in one go. Arrow's BitWriter is the same
    // primitive Parquet (and downstream: Snowflake / BigQuery / Athena
    // / ClickHouse via Parquet) uses for bit-packed integers.
    const size_t max_bytes = ((count * width) + 7U) / 8U + 8U; // +8 slack for Flush alignment
    std::vector<uint8_t> packed(max_bytes, 0);
    arrow::bit_util::BitWriter bw(packed.data(), static_cast<int>(packed.size()));
    for (size_t i = 0; i < count; ++i) {
        const bool ok = bw.PutValue(values[i], width);
        DCHECK(ok) << "Arrow BitWriter overflow in SpimiPforEncoder::EncodeBlock";
    }
    bw.Flush(false);
    const int payload_bytes = bw.bytes_written();
    if (payload_bytes > 0) {
        out->WriteBytes(packed.data(), static_cast<size_t>(payload_bytes));
    }

    return static_cast<size_t>(out->FilePointer() - start_offset);
}

std::vector<uint8_t> SpimiPforEncoder::EncodeBlockToBytes(const std::vector<uint32_t>& values) {
    MemoryByteOutput out;
    std::vector<uint32_t> scratch = values;
    if (!scratch.empty()) {
        (void)EncodeBlock(scratch.data(), scratch.size(), &out);
    }
    return out.bytes();
}

namespace {

// Tiny VInt + Byte reader over a byte vector. Mirrors Arrow's BitReader
// for the parts we care about (BitReader doesn't have a public VInt
// API we can leverage here).
class ByteCursor {
public:
    ByteCursor(const uint8_t* data, size_t len) : _data(data), _len(len) {}
    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block cursor underflow");
        }
        return _data[_pos++];
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = ReadByte();
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq PFOR VInt: shift overflow on crafted input");
            }
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int32_t>(v);
    }
    const uint8_t* tail() const { return _data + _pos; }
    size_t remaining() const { return _len - _pos; }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

} // namespace

size_t SpimiPforDecoder::DecodeBlockFromBytes(const std::vector<uint8_t>& in,
                                              std::vector<uint32_t>* out) {
    DCHECK(out != nullptr);
    out->clear();
    if (in.empty()) {
        return 0;
    }

    ByteCursor cur(in.data(), in.size());
    const size_t count = static_cast<size_t>(cur.ReadVInt());
    const uint8_t width = cur.ReadByte() & 0x3FU; // mask off the patched-PFOR signal bit
    // Hard-fail on untrusted-byte invariants. Attacker bytes in
    // `in` could otherwise drive `out->resize(huge_count)` →
    // bad_alloc, or pass an out-of-range bit width that
    // Arrow's BitReader UB-shifts past `sizeof(uint32_t)*8`.
    if (count == 0U || count > SpimiPforEncoder::kBlockSize) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block count out of range");
    }
    if (width == 0U || width > 32U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block width out of range");
    }

    out->resize(count);
    arrow::bit_util::BitReader br(cur.tail(), static_cast<int>(cur.remaining()));
    for (size_t i = 0; i < count; ++i) {
        uint32_t v = 0;
        const bool ok = br.GetValue(width, &v);
        if (!ok) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block: BitReader underflow");
        }
        (*out)[i] = v;
    }
    return count;
}

} // namespace doris::segment_v2::inverted_index::spimi
