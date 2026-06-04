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
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"

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

// Patch-list signal bit in the width byte. When set, a patch trailer
// follows the bitpacked payload (see pfor_encoder.h). Legacy widths are
// 1..32 (6 bits) so the bit was always clear before this change; all
// decode sites mask it off with `& kWidthMask`.
constexpr uint8_t kPatchFlag = 0x80U;
constexpr uint8_t kWidthMask = 0x3FU;

// Bit-packs `values[0..count)` at `width` bits each into `out` via
// Arrow's BitWriter (the same primitive Parquet uses). Returns bytes
// written. Used for both the plain payload and the exception high-bit
// payload.
inline void pack_bits(const uint32_t* values, size_t count, uint8_t width, ByteOutput* out) {
    const size_t max_bytes = ((count * width) + 7U) / 8U + 8U; // +8 slack for Flush alignment
    std::vector<uint8_t> packed(max_bytes, 0);
    arrow::bit_util::BitWriter bw(packed.data(), static_cast<int>(packed.size()));
    for (size_t i = 0; i < count; ++i) {
        const bool ok = bw.PutValue(values[i], width);
        DCHECK(ok) << "Arrow BitWriter overflow in SpimiPforEncoder pack_bits";
    }
    bw.Flush(false);
    const int payload_bytes = bw.bytes_written();
    if (payload_bytes > 0) {
        out->WriteBytes(packed.data(), static_cast<size_t>(payload_bytes));
    }
}

// Deterministically chooses a patched encoding for `values[0..count)`.
// Returns true and fills *out_base_width / *out_positions / *out_highs /
// *out_except_width when a patched encoding is STRICTLY smaller than the
// plain block; otherwise returns false (caller emits the plain block).
//
// Heuristic (deterministic so golden-byte tests are stable): treat the
// top ~1/16 of values (Lucene's default exception ratio) as exceptions
// and pick the base width as the width of the largest non-exception
// value. Then compute the exact byte cost and accept only on a strict win.
bool select_patch(const uint32_t* values, size_t count, uint8_t plain_width,
                  uint8_t* out_base_width, std::vector<uint8_t>* out_positions,
                  std::vector<uint32_t>* out_highs, uint8_t* out_except_width) {
    if (count < 2) {
        return false; // nothing to gain from a single value
    }
    // Sorted copy to find the exception threshold deterministically.
    std::vector<uint32_t> sorted(values, values + count);
    std::sort(sorted.begin(), sorted.end());
    // Number of exceptions: top ~1/16, at least 1, but leave at least one
    // non-exception value to anchor the base width.
    size_t k = count / 16;
    if (k == 0) {
        k = 1;
    }
    if (k >= count) {
        k = count - 1;
    }
    // base value = largest value NOT treated as an exception.
    const uint32_t base_max = sorted[count - k - 1];
    const uint8_t base_width = bit_width_for(base_max);
    if (base_width >= plain_width) {
        return false; // exceptions don't lower the base width — no win possible
    }
    // Gather the actual exception positions/high-bit parts: every value
    // whose magnitude exceeds what base_width can hold is an exception.
    const uint32_t base_cap = (base_width >= 32U) ? 0xFFFFFFFFU : ((1U << base_width) - 1U);
    std::vector<uint8_t> positions;
    std::vector<uint32_t> highs;
    uint32_t max_high = 0;
    for (size_t i = 0; i < count; ++i) {
        if (values[i] > base_cap) {
            positions.push_back(static_cast<uint8_t>(i));
            const uint32_t high = values[i] >> base_width;
            highs.push_back(high);
            max_high = std::max(max_high, high);
        }
    }
    const size_t num_exc = positions.size();
    // num_exc could differ from k (ties at the threshold); require it to be
    // a non-empty, valid trailer. If every value fit under base_cap the high
    // bits are all zero — that cannot happen because base_max < some value.
    if (num_exc == 0 || num_exc > count || num_exc > 255U) {
        return false;
    }
    const uint8_t except_width = bit_width_for(max_high);
    if (except_width == 0 || static_cast<uint32_t>(base_width) + except_width > 32U) {
        return false; // trailer would not satisfy the decoder's bounds
    }
    // Exact byte cost comparison (excluding the shared width byte, which both
    // forms pay identically; neither form stores a count).
    const size_t plain_payload = (count * plain_width + 7U) / 8U;
    const size_t patched_payload = (count * base_width + 7U) / 8U;
    const size_t trailer = 1U /*num_exc*/ + 1U /*except_width*/ + num_exc /*positions*/ +
                           (num_exc * except_width + 7U) / 8U /*highbits*/;
    if (patched_payload + trailer >= plain_payload) {
        return false; // not strictly smaller — emit plain
    }
    *out_base_width = base_width;
    *out_positions = std::move(positions);
    *out_highs = std::move(highs);
    *out_except_width = except_width;
    return true;
}

} // namespace

size_t SpimiPforEncoder::EncodeBlock(uint32_t* values, size_t count, ByteOutput* out,
                                     bool allow_patch) {
    DCHECK(out != nullptr);
    DCHECK_LE(count, kBlockSize);
    DCHECK_GT(count, 0U);

    const int64_t start_offset = out->FilePointer();

    uint32_t max_value = 0;
    for (size_t i = 0; i < count; ++i) {
        max_value = std::max(max_value, values[i]);
    }
    const uint8_t width = bit_width_for(max_value);
    DCHECK_LE(width, 32U);

    // Opt-in patched encoding. Try to split the few high-magnitude
    // outliers into a patch trailer so the bitpacked payload packs at a
    // narrower base width. Only accepted on a strict size win, so the 0x80
    // flag stays clear on no-outlier blocks and the output is the plain
    // width-byte + bitpacked form.
    if (allow_patch) {
        uint8_t base_width = 0;
        uint8_t except_width = 0;
        std::vector<uint8_t> positions;
        std::vector<uint32_t> highs;
        if (select_patch(values, count, width, &base_width, &positions, &highs, &except_width)) {
            out->WriteByte(static_cast<uint8_t>(base_width | kPatchFlag));
            // Bitpack the LOW base_width bits of every value (exceptions are
            // truncated; their high bits live in the trailer). Arrow's
            // BitWriter::PutValue HARD-ASSERTS that the value fits in
            // `num_bits`, so we must mask off the high bits explicitly here
            // rather than rely on PutValue truncating.
            const uint32_t base_mask =
                    (base_width >= 32U) ? 0xFFFFFFFFU : ((1U << base_width) - 1U);
            std::vector<uint32_t> low(count);
            for (size_t i = 0; i < count; ++i) {
                low[i] = values[i] & base_mask;
            }
            pack_bits(low.data(), count, base_width, out);
            // Patch trailer.
            out->WriteByte(static_cast<uint8_t>(positions.size()));
            out->WriteByte(except_width);
            out->WriteBytes(positions.data(), positions.size());
            pack_bits(highs.data(), highs.size(), except_width, out);
            return static_cast<size_t>(out->FilePointer() - start_offset);
        }
    }

    // Plain block: width byte + bitpacked payload (no leading count).
    out->WriteByte(width);
    pack_bits(values, count, width, out);
    return static_cast<size_t>(out->FilePointer() - start_offset);
}

std::vector<uint8_t> SpimiPforEncoder::EncodeBlockToBytes(const std::vector<uint32_t>& values,
                                                          bool allow_patch) {
    MemoryByteOutput out;
    std::vector<uint32_t> scratch = values;
    if (!scratch.empty()) {
        (void)EncodeBlock(scratch.data(), scratch.size(), &out, allow_patch);
    }
    return out.bytes();
}

namespace {

// Tiny Byte reader over a byte vector, plus an Arrow-BitReader-backed
// bitpacked unpack. The block format no longer carries a leading VInt
// count, so no VInt reader is needed here.
class ByteCursor {
public:
    ByteCursor(const uint8_t* data, size_t len) : _data(data), _len(len) {}
    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block cursor underflow");
        }
        return _data[_pos++];
    }
    // Reads `width` low-bit/high-bit bitpacked values starting at the
    // current cursor position via Arrow's BitReader, then advances the
    // cursor past the consumed payload bytes. Hard-fails on underflow.
    void ReadBitpacked(uint8_t width, size_t count, uint32_t* out) {
        const size_t bit_bytes = (count * width + 7U) / 8U;
        if (_pos + bit_bytes > _len || _pos + bit_bytes < _pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR bitpacked payload underflow");
        }
        arrow::bit_util::BitReader br(_data + _pos, static_cast<int>(bit_bytes));
        const int got =
                br.GetBatch<uint32_t>(static_cast<int>(width), out, static_cast<int>(count));
        if (got != static_cast<int>(count)) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block: BitReader underflow");
        }
        _pos += bit_bytes;
    }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

} // namespace

size_t SpimiPforDecoder::DecodeBlockFromBytes(const std::vector<uint8_t>& in, size_t count,
                                              std::vector<uint32_t>* out) {
    DCHECK(out != nullptr);
    out->clear();
    // `count` is supplied out-of-band (the block no longer stores it).
    // Reject a count that the format cannot represent so a crafted run
    // total can't drive `out->resize(huge_count)` → bad_alloc.
    if (count == 0U || count > SpimiPforEncoder::kBlockSize) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block count out of range");
    }
    if (in.empty()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block: empty buffer");
    }

    ByteCursor cur(in.data(), in.size());
    const uint8_t raw_width = cur.ReadByte();
    const bool patched = (raw_width & kPatchFlag) != 0U; // patched-PFOR signal bit
    const uint8_t width = raw_width & kWidthMask;        // base bit width
    // Hard-fail on untrusted-byte invariants. An out-of-range bit width
    // would otherwise drive Arrow's BitReader to UB-shift past
    // `sizeof(uint32_t)*8`.
    if (width == 0U || width > 32U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq PFOR sub-block width out of range");
    }

    out->resize(count);
    // Batched unpack: GetBatch decodes the identical LSB-first
    // little-endian stream the encoder wrote (same primitive, same
    // buffer, same start offset, same width) and engages the SIMD kernel
    // (AVX2/AVX512/NEON, runtime-dispatched) for the 32-aligned interior;
    // the <32-value tail falls back to the scalar GetValue_ path. For a
    // patched block this unpacks the LOW base_width bits of every value;
    // the trailer then ORs the exception high bits back in.
    cur.ReadBitpacked(width, count, out->data());

    if (patched) {
        // Patch trailer: byte k, byte except_width, k position bytes,
        // ceil(k*except_width/8) bitpacked high-bit values. All reads are
        // bounds-checked against the (untrusted) buffer via ByteCursor.
        const size_t num_exc = static_cast<size_t>(cur.ReadByte());
        const uint8_t except_width = cur.ReadByte();
        // Untrusted-byte bounds: k in 1..count; except_width in
        // 1..(32-base_width) so the OR can't overflow uint32 or UB-shift.
        if (num_exc == 0U || num_exc > count) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR patch: num_exceptions out of range");
        }
        if (except_width == 0U || static_cast<uint32_t>(width) + except_width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq PFOR patch: except_width out of range");
        }
        std::vector<uint8_t> positions(num_exc);
        for (size_t i = 0; i < num_exc; ++i) {
            positions[i] = cur.ReadByte();
            if (positions[i] >= count) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq PFOR patch: exception position out of range");
            }
        }
        std::vector<uint32_t> highs(num_exc);
        cur.ReadBitpacked(except_width, num_exc, highs.data());
        for (size_t i = 0; i < num_exc; ++i) {
            (*out)[positions[i]] |= (highs[i] << width);
        }
    }
    return count;
}

} // namespace doris::segment_v2::inverted_index::spimi
