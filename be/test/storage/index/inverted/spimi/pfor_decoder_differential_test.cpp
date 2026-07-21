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

// Differential proof that the production SpimiPforDecoder (which now
// pulls every value in one Arrow GetBatch<uint32_t> call, engaging the
// runtime-dispatched SIMD internal::unpack32 fast path) decodes a
// byte-IDENTICAL integer vector to an INDEPENDENT scalar reference
// unpacker.
//
// IMPORTANT — independence: the scalar reference here does NOT call any
// Arrow primitive. It re-parses the width-byte header (the count is
// supplied out-of-band) and then extracts each value bit-by-bit out of
// the raw payload
// bytes, LSB-first / little-endian, exactly matching the layout Arrow's
// BitWriter wrote (documented "little-endian format" in
// arrow/util/bit_stream_utils.h). Because the oracle shares no code
// with Arrow, agreement between it and the batched decoder proves the
// new decode path reads the same bits the encoder wrote, independent of
// any shared Arrow bug.
//
// Coverage (fixed RNG seeds for reproducibility):
//   - every bit width 1..32
//   - block sizes 1, 7, 64, 127, 128, and 1000 (1000 spans multiple
//     128-value PFOR blocks, exercising the chunked write/read path)
//   - fuzz patterns: all-zero, all-max (2^width - 1), ascending ramp,
//     and width-masked random

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// ---------------------------------------------------------------------
// Independent scalar reference unpacker. No Arrow. Reads the SPIMI PFOR
// block format directly (the count is supplied out-of-band, not stored):
//
//   byte   bit_width     (low 6 bits; 0x80 reserved patch flag masked off)
//   bytes  bitpacked     (count * bit_width bits, LSB-first little-endian)
//
// Each value's `width` bits are read starting at the current global bit
// offset; bit b of the payload lives in byte (b/8) at position (b%8),
// least-significant bit first — the canonical little-endian bitpacking
// Arrow's BitWriter emits.
// ---------------------------------------------------------------------
std::vector<uint32_t> IndependentScalarDecode(const std::vector<uint8_t>& in, size_t count) {
    std::vector<uint32_t> out;
    if (in.empty()) {
        return out;
    }
    size_t pos = 0;

    // Width byte (mask the reserved patch-signal bit).
    EXPECT_LT(pos, in.size()) << "ref decode: width-byte underflow";
    const uint32_t width = in[pos++] & 0x3FU;
    EXPECT_GE(width, 1U);
    EXPECT_LE(width, 32U);

    const uint8_t* payload = in.data() + pos;
    const size_t payload_len = in.size() - pos;

    out.resize(count);
    uint64_t bit_offset = 0;
    for (uint32_t i = 0; i < count; ++i) {
        uint64_t v = 0;
        for (uint32_t b = 0; b < width; ++b) {
            const uint64_t global_bit = bit_offset + b;
            const size_t byte_idx = static_cast<size_t>(global_bit >> 3U);
            const uint32_t bit_in_byte = static_cast<uint32_t>(global_bit & 7U);
            EXPECT_LT(byte_idx, payload_len) << "ref decode: payload underflow at value " << i;
            const uint64_t bit = (static_cast<uint64_t>(payload[byte_idx]) >> bit_in_byte) & 1ULL;
            v |= bit << b;
        }
        out[i] = static_cast<uint32_t>(v);
        bit_offset += width;
    }
    return out;
}

// Encode `values` as one or more 128-value PFOR blocks. Returns per-block
// (bytes, value_count) pairs — the block no longer self-describes its count,
// so the count is tracked alongside the bytes here exactly as the run decoder
// derives it. Mirrors how the .frq writer chunks a posting list into
// kBlockSize sub-blocks, so block size 1000 is genuinely exercised end to end.
std::vector<std::pair<std::vector<uint8_t>, size_t>> EncodeChunked(
        const std::vector<uint32_t>& values) {
    std::vector<std::pair<std::vector<uint8_t>, size_t>> blocks;
    const size_t bs = SpimiPforEncoder::kBlockSize;
    for (size_t off = 0; off < values.size(); off += bs) {
        const size_t n = std::min(bs, values.size() - off);
        std::vector<uint32_t> chunk(values.begin() + static_cast<std::ptrdiff_t>(off),
                                    values.begin() + static_cast<std::ptrdiff_t>(off + n));
        blocks.emplace_back(SpimiPforEncoder::EncodeBlockToBytes(chunk), n);
    }
    return blocks;
}

// Core assertion: production batched decode == independent scalar
// reference == original input, block by block, for an arbitrary-length
// value vector (split into 128-value PFOR blocks).
void ExpectByteIdentical(const std::vector<uint32_t>& values) {
    const auto blocks = EncodeChunked(values);
    std::vector<uint32_t> rebuilt;
    rebuilt.reserve(values.size());
    for (const auto& [block, count] : blocks) {
        std::vector<uint32_t> batched;
        const size_t n = SpimiPforDecoder::DecodeBlockFromBytes(block, count, &batched);
        ASSERT_EQ(n, batched.size());
        const auto reference = IndependentScalarDecode(block, count);
        // The load-bearing assertion: SIMD/batched production decode is
        // bit-for-bit the same vector as the hand-rolled scalar unpack.
        ASSERT_EQ(batched, reference)
                << "batched production decode diverged from independent scalar reference";
        rebuilt.insert(rebuilt.end(), batched.begin(), batched.end());
    }
    ASSERT_EQ(rebuilt, values) << "decode did not reproduce the encoded input";
}

constexpr size_t kBlockSizes[] = {1, 7, 64, 127, 128, 1000};

uint32_t WidthMask(int width) {
    return (width == 32) ? 0xFFFFFFFFU : ((1U << width) - 1U);
}

} // namespace

// Every bit width 1..32 x every required block size, filled with
// width-masked random values (value[0] forced to the full mask so the
// encoder actually selects this width). Fixed seed => reproducible.
TEST(SpimiPforDecoderDifferentialTest, RandomAcrossWidthsAndSizes) {
    std::mt19937 rng(0xA11CE5EDU);
    for (int width = 1; width <= 32; ++width) {
        const uint32_t mask = WidthMask(width);
        for (size_t count : kBlockSizes) {
            std::vector<uint32_t> values(count);
            values[0] = mask; // pin the per-block max to exactly `width` bits
            for (size_t i = 1; i < count; ++i) {
                values[i] = rng() & mask;
            }
            // For the 1000-value (multi-block) case, also pin the first
            // value of each 128-block so every block keeps this width.
            for (size_t off = SpimiPforEncoder::kBlockSize; off < count;
                 off += SpimiPforEncoder::kBlockSize) {
                values[off] = mask;
            }
            ExpectByteIdentical(values);
        }
    }
}

// Fuzz pattern: all values zero (encoder clamps width to 1).
TEST(SpimiPforDecoderDifferentialTest, AllZeroAcrossSizes) {
    for (int width = 1; width <= 32; ++width) {
        // Width is irrelevant for all-zero (encoder will pick width=1),
        // but iterate sizes once; the width loop just repeats the cases
        // harmlessly. Keep a single representative pass per size.
        if (width != 1) {
            continue;
        }
        for (size_t count : kBlockSizes) {
            std::vector<uint32_t> values(count, 0U);
            ExpectByteIdentical(values);
        }
    }
}

// Fuzz pattern: every value at the full 2^width - 1 saturation, forcing
// the encoder to the exact target width across all sizes.
TEST(SpimiPforDecoderDifferentialTest, AllMaxAcrossWidthsAndSizes) {
    for (int width = 1; width <= 32; ++width) {
        const uint32_t mask = WidthMask(width);
        for (size_t count : kBlockSizes) {
            std::vector<uint32_t> values(count, mask);
            ExpectByteIdentical(values);
        }
    }
}

// Fuzz pattern: ascending ramp (i mod (mask+1)). Exercises every bit
// position turning on/off across the packed stream, which a faulty SIMD
// lane shuffle would corrupt.
TEST(SpimiPforDecoderDifferentialTest, RampAcrossWidthsAndSizes) {
    for (int width = 1; width <= 32; ++width) {
        const uint32_t mask = WidthMask(width);
        for (size_t count : kBlockSizes) {
            std::vector<uint32_t> values(count);
            for (size_t i = 0; i < count; ++i) {
                // mask+1 can overflow at width 32; use the mask directly
                // (& mask) which yields the low `width` bits of i — a
                // clean ramp that still maxes out at `mask`.
                values[i] = static_cast<uint32_t>(i) & mask;
            }
            // Pin per-block max to `width` so the encoder selects it even
            // when the ramp alone wouldn't reach the high bit.
            values[0] = mask;
            for (size_t off = SpimiPforEncoder::kBlockSize; off < count;
                 off += SpimiPforEncoder::kBlockSize) {
                values[off] = mask;
            }
            ExpectByteIdentical(values);
        }
    }
}

// A second independent random seed, interleaving every block size, to
// widen fuzz coverage of the batched-vs-scalar agreement.
TEST(SpimiPforDecoderDifferentialTest, SecondSeedRandomFuzz) {
    std::mt19937 rng(0x5EED0002U);
    for (int trial = 0; trial < 200; ++trial) {
        const int width = 1 + (trial % 32);
        const uint32_t mask = WidthMask(width);
        const size_t count = kBlockSizes[trial % (sizeof(kBlockSizes) / sizeof(kBlockSizes[0]))];
        std::vector<uint32_t> values(count);
        values[0] = mask;
        for (size_t i = 1; i < count; ++i) {
            values[i] = rng() & mask;
        }
        for (size_t off = SpimiPforEncoder::kBlockSize; off < count;
             off += SpimiPforEncoder::kBlockSize) {
            values[off] = mask;
        }
        ExpectByteIdentical(values);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
