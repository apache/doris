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

#include <arrow/util/bit_stream_utils.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Helper: encode + decode and check round-trip.
void ExpectRoundTrip(const std::vector<uint32_t>& values) {
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    std::vector<uint32_t> back;
    const size_t n = SpimiPforDecoder::DecodeBlockFromBytes(bytes, &back);
    ASSERT_EQ(n, values.size());
    ASSERT_EQ(back, values);
}

// Reference decoder: a hand-written scalar per-value GetValue loop over
// the SAME encoded bytes the production batched decoder consumes. This
// is the pre-optimization decode path. Mirrors the VInt + width-byte
// parse in DecodeBlockFromBytes, then pulls one value at a time. Used
// as the differential oracle proving the batched GetBatch decode is
// byte-identical to the scalar loop for every width 1..32.
std::vector<uint32_t> ReferenceScalarDecode(const std::vector<uint8_t>& in) {
    std::vector<uint32_t> out;
    if (in.empty()) {
        return out;
    }
    size_t pos = 0;
    // Parse VInt(count) exactly as ByteCursor::ReadVInt does.
    uint32_t count = 0;
    uint32_t shift = 0;
    while (true) {
        const uint8_t b = in[pos++];
        count |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
    }
    const uint8_t width = in[pos++] & 0x3FU;
    out.resize(count);
    arrow::bit_util::BitReader br(in.data() + pos, static_cast<int>(in.size() - pos));
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t v = 0;
        const bool ok = br.GetValue(static_cast<int>(width), &v);
        EXPECT_TRUE(ok) << "reference scalar decode underflow at i=" << i;
        out[i] = v;
    }
    return out;
}

// Differential proof: the production (batched GetBatch) decode must
// equal the reference scalar GetValue loop over the identical bytes.
void ExpectDecodeMatchesScalarReference(const std::vector<uint32_t>& values) {
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    std::vector<uint32_t> batched;
    const size_t n = SpimiPforDecoder::DecodeBlockFromBytes(bytes, &batched);
    ASSERT_EQ(n, values.size());
    const auto reference = ReferenceScalarDecode(bytes);
    ASSERT_EQ(batched, reference) << "batched decode diverged from scalar GetValue loop";
    ASSERT_EQ(batched, values);
}

} // namespace

TEST(SpimiPforEncoderTest, SingleValueOneBitWidth) {
    // Smallest possible block: one value of 0 → bit_width=1, payload=1 byte.
    const std::vector<uint32_t> values {0};
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    // Layout: VInt(1) = 0x01; byte(width=1); 1 byte of payload (zero).
    ASSERT_EQ(bytes.size(), 3U);
    EXPECT_EQ(bytes[0], 0x01U);
    EXPECT_EQ(bytes[1], 0x01U);
    EXPECT_EQ(bytes[2], 0x00U);
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, SingleValueLargeFits32Bits) {
    const std::vector<uint32_t> values {0xFFFFFFFFU};
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    // VInt(1) + byte(width=32) + 4 bytes payload = 6 bytes.
    ASSERT_EQ(bytes.size(), 6U);
    EXPECT_EQ(bytes[0], 0x01U);
    EXPECT_EQ(bytes[1], 0x20U); // width=32
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, FullBlockSmallValues) {
    // 128 values all in [0, 15] ⇒ bit_width = 4. Payload = 128 × 4 = 512 bits = 64 bytes.
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<uint32_t>(i % 16);
    }
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    EXPECT_GE(bytes.size(), 2U + 64U); // VInt(128) is 2 bytes
    EXPECT_LE(bytes.size(), 2U + 64U + 8U);
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, FullBlockMaxBits32) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = 0x80000000U | static_cast<uint32_t>(i);
    }
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, BitWidthBoundary8) {
    // All values in [0, 255] ⇒ bit_width = 8 ⇒ payload = N bytes exact.
    std::vector<uint32_t> values {0, 1, 127, 128, 255};
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    // VInt(5) (1 byte) + byte(width=8) (1 byte) + 5 bytes payload = 7 bytes.
    ASSERT_EQ(bytes.size(), 7U);
    EXPECT_EQ(bytes[1], 0x08U);
    EXPECT_EQ(bytes[2], 0x00U);
    EXPECT_EQ(bytes[3], 0x01U);
    EXPECT_EQ(bytes[4], 0x7FU);
    EXPECT_EQ(bytes[5], 0x80U);
    EXPECT_EQ(bytes[6], 0xFFU);
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, BitWidthBoundaries1Through32) {
    // Drive every possible bit-width by setting max value to (1<<w)-1.
    // For each width, the block contains one value of width-1 ones
    // followed by a few smaller values; the encoder must pick width.
    for (int width = 1; width <= 32; ++width) {
        const uint32_t max_v = (width < 32) ? ((1U << width) - 1U) : 0xFFFFFFFFU;
        std::vector<uint32_t> values {max_v, 0, 1, 2};
        ExpectRoundTrip(values);
    }
}

TEST(SpimiPforEncoderTest, RandomBlockRoundTrip) {
    // 100 random blocks of random sizes / value ranges. Each block
    // should encode and decode to itself.
    std::mt19937 rng(0xC0FFEEU);
    for (int trial = 0; trial < 100; ++trial) {
        const size_t n = 1 + (rng() % SpimiPforEncoder::kBlockSize);
        const int width = 1 + (rng() % 31); // 1..31 to avoid full-32 special case
        const uint32_t max_v = (width >= 31) ? 0x7FFFFFFFU : ((1U << width) - 1U);
        std::vector<uint32_t> values(n);
        for (size_t i = 0; i < n; ++i) {
            values[i] = rng() % (max_v + 1U);
        }
        ExpectRoundTrip(values);
    }
}

TEST(SpimiPforEncoderTest, AllZeroBlockUsesOneBitWidth) {
    // bit_width is clamped to at least 1, never 0. Decoder must read
    // back the zeros correctly.
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 0U);
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    // VInt(128) is 2 bytes, byte(width=1), payload = 128 bits = 16 bytes.
    EXPECT_EQ(bytes.size(), 2U + 1U + 16U);
    EXPECT_EQ(bytes[2], 0x01U) << "bit_width clamps to 1 for all-zero block";
    ExpectRoundTrip(values);
}

TEST(SpimiPforEncoderTest, DocDeltaBlockProducesSmallPayload) {
    // Realistic doc-delta workload: 128 docs with small deltas (1-4).
    // Bit-width should land at 3 (covers 0..7), giving 128*3/8 = 48
    // byte payload vs 128 bytes for raw VInt of all 1s.
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize);
    std::mt19937 rng(0xBEEFU);
    for (auto& v : values) {
        v = 1U + (rng() % 4U);
    }
    const auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values);
    // VInt(128)=2 + width byte=1 + 48 bytes payload = 51 bytes total.
    EXPECT_LE(bytes.size(), 60U) << "small-delta block should compress well";
    EXPECT_GE(bytes.size(), 50U);
    ExpectRoundTrip(values);
}

// Sweep every bit width 1..32 against a representative set of block
// counts (including the unpack32 boundaries 32/64/128, off-by-one
// tails, and tiny blocks). Each block of random width-masked values
// must (a) round-trip and (b) decode identically to the scalar
// GetValue reference loop. This is the byte-identity differential
// proof for the batched-GetBatch optimization across all widths.
TEST(SpimiPforEncoderTest, BatchedDecodeMatchesScalarAcrossWidthsAndCounts) {
    const size_t counts[] = {1, 2, 7, 8, 31, 32, 33, 63, 64, 127, 128};
    std::mt19937 rng(0xD15EA5EU);
    for (int width = 1; width <= 32; ++width) {
        const uint32_t mask = (width == 32) ? 0xFFFFFFFFU : ((1U << width) - 1U);
        for (size_t count : counts) {
            std::vector<uint32_t> values(count);
            // Force the block's max value to exactly `width` bits so the
            // encoder selects this width, then fill the rest randomly.
            values[0] = mask;
            for (size_t i = 1; i < count; ++i) {
                values[i] = rng() & mask;
            }
            ExpectDecodeMatchesScalarReference(values);
        }
    }
}

// ---------------------------------------------------------------------------
// Patched-PFOR (OPT_PFOR_PATCH_FREQS) tests.
// ---------------------------------------------------------------------------

// A freq block with a few large outliers among many small values: the
// patch path must (a) be chosen (0x80 set), (b) be strictly smaller than
// the unpatched form, and (c) decode bit-for-bit.
TEST(SpimiPforEncoderTest, PatchedBlockShrinksAndRoundTrips) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 1U);
    // A handful of high outliers force the unpatched width to ~16 bits while
    // 95% of values fit in 1 bit.
    values[3] = 50000U;
    values[40] = 60000U;
    values[99] = 45000U;

    const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);

    // Patch flag set on the width byte (byte index 1, after the 1-byte VInt(128)
    // continuation... VInt(128) is 2 bytes, so width byte is index 2).
    ASSERT_GE(patched.size(), 3U);
    EXPECT_NE(patched[2] & 0x80U, 0U) << "patched block must set the 0x80 flag";
    EXPECT_EQ(plain[2] & 0x80U, 0U) << "plain block must leave 0x80 clear";
    EXPECT_LT(patched.size(), plain.size()) << "patched encoding must be strictly smaller";

    // Decode fidelity through the production decoder.
    std::vector<uint32_t> back;
    const size_t n = SpimiPforDecoder::DecodeBlockFromBytes(patched, &back);
    ASSERT_EQ(n, values.size());
    EXPECT_EQ(back, values);
}

// No-outlier block: allow_patch=true must produce byte-identical output to
// allow_patch=false (the 0x80 flag stays clear). Golden-byte stability.
TEST(SpimiPforEncoderTest, PatchAllowedButNoOutlierIsByteIdentical) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<uint32_t>(i % 16); // uniform small values, no outlier
    }
    const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
    const auto maybe = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    EXPECT_EQ(plain, maybe) << "no-outlier patched encode must be byte-identical to plain";
    EXPECT_EQ(maybe[2] & 0x80U, 0U) << "0x80 must stay clear with no outlier";
}

// An OLD-format (0x80 clear) block decodes identically under the patch-aware
// decoder: backward-compat proof that the new decoder reads legacy bytes.
TEST(SpimiPforEncoderTest, LegacyBlockDecodesUnderPatchAwareDecoder) {
    std::mt19937 rng(0xFA11BACU);
    for (int trial = 0; trial < 50; ++trial) {
        const size_t n = 1 + (rng() % SpimiPforEncoder::kBlockSize);
        std::vector<uint32_t> values(n);
        for (auto& v : values) {
            v = rng() % 1000U;
        }
        const auto legacy = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
        std::vector<uint32_t> back;
        const size_t got = SpimiPforDecoder::DecodeBlockFromBytes(legacy, &back);
        ASSERT_EQ(got, values.size());
        EXPECT_EQ(back, values);
    }
}

// Differential: for the same input, patch-enabled and patch-disabled decode
// to identical values, and patched_size <= plain_size for every block.
TEST(SpimiPforEncoderTest, PatchedAndPlainDecodeIdenticallyAcrossWorkloads) {
    std::mt19937 rng(0x5EED1234U);
    for (int trial = 0; trial < 200; ++trial) {
        const size_t n = 1 + (rng() % SpimiPforEncoder::kBlockSize);
        std::vector<uint32_t> values(n);
        // Mostly small values; sprinkle occasional outliers to exercise the
        // patch path on a fraction of trials.
        for (auto& v : values) {
            v = (rng() % 16U == 0U) ? (1000U + rng() % 100000U) : (rng() % 8U);
        }
        const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
        const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
        EXPECT_LE(patched.size(), plain.size())
                << "patched must never be larger (trial " << trial << ")";
        std::vector<uint32_t> dp;
        std::vector<uint32_t> dpp;
        ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(plain, &dp), n);
        ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &dpp), n);
        EXPECT_EQ(dp, values);
        EXPECT_EQ(dpp, values);
    }
}

// ---------------------------------------------------------------------------
// Tester-added coverage: explicit no-trigger/trigger byte oracles, structural
// workloads (all-same / all-max / ascending), hand-constructed OLD-format
// backward-compat decode, fixed-seed adversarial fuzz, and patch-trailer
// untrusted-byte hard-fails. These supplement the implementer's tests above.
// ---------------------------------------------------------------------------

namespace {

// Hand-builds a raw OLD-format (pre-patch, 0x80-clear) PFOR block from a
// width and an explicit list of payload bytes. This bypasses the encoder
// entirely so the test asserts the decoder reads bytes that could have come
// from a segment written before OPT_PFOR_PATCH_FREQS existed.
std::vector<uint8_t> BuildLegacyBlock(uint32_t count, uint8_t width,
                                      const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> bytes;
    // VInt(count) — same little-endian 7-bit continuation the encoder writes.
    uint32_t v = count;
    while (true) {
        uint8_t b = static_cast<uint8_t>(v & 0x7FU);
        v >>= 7;
        if (v != 0U) {
            b |= 0x80U;
            bytes.push_back(b);
        } else {
            bytes.push_back(b);
            break;
        }
    }
    bytes.push_back(width); // 0x80 deliberately clear: legacy width byte
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

} // namespace

// (a) No-trigger oracle: a block with NO outliers must emit the OLD bytes.
// We compare patch-enabled output byte-for-byte against a hand-built legacy
// block (independent of the encoder's own plain path) AND decode identically.
TEST(SpimiPforEncoderTest, NoOutlierEmitsLegacyBytesExactly) {
    // 8 values all in [0,255] ⇒ width=8, payload = the values themselves.
    std::vector<uint32_t> values {3, 7, 200, 11, 250, 0, 1, 128};
    const std::vector<uint8_t> payload {3, 7, 200, 11, 250, 0, 1, 128};
    const auto legacy = BuildLegacyBlock(8, /*width=*/8, payload);

    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    EXPECT_EQ(patched, legacy)
            << "no-outlier patch-enabled encode must equal hand-built legacy bytes";
    EXPECT_EQ(patched[1] & 0x80U, 0U) << "0x80 must be clear (width byte at index 1 here)";

    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &back), values.size());
    EXPECT_EQ(back, values);
}

// (d) Backward-compat: decode a hand-constructed OLD-format block (never
// produced by this encoder run) under the patch-aware decoder. width=4,
// 4 values {1,2,3,4}: nibble-packed LSB-first ⇒ bytes 0x21, 0x43.
TEST(SpimiPforEncoderTest, HandBuiltLegacyBlockDecodes) {
    const std::vector<uint8_t> payload {0x21, 0x43};
    const auto legacy = BuildLegacyBlock(/*count=*/4, /*width=*/4, payload);
    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(legacy, &back), 4U);
    const std::vector<uint32_t> expected {1, 2, 3, 4};
    EXPECT_EQ(back, expected) << "patch-aware decoder must read legacy nibble-packed bytes";
}

// (c) all-same: every value identical. Both modes round-trip; no patch.
TEST(SpimiPforEncoderTest, AllSameValueRoundTrips) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 0x1234U);
    ExpectRoundTrip(values);
    const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    EXPECT_EQ(plain, patched) << "uniform block has no outlier ⇒ byte-identical";
    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &back), values.size());
    EXPECT_EQ(back, values);
}

// (c) all-max: every value 0xFFFFFFFF ⇒ width=32. No narrower base exists,
// so the patch path can never win; output stays plain and round-trips.
TEST(SpimiPforEncoderTest, AllMaxValueRoundTripsAndStaysPlain) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 0xFFFFFFFFU);
    const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    EXPECT_EQ(plain, patched) << "all-max ⇒ no narrower base ⇒ plain, byte-identical";
    EXPECT_EQ(patched[2] & 0x80U, 0U) << "0x80 clear (width byte index 2 for VInt(128))";
    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &back), values.size());
    EXPECT_EQ(back, values);
}

// (c) ascending: strictly increasing values, both modes round-trip and the
// batched decode matches the scalar reference oracle.
TEST(SpimiPforEncoderTest, AscendingValuesRoundTrip) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<uint32_t>(i) * 7U + 1U;
    }
    ExpectRoundTrip(values);
    ExpectDecodeMatchesScalarReference(values);
    // allow_patch may or may not trigger; in either case it must round-trip.
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &back), values.size());
    EXPECT_EQ(back, values);
}

// (b) Trigger case at the high-magnitude extreme: outliers at 32 bits among
// 1-bit values. The patch path must round-trip exactly (exception high bits
// reconstructed) — the adversarial maximum for the OR-back-in step.
TEST(SpimiPforEncoderTest, PatchedWith32BitOutliersRoundTrips) {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 1U);
    values[10] = 0xFFFFFFFFU;
    values[77] = 0x80000000U;
    values[120] = 0xCAFEBABEU;
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    std::vector<uint32_t> back;
    ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &back), values.size());
    EXPECT_EQ(back, values) << "32-bit outliers must reconstruct through the patch trailer";
}

// (c) Fixed-seed adversarial fuzz: many distributions (all-same, sparse
// outliers, dense outliers, bimodal, full random) all decode identically
// under both modes, patched is never larger, and the batched decode matches
// the scalar reference. Deterministic via a fixed seed.
TEST(SpimiPforEncoderTest, FuzzPatchedAndPlainAgreeFixedSeed) {
    std::mt19937 rng(0xABCDEF01U);
    for (int trial = 0; trial < 500; ++trial) {
        const size_t n = 1 + (rng() % SpimiPforEncoder::kBlockSize);
        std::vector<uint32_t> values(n);
        const int shape = trial % 5;
        for (size_t i = 0; i < n; ++i) {
            switch (shape) {
            case 0: // all-same
                values[i] = 42U;
                break;
            case 1: // sparse large outliers
                values[i] = (rng() % 20U == 0U) ? (1U << (rng() % 31)) : (rng() % 4U);
                break;
            case 2: // dense outliers (half are large)
                values[i] = (rng() % 2U == 0U) ? (rng() % 1000000U) : (rng() % 4U);
                break;
            case 3: // bimodal tight clusters
                values[i] = (rng() % 2U == 0U) ? (rng() % 8U) : (60000U + rng() % 8U);
                break;
            default: // full-range random
                values[i] = rng();
                break;
            }
        }
        const auto plain = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/false);
        const auto patched = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
        EXPECT_LE(patched.size(), plain.size()) << "patched never larger (trial " << trial << ")";

        std::vector<uint32_t> dp;
        std::vector<uint32_t> dpp;
        ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(plain, &dp), n) << "trial " << trial;
        ASSERT_EQ(SpimiPforDecoder::DecodeBlockFromBytes(patched, &dpp), n) << "trial " << trial;
        EXPECT_EQ(dp, values) << "plain decode mismatch (trial " << trial << ")";
        EXPECT_EQ(dpp, values) << "patched decode mismatch (trial " << trial << ")";

        // Whenever the plain block is purely bitpacked (0x80 clear), the
        // batched decode must match the scalar reference loop bit-for-bit.
        const auto reference = ReferenceScalarDecode(plain);
        EXPECT_EQ(dp, reference) << "batched vs scalar divergence (trial " << trial << ")";
    }
}

// ---------------------------------------------------------------------------
// Patch-trailer untrusted-byte hard-fails. Each test crafts a structurally
// valid patched block, then corrupts exactly one trailer field and asserts
// the decoder throws (SPIMI_THROW_CORRUPT → doris::Exception) rather than
// reading out of bounds or producing garbage.
// ---------------------------------------------------------------------------

namespace {

// Produces a real patched block (0x80 set) for use as a corruption seed.
std::vector<uint8_t> MakePatchedSeed() {
    std::vector<uint32_t> values(SpimiPforEncoder::kBlockSize, 1U);
    values[3] = 50000U;
    values[40] = 60000U;
    values[99] = 45000U;
    auto bytes = SpimiPforEncoder::EncodeBlockToBytes(values, /*allow_patch=*/true);
    EXPECT_NE(bytes[2] & 0x80U, 0U) << "seed must actually be patched";
    return bytes;
}

// Locates the trailer offset = first byte after VInt(count) + width byte +
// ceil(count*base_width/8) bitpacked payload. count is 128 here (VInt = 2
// bytes), base_width is the low 6 bits of byte[2].
size_t TrailerOffset(const std::vector<uint8_t>& patched) {
    const size_t count = 128; // MakePatchedSeed uses a full block
    const uint8_t base_width = patched[2] & 0x3FU;
    const size_t payload = (count * base_width + 7U) / 8U;
    return 2U /*VInt(128)*/ + 1U /*width*/ + payload;
}

} // namespace

TEST(SpimiPforEncoderTest, PatchTrailerNumExceptionsZeroHardFails) {
    auto bytes = MakePatchedSeed();
    const size_t off = TrailerOffset(bytes);
    ASSERT_LT(off, bytes.size());
    bytes[off] = 0x00U; // num_exceptions = 0 is illegal
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

TEST(SpimiPforEncoderTest, PatchTrailerNumExceptionsTooLargeHardFails) {
    auto bytes = MakePatchedSeed();
    const size_t off = TrailerOffset(bytes);
    ASSERT_LT(off, bytes.size());
    bytes[off] = 0xFFU; // 255 > count(128) ⇒ out of range
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

TEST(SpimiPforEncoderTest, PatchTrailerExceptWidthZeroHardFails) {
    auto bytes = MakePatchedSeed();
    const size_t off = TrailerOffset(bytes);
    ASSERT_LT(off + 1U, bytes.size());
    bytes[off + 1U] = 0x00U; // except_width = 0 is illegal
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

TEST(SpimiPforEncoderTest, PatchTrailerExceptWidthOverflowsHardFails) {
    auto bytes = MakePatchedSeed();
    const size_t off = TrailerOffset(bytes);
    ASSERT_LT(off + 1U, bytes.size());
    // base_width + except_width must be ≤ 32; force a value that overflows.
    bytes[off + 1U] = 33U;
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

TEST(SpimiPforEncoderTest, PatchTrailerExceptionPositionOutOfRangeHardFails) {
    auto bytes = MakePatchedSeed();
    const size_t off = TrailerOffset(bytes);
    // First position byte sits at off+2 (after num_exc, except_width).
    ASSERT_LT(off + 2U, bytes.size());
    bytes[off + 2U] = 200U; // position ≥ count(128) ⇒ out of range
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

TEST(SpimiPforEncoderTest, PatchTrailerTruncatedHighBitsHardFails) {
    auto bytes = MakePatchedSeed();
    // Drop the final high-bit payload byte ⇒ ReadBitpacked underflow.
    bytes.pop_back();
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, &out), doris::Exception);
}

} // namespace doris::segment_v2::inverted_index::spimi
