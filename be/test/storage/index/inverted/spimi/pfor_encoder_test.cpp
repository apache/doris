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

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"

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

} // namespace doris::segment_v2::inverted_index::spimi
