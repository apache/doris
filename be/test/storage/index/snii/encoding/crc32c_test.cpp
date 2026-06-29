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

#include "snii/encoding/crc32c.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/status.h"

// Integrated crc32c.h pulls in the thirdparty `namespace crc32c`, so a blanket
// `using namespace snii;` makes a bare crc32c() call ambiguous; pull in only the
// Slice type and qualify the snii::crc32c free functions explicitly.
using snii::Slice;

// leveldb/RocksDB standard CRC32C(Castagnoli) test vectors.
TEST(SniiCrc32c, KnownVectors) {
    std::vector<uint8_t> zeros(32, 0x00);
    EXPECT_EQ(snii::crc32c(Slice(zeros)), 0x8a9136aaU);
    std::vector<uint8_t> ff(32, 0xff);
    EXPECT_EQ(snii::crc32c(Slice(ff)), 0x62a8ab43U);
    std::vector<uint8_t> ramp(32);
    for (int i = 0; i < 32; ++i) {
        ramp[i] = static_cast<uint8_t>(i);
    }
    EXPECT_EQ(snii::crc32c(Slice(ramp)), 0x46dd794eU);
}

TEST(SniiCrc32c, ExtendEqualsContiguous) {
    std::vector<uint8_t> v {1, 2, 3, 4, 5, 6, 7, 8};
    uint32_t whole = snii::crc32c(Slice(v));
    uint32_t part = snii::crc32c(Slice(v.data(), 4));
    part = snii::crc32c_extend(part, Slice(v.data() + 4, 4));
    EXPECT_EQ(whole, part);
}

namespace {

// Independent byte-at-a-time CRC32C reference (bit-reflected Castagnoli). Used to
// pin the optimized slice-by-8 / hardware path to the canonical scalar result.
uint32_t crc32c_ref(const std::vector<uint8_t>& data) {
    static constexpr uint32_t kPoly = 0x82F63B78U;
    uint32_t crc = ~0U;
    for (uint8_t b : data) {
        crc ^= b;
        for (int k = 0; k < 8; ++k) {
            crc = (crc & 1) ? (kPoly ^ (crc >> 1)) : (crc >> 1);
        }
    }
    return ~crc;
}

} // namespace

// Sweep every length 0..2048: stresses the 8-byte main loop plus the 0..7 residue
// tail (and the hardware u32/u8 tails) against the scalar reference. Catches any
// slice-by-8 table or unaligned-load bug that the fixed-size vectors above miss.
TEST(SniiCrc32c, MatchesScalarReferenceAllLengths) {
    std::vector<uint8_t> data;
    data.reserve(2048);
    uint32_t x = 0x12345678U;
    for (size_t len = 0; len <= 2048; ++len) {
        EXPECT_EQ(snii::crc32c(Slice(data)), crc32c_ref(data)) << "len=" << len;
        // Pseudo-random next byte (xorshift) so the stream is non-trivial.
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        data.push_back(static_cast<uint8_t>(x));
    }
}

// Splitting a buffer at an arbitrary boundary and extending must equal the
// one-shot crc -- the property the windowed/region layout relies on.
TEST(SniiCrc32c, ExtendAcrossArbitrarySplits) {
    std::vector<uint8_t> data(300);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>(i * 7 + 1);
    }
    const uint32_t whole = snii::crc32c(Slice(data));
    for (size_t split : {0U, 1U, 7U, 8U, 9U, 16U, 100U, 299U, 300U}) {
        uint32_t c = snii::crc32c(Slice(data.data(), split));
        c = snii::crc32c_extend(c, Slice(data.data() + split, data.size() - split));
        EXPECT_EQ(c, whole) << "split=" << split;
    }
}
