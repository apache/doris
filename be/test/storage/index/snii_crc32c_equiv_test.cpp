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

// R05-crc32c: proves the snii crc32c header (now a thin inline wrapper over the
// Doris/Google crc32c thirdparty) yields byte-identical checksums to both (a) the
// well-known canonical CRC32C test vectors and (b) the Doris ::crc32c library it
// delegates to. (a) guards the on-disk format value; (b) guards the delegation
// contract for every call site (one-shot, seeded extend, and segmented chaining).
// These checks are deterministic and need no pre-captured golden fixture.

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/crc32c.h"

namespace {

using snii::Slice;

// Fills n deterministic bytes from a seeded engine. The exact bytes are
// irrelevant to the equivalence asserts (both sides hash the same buffer); the
// fixed seed only keeps runs reproducible.
std::vector<uint8_t> make_bytes(std::mt19937_64& rng, size_t n) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>(rng() & 0xFFU);
    }
    return v;
}

// Lengths exercising the slice-by-8 main loop plus every <8-byte tail remainder,
// the 4-byte step, sub-block sizes, and several >1KB buffers.
constexpr size_t kLengths[] = {0,  1,  2,  3,  4,   5,   7,   8,   9,    15,   16,   17,   31,  32,
                               33, 63, 64, 65, 127, 128, 255, 256, 1023, 1024, 1025, 4096, 5000};

} // namespace

// (a) Hardcoded canonical CRC32C vectors. A reuse that altered the value -- and
// therefore the on-disk format -- would break these.
TEST(SniiCrc32cEquiv, StandardVectors) {
    // Classic CRC-32C / iSCSI check value for the ASCII string "123456789".
    {
        const char* s = "123456789";
        EXPECT_EQ(0xE3069283U, snii::crc32c(Slice(reinterpret_cast<const uint8_t*>(s), 9)));
    }
    // Empty input conditions to 0 (Extend(0, ., 0)).
    EXPECT_EQ(0x00000000U, snii::crc32c(Slice(nullptr, 0)));

    // RFC 3720 section B.4 vectors, matching be/test/util/crc32c_test.cpp.
    uint8_t buf[32];
    std::memset(buf, 0x00, sizeof(buf));
    EXPECT_EQ(0x8A9136AAU, snii::crc32c(Slice(buf, sizeof(buf))));

    std::memset(buf, 0xFF, sizeof(buf));
    EXPECT_EQ(0x62A8AB43U, snii::crc32c(Slice(buf, sizeof(buf))));

    for (int i = 0; i < 32; ++i) {
        buf[i] = static_cast<uint8_t>(i);
    }
    EXPECT_EQ(0x46DD794EU, snii::crc32c(Slice(buf, sizeof(buf))));

    for (int i = 0; i < 32; ++i) {
        buf[i] = static_cast<uint8_t>(31 - i);
    }
    EXPECT_EQ(0x113FDB5CU, snii::crc32c(Slice(buf, sizeof(buf))));
}

// (b1) One-shot equivalence: snii::crc32c(Slice(buf, n)) == ::crc32c::Crc32c(buf, n).
TEST(SniiCrc32cEquiv, OneShotMatchesDorisLib) {
    std::mt19937_64 rng(0xC0FFEEULL);
    for (size_t n : kLengths) {
        const std::vector<uint8_t> buf = make_bytes(rng, n);
        const uint8_t* p = buf.data();
        EXPECT_EQ(snii::crc32c(Slice(p, n)), ::crc32c::Crc32c(p, n)) << "len=" << n;
    }
}

// (b2) Seeded extend equivalence: snii::crc32c_extend(prev, Slice) == ::crc32c::Extend(prev, ...),
// covering prev == 0 and several non-zero priors.
TEST(SniiCrc32cEquiv, ExtendMatchesDorisLib) {
    std::mt19937_64 rng(0x12345678ULL);
    const uint32_t priors[] = {0U, 1U, 0xFFFFFFFFU, 0xDEADBEEFU, 0x80000000U};
    for (uint32_t prev : priors) {
        for (size_t n : kLengths) {
            const std::vector<uint8_t> buf = make_bytes(rng, n);
            const uint8_t* p = buf.data();
            EXPECT_EQ(snii::crc32c_extend(prev, Slice(p, n)), ::crc32c::Extend(prev, p, n))
                    << "prev=" << prev << " len=" << n;
        }
    }
}

// (b3) Segmented chaining: extend over a split equals the one-shot over the whole,
// and matches the Doris lib's Extend(Crc32c(prefix), suffix) -- i.e. the
// Extend(Extend(0, a), b) == Crc32c(a ++ b) property every framed section relies on.
TEST(SniiCrc32cEquiv, ExtendSplitEqualsWhole) {
    std::mt19937_64 rng(0xABCDEFULL);
    const size_t totals[] = {1, 2, 8, 9, 16, 17, 100, 1024, 4096};
    for (size_t n : totals) {
        const std::vector<uint8_t> buf = make_bytes(rng, n);
        const uint8_t* p = buf.data();
        const uint32_t whole = snii::crc32c(Slice(p, n));
        for (size_t k = 0; k <= n; ++k) {
            const uint32_t chained =
                    snii::crc32c_extend(snii::crc32c(Slice(p, k)), Slice(p + k, n - k));
            EXPECT_EQ(whole, chained) << "n=" << n << " k=" << k;
            EXPECT_EQ(chained, ::crc32c::Extend(::crc32c::Crc32c(p, k), p + k, n - k))
                    << "n=" << n << " k=" << k;
        }
    }
}

// (b4) Cross-decode both directions: a crc stamped by the snii wrapper verifies
// under the Doris lib, and a crc stamped by the Doris lib verifies under the snii
// wrapper -- the bidirectional guarantee that old and new on-disk bytes interop.
TEST(SniiCrc32cEquiv, CrossDecodeBidirectional) {
    std::mt19937_64 rng(0x5A5A5A5AULL);
    for (size_t n : kLengths) {
        const std::vector<uint8_t> buf = make_bytes(rng, n);
        const uint8_t* p = buf.data();
        EXPECT_EQ(snii::crc32c(Slice(p, n)), ::crc32c::Crc32c(p, n)) << "len=" << n;
        EXPECT_EQ(::crc32c::Crc32c(p, n), snii::crc32c(Slice(p, n))) << "len=" << n;
    }
}
