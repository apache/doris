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

#include "snii/format/bsbf.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/io/local_file.h"

namespace snii::format {
namespace {

// Independent Parquet-canonical oracle (scalar): XXH64 seed 0 hash (via bsbf_hash),
// fastrange block index, SALT masks. Verifies BsbfBuilder (which may run AVX2) is
// algorithmically correct AND Parquet-canonical.
struct Oracle {
    std::vector<uint32_t> w;
    uint32_t nblocks = 0;
    void init(uint32_t ndv, double fpp) {
        const uint32_t nb = bsbf_optimal_num_bytes(ndv, fpp);
        nblocks = nb / kBsbfBytesPerBlock;
        w.assign(nb / 4, 0U);
    }
    static void masks(uint32_t key, uint32_t m[8]) {
        for (int i = 0; i < 8; ++i) {
            m[i] = 1U << ((key * kBsbfSalt[i]) >> 27);
        }
    }
    void insert(uint64_t h) {
        const auto b = static_cast<uint32_t>(((h >> 32) * nblocks) >> 32);
        uint32_t m[8];
        masks(static_cast<uint32_t>(h), m);
        for (int i = 0; i < 8; ++i) {
            w[b * 8 + i] |= m[i];
        }
    }
    bool find(uint64_t h) const {
        const auto b = static_cast<uint32_t>(((h >> 32) * nblocks) >> 32);
        uint32_t m[8];
        masks(static_cast<uint32_t>(h), m);
        for (int i = 0; i < 8; ++i) {
            if ((w[b * 8 + i] & m[i]) != m[i]) {
                return false;
            }
        }
        return true;
    }
};

std::string tmp_path() {
    return "/tmp/snii_bsbf_" + std::to_string(::getpid()) + "_" + std::to_string(std::rand()) +
           ".bin";
}

TEST(SniiBsbf, BuildProbeMatchesParquetOracle) {
    const uint32_t n = 100000;
    const double fpp = 0.01;
    BsbfBuilder b;
    ASSERT_TRUE(BsbfBuilder::create(n, fpp, &b).ok());
    Oracle o;
    o.init(n, fpp);
    ASSERT_EQ(b.num_blocks(), o.nblocks);
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t h = bsbf_hash("t" + std::to_string(i));
        b.insert(h);
        o.insert(h);
    }
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t h = bsbf_hash("t" + std::to_string(i));
        EXPECT_TRUE(b.maybe_contains(h)) << i;     // no false negatives
        EXPECT_EQ(b.maybe_contains(h), o.find(h)); // == Parquet oracle (AVX2==scalar)
    }
    uint32_t fp = 0;
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t h = bsbf_hash("absent_" + std::to_string(i));
        EXPECT_EQ(b.maybe_contains(h), o.find(h));
        if (b.maybe_contains(h)) {
            ++fp;
        }
    }
    EXPECT_LT(fp, static_cast<uint32_t>(n * 0.05)); // FPR << 5% (target 1%)
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiBsbf, SerializeParseOnDemandProbe) {
    const uint32_t n = 50000;
    BsbfBuilder b;
    ASSERT_TRUE(BsbfBuilder::create(n, 0.01, &b).ok());
    std::vector<uint64_t> keys;
    for (uint32_t i = 0; i < n; ++i) {
        const uint64_t h = bsbf_hash("k" + std::to_string(i));
        keys.push_back(h);
        b.insert(h);
    }
    // Serialize at a non-zero section base (prefix) to exercise the constant offset.
    ByteSink sink;
    const uint64_t prefix = 777;
    for (uint64_t i = 0; i < prefix; ++i) {
        sink.put_u8(0xAB);
    }
    ASSERT_TRUE(b.serialize(&sink).ok());
    EXPECT_EQ(sink.size(), prefix + kBsbfHeaderSize + b.num_bytes()); // 28B header, exact

    const std::string path = tmp_path();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        ASSERT_TRUE(w.append(sink.view()).ok());
        ASSERT_TRUE(w.finalize().ok());
    }
    io::LocalFileReader r;
    ASSERT_TRUE(r.open(path).ok());
    std::vector<uint8_t> hdr;
    ASSERT_TRUE(r.read_at(prefix, kBsbfHeaderSize, &hdr).ok());
    BsbfHeader h;
    ASSERT_TRUE(BsbfHeader::parse(Slice(hdr), prefix, &h).ok());
    EXPECT_EQ(h.num_bytes, b.num_bytes());
    EXPECT_EQ(h.bitset_base, prefix + kBsbfHeaderSize);

    for (uint32_t i = 0; i < 2000; ++i) {
        bool mp = false;
        ASSERT_TRUE(bsbf_probe(&r, h, keys[i], &mp).ok());
        EXPECT_TRUE(mp) << i;                     // present -> maybe
        EXPECT_EQ(mp, b.maybe_contains(keys[i])); // on-demand == in-memory
    }
    for (uint32_t i = 0; i < 2000; ++i) {
        const uint64_t a = bsbf_hash("absent_" + std::to_string(i));
        bool mp = false;
        ASSERT_TRUE(bsbf_probe(&r, h, a, &mp).ok());
        EXPECT_EQ(mp, b.maybe_contains(a)); // incl. any false positive, identical
    }
    std::remove(path.c_str());
}

TEST(SniiBsbf, HeaderValidation) {
    BsbfBuilder b;
    ASSERT_TRUE(BsbfBuilder::create(1000, 0.01, &b).ok());
    ByteSink sink;
    ASSERT_TRUE(b.serialize(&sink).ok());
    const std::vector<uint8_t> bytes(sink.buffer());
    BsbfHeader h;
    EXPECT_TRUE(BsbfHeader::parse(Slice(bytes.data(), kBsbfHeaderSize), 0, &h).ok());

    auto fails = [](std::vector<uint8_t> c) {
        BsbfHeader hh;
        return !BsbfHeader::parse(Slice(c.data(), kBsbfHeaderSize), 0, &hh).ok();
    };
    {
        auto c = bytes;
        c[0] = 'X';
        EXPECT_TRUE(fails(c));
    } // magic
    {
        auto c = bytes;
        c[4] = 9;
        EXPECT_TRUE(fails(c));
    } // version
    {
        auto c = bytes;
        c[5] = 1;
        EXPECT_TRUE(fails(c));
    } // hash strategy
    {
        auto c = bytes;
        c[8] ^= 0xFF;
        EXPECT_TRUE(fails(c));
    }                                                                     // field -> header crc
    EXPECT_FALSE(BsbfHeader::parse(Slice(bytes.data(), 10), 0, &h).ok()); // short
}

TEST(SniiBsbf, FastrangeNotMaskVariant) {
    // The fastrange index must differ from Doris's `&(n-1)` for some hash, proving we
    // use the Parquet-canonical multiply-shift (not the mask variant).
    const uint32_t nblocks = 4096; // power of 2 so both formulas are defined
    bool differ = false;
    for (uint64_t s = 1; s < 200 && !differ; ++s) {
        const uint64_t h = bsbf_hash("probe" + std::to_string(s));
        const uint32_t fastrange = bsbf_block_index(h, nblocks);
        const uint32_t mask = static_cast<uint32_t>(h >> 32) & (nblocks - 1);
        if (fastrange != mask) {
            differ = true;
        }
    }
    EXPECT_TRUE(differ);
}

} // namespace
} // namespace snii::format
