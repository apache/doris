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

#include "storage/index/snii/encoding/pfor.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"

using namespace doris::snii;

static void roundtrip(const std::vector<uint32_t>& v) {
    ByteSink sink;
    pfor_encode(v.data(), v.size(), &sink);
    ByteSource src(sink.view());
    std::vector<uint32_t> out(v.size());
    ASSERT_TRUE(pfor_decode(&src, v.size(), out.data()).ok());
    EXPECT_EQ(out, v);
}

TEST(SniiPfor, Uniform) {
    roundtrip(std::vector<uint32_t>(200, 5));
}

TEST(SniiPfor, Ramp) {
    std::vector<uint32_t> v;
    for (uint32_t i = 0; i < 256; ++i) {
        v.push_back(i);
    }
    roundtrip(v);
}

TEST(SniiPfor, WithOutliers) {
    std::vector<uint32_t> v(128, 3);
    v[10] = 1000000;
    v[77] = 999;
    roundtrip(v);
}

TEST(SniiPfor, AllZero) {
    roundtrip(std::vector<uint32_t>(64, 0));
}

TEST(SniiPfor, MaxValues) {
    std::vector<uint32_t> v(32, 0xFFFFFFFFU);
    v[0] = 0;
    roundtrip(v);
}

// Exhaustive: every bit width 0..32, capped random values, across a spread of n
// (including non-power-of-two and prime lengths) so the bit-unpacker's tail handling
// (partial trailing 64-bit word / partial byte) is exercised for each width. A
// deterministic LCG keeps it reproducible without Math.random.
TEST(SniiPfor, AllWidthsRandomLengths) {
    uint64_t rng = 0x9E3779B97F4A7C15ULL;
    auto next = [&rng]() {
        rng = rng * 6364136223846793005ULL + 1442695040888963407ULL;
        return static_cast<uint32_t>(rng >> 32);
    };
    const size_t lens[] = {1, 2, 3, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 200, 257};
    for (int w = 0; w <= 32; ++w) {
        const uint32_t cap = (w >= 32) ? 0xFFFFFFFFU : ((1U << w) - 1U);
        for (size_t n : lens) {
            std::vector<uint32_t> v(n);
            for (size_t i = 0; i < n; ++i) {
                v[i] = (w == 0) ? 0U : (next() & cap);
            }
            roundtrip(v); // byte-identical decode required for every (w, n)
        }
    }
}

// A few exceptions sprinkled into otherwise-narrow data, at boundary indices, to
// cover the exception-patch path alongside the fast unpack.
TEST(SniiPfor, ExceptionsAtBoundaries) {
    std::vector<uint32_t> v(130, 2);
    v[0] = 0x00ABCDEF;  // first
    v[63] = 0x12345678; // around the 64-value word boundary
    v[64] = 0x0BADF00D;
    v[129] = 0xFFFFFFFF; // last
    roundtrip(v);
}

// ---------------------------------------------------------------------------
// T11: histogram + suffix-sum choose_width and single-pass pfor_encode.
//
// The oracle helpers below are a faithful transcription of the PRE-T11 encoder
// (O(maxw*n) bits_for width selection + std::vector low/exc split). The optimized
// pfor_encode must (a) select the same bit_width and (b) emit byte-identical
// output for arbitrary inputs -- the strongest guard that the on-disk format is
// unchanged. The deterministic op-count seam proves each value's bit-width is
// evaluated exactly once per run (vs the former ~(maxw+2) times).
// ---------------------------------------------------------------------------
namespace {

uint8_t ref_bits_for(uint32_t v) {
    uint8_t b = 0;
    while (v) {
        ++b;
        v >>= 1;
    }
    return b;
}

// Original O(maxw*n) width selection -- identical cost formula and ascending-w
// strict-'<' tie-break.
uint8_t ref_choose_width(const std::vector<uint32_t>& v) {
    const size_t n = v.size();
    uint8_t maxw = 0;
    for (size_t i = 0; i < n; ++i) {
        // NOLINTNEXTLINE(readability-use-std-min-max): naive reference mirror of the original
        if (ref_bits_for(v[i]) > maxw) {
            maxw = ref_bits_for(v[i]);
        }
    }
    uint8_t best = maxw;
    size_t best_cost = SIZE_MAX;
    for (uint8_t w = 0; w <= maxw; ++w) {
        size_t exc = 0;
        for (size_t i = 0; i < n; ++i) {
            if (ref_bits_for(v[i]) > w) {
                ++exc;
            }
        }
        const size_t cost = (static_cast<size_t>(w) * n + 7) / 8 + exc * 6;
        if (cost < best_cost) {
            best_cost = cost;
            best = w;
        }
    }
    return best;
}

uint32_t ref_low_mask(uint8_t w) {
    return (w >= 32) ? 0xFFFFFFFFU : ((1U << w) - 1U);
}

// Faithful transcription of the pre-T11 pfor_encode byte layout.
std::vector<uint8_t> reference_pfor_encode(const std::vector<uint32_t>& values) {
    const size_t n = values.size();
    const uint8_t w = ref_choose_width(values);
    ByteSink sink;
    std::vector<std::pair<uint32_t, uint32_t>> exc; // (index, full value)
    std::vector<uint32_t> low(values.begin(), values.end());
    for (size_t i = 0; i < n; ++i) {
        if (ref_bits_for(values[i]) > w) {
            exc.emplace_back(static_cast<uint32_t>(i), values[i]);
            low[i] = 0;
        }
    }
    sink.put_u8(w);
    sink.put_varint32(static_cast<uint32_t>(exc.size()));
    if (w != 0) {
        uint64_t acc = 0;
        int filled = 0;
        for (size_t i = 0; i < n; ++i) {
            acc |= static_cast<uint64_t>(low[i] & ref_low_mask(w)) << filled;
            filled += w;
            while (filled >= 8) {
                sink.put_u8(static_cast<uint8_t>(acc));
                acc >>= 8;
                filled -= 8;
            }
        }
        if (filled > 0) {
            sink.put_u8(static_cast<uint8_t>(acc));
        }
    }
    uint32_t prev = 0;
    for (const auto& e : exc) {
        sink.put_varint32(e.first - prev);
        sink.put_varint32(e.second);
        prev = e.first;
    }
    return sink.buffer();
}

// Deterministic LCG (same constants as AllWidthsRandomLengths above).
struct Lcg {
    uint64_t state;
    explicit Lcg(uint64_t seed) : state(seed) {}
    uint32_t next() {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return static_cast<uint32_t>(state >> 32);
    }
};

std::vector<uint8_t> encode_to_bytes(const std::vector<uint32_t>& v) {
    ByteSink sink;
    pfor_encode(v.data(), v.size(), &sink);
    return sink.buffer();
}

// Representative inputs spanning the layout's branches: all-zero, all-one,
// monotonic delta, freq-like (mostly 1 with bursts), bit31-set large values, and
// narrow data with sparse wide exceptions.
std::vector<std::vector<uint32_t>> representative_inputs() {
    std::vector<std::vector<uint32_t>> sets;
    sets.emplace_back(256, 0U); // all zero -> w == 0
    sets.emplace_back(256, 1U); // all one  -> w == 1
    {
        std::vector<uint32_t> ramp(256);
        for (uint32_t i = 0; i < 256; ++i) {
            ramp[i] = i; // monotonic delta
        }
        sets.push_back(std::move(ramp));
    }
    {
        std::vector<uint32_t> freq(256, 1U); // freq-like: mostly 1, a few bursts
        freq[3] = 5;
        freq[100] = 9;
        freq[255] = 2;
        sets.push_back(std::move(freq));
    }
    {
        std::vector<uint32_t> big(64, 7U); // some bit31-set values (width 32)
        big[0] = 0x80000000U;
        big[31] = 0xFFFFFFFFU;
        big[63] = 0x80000001U;
        sets.push_back(std::move(big));
    }
    {
        std::vector<uint32_t> mixed(200, 4U); // narrow with sparse wide exceptions
        mixed[5] = 70000;
        mixed[6] = 70000;
        mixed[199] = 1234567;
        sets.push_back(std::move(mixed));
    }
    return sets;
}

} // namespace

// FW-01: random data (mostly small, occasional large to force exceptions) round-trips
// exactly and consumes the whole stream.
TEST(SniiPforTest, RoundTripRandom) {
    Lcg rng(0xD1B54A32D192ED03ULL);
    for (int trial = 0; trial < 50; ++trial) {
        std::vector<uint32_t> v(256);
        for (uint32_t& vi : v) {
            const uint32_t r = rng.next();
            vi = (r % 16 == 0) ? r : (r & 0xFFU); // ~1/16 large, rest in [0,255]
        }
        ByteSink sink;
        pfor_encode(v.data(), v.size(), &sink);
        ByteSource src(sink.view());
        std::vector<uint32_t> out(v.size());
        ASSERT_TRUE(pfor_decode(&src, v.size(), out.data()).ok());
        EXPECT_EQ(out, v);
        EXPECT_TRUE(src.eof());
    }
}

// FW-02: the chosen bit_width (encoded as byte 0) matches the original linear-scan
// selection for representative + random inputs.
TEST(SniiPforTest, WidthMatchesLinearScan) {
    std::vector<std::vector<uint32_t>> sets = representative_inputs();
    Lcg rng(0x0123456789ABCDEFULL);
    for (int t = 0; t < 60; ++t) {
        const uint32_t cap_bits = rng.next() % 33; // target max width 0..32
        const uint32_t cap = (cap_bits >= 32) ? 0xFFFFFFFFU : ((1U << cap_bits) - 1U);
        const size_t n = 1 + (rng.next() % 256);
        std::vector<uint32_t> v(n);
        for (size_t i = 0; i < n; ++i) {
            v[i] = (cap == 0) ? 0U : (rng.next() & cap);
        }
        sets.push_back(std::move(v));
    }
    for (const auto& v : sets) {
        const std::vector<uint8_t> buf = encode_to_bytes(v);
        ASSERT_FALSE(buf.empty());
        EXPECT_EQ(static_cast<int>(buf[0]), static_cast<int>(ref_choose_width(v)));
    }
}

// FW-03: exact, hand-verified golden bytes freeze the on-disk encoding for the
// clean cases (w==0, and clean bit-packing at w==1/4/8). Any drift in width
// choice or bit-packing is caught immediately.
TEST(SniiPforTest, GoldenByteIdentical) {
    using B = std::vector<uint8_t>;
    EXPECT_EQ(encode_to_bytes(std::vector<uint32_t>(8, 0U)), (B {0x00, 0x00}));
    EXPECT_EQ(encode_to_bytes(std::vector<uint32_t>(8, 1U)), (B {0x01, 0x00, 0xFF}));
    EXPECT_EQ(encode_to_bytes(std::vector<uint32_t>(4, 10U)), (B {0x04, 0x00, 0xAA, 0xAA}));
    EXPECT_EQ(encode_to_bytes(std::vector<uint32_t>(4, 200U)),
              (B {0x08, 0x00, 0xC8, 0xC8, 0xC8, 0xC8}));
}

// FW-03 (extended): the optimized encoder is byte-identical to the pre-T11
// reference for representative inputs AND a large random sweep over mixed widths,
// lengths (including n==0 and n>256), and exception densities.
TEST(SniiPforTest, EncodeMatchesLegacyReference) {
    for (const auto& v : representative_inputs()) {
        EXPECT_EQ(encode_to_bytes(v), reference_pfor_encode(v));
    }
    Lcg rng(0x0F0F0F0F0F0F0F0FULL);
    for (int t = 0; t < 300; ++t) {
        const uint32_t cap_bits = rng.next() % 33;
        const uint32_t cap = (cap_bits >= 32) ? 0xFFFFFFFFU : ((1U << cap_bits) - 1U);
        const size_t n = rng.next() % 300; // spans n==0 and the n>256 heap path
        std::vector<uint32_t> v(n);
        for (size_t i = 0; i < n; ++i) {
            // ~1/32 chance of a full-width value above the cap -> an exception.
            uint32_t vi = 0U;
            if (rng.next() % 32 == 0) {
                vi = rng.next();
            } else if (cap != 0) {
                vi = rng.next() & cap;
            }
            v[i] = vi;
        }
        EXPECT_EQ(encode_to_bytes(v), reference_pfor_encode(v))
                << "n=" << n << " cap_bits=" << cap_bits;
    }
}

// FW-04: all zeros -> width 0 (the clz(0) guard), no packed bytes, no exceptions.
TEST(SniiPforTest, AllZeros) {
    std::vector<uint32_t> v(256, 0U);
    ByteSink sink;
    pfor_encode(v.data(), v.size(), &sink);
    const std::vector<uint8_t>& buf = sink.buffer();
    ASSERT_EQ(buf.size(), 2U); // [w=0][n_exc=0], nothing else
    EXPECT_EQ(buf[0], 0U);
    EXPECT_EQ(buf[1], 0U);
    ByteSource src(sink.view());
    std::vector<uint32_t> out(v.size(), 0xDEADBEEFU);
    ASSERT_TRUE(pfor_decode(&src, v.size(), out.data()).ok());
    EXPECT_EQ(out, v);
    EXPECT_TRUE(src.eof());
}

// FW-05: single-element runs (small, zero, max).
TEST(SniiPforTest, SingleElement) {
    roundtrip(std::vector<uint32_t> {42});
    roundtrip(std::vector<uint32_t> {0});
    roundtrip(std::vector<uint32_t> {0xFFFFFFFFU});
}

// FW-06: empty run -> [w=0][n_exc=0]; decode of 0 values is a no-op and consumes
// the header. nullptr values with n==0 must not be dereferenced.
TEST(SniiPforTest, EmptyRun) {
    ByteSink sink;
    pfor_encode(nullptr, 0, &sink);
    ASSERT_EQ(sink.buffer().size(), 2U);
    EXPECT_EQ(sink.buffer()[0], 0U);
    EXPECT_EQ(sink.buffer()[1], 0U);
    ByteSource src(sink.view());
    std::vector<uint32_t> out(1, 0xDEADBEEFU);
    ASSERT_TRUE(pfor_decode(&src, 0, out.data()).ok());
    EXPECT_EQ(out[0], 0xDEADBEEFU); // nothing written
    EXPECT_TRUE(src.eof());
}

// FW-07: values with bit31 set (width 32) round-trip, exercising the width-32
// path (no `value >> w` UB; the exception split uses the cached width comparison).
TEST(SniiPforTest, TopBitSet) {
    std::vector<uint32_t> v(64, 5U);
    v[0] = 0x80000000U;
    v[30] = 0xFFFFFFFFU;
    v[63] = 0xC0000000U;
    roundtrip(v);
    roundtrip(std::vector<uint32_t>(40, 0x80000000U)); // chosen width == 32
}

// FW-08: bimodal data (mostly narrow, a large minority very wide) keeps the chosen
// width small, sending many values to the exception table. Byte-identical to the
// legacy split and round-trips.
TEST(SniiPforTest, ManyExceptions) {
    std::vector<uint32_t> v(256, 5U); // width 3
    Lcg rng(0x55AA55AA55AA55AAULL);
    for (size_t i = 0; i < v.size(); ++i) {
        if (i % 4 == 0) {
            v[i] = 0x20000000U | (rng.next() & 0x1FFFFFFFU); // width 30
        }
    }
    const std::vector<uint8_t> buf = encode_to_bytes(v);
    EXPECT_EQ(buf, reference_pfor_encode(v));
    size_t exceptions = 0;
    for (uint32_t x : v) {
        if (ref_bits_for(x) > buf[0]) {
            ++exceptions;
        }
    }
    EXPECT_GT(exceptions, 50U); // exception-heavy path actually exercised
    roundtrip(v);
}

// FW-09: runs larger than the 256-element stack buffer exercise the heap fallback;
// byte-identical to the reference and round-trip exact.
TEST(SniiPforTest, LargeRunHeapFallback) {
    for (size_t n : {257U, 300U, 512U}) {
        Lcg rng(0xC0FFEEULL + n);
        std::vector<uint32_t> v(n);
        for (size_t i = 0; i < n; ++i) {
            v[i] = rng.next() & 0x3FFU;
        }
        v[n / 2] = 0xABCDEF01U; // an exception
        EXPECT_EQ(encode_to_bytes(v), reference_pfor_encode(v)) << "n=" << n;
        roundtrip(v);
    }
}

// FW-10: a stream whose exception index is out of range must return Corruption,
// not throw. (Decoder path, unchanged by T11, guarded here against regressions.)
TEST(SniiPforTest, CorruptExceptionIndexReturnsCorruption) {
    ByteSink sink;
    sink.put_u8(0);        // w = 0 -> no packed region
    sink.put_varint32(1);  // n_exc = 1
    sink.put_varint32(10); // index_delta = 10 -> idx = 10
    sink.put_varint32(7);  // exception value
    ByteSource src(sink.view());
    std::vector<uint32_t> out(4, 0U); // n = 4, so idx 10 is out of range
    auto st = pfor_decode(&src, 4, out.data());
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << st.to_string();
}

// Perf (deterministic): exactly one bit-width evaluation per value per run -- the
// single histogram pass, with the encoder reusing the cached widths (no 3rd
// bits_for pass, no per-candidate O(maxw*n) rescan). RED on the un-optimized code
// (which evaluated ~(maxw+2)*n times).
TEST(SniiPforPerfTest, WidthEvalsEqualsNPerRun) {
    Lcg rng(0x1357924680ULL);
    for (size_t n : {0U, 1U, 7U, 8U, 9U, 64U, 255U, 256U, 257U, 300U}) {
        std::vector<uint32_t> v(n);
        for (size_t i = 0; i < n; ++i) {
            v[i] = rng.next() & 0xFFFFU;
        }
        doris::snii::testing::reset_pfor_width_evals();
        ByteSink sink;
        pfor_encode(v.data(), v.size(), &sink);
        EXPECT_EQ(doris::snii::testing::pfor_width_evals(), static_cast<uint64_t>(n)) << "n=" << n;
    }
}

// Perf (deterministic): cumulative evaluations across multiple runs equal the total
// number of values encoded.
TEST(SniiPforPerfTest, WidthEvalsEqualsTotalValues) {
    const std::vector<size_t> runs = {256, 256, 100, 1, 256};
    Lcg rng(0x2468ACE0ULL);
    doris::snii::testing::reset_pfor_width_evals();
    size_t total = 0;
    for (size_t n : runs) {
        std::vector<uint32_t> v(n);
        for (size_t i = 0; i < n; ++i) {
            v[i] = rng.next() & 0xFFFFFFU;
        }
        ByteSink sink;
        pfor_encode(v.data(), v.size(), &sink);
        total += n;
    }
    EXPECT_EQ(doris::snii::testing::pfor_width_evals(), static_cast<uint64_t>(total));
}

// Perf (deterministic): worst case for the old O(maxw*n) scan is maxw==32. The old
// encoder evaluated each value ~(maxw+2)==34 times; the histogram path evaluates
// each exactly once, so evals stay == n even when maxw is maximal.
TEST(SniiPforPerfTest, ChooseWidthHasNoInnerRescan) {
    const size_t n = 256;
    std::vector<uint32_t> v(n, 1U);
    v[0] = 0x80000000U; // forces maxw == 32
    doris::snii::testing::reset_pfor_width_evals();
    ByteSink sink;
    pfor_encode(v.data(), v.size(), &sink);
    EXPECT_EQ(doris::snii::testing::pfor_width_evals(), static_cast<uint64_t>(n));
    EXPECT_LT(doris::snii::testing::pfor_width_evals(), static_cast<uint64_t>(2 * n));
}
