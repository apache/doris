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

#include "storage/index/snii/encoding/crc32c.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

// Integrated crc32c.h pulls in the thirdparty `namespace crc32c`, so a blanket
// `using namespace doris::snii;` makes a bare crc32c() call ambiguous; pull in only the
// Slice type and qualify the doris::snii::crc32c free functions explicitly.
using doris::snii::Slice;

// leveldb/RocksDB standard CRC32C(Castagnoli) test vectors.
TEST(SniiCrc32c, KnownVectors) {
    std::vector<uint8_t> zeros(32, 0x00);
    EXPECT_EQ(doris::snii::crc32c(Slice(zeros)), 0x8a9136aaU);
    std::vector<uint8_t> ff(32, 0xff);
    EXPECT_EQ(doris::snii::crc32c(Slice(ff)), 0x62a8ab43U);
    std::vector<uint8_t> ramp(32);
    for (int i = 0; i < 32; ++i) {
        ramp[i] = static_cast<uint8_t>(i);
    }
    EXPECT_EQ(doris::snii::crc32c(Slice(ramp)), 0x46dd794eU);
}

TEST(SniiCrc32c, ExtendEqualsContiguous) {
    std::vector<uint8_t> v {1, 2, 3, 4, 5, 6, 7, 8};
    uint32_t whole = doris::snii::crc32c(Slice(v));
    uint32_t part = doris::snii::crc32c(Slice(v.data(), 4));
    part = doris::snii::crc32c_extend(part, Slice(v.data() + 4, 4));
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
        EXPECT_EQ(doris::snii::crc32c(Slice(data)), crc32c_ref(data)) << "len=" << len;
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
    const uint32_t whole = doris::snii::crc32c(Slice(data));
    for (size_t split : {0U, 1U, 7U, 8U, 9U, 16U, 100U, 299U, 300U}) {
        uint32_t c = doris::snii::crc32c(Slice(data.data(), split));
        c = doris::snii::crc32c_extend(c, Slice(data.data() + split, data.size() - split));
        EXPECT_EQ(c, whole) << "split=" << split;
    }
}

// =============================================================================
// T21 -- CRC32C 3-way interleaved hardware reference paths.
//
// Production crc32c()/crc32c_extend() delegate to the bundled Google crc32c
// thirdparty (already hardware-accelerated + interleaved). The doris::snii::detail
// seam below exposes three reference sub-paths -- portable slice-by-8, serial
// SSE4.2 hardware, and the 3-way interleaved SSE4.2 hardware algorithm with a
// GF(2) shift-combine -- so these tests can prove, byte-for-byte, that the
// production path equals the canonical CRC32C across every size/alignment and that
// the hardware path is engaged. detail::* is compiled only under BE_TEST.
// =============================================================================

namespace {

// Alias only the detail seam; the free crc32c()/crc32c_extend() are qualified at
// every call site below because a bare `crc32c` is ambiguous with the thirdparty
// `crc32c` namespace pulled in by <crc32c/crc32c.h> (see the file-header note).
namespace detail = doris::snii::detail;

// Deterministic pseudo-random buffer with `pad` leading bytes, so a Slice can start
// at an arbitrary alignment offset into the heap allocation. The exact bytes are
// irrelevant to the equivalence asserts (all paths hash the same buffer); the
// seeded engine only keeps runs reproducible.
std::vector<uint8_t> make_padded_bytes(uint64_t seed, size_t pad, size_t n) {
    std::vector<uint8_t> v(pad + n);
    std::mt19937_64 rng(seed);
    for (auto& b : v) {
        b = static_cast<uint8_t>(rng() & 0xFFU);
    }
    return v;
}

// Sizes spanning the 8-byte main loop, every <8 tail, the 4-byte step, and buffers
// straddling the 1024-byte interleave threshold (below/at/above and exact 3x).
constexpr size_t kSweepSizes[] = {0,   1,    7,    8,    9,    15,   16,   255,
                                  256, 1023, 1024, 1025, 3072, 4096, 65536};
constexpr size_t kAlignments[] = {0, 1, 2, 3, 4, 7};

} // namespace

// FV-1: hw3 == slice8 == hw_serial == production crc32c across all sizes/alignments.
TEST(SniiCrc32cTest, Hw3MatchesSlice8AcrossSizesAndAlignments) {
    for (size_t n : kSweepSizes) {
        for (size_t align : kAlignments) {
            const std::vector<uint8_t> buf =
                    make_padded_bytes(0x9E3779B97F4A7C15ULL + n * 131 + align, align, n);
            const Slice s(buf.data() + align, n);
            const uint32_t lib = doris::snii::crc32c(s);
            const uint32_t sw = detail::crc32c_slice8_extend(0, s);
            const uint32_t hws = detail::crc32c_hw_serial_extend(0, s);
            const uint32_t hw3 = detail::crc32c_hw3_extend(0, s);
            EXPECT_EQ(hw3, sw) << "n=" << n << " align=" << align;
            EXPECT_EQ(hw3, hws) << "n=" << n << " align=" << align;
            EXPECT_EQ(hw3, lib) << "n=" << n << " align=" << align;
        }
    }
}

// FV-2: degenerate + exact-divisible boundaries. Empty -> canonical crc32c("")==0
// on every path; n=3072 is exactly 3*1024 (zero interleave tail).
TEST(SniiCrc32cTest, DegenerateAndExactDivisibleBoundaries) {
    const Slice empty(nullptr, 0);
    EXPECT_EQ(0U, doris::snii::crc32c(empty));
    EXPECT_EQ(0U, detail::crc32c_slice8_extend(0, empty));
    EXPECT_EQ(0U, detail::crc32c_hw_serial_extend(0, empty));
    EXPECT_EQ(0U, detail::crc32c_hw3_extend(0, empty));

    for (size_t n : {size_t {1}, size_t {7}, size_t {8}, size_t {3072}}) {
        const std::vector<uint8_t> buf = make_padded_bytes(0xD1CE5EEDULL + n, 0, n);
        const Slice s(buf.data(), n);
        const uint32_t lib = doris::snii::crc32c(s);
        EXPECT_EQ(detail::crc32c_slice8_extend(0, s), lib) << "n=" << n;
        EXPECT_EQ(detail::crc32c_hw_serial_extend(0, s), lib) << "n=" << n;
        EXPECT_EQ(detail::crc32c_hw3_extend(0, s), lib) << "n=" << n;
    }
}

// FV-3: seeded-extend chaining equals the one-shot over the whole, including a
// length that crosses the 1024 threshold (4097). Covers the seed chain + the hw3
// GF(2) combine linearity (the property every framed section relies on).
TEST(SniiCrc32cTest, ExtendEqualsConcatenationAcrossThreshold) {
    for (size_t total : {size_t {16}, size_t {1024}, size_t {4097}}) {
        const std::vector<uint8_t> buf = make_padded_bytes(0xABCDEF01ULL + total, 0, total);
        const uint8_t* p = buf.data();
        const uint32_t whole = doris::snii::crc32c(Slice(p, total));
        for (size_t k :
             {size_t {0}, size_t {1}, size_t {7}, size_t {8}, total / 2, total - 1, total}) {
            const Slice a(p, k);
            const Slice b(p + k, total - k);
            // Public seeded-extend chaining (delegates to the bundled library).
            EXPECT_EQ(doris::snii::crc32c_extend(doris::snii::crc32c(a), b), whole)
                    << "total=" << total << " k=" << k;
            // The hw3 reference threads the same seed through lane A and the combine.
            EXPECT_EQ(detail::crc32c_hw3_extend(doris::snii::crc32c(a), b), whole)
                    << "total=" << total << " k=" << k;
        }
    }
}

// FV-4: canonical CRC32C vectors from an external authority. A path that altered
// the value -- and hence the on-disk format -- would break these.
TEST(SniiCrc32cTest, KnownVectorsMatchExternalAuthority) {
    const char* digits = "123456789";
    const Slice ds(reinterpret_cast<const uint8_t*>(digits), 9);
    EXPECT_EQ(0xE3069283U, doris::snii::crc32c(ds));
    EXPECT_EQ(0xE3069283U, detail::crc32c_slice8_extend(0, ds));
    EXPECT_EQ(0xE3069283U, detail::crc32c_hw_serial_extend(0, ds));
    EXPECT_EQ(0xE3069283U, detail::crc32c_hw3_extend(0, ds));

    EXPECT_EQ(0U, doris::snii::crc32c(Slice(nullptr, 0)));

    // 4096 x 'a' crosses the interleave threshold; cross-check every path against
    // the independent byte-at-a-time reference defined above.
    const std::vector<uint8_t> aaa(4096, static_cast<uint8_t>('a'));
    const Slice as(aaa.data(), aaa.size());
    const uint32_t ref = crc32c_ref(aaa);
    EXPECT_EQ(ref, doris::snii::crc32c(as));
    EXPECT_EQ(ref, detail::crc32c_slice8_extend(0, as));
    EXPECT_EQ(ref, detail::crc32c_hw_serial_extend(0, as));
    EXPECT_EQ(ref, detail::crc32c_hw3_extend(0, as));
}

// FV-5: single-bit-flip regression -- the checksum still distinguishes a one-bit
// change (verify capability not degraded) on both the production and hw3 paths.
TEST(SniiCrc32cTest, SingleBitFlipChangesChecksum) {
    std::vector<uint8_t> buf = make_padded_bytes(0x5A5A5A5AULL, 0, 2048); // crosses threshold
    const Slice s(buf.data(), buf.size());
    const uint32_t base_lib = doris::snii::crc32c(s);
    const uint32_t base_hw3 = detail::crc32c_hw3_extend(0, s);
    for (size_t bit : {size_t {0}, size_t {1}, size_t {8 * 7 + 3}, size_t {8 * 1000 + 5},
                       size_t {8 * 2047 + 7}}) {
        const size_t byte = bit / 8;
        const auto mask = static_cast<uint8_t>(1U << (bit % 8));
        buf[byte] ^= mask; // flip
        const Slice fs(buf.data(), buf.size());
        EXPECT_NE(base_lib, doris::snii::crc32c(fs)) << "bit=" << bit;
        EXPECT_NE(base_hw3, detail::crc32c_hw3_extend(0, fs)) << "bit=" << bit;
        buf[byte] ^= mask; // restore
    }
}

// FV-6 ([perf-deterministic]): the interleave optimization is engaged and correct.
// threshold > 0, and when hardware CRC is present the dispatcher-selected hardware
// paths equal the authoritative library value.
TEST(SniiCrc32cPerfTest, OptimizationEngagedThresholdAndHardwarePath) {
    EXPECT_GT(detail::crc32c_interleave_threshold(), 0U);

    const std::vector<uint8_t> buf = make_padded_bytes(0xC0FFEEULL, 0, 4096); // >= threshold
    const Slice s(buf.data(), buf.size());
    const uint32_t lib = doris::snii::crc32c(s);
    // Portable path is always available and correct.
    EXPECT_EQ(lib, detail::crc32c_slice8_extend(0, s));
    if (detail::crc32c_has_hw()) {
        EXPECT_EQ(lib, detail::crc32c_hw_serial_extend(0, s));
        EXPECT_EQ(lib, detail::crc32c_hw3_extend(0, s));
    }
}

// [perf-report-only]: wall-clock throughput, never a CI gate. The only assertions
// are deterministic byte-equivalence; the GB/s figures are printed for reference.
TEST(SniiCrc32cPerfTest, Hw3ThroughputReportOnly) {
    constexpr size_t kBytes = 1U << 16; // 64 KiB
    const std::vector<uint8_t> buf = make_padded_bytes(0x1234ULL, 0, kBytes);
    const Slice s(buf.data(), buf.size());
    const uint32_t expected = detail::crc32c_slice8_extend(0, s);
    ASSERT_EQ(expected, detail::crc32c_hw_serial_extend(0, s));
    ASSERT_EQ(expected, detail::crc32c_hw3_extend(0, s));

    constexpr int kIters = 2000;
    auto time_path = [&](auto fn) {
        volatile uint32_t sink = 0;
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < kIters; ++i) {
            sink ^= fn(0, s);
        }
        const auto t1 = std::chrono::steady_clock::now();
        (void)sink;
        return std::chrono::duration<double>(t1 - t0).count();
    };
    const double hw3_s = time_path(detail::crc32c_hw3_extend);
    const double hws_s = time_path(detail::crc32c_hw_serial_extend);
    const double gigabytes = static_cast<double>(kBytes) * kIters / 1e9;
    std::cout << "[ REPORT   ] crc32c 64KiB hw3=" << (hw3_s > 0 ? gigabytes / hw3_s : 0.0)
              << " GB/s hw_serial=" << (hws_s > 0 ? gigabytes / hws_s : 0.0)
              << " GB/s (report-only, not a gate)\n";
    SUCCEED();
}

// FV-7: reader black-box regression. build_reader stamps every dict-block /
// prx-window / frq-region crc via the production crc32c(); open_index and the
// queries re-verify them, so assert_ok fails on any Status::Corruption from the
// dict_block / prx_pod / frq_pod verify paths. Results must match the known-good
// sets (identical to SniiPhraseQueryTest), confirming the seam addition perturbs
// nothing and stored CRCs still verify byte-identically.
TEST(SniiCrc32cTest, ReaderPhraseAndTermQueriesVerifyCrc) {
    using doris::snii::reader::LogicalIndexReader;
    using doris::snii::reader::SniiSegmentReader;
    using doris::snii::snii_test::assert_ok;
    using doris::snii::snii_test::build_reader;
    using doris::snii::snii_test::MemoryFile;

    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    // Term lookup drives the dict-block crc verify path.
    bool found = false;
    doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index_reader.lookup("failed", &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);

    // Phrase query drives the framed prx-window crc verify path.
    std::vector<uint32_t> phrase_docids;
    assert_ok(doris::snii::query::phrase_query(index_reader, {"failed", "order"}, &phrase_docids));
    EXPECT_EQ(phrase_docids, (std::vector<uint32_t> {5000, 7000, 8000}));

    // Phrase-prefix query drives term expansion + prx crc paths.
    std::vector<uint32_t> prefix_docids;
    assert_ok(doris::snii::query::phrase_prefix_query(index_reader, {"failed", "ord"},
                                                      &prefix_docids, 10));
    EXPECT_EQ(prefix_docids, (std::vector<uint32_t> {5000, 6000, 7000, 8000}));
}
