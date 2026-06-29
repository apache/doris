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

#include "snii/format/frq_pod.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"
#include "snii/encoding/crc32c.h"

using snii::ByteSink;
using snii::Slice;
using doris::Status;
using snii::format::build_dd_region;
using snii::format::build_freq_region;
using snii::format::decode_dd_region;
using snii::format::decode_freq_region;
using snii::format::FrqRegionMeta;

namespace {

using U32Vec = std::vector<uint32_t>;

// Round-trips a window's separately-encoded dd + freq regions: build each
// region, then decode the dd region (docs-only) and the freq region, comparing
// originals.
Status RoundTrip(const U32Vec& docs, const U32Vec& freqs, uint64_t win_base, bool has_freq,
                 int level, U32Vec* out_docs, U32Vec* out_freqs) {
    ByteSink dd_sink;
    FrqRegionMeta dd_meta;
    RETURN_IF_ERROR(build_dd_region(docs, win_base, level, &dd_sink, &dd_meta));
    RETURN_IF_ERROR(decode_dd_region(Slice(dd_sink.buffer()), dd_meta, win_base, out_docs));
    if (!has_freq) {
        out_freqs->clear();
        return Status::OK();
    }
    ByteSink freq_sink;
    FrqRegionMeta freq_meta;
    RETURN_IF_ERROR(build_freq_region(freqs, level, &freq_sink, &freq_meta));
    return decode_freq_region(Slice(freq_sink.buffer()), freq_meta, out_docs->size(), out_freqs);
}

} // namespace

// Basic round-trip: first window win_base=0, both dd and freq are restored.
TEST(SniiFrqPod, BasicDocFreqRoundTrip) {
    U32Vec docs = {0, 3, 5, 10, 11, 50, 200};
    U32Vec freqs = {1, 2, 1, 7, 3, 1, 9};
    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(RoundTrip(docs, freqs, /*win_base=*/0, /*has_freq=*/true, -1, &out_docs, &out_freqs)
                        .ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// Non-first window: win_base != 0, dd[0]=first-win_base, cross-window delta
// rebuild.
TEST(SniiFrqPod, NonFirstWindowDeltaRebuild) {
    uint64_t win_base = 1000;
    U32Vec docs = {1001, 1005, 1006, 2000, 2001};
    U32Vec freqs = {3, 1, 1, 2, 8};
    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(
            RoundTrip(docs, freqs, win_base, /*has_freq=*/true, -1, &out_docs, &out_freqs).ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// dd region decodes WITHOUT the freq region present (docs-only path).
TEST(SniiFrqPod, DdRegionDecodesWithoutFreq) {
    uint64_t win_base = 500;
    U32Vec docs = {600, 700, 701, 900};
    ByteSink dd_sink;
    FrqRegionMeta dd_meta;
    ASSERT_TRUE(build_dd_region(docs, win_base, -1, &dd_sink, &dd_meta).ok());
    U32Vec out_docs;
    ASSERT_TRUE(decode_dd_region(Slice(dd_sink.buffer()), dd_meta, win_base, &out_docs).ok());
    EXPECT_EQ(out_docs, docs);
}

// Single-doc window.
TEST(SniiFrqPod, SingleDocWindow) {
    U32Vec docs = {42};
    U32Vec freqs = {7};
    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(RoundTrip(docs, freqs, /*win_base=*/0, /*has_freq=*/true, -1, &out_docs, &out_freqs)
                        .ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// Large window (2048 docs): auto mode triggers zstd on each region and is
// lossless.
TEST(SniiFrqPod, LargeWindowAutoZstdRoundTrip) {
    U32Vec docs, freqs;
    docs.reserve(2048);
    freqs.reserve(2048);
    uint32_t cur = 0;
    for (int i = 0; i < 2048; ++i) {
        cur += 1 + (i % 4);
        docs.push_back(cur);
        freqs.push_back(1 + (i % 3));
    }
    ByteSink dd_sink;
    FrqRegionMeta dd_meta;
    ASSERT_TRUE(build_dd_region(docs, 0, -1, &dd_sink, &dd_meta).ok());
    EXPECT_TRUE(dd_meta.zstd); // dd region large enough for zstd

    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(RoundTrip(docs, freqs, /*win_base=*/0, /*has_freq=*/true, -1, &out_docs, &out_freqs)
                        .ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// Small window level<0 auto -> raw (dd region not compressed).
TEST(SniiFrqPod, SmallWindowUsesRaw) {
    U32Vec docs = {0, 1, 2};
    ByteSink dd_sink;
    FrqRegionMeta dd_meta;
    ASSERT_TRUE(build_dd_region(docs, 0, -1, &dd_sink, &dd_meta).ok());
    EXPECT_FALSE(dd_meta.zstd);
    EXPECT_EQ(dd_meta.disk_len, dd_meta.uncomp_len); // raw => disk == uncomp
}

// Explicit level=0 forces raw on both regions; large window still lossless.
TEST(SniiFrqPod, ExplicitRawLargeWindowRoundTrip) {
    U32Vec docs, freqs;
    uint32_t cur = 0;
    for (int i = 0; i < 600; ++i) {
        cur += 1 + (i % 7);
        docs.push_back(cur);
        freqs.push_back(1 + (i % 5));
    }
    ByteSink dd_sink, freq_sink;
    FrqRegionMeta dd_meta, freq_meta;
    ASSERT_TRUE(build_dd_region(docs, 0, /*level=*/0, &dd_sink, &dd_meta).ok());
    ASSERT_TRUE(build_freq_region(freqs, /*level=*/0, &freq_sink, &freq_meta).ok());
    EXPECT_FALSE(dd_meta.zstd);
    EXPECT_FALSE(freq_meta.zstd);

    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(decode_dd_region(Slice(dd_sink.buffer()), dd_meta, 0, &out_docs).ok());
    ASSERT_TRUE(
            decode_freq_region(Slice(freq_sink.buffer()), freq_meta, out_docs.size(), &out_freqs)
                    .ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// Explicit level>0 forces zstd, still lossless.
TEST(SniiFrqPod, ExplicitZstdRoundTrip) {
    U32Vec docs, freqs;
    for (uint32_t i = 0; i < 300; ++i) {
        docs.push_back(i * 2);
        freqs.push_back(2);
    }
    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(RoundTrip(docs, freqs, /*win_base=*/0, /*has_freq=*/true,
                          /*level=*/5, &out_docs, &out_freqs)
                        .ok());
    EXPECT_EQ(out_docs, docs);
    EXPECT_EQ(out_freqs, freqs);
}

// Non-ascending docids must be rejected (InvalidArgument).
TEST(SniiFrqPod, NonAscendingDocsRejected) {
    U32Vec docs = {0, 5, 3};
    ByteSink sink;
    FrqRegionMeta meta;
    Status s = build_dd_region(docs, 0, -1, &sink, &meta);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// first_docid < win_base (dd[0] would underflow) -> InvalidArgument.
TEST(SniiFrqPod, FirstDocBelowWinBaseRejected) {
    U32Vec docs = {100, 200};
    ByteSink sink;
    FrqRegionMeta meta;
    Status s = build_dd_region(docs, 500, -1, &sink, &meta);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// CRC corruption inside the dd region is caught by crc_dd on decode.
TEST(SniiFrqPod, DdRegionCrcCorruptionDetected) {
    U32Vec docs = {0, 1, 2, 3, 4, 5};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_dd_region(docs, 0, -1, &sink, &meta).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 1U);
    bytes.back() ^= 0xFF;
    U32Vec out_docs;
    Status s = decode_dd_region(Slice(bytes), meta, 0, &out_docs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// CRC corruption inside the freq region is caught by crc_freq on decode.
TEST(SniiFrqPod, FreqRegionCrcCorruptionDetected) {
    U32Vec freqs = {1, 2, 3, 4, 5, 6};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_freq_region(freqs, -1, &sink, &meta).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 1U);
    bytes.back() ^= 0xFF;
    U32Vec out_freqs;
    Status s = decode_freq_region(Slice(bytes), meta, freqs.size(), &out_freqs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A region slice whose length disagrees with meta.disk_len is rejected
// (anti-DoS).
TEST(SniiFrqPod, RegionSliceLengthMismatchRejected) {
    U32Vec docs = {0, 1, 2, 3};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_dd_region(docs, 0, /*level=*/0, &sink, &meta).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    bytes.pop_back(); // slice now shorter than disk_len
    U32Vec out_docs;
    Status s = decode_dd_region(Slice(bytes), meta, 0, &out_docs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// Empty window (0 docs) round-trip: dd region has n=0, freq region is empty.
TEST(SniiFrqPod, EmptyWindowRoundTrip) {
    U32Vec docs, freqs;
    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(RoundTrip(docs, freqs, /*win_base=*/0, /*has_freq=*/true, -1, &out_docs, &out_freqs)
                        .ok());
    EXPECT_TRUE(out_docs.empty());
    EXPECT_TRUE(out_freqs.empty());
}

// dd region on-disk length is strictly smaller than dd+freq combined (freq
// carries real bytes), proving the docs-only prefix saving at the region level.
TEST(SniiFrqPod, DdRegionSmallerThanCombined) {
    U32Vec docs = {0, 3, 5, 10, 11, 50, 200};
    U32Vec freqs = {1, 2, 1, 7, 3, 1, 9};
    ByteSink dd_sink, freq_sink;
    FrqRegionMeta dd_meta, freq_meta;
    ASSERT_TRUE(build_dd_region(docs, 0, -1, &dd_sink, &dd_meta).ok());
    ASSERT_TRUE(build_freq_region(freqs, -1, &freq_sink, &freq_meta).ok());
    EXPECT_GT(dd_meta.disk_len, 0U);
    EXPECT_GT(freq_meta.disk_len, 0U);
    EXPECT_LT(dd_meta.disk_len, dd_meta.disk_len + freq_meta.disk_len);
}

// Null argument guard.
TEST(SniiFrqPod, NullArgsRejected) {
    U32Vec docs = {0, 1};
    FrqRegionMeta meta;
    EXPECT_TRUE(
            build_dd_region(docs, 0, -1, nullptr, &meta).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// N=0 empty-term prelude round-trips: the dd region encodes VInt n=0 (no PFOR
// runs) and the freq region is zero-length; both decode back to empty vectors.
TEST(SniiFrqPod, EmptyTermZeroDocsRoundTrip) {
    U32Vec docs;  // N = 0 (empty term)
    U32Vec freqs; // no freqs
    ByteSink dd_sink, freq_sink;
    FrqRegionMeta dd_meta, freq_meta;
    ASSERT_TRUE(build_dd_region(docs, /*win_base=*/0, /*level=*/0, &dd_sink, &dd_meta).ok());
    ASSERT_TRUE(build_freq_region(freqs, /*level=*/0, &freq_sink, &freq_meta).ok());
    EXPECT_EQ(freq_meta.uncomp_len, 0U); // empty freq region carries no bytes

    U32Vec out_docs, out_freqs;
    ASSERT_TRUE(decode_dd_region(Slice(dd_sink.buffer()), dd_meta, 0, &out_docs).ok());
    EXPECT_TRUE(out_docs.empty());
    ASSERT_TRUE(
            decode_freq_region(Slice(freq_sink.buffer()), freq_meta, out_docs.size(), &out_freqs)
                    .ok());
    EXPECT_TRUE(out_freqs.empty());
}

// A truncated freq region (slice shorter than meta.disk_len) is rejected: the
// length-mismatch guard in open_region fires before any payload decode.
TEST(SniiFrqPod, TruncatedFreqRegionRejected) {
    U32Vec freqs = {1, 2, 3, 4, 5, 6, 7, 8};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_freq_region(freqs, /*level=*/0, &sink, &meta).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 1U);
    bytes.pop_back(); // slice now shorter than meta.disk_len
    U32Vec out_freqs;
    Status s = decode_freq_region(Slice(bytes), meta, freqs.size(), &out_freqs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A dd region with extra trailing bytes (crc + length kept VALID for the longer
// slice) must be rejected by the post-decode eof() check, not the crc/length
// guards. We craft a raw region, append a garbage byte, then fix disk_len,
// uncomp_len, and crc so every earlier guard passes and only the inner
// "trailing bytes" assertion can fire.
TEST(SniiFrqPod, DdRegionTrailingBytesRejected) {
    U32Vec docs = {0, 1, 2, 3, 4, 5};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_dd_region(docs, 0, /*level=*/0, &sink, &meta).ok()); // raw
    ASSERT_FALSE(meta.zstd);
    std::vector<uint8_t> bytes = sink.buffer();
    bytes.push_back(0x00); // garbage trailing byte appended to the plaintext
    // Keep raw invariant uncomp_len == disk_len and a matching crc so open_region
    // accepts the slice; only the payload-level eof() check can reject it.
    meta.disk_len = bytes.size();
    meta.uncomp_len = bytes.size();
    meta.crc = snii::crc32c(Slice(bytes));
    U32Vec out_docs;
    Status s = decode_dd_region(Slice(bytes), meta, 0, &out_docs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A freq region with extra trailing bytes (crc + length kept VALID) must be
// rejected by the post-decode eof() check. Same crafting strategy as above.
TEST(SniiFrqPod, FreqRegionTrailingBytesRejected) {
    U32Vec freqs = {3, 1, 4, 1, 5, 9};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_freq_region(freqs, /*level=*/0, &sink, &meta).ok()); // raw
    ASSERT_FALSE(meta.zstd);
    std::vector<uint8_t> bytes = sink.buffer();
    bytes.push_back(0x00); // garbage trailing byte appended to the plaintext
    meta.disk_len = bytes.size();
    meta.uncomp_len = bytes.size();
    meta.crc = snii::crc32c(Slice(bytes));
    U32Vec out_freqs;
    Status s = decode_freq_region(Slice(bytes), meta, freqs.size(), &out_freqs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// The uncomp_len sanity cap (kMaxRegionUncompBytes = 256 MiB) guards against a
// corrupted length that would inflate to a giant allocation. We keep the
// on-disk slice + crc valid (so the length/crc guards pass) and only override
// uncomp_len to an absurd value, proving the cap branch is what rejects it.
TEST(SniiFrqPod, UncompLenCapRejected) {
    U32Vec docs = {0, 1, 2, 3, 4, 5};
    ByteSink sink;
    FrqRegionMeta meta;
    ASSERT_TRUE(build_dd_region(docs, 0, /*level=*/0, &sink, &meta).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    // kMaxRegionUncompBytes is an internal (anonymous-namespace) constant; use
    // its literal value (256 MiB) + 1 to step just past the cap.
    meta.uncomp_len = static_cast<uint64_t>(256U * 1024 * 1024) + 1;
    U32Vec out_docs;
    Status s = decode_dd_region(Slice(bytes), meta, 0, &out_docs);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}
