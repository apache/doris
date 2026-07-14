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

#include "storage/index/snii/format/prx_pod.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/format_constants.h"

using doris::Status; // RETURN_IF_ERROR expands to bare Status
using doris::snii::ByteSink;
using doris::snii::ByteSource;
using doris::snii::pfor_encode;
using doris::snii::Slice;
using doris::snii::format::build_prx_window;
using doris::snii::format::build_prx_window_flat;
using doris::snii::format::kFrqBaseUnit;
using doris::snii::format::PrxCodec;
using doris::snii::format::read_prx_window;
using doris::snii::format::read_prx_window_csr;
using doris::snii::format::read_prx_window_csr_selective;

// Integrated SNII reports corruption via doris ErrorCode::INVERTED_INDEX_FILE_CORRUPTED
// (the standalone build used doris::snii::StatusCode::kCorruption) and writer-side precondition
// violations via ErrorCode::INVALID_ARGUMENT; the negative tests below assert those codes.

namespace {

using PerDoc = std::vector<std::vector<uint32_t>>;

// Encodes a uint32 array as the PFOR_runs wire form the prx PFOR payload uses:
// fixed-size runs of kFrqBaseUnit values, run count derived by the decoder from
// the total length (so it is not stored). Mirrors the in-source
// encode_pfor_runs so a test can hand-assemble a CRC-VALID PFOR payload and
// exercise the INNER decode branches (sum check / trailing bytes) instead of
// the outer CRC/codec.
void AppendPforRuns(const std::vector<uint32_t>& values, ByteSink* out) {
    const size_t n = values.size();
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        pfor_encode(values.data() + off, run, out);
    }
}

// Assembles a complete CRC-valid PFOR window: codec, uncomp_len, payload, crc.
// The payload header fields (doc_count, total_pos) are passed explicitly so a
// test can declare values that disagree with the encoded runs, and `trailing`
// lets a test pad the declared payload with undecodable bytes. The CRC always
// matches the emitted frame, so reads fail (if at all) on an INNER payload
// check, never CRC.
std::vector<uint8_t> MakePforWindow(uint32_t doc_count, uint32_t total_pos,
                                    const std::vector<uint32_t>& freqs,
                                    const std::vector<uint32_t>& deltas,
                                    const std::vector<uint8_t>& trailing = {}) {
    ByteSink payload;
    payload.put_varint32(doc_count);
    payload.put_varint32(total_pos);
    AppendPforRuns(freqs, &payload);
    AppendPforRuns(deltas, &payload);
    payload.put_bytes(Slice(trailing));

    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    framed.put_varint32(static_cast<uint32_t>(payload.view().size()));
    framed.put_bytes(payload.view());

    ByteSink full;
    full.put_bytes(framed.view());
    full.put_fixed32(doris::snii::crc32c(framed.view()));
    return full.buffer();
}

// Flattens per-doc lists into (flat positions, freqs) the way the accumulator
// stores them, so the flat builder can be checked for byte-identity.
void Flatten(const PerDoc& in, std::vector<uint32_t>* flat, std::vector<uint32_t>* freqs) {
    flat->clear();
    freqs->clear();
    for (const auto& doc : in) {
        freqs->push_back(static_cast<uint32_t>(doc.size()));
        flat->insert(flat->end(), doc.begin(), doc.end());
    }
}

// Build data above/below the raw threshold consistent with the production path;
// provides a stable, controlled round-trip helper.
Status RoundTrip(const PerDoc& in, int level, PerDoc* out) {
    ByteSink sink;
    RETURN_IF_ERROR(build_prx_window(in, level, &sink));
    ByteSource src(sink.view());
    RETURN_IF_ERROR(read_prx_window(&src, out));
    return Status::OK();
}

} // namespace

// Single doc with ascending positions: delta-encoded round-trip must be
// lossless.
TEST(SniiPrxPod, SingleDocRoundTrip) {
    PerDoc in = {{3, 7, 7, 10, 100}}; // includes duplicate positions (delta=0)
    PerDoc out;
    ASSERT_TRUE(RoundTrip(in, -1, &out).ok());
    EXPECT_EQ(out, in);
}

// The FLAT builder (used by the writer to avoid materializing a
// vector-of-vectors for high-df terms) must produce BYTE-IDENTICAL window bytes
// to the per-doc builder for the same logical positions, at every codec level.
// This is the load-bearing guarantee that the flat refactor keeps .prx
// byte-identical.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPrxPod, FlatBuilderMatchesPerDocBytes) {
    const std::vector<PerDoc> cases = {
            {},                                  // 0 docs
            {{}, {3}, {}, {}, {1, 2}},           // empty docs interleaved
            {{0, 5, 12}, {1}, {2, 2, 9, 9, 40}}, // duplicate positions (delta 0)
            {{3, 7, 7, 10, 100}},                // single doc
    };
    for (const auto& in : cases) {
        std::vector<uint32_t> flat, freqs;
        Flatten(in, &flat, &freqs);
        for (int level : {-1, 0, 3}) {
            ByteSink per_doc_sink, flat_sink;
            ASSERT_TRUE(build_prx_window(in, level, &per_doc_sink).ok());
            ASSERT_TRUE(build_prx_window_flat(flat, freqs, level, &flat_sink).ok());
            const Slice a = per_doc_sink.view();
            const Slice b = flat_sink.view();
            ASSERT_EQ(a.size(), b.size()) << "level=" << level;
            EXPECT_EQ(0, std::memcmp(a.data(), b.data(), a.size())) << "level=" << level;
            // The flat-built window still decodes back to the original per-doc lists.
            PerDoc out;
            ByteSource src(flat_sink.view());
            ASSERT_TRUE(read_prx_window(&src, &out).ok());
            EXPECT_EQ(out, in) << "level=" << level;
        }
    }
}

// A large window built flat (auto codec) must equal the per-doc build. The auto
// path picks the smaller of PFOR vs zstd; for these deltas it compresses (not
// raw), proving the flat path is byte-identical to the per-doc path through the
// same auto codec.
TEST(SniiPrxPod, FlatBuilderMatchesPerDocLargePfor) {
    PerDoc in;
    for (uint32_t d = 0; d < 300; ++d) {
        in.push_back({d, d + 1U, d + 2U});
    }
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);
    ByteSink per_doc_sink, flat_sink;
    ASSERT_TRUE(build_prx_window(in, -1, &per_doc_sink).ok());
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &flat_sink).ok());
    const Slice a = per_doc_sink.view();
    const Slice b = flat_sink.view();
    ASSERT_EQ(a.size(), b.size());
    EXPECT_EQ(0, std::memcmp(a.data(), b.data(), a.size()));
    EXPECT_NE(a.data()[0], static_cast<uint8_t>(doris::snii::format::PrxCodec::kRaw));
    // Round-trips losslessly back to the per-doc lists.
    PerDoc out;
    ByteSource src(flat_sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    EXPECT_EQ(out, in);
}

TEST(SniiPrxPod, CsrPforRoundTripMatchesPerDoc) {
    PerDoc in;
    for (uint32_t d = 0; d < 300; ++d) {
        if (d % 7 == 0) {
            in.emplace_back();
        } else if (d % 5 == 0) {
            in.push_back({d, d + 2U, d + 9U});
        } else {
            in.push_back({d + 1U});
        }
    }
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);

    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));

    std::vector<uint32_t> pos_flat = {999U};
    std::vector<uint32_t> pos_off = {777U};
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window_csr(&src, &pos_flat, &pos_off).ok());

    ASSERT_EQ(pos_off.size(), in.size() + 1);
    EXPECT_EQ(pos_off.front(), 0U);
    EXPECT_EQ(pos_off.back(), pos_flat.size());
    PerDoc got;
    got.reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        got.emplace_back(pos_flat.begin() + pos_off[i], pos_flat.begin() + pos_off[i + 1]);
    }
    EXPECT_EQ(got, in);
}

TEST(SniiPrxPod, SelectiveCsrPforReturnsOnlyRequestedDocs) {
    PerDoc in;
    for (uint32_t d = 0; d < 320; ++d) {
        if (d % 11 == 0) {
            in.emplace_back();
        } else if (d % 7 == 0) {
            in.push_back({d, d + 3U, d + 8U});
        } else {
            in.push_back({d + 1U});
        }
    }
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);

    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));

    const std::vector<uint32_t> ordinals = {0, 1, 7, 77, 255, 319};
    std::vector<uint32_t> pos_flat = {999U};
    std::vector<uint32_t> pos_off = {777U};
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window_csr_selective(&src, ordinals, &pos_flat, &pos_off).ok());

    ASSERT_EQ(pos_off.size(), ordinals.size() + 1);
    EXPECT_EQ(pos_off.front(), 0U);
    EXPECT_EQ(pos_off.back(), pos_flat.size());
    PerDoc got;
    got.reserve(ordinals.size());
    for (size_t i = 0; i < ordinals.size(); ++i) {
        got.emplace_back(pos_flat.begin() + pos_off[i], pos_flat.begin() + pos_off[i + 1]);
    }
    PerDoc want;
    for (uint32_t ordinal : ordinals) {
        want.push_back(in[ordinal]);
    }
    EXPECT_EQ(got, want);
}

TEST(SniiPrxPod, SelectiveCsrRejectsInvalidOrdinals) {
    PerDoc in = {{1}, {2}, {3}};
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());

    std::vector<uint32_t> pos_flat;
    std::vector<uint32_t> pos_off;
    {
        const std::vector<uint32_t> unsorted = {1, 1};
        ByteSource src(sink.view());
        EXPECT_TRUE(read_prx_window_csr_selective(&src, unsorted, &pos_flat, &pos_off)
                            .is<doris::ErrorCode::INVALID_ARGUMENT>());
    }
    {
        const std::vector<uint32_t> out_of_range = {3};
        ByteSource src(sink.view());
        EXPECT_TRUE(read_prx_window_csr_selective(&src, out_of_range, &pos_flat, &pos_off)
                            .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}

// Multiple docs, positions ascending within each doc, no shared baseline across
// docs.
TEST(SniiPrxPod, MultiDocRoundTrip) {
    PerDoc in = {{0, 5, 12}, {1}, {2, 2, 9, 9, 40}, {7, 8, 9, 10}};
    PerDoc out;
    ASSERT_TRUE(RoundTrip(in, -1, &out).ok());
    EXPECT_EQ(out, in);
}

// Empty positions: supports both 0 docs and 0 positions within a doc.
TEST(SniiPrxPod, EmptyPositions) {
    PerDoc empty_window; // 0 docs
    PerDoc out1;
    ASSERT_TRUE(RoundTrip(empty_window, -1, &out1).ok());
    EXPECT_EQ(out1, empty_window);

    PerDoc with_empty_docs = {{}, {3}, {}, {}, {1, 2}}; // contains empty docs
    PerDoc out2;
    ASSERT_TRUE(RoundTrip(with_empty_docs, -1, &out2).ok());
    EXPECT_EQ(out2, with_empty_docs);
}

// Small window (level<0 auto) now uses PFOR: the auto codec always bit-packs
// deltas (PFOR is cheap and competitive even for small windows; no zstd, no
// size-based raw fallback). codec byte == kPfor and it round-trips.
TEST(SniiPrxPod, SmallWindowUsesPfor) {
    PerDoc in = {{1, 2, 3}, {4, 5}};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window(in, -1, &sink).ok());

    ByteSource src(sink.view());
    uint8_t codec = 0xFF;
    ASSERT_TRUE(src.get_u8(&codec).ok());
    EXPECT_EQ(codec, static_cast<uint8_t>(doris::snii::format::PrxCodec::kPfor));

    PerDoc out;
    ByteSource rt(sink.view());
    ASSERT_TRUE(read_prx_window(&rt, &out).ok());
    EXPECT_EQ(out, in);
}

// Large window (all-1 deltas) auto-encodes as PFOR; for tiny constant deltas
// the 1-bit-packed PFOR payload is much smaller than the forced raw varint
// encoding.
TEST(SniiPrxPod, LargeWindowTriggersPforAndIsSmaller) {
    PerDoc in;
    in.reserve(64);
    for (int d = 0; d < 64; ++d) {
        std::vector<uint32_t> doc;
        uint32_t p = 0;
        for (int i = 0; i < 256; ++i) {
            p += 1; // all-1 deltas: pack to ~1 bit each
            doc.push_back(p);
        }
        in.push_back(std::move(doc));
    }

    ByteSink auto_sink;
    ASSERT_TRUE(build_prx_window(in, -1, &auto_sink).ok());
    ByteSource probe(auto_sink.view());
    uint8_t codec = 0xFF;
    ASSERT_TRUE(probe.get_u8(&codec).ok());
    // Auto path compresses with the smaller of PFOR vs zstd; assert it is not raw.
    EXPECT_NE(codec, static_cast<uint8_t>(doris::snii::format::PrxCodec::kRaw));

    ByteSink raw_sink;
    ASSERT_TRUE(build_prx_window(in, /*level=*/0, &raw_sink).ok());
    ByteSource raw_probe(raw_sink.view());
    uint8_t raw_codec = 0xFF;
    ASSERT_TRUE(raw_probe.get_u8(&raw_codec).ok());
    EXPECT_EQ(raw_codec, static_cast<uint8_t>(doris::snii::format::PrxCodec::kRaw));

    EXPECT_LT(auto_sink.size(), raw_sink.size());

    // The PFOR path restores losslessly.
    PerDoc out;
    ByteSource z(auto_sink.view());
    ASSERT_TRUE(read_prx_window(&z, &out).ok());
    EXPECT_EQ(out, in);
}

// PFOR round-trips arbitrary multi-doc windows (including empty docs, duplicate
// positions, and large jumps that force PFOR exceptions) losslessly.
TEST(SniiPrxPod, PforRoundTripVariety) {
    const std::vector<PerDoc> cases = {
            {{0, 5, 12}, {1}, {2, 2, 9, 9, 40}, {7, 8, 9, 10}},
            {{}, {3}, {}, {}, {1, 2}},
            {{0, 1000000, 1000001}, {5}}, // large jump => PFOR exception
            {{3, 7, 7, 10, 100}},         // duplicate positions (delta 0)
    };
    for (const auto& in : cases) {
        ByteSink sink;
        ASSERT_TRUE(build_prx_window(in, -1, &sink).ok());
        ByteSource probe(sink.view());
        uint8_t codec = 0xFF;
        ASSERT_TRUE(probe.get_u8(&codec).ok());
        EXPECT_EQ(codec, static_cast<uint8_t>(doris::snii::format::PrxCodec::kPfor));
        PerDoc out;
        ByteSource src(sink.view());
        ASSERT_TRUE(read_prx_window(&src, &out).ok());
        EXPECT_EQ(out, in);
    }
}

// level=0 explicitly forces raw (no compression), useful for testing and the
// oversized single-doc degenerate path.
TEST(SniiPrxPod, ExplicitRawLevelRoundTrip) {
    PerDoc in = {{10, 20, 30}, {40}};
    PerDoc out;
    ASSERT_TRUE(RoundTrip(in, /*level=*/0, &out).ok());
    EXPECT_EQ(out, in);
}

// CRC corruption is detectable: flipping any byte in the payload causes read to
// return Corruption.
TEST(SniiPrxPod, CrcCorruptionDetected) {
    PerDoc in = {{1, 2, 3, 4, 5}, {6, 7, 8}};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window(in, -1, &sink).ok());

    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 1U);
    bytes.back() ^= 0xFF; // corrupt the last byte of the payload

    Slice corrupt(bytes);
    ByteSource src(corrupt);
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// A corrupted codec byte (invalid codec value) should also be rejected.
TEST(SniiPrxPod, InvalidCodecRejected) {
    PerDoc in = {{1, 2}};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window(in, -1, &sink).ok());

    std::vector<uint8_t> bytes = sink.buffer();
    bytes[0] = 0x7F; // invalid codec (bits 0-5 exceed known values)

    Slice corrupt(bytes);
    ByteSource src(corrupt);
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_FALSE(s.ok());
}

// Truncated input (insufficient payload) should return an error rather than
// crash.
TEST(SniiPrxPod, TruncatedInputRejected) {
    PerDoc in = {{1, 2, 3, 4, 5, 6, 7, 8}};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window(in, -1, &sink).ok());

    std::vector<uint8_t> bytes = sink.buffer();
    bytes.resize(bytes.size() / 2); // truncate to half

    Slice truncated(bytes);
    ByteSource src(truncated);
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_FALSE(s.ok());
}

// DoS prevention: an uncomp_len corrupted to a huge value must be rejected
// before allocation/decompression.
TEST(SniiPrxPod, OversizedUncompLenRejected) {
    ByteSink sink;
    sink.put_u8(static_cast<uint8_t>(doris::snii::format::PrxCodec::kZstd));
    sink.put_varint32(300U * 1024 * 1024); // > 256MiB window limit
    // No need to construct the subsequent comp_len/payload/crc — the cap check
    // triggers immediately after reading uncomp_len.
    ByteSource src(sink.view());
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// DoS prevention: a CRC-VALID raw frame whose decoded doc_count is absurd must
// return Corruption, not a giant reserve()/assign() -> std::bad_alloc. Distinct
// from the CRC and uncomp_len tests above, which are caught BEFORE the inner
// doc_count is read.
TEST(SniiPrxPod, OversizedDocCountRejected) {
    ByteSink payload;
    payload.put_varint32(0x02000000U); // doc_count = 33M, > kMaxWindowDocs (1<<24)
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(doris::snii::format::PrxCodec::kRaw));
    framed.put_varint32(static_cast<uint32_t>(payload.view().size())); // uncomp_len
    framed.put_bytes(payload.view());
    ByteSink full;
    full.put_bytes(framed.view());
    full.put_fixed32(doris::snii::crc32c(framed.view())); // valid crc over codec+uncomp_len+payload
    ByteSource src(full.view());
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// Sanity: the MakePforWindow helper builds a window the production reader
// accepts, so the negative variants below isolate the ONE field they tamper
// with (not a helper bug). doc_count=2, total_pos=3, freqs sum to total_pos,
// deltas match.
TEST(SniiPrxPod, CraftedPforWindowRoundTrips) {
    // doc0 = {5, 6} (deltas 5,1), doc1 = {9} (delta 9); pos_counts {2,1},
    // total 3.
    auto bytes = MakePforWindow(/*doc_count=*/2, /*total_pos=*/3,
                                /*freqs=*/ {2, 1}, /*deltas=*/ {5, 1, 9});
    Slice s(bytes);
    ByteSource src(s);
    PerDoc out;
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    PerDoc expected = {{5, 6}, {9}};
    EXPECT_EQ(out, expected);
}

// CRC-VALID PFOR frame whose declared total_pos disagrees with sum(pos_counts):
// the reader must reach the INNER "pos_count sum mismatch" branch (after the
// cap checks and after decoding the freqs runs) and return Corruption -- NOT a
// CRC or codec rejection. freqs sum to 3 but total_pos is declared 4.
TEST(SniiPrxPod, PforPosCountSumMismatchRejected) {
    auto bytes = MakePforWindow(/*doc_count=*/2, /*total_pos=*/4,
                                /*freqs=*/ {2, 1}, /*deltas=*/ {5, 1, 9});
    Slice slice(bytes);
    ByteSource src(slice);
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// CRC-VALID PFOR frame with extra bytes appended INSIDE the declared payload
// (uncomp_len covers them, so the CRC is over them too). After
// decode_pfor_payload consumes the real doc_count/total_pos/freqs/deltas, the
// source is not at EOF, so the INNER "trailing bytes after pfor payload" branch
// must fire -> Corruption.
TEST(SniiPrxPod, PforTrailingBytesRejected) {
    auto bytes = MakePforWindow(/*doc_count=*/2, /*total_pos=*/3,
                                /*freqs=*/ {2, 1}, /*deltas=*/ {5, 1, 9},
                                /*trailing=*/ {0xAA, 0xBB, 0xCC});
    Slice slice(bytes);
    ByteSource src(slice);
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// FLAT builder precondition: a (positions_flat, freqs) mismatch where
// sum(freqs) overruns positions_flat would index flat[off+i] past the span end.
// The guard must return a CLEAN InvalidArgument (never OOB / UB) at every flat
// codec level. Here freqs sum to 5 but only 3 positions are supplied.
TEST(SniiPrxPod, FlatBuilderShortPositionsRejectedNotOob) {
    std::vector<uint32_t> positions_flat = {1, 2, 3}; // 3 positions
    std::vector<uint32_t> freqs = {2, 3};             // claims 5 positions
    for (int level : {-1, 0, 3}) {                    // pfor, raw, zstd flat encoders all guard
        ByteSink sink;
        Status s = build_prx_window_flat(positions_flat, freqs, level, &sink);
        EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>())
                << "level=" << level << " msg=" << s.to_string();
        EXPECT_EQ(sink.size(), 0U) << "level=" << level; // nothing emitted on reject
    }
}

// FLAT builder precondition (other direction): sum(freqs) < positions_flat
// leaves trailing positions unused -- also a writer-side mismatch. The guard
// requires EXACT equality, so this is rejected too (a silently-dropped position
// is a bug).
TEST(SniiPrxPod, FlatBuilderExtraPositionsRejected) {
    std::vector<uint32_t> positions_flat = {1, 2, 3, 4, 5}; // 5 positions
    std::vector<uint32_t> freqs = {2, 1};                   // only addresses 3
    for (int level : {-1, 0, 3}) {
        ByteSink sink;
        Status s = build_prx_window_flat(positions_flat, freqs, level, &sink);
        EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>())
                << "level=" << level << " msg=" << s.to_string();
    }
}

// ---------------------------------------------------------------------------
// T14: auto-mode (kAutoZstd) single-encode -- the auto builders derive per-doc
// deltas once and only materialize the throwaway raw plaintext payload when it is
// large enough (>= 512 B) to make a zstd attempt worthwhile. The tests below
// cover the deterministic raw-build counter seam plus the byte/codec equivalence
// guarantees that prove zero on-disk format change.
// ---------------------------------------------------------------------------

namespace {

using doris::snii::format::testing::prx_raw_build_count;
using doris::snii::format::testing::reset_prx_raw_build_count;

// Byte length of put_varint32(v); mirrors the in-source varint32_size so the
// brute-force codec recomputation below can size frames exactly.
size_t VarintSize(uint32_t v) {
    size_t n = 1;
    while (v >= 128) {
        v >>= 7;
        ++n;
    }
    return n;
}

// Auto constants restated from prx_pod.cpp (anonymous there). The brute-force
// codec check must use the SAME threshold/level write_auto_pfor_or_zstd uses.
constexpr size_t kAutoZstdMinBytes = 512;
constexpr int kDefaultZstdLevel = 3;

// Per-doc -> flat delta stream (first position of each doc absolute, the rest
// delta-within-doc): the exact sequence both .prx payload encoders serialize.
std::vector<uint32_t> FlatDeltas(const std::vector<uint32_t>& flat,
                                 const std::vector<uint32_t>& freqs) {
    std::vector<uint32_t> deltas;
    deltas.reserve(flat.size());
    size_t off = 0;
    for (uint32_t fc : freqs) {
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            deltas.push_back(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    return deltas;
}

// Independently recompute the codec byte the auto builder must emit, replicating
// write_auto_pfor_or_zstd: try zstd only when the EXACT plaintext payload size
// reaches the threshold, and choose zstd only when its frame is strictly smaller
// than the PFOR frame.
uint8_t BruteForceAutoCodec(const std::vector<uint32_t>& freqs,
                            const std::vector<uint32_t>& deltas) {
    ByteSink pfor_payload;
    pfor_payload.put_varint32(static_cast<uint32_t>(freqs.size()));
    pfor_payload.put_varint32(static_cast<uint32_t>(deltas.size()));
    AppendPforRuns(freqs, &pfor_payload);
    AppendPforRuns(deltas, &pfor_payload);
    const size_t pfor_size = pfor_payload.view().size();

    ByteSink plain;
    plain.put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        plain.put_varint32(fc);
        for (uint32_t i = 0; i < fc; ++i) {
            plain.put_varint32(deltas[off + i]);
        }
        off += fc;
    }
    const size_t plain_size = plain.view().size();

    const auto kPfor = static_cast<uint8_t>(PrxCodec::kPfor);
    const auto kZstd = static_cast<uint8_t>(PrxCodec::kZstd);
    if (plain_size >= kAutoZstdMinBytes) {
        std::vector<uint8_t> comp;
        EXPECT_TRUE(doris::snii::zstd_compress(plain.view(), kDefaultZstdLevel, &comp).ok());
        const size_t zstd_frame = 1 + VarintSize(static_cast<uint32_t>(plain_size)) +
                                  VarintSize(static_cast<uint32_t>(comp.size())) + comp.size() +
                                  sizeof(uint32_t);
        const size_t pfor_frame =
                1 + VarintSize(static_cast<uint32_t>(pfor_size)) + pfor_size + sizeof(uint32_t);
        if (zstd_frame < pfor_frame) {
            return kZstd;
        }
    }
    return kPfor;
}

// One doc whose raw plaintext payload is EXACTLY `target` bytes. Positions
// {0,1,...,fc-1} make every delta a 1-byte varint (0 then 1s), so the plaintext
// is varint32_size(doc_count=1) + varint32_size(fc) + fc. With fc in
// [128, 16383] the fc varint is 2 bytes, so plaintext == 3 + fc => fc = target-3.
std::vector<uint32_t> SingleDocWithPlaintextSize(size_t target) {
    EXPECT_GE(target, 131U); // need fc >= 128 so the count varint is 2 bytes
    const auto fc = static_cast<uint32_t>(target - 3);
    std::vector<uint32_t> doc(fc);
    for (uint32_t i = 0; i < fc; ++i) {
        doc[i] = i;
    }
    return doc;
}

} // namespace

// [perf-deterministic] A sub-threshold window (<512 B plaintext) must NOT
// materialize the throwaway raw plaintext payload: the auto path emits PFOR
// directly. On the pre-optimization code (which always materialized) this counter
// would be 1 -> this is the RED guard for the single-encode change.
TEST(SniiPrxPodCounterTest, SmallWindowSkipsRawPlaintextBuild) {
    PerDoc in = {{1}, {3, 5}, {2}};
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);

    reset_prx_raw_build_count();
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    EXPECT_EQ(prx_raw_build_count(), 0U);

    // codec is PFOR and the window still round-trips.
    EXPECT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));
    PerDoc out;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    EXPECT_EQ(out, in);
}

// [perf-deterministic] A >=512 B window materializes the raw plaintext EXACTLY
// once (needed for the zstd-vs-pfor comparison) -- not zero (would skip a viable
// zstd) and not twice (the old double-encode).
TEST(SniiPrxPodCounterTest, LargeWindowStillBuildsRawPlaintextOnce) {
    PerDoc in = {SingleDocWithPlaintextSize(512)};
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);

    reset_prx_raw_build_count();
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    EXPECT_EQ(prx_raw_build_count(), 1U);
}

// [perf-deterministic] N consecutive small windows materialize ZERO raw
// plaintext payloads in aggregate (the per-window throwaway ByteSink is the
// allocation this task removes for the common Zipfian case).
TEST(SniiPrxPodCounterTest, ManySmallWindowsBuildZeroRawPlaintext) {
    reset_prx_raw_build_count();
    for (uint32_t w = 0; w < 64; ++w) {
        PerDoc in = {{w}, {w + 1U, w + 3U}, {w + 2U}};
        std::vector<uint32_t> flat, freqs;
        Flatten(in, &flat, &freqs);
        ByteSink sink;
        ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    }
    EXPECT_EQ(prx_raw_build_count(), 0U);
}

// [perf-deterministic] The per-doc (vector) builder funnels through the SAME auto
// path as the flat builder: small window materializes nothing, large window once.
TEST(SniiPrxPodCounterTest, VectorBuilderSharesSingleEncodePath) {
    reset_prx_raw_build_count();
    PerDoc small = {{1, 2, 3}, {4, 5}};
    ByteSink small_sink;
    ASSERT_TRUE(build_prx_window(small, -1, &small_sink).ok());
    EXPECT_EQ(prx_raw_build_count(), 0U);

    reset_prx_raw_build_count();
    PerDoc large = {SingleDocWithPlaintextSize(512)};
    ByteSink large_sink;
    ASSERT_TRUE(build_prx_window(large, -1, &large_sink).ok());
    EXPECT_EQ(prx_raw_build_count(), 1U);
}

// [functional/perf] The flat and per-doc auto builders must produce BYTE-IDENTICAL
// windows for the same logical positions, across sizes spanning the 512-byte zstd
// threshold (empty, single, tiny, just-below, exactly-at, well-above). This is the
// load-bearing proof that the single-encode refactor changed zero on-disk bytes.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPrxPodTest, FlatAutoProducesByteIdenticalOutputAcrossSizes) {
    std::vector<PerDoc> cases;
    cases.emplace_back();                               // empty window
    cases.push_back({{7}});                             // single doc/pos
    cases.push_back({{1}, {3, 5}, {2}});                // tiny (<512 plaintext)
    cases.push_back({{}, {3}, {}, {}, {1, 2}});         // empty docs interleaved
    cases.push_back({SingleDocWithPlaintextSize(511)}); // just below threshold
    cases.push_back({SingleDocWithPlaintextSize(512)}); // exactly at threshold
    {
        PerDoc big; // well above threshold
        for (uint32_t d = 0; d < 280; ++d) {
            big.push_back({d, d + 1U, d + 2U});
        }
        cases.push_back(std::move(big));
    }

    for (const auto& in : cases) {
        std::vector<uint32_t> flat, freqs;
        Flatten(in, &flat, &freqs);
        ByteSink per_doc_sink, flat_sink;
        ASSERT_TRUE(build_prx_window(in, -1, &per_doc_sink).ok());
        ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &flat_sink).ok());
        const Slice a = per_doc_sink.view();
        const Slice b = flat_sink.view();
        ASSERT_EQ(a.size(), b.size()) << "doc_count=" << in.size();
        EXPECT_EQ(0, std::memcmp(a.data(), b.data(), a.size())) << "doc_count=" << in.size();
        // Auto mode never emits the forced raw codec.
        ASSERT_GT(a.size(), 0U);
        EXPECT_NE(a.data()[0], static_cast<uint8_t>(PrxCodec::kRaw)) << "doc_count=" << in.size();
        // The flat-built window round-trips back to the original per-doc lists.
        PerDoc out;
        ByteSource src(flat_sink.view());
        ASSERT_TRUE(read_prx_window(&src, &out).ok());
        EXPECT_EQ(out, in) << "doc_count=" << in.size();
    }
}

// [functional/perf] At the EXACT 511- vs 512-byte plaintext boundary the codec
// the builder emits must equal an independent brute-force frame-size comparison.
// This guards the risk that an estimated (rather than exact) plaintext size flips
// the codec choice near the threshold and silently changes the on-disk bytes.
TEST(SniiPrxPodTest, AutoCodecChoiceMatchesBruteForceAtThreshold) {
    for (size_t target : {size_t {511}, size_t {512}}) {
        PerDoc in = {SingleDocWithPlaintextSize(target)};
        std::vector<uint32_t> flat, freqs;
        Flatten(in, &flat, &freqs);

        // Confirm the construction lands on exactly the intended plaintext size.
        const std::vector<uint32_t> deltas = FlatDeltas(flat, freqs);
        size_t plain_size = VarintSize(static_cast<uint32_t>(freqs.size()));
        for (uint32_t fc : freqs) {
            plain_size += VarintSize(fc);
        }
        for (uint32_t d : deltas) {
            plain_size += VarintSize(d);
        }
        ASSERT_EQ(plain_size, target);

        ByteSink sink;
        ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
        const uint8_t emitted = sink.view().data()[0];
        EXPECT_EQ(emitted, BruteForceAutoCodec(freqs, deltas)) << "target=" << target;
        if (target < kAutoZstdMinBytes) {
            // Below the threshold zstd is never attempted: must be plain PFOR.
            EXPECT_EQ(emitted, static_cast<uint8_t>(PrxCodec::kPfor));
        }
        // Round-trips regardless of the chosen codec.
        PerDoc out;
        ByteSource src(sink.view());
        ASSERT_TRUE(read_prx_window(&src, &out).ok());
        EXPECT_EQ(out, in) << "target=" << target;
    }
}

// [functional] F-RT-small: a small flat window round-trips through the CSR reader
// (codec=pfor path) back to the original per-doc positions.
TEST(SniiPrxPodTest, SmallFlatWindowRoundTripsViaCsr) {
    PerDoc in = {{1}, {3, 5}, {2}};
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    EXPECT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));

    std::vector<uint32_t> pos_flat, pos_off;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window_csr(&src, &pos_flat, &pos_off).ok());
    ASSERT_EQ(pos_off.size(), in.size() + 1);
    PerDoc out;
    for (size_t d = 0; d < in.size(); ++d) {
        out.emplace_back(pos_flat.begin() + pos_off[d], pos_flat.begin() + pos_off[d + 1]);
    }
    EXPECT_EQ(out, in);
}

// [functional] F-RT-large: a >=512 B flat window (exercising the zstd-vs-pfor
// pick) round-trips losslessly through the per-doc reader.
TEST(SniiPrxPodTest, LargeFlatWindowRoundTrips) {
    PerDoc in;
    for (uint32_t d = 0; d < 280; ++d) {
        in.push_back({d, d + 4U, d + 9U});
    }
    std::vector<uint32_t> flat, freqs;
    Flatten(in, &flat, &freqs);
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    EXPECT_NE(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kRaw));
    PerDoc out;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    EXPECT_EQ(out, in);
}

// [functional/boundary] F-EMPTY: a 0-doc window builds OK, materializes no raw
// plaintext, decodes to an empty result.
TEST(SniiPrxPodTest, EmptyFlatWindowRoundTripsWithNoRawBuild) {
    std::vector<uint32_t> flat, freqs; // 0 docs, 0 positions
    reset_prx_raw_build_count();
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    EXPECT_EQ(prx_raw_build_count(), 0U);
    EXPECT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));
    PerDoc out;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    EXPECT_TRUE(out.empty());
}

// [functional/boundary] F-SINGLE: a single doc with a single position round-trips.
TEST(SniiPrxPodTest, SingleDocSinglePositionFlatRoundTrips) {
    std::vector<uint32_t> flat = {42};
    std::vector<uint32_t> freqs = {1};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    PerDoc out;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    PerDoc expected = {{42}};
    EXPECT_EQ(out, expected);
}

// [error] F-ERR-asc: descending positions within a doc are rejected by the
// single delta walk -- the ascending check is not lost when the second encode is
// skipped. Nothing is emitted.
TEST(SniiPrxPodTest, NonAscendingPositionsRejected) {
    std::vector<uint32_t> flat = {5, 3}; // within one doc, descending
    std::vector<uint32_t> freqs = {2};
    ByteSink sink;
    Status s = build_prx_window_flat(flat, freqs, -1, &sink);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s.to_string();
    EXPECT_EQ(sink.size(), 0U);
}

// [error] F-ERR-part: a (flat, freqs) partition mismatch (sum(freqs) != size) is
// rejected before any indexing -- the partition check is preserved.
TEST(SniiPrxPodTest, PartitionMismatchRejected) {
    std::vector<uint32_t> flat = {1, 2, 3};
    std::vector<uint32_t> freqs = {2, 3}; // sum 5 != 3
    ByteSink sink;
    Status s = build_prx_window_flat(flat, freqs, -1, &sink);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s.to_string();
    EXPECT_EQ(sink.size(), 0U);
}

// [error] F-NULL: a null sink is rejected by both auto builders before any work.
TEST(SniiPrxPodTest, NullSinkRejected) {
    std::vector<uint32_t> flat = {1, 2};
    std::vector<uint32_t> freqs = {2};
    Status s = build_prx_window_flat(flat, freqs, -1, nullptr);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s.to_string();

    PerDoc per_doc = {{1, 2}};
    Status s2 = build_prx_window(per_doc, -1, nullptr);
    EXPECT_TRUE(s2.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s2.to_string();
}

// ===========================================================================
// T22 -- single-copy framing (write_pfor / write_raw / write_zstd_compressed +
// SectionFramer::write) and the bitpack capacity reserve. The framing refactor
// writes [codec/type][len][payload] DIRECTLY into the caller's sink and crc's
// that span in place (no temp ByteSink), so these tests pin the on-disk bytes to
// an INDEPENDENT hand-assembly of the wire frame. A wrong crc range, a view()
// taken at the wrong time, or a wrong start offset shows up as a byte or Status
// mismatch. All bytes MUST stay identical to the pre-refactor output.
// ===========================================================================

namespace {

using doris::snii::FramedSection;
using doris::snii::pfor_decode;
using doris::snii::SectionFramer;

// Appends the 4-byte crc32c(prefix) tail to `prefix`, returning the full on-disk
// frame. Mirrors the reader's crc coverage: the crc is over
// [codec/type][len][payload] ONLY (the bytes already in prefix), never itself.
std::vector<uint8_t> WithCrc(const ByteSink& prefix) {
    ByteSink full;
    full.put_bytes(prefix.view());
    full.put_fixed32(doris::snii::crc32c(prefix.view()));
    return full.buffer();
}

// The RAW/ZSTD plaintext payload (encode_payload_flat wire form): VInt doc_count,
// then per doc VInt pos_count followed by that doc's position deltas (VInt).
ByteSink RawPlaintext(const std::vector<uint32_t>& flat, const std::vector<uint32_t>& freqs) {
    const std::vector<uint32_t> deltas = FlatDeltas(flat, freqs);
    ByteSink plain;
    plain.put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        plain.put_varint32(fc);
        for (uint32_t i = 0; i < fc; ++i) {
            plain.put_varint32(deltas[off + i]);
        }
        off += fc;
    }
    return plain;
}

// Deterministic pseudo-random uint32 run whose values fit in `w` bits, with a
// couple of exact-`w`-bit values forced so choose_width sees width w. Round-trip
// must be lossless regardless of the width PFOR actually picks.
std::vector<uint32_t> MakeWidthRun(uint8_t w, size_t n) {
    std::vector<uint32_t> run(n, 0U);
    uint32_t mask = 0;
    if (w >= 32) {
        mask = 0xFFFFFFFFU;
    } else if (w != 0) {
        mask = (1U << w) - 1U;
    }
    uint32_t state = 0x12345678U ^ (static_cast<uint32_t>(w) << 24) ^ static_cast<uint32_t>(n);
    for (size_t i = 0; i < n; ++i) {
        state = state * 1664525U + 1013904223U; // LCG
        run[i] = state & mask;
    }
    if (n > 0 && w > 0) {
        run[0] = mask;     // force a full-width value
        run[n / 2] = mask; // and one mid-run (exercises exceptions if a smaller w is picked)
    }
    return run;
}

} // namespace

// [perf-deterministic] PRX-BYTE-PFOR: an auto (<512 B) window emits PFOR and its
// on-disk bytes equal an independent [kPfor][len][payload][crc] hand-assembly,
// proving write_pfor's single-copy framing is byte-identical.
TEST(SniiPrxPodTest, PforFramingProducesByteIdenticalOutput) {
    std::vector<uint32_t> flat = {1, 2, 3, 4, 5};
    std::vector<uint32_t> freqs = {3, 2};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, -1, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kPfor));

    const std::vector<uint32_t> deltas = FlatDeltas(flat, freqs);
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(freqs.size()));
    payload.put_varint32(static_cast<uint32_t>(deltas.size()));
    AppendPforRuns(freqs, &payload);
    AppendPforRuns(deltas, &payload);
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    framed.put_varint32(static_cast<uint32_t>(payload.view().size()));
    framed.put_bytes(payload.view());

    EXPECT_EQ(sink.buffer(), WithCrc(framed));
}

// [perf-deterministic] PRX-BYTE-RAW: level=0 forces raw; the bytes equal an
// independent [kRaw][len][plaintext][crc] assembly (write_raw single-copy).
TEST(SniiPrxPodTest, RawFramingProducesByteIdenticalOutput) {
    std::vector<uint32_t> flat = {10, 20, 30, 40};
    std::vector<uint32_t> freqs = {3, 1};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, /*level=*/0, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kRaw));

    ByteSink plain = RawPlaintext(flat, freqs);
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    framed.put_varint32(static_cast<uint32_t>(plain.view().size()));
    framed.put_bytes(plain.view());

    EXPECT_EQ(sink.buffer(), WithCrc(framed));
}

// [perf-deterministic] PRX-BYTE-ZSTD: level>0 forces zstd; the bytes equal an
// independent [kZstd][uncomp_len][comp_len][zstd(plaintext)][crc] assembly
// (write_zstd_compressed single-copy). zstd is deterministic at a fixed level.
TEST(SniiPrxPodTest, ZstdFramingProducesByteIdenticalOutput) {
    std::vector<uint32_t> flat, freqs;
    for (uint32_t d = 0; d < 200; ++d) {
        flat.push_back(d);
        freqs.push_back(1);
    }
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, /*level=*/3, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kZstd));

    ByteSink plain = RawPlaintext(flat, freqs);
    std::vector<uint8_t> comp;
    ASSERT_TRUE(doris::snii::zstd_compress(plain.view(), /*level=*/3, &comp).ok());
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kZstd));
    framed.put_varint32(static_cast<uint32_t>(plain.view().size()));
    framed.put_varint32(static_cast<uint32_t>(comp.size()));
    framed.put_bytes(Slice(comp));

    EXPECT_EQ(sink.buffer(), WithCrc(framed));

    // Round-trips losslessly back to the original per-doc positions.
    PerDoc out;
    ByteSource src(sink.view());
    ASSERT_TRUE(read_prx_window(&src, &out).ok());
    ASSERT_EQ(out.size(), flat.size());
    for (uint32_t d = 0; d < flat.size(); ++d) {
        ASSERT_EQ(out[d].size(), 1U);
        EXPECT_EQ(out[d][0], flat[d]);
    }
}

// [perf-deterministic] Two windows appended to ONE sink: the second window's crc
// must cover only its own [codec][len][payload] (start offset != 0), and each
// window's bytes must be identical to building it standalone. This is the guard
// that write_raw / write_pfor use `start = sink->size()` (not absolute 0).
TEST(SniiPrxPodTest, MultipleWindowsInOneSinkAreFramedByteIdentical) {
    std::vector<uint32_t> flat_a = {1, 2, 3}, freqs_a = {2, 1}; // -> raw
    std::vector<uint32_t> flat_b = {5, 6}, freqs_b = {1, 1};    // -> pfor
    ByteSink combined;
    ASSERT_TRUE(build_prx_window_flat(flat_a, freqs_a, /*level=*/0, &combined).ok());
    const size_t boundary = combined.size();
    ASSERT_TRUE(build_prx_window_flat(flat_b, freqs_b, -1, &combined).ok());

    ByteSink only_a, only_b;
    ASSERT_TRUE(build_prx_window_flat(flat_a, freqs_a, /*level=*/0, &only_a).ok());
    ASSERT_TRUE(build_prx_window_flat(flat_b, freqs_b, -1, &only_b).ok());
    ASSERT_EQ(boundary, only_a.size());
    ASSERT_EQ(combined.size(), only_a.size() + only_b.size());
    const std::vector<uint8_t>& c = combined.buffer();
    EXPECT_EQ(0, std::memcmp(c.data(), only_a.buffer().data(), only_a.size()));
    EXPECT_EQ(0, std::memcmp(c.data() + boundary, only_b.buffer().data(), only_b.size()));

    // Both windows read back sequentially (crc verified per window).
    ByteSource src(combined.view());
    PerDoc a, b;
    ASSERT_TRUE(read_prx_window(&src, &a).ok());
    ASSERT_TRUE(read_prx_window(&src, &b).ok());
    EXPECT_TRUE(src.eof());
    PerDoc want_a = {{1, 2}, {3}};
    PerDoc want_b = {{5}, {6}};
    EXPECT_EQ(a, want_a);
    EXPECT_EQ(b, want_b);
}

// [functional/error] PRX-CRC-CORRUPT: flipping a byte inside the raw payload (the
// crc-covered [codec][len][payload] span) is detected as Corruption -- confirms
// write_raw's crc range is exactly [codec][len][payload].
TEST(SniiPrxPodTest, RawFrameCrcCorruptionDetected) {
    std::vector<uint32_t> flat = {10, 20, 30, 40};
    std::vector<uint32_t> freqs = {3, 1};
    ByteSink sink;
    ASSERT_TRUE(build_prx_window_flat(flat, freqs, /*level=*/0, &sink).ok());
    ASSERT_EQ(sink.view().data()[0], static_cast<uint8_t>(PrxCodec::kRaw));

    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 5U);
    bytes[bytes.size() - 5] ^= 0xFF; // last payload byte, just before the 4-byte crc
    ByteSource src((Slice(bytes)));
    PerDoc out;
    Status s = read_prx_window(&src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// [perf-deterministic] SF-BYTE: SectionFramer::write bytes equal an independent
// [type][varint64 len][payload][crc] hand-assembly (single-copy framing).
TEST(SniiPrxPodTest, SectionFramerProducesByteIdenticalOutput) {
    const std::vector<uint8_t> payload_bytes = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22};
    const uint8_t type = 7;
    ByteSink sink;
    SectionFramer::write(sink, type, Slice(payload_bytes));

    ByteSink framed;
    framed.put_u8(type);
    framed.put_varint64(payload_bytes.size());
    framed.put_bytes(Slice(payload_bytes));
    EXPECT_EQ(sink.buffer(), WithCrc(framed));
}

// [perf-deterministic] SectionFramer::write into a NON-empty sink frames ONLY its
// own region: the crc covers [type][len][payload] starting at `start`, not the
// pre-existing prefix. Guards the start-offset in the single-copy rewrite.
TEST(SniiPrxPodTest, SectionFramerFramesOnlyItsOwnRegionInNonEmptySink) {
    const std::vector<uint8_t> prefix = {0xAA, 0xBB};
    const std::vector<uint8_t> payload_bytes = {0x01, 0x02, 0x03, 0x04};
    ByteSink sink;
    sink.put_bytes(Slice(prefix));
    SectionFramer::write(sink, /*type=*/5, Slice(payload_bytes));

    ByteSink framed;
    framed.put_u8(5);
    framed.put_varint64(payload_bytes.size());
    framed.put_bytes(Slice(payload_bytes));
    const std::vector<uint8_t> expected_region = WithCrc(framed);

    const std::vector<uint8_t>& got = sink.buffer();
    ASSERT_EQ(got.size(), prefix.size() + expected_region.size());
    EXPECT_EQ(0, std::memcmp(got.data(), prefix.data(), prefix.size()));
    EXPECT_EQ(0, std::memcmp(got.data() + prefix.size(), expected_region.data(),
                             expected_region.size()));
}

// [functional] SF-RT: two sections written into one sink (second at start != 0)
// both read back with the correct type + payload and the crc verifies.
TEST(SniiPrxPodTest, SectionFramerWriteReadRoundTripAcrossSections) {
    const std::vector<uint8_t> p0 = {1, 2, 3};
    const std::vector<uint8_t> p1 = {9, 8, 7, 6, 5};
    ByteSink sink;
    SectionFramer::write(sink, /*type=*/3, Slice(p0));
    SectionFramer::write(sink, /*type=*/9, Slice(p1));

    ByteSource src(sink.view());
    FramedSection s0, s1;
    ASSERT_TRUE(SectionFramer::read(src, &s0).ok());
    ASSERT_TRUE(SectionFramer::read(src, &s1).ok());
    EXPECT_TRUE(src.eof());
    EXPECT_EQ(s0.type, 3);
    EXPECT_EQ(s1.type, 9);
    ASSERT_EQ(s0.payload.size(), p0.size());
    ASSERT_EQ(s1.payload.size(), p1.size());
    EXPECT_EQ(0, std::memcmp(s0.payload.data(), p0.data(), p0.size()));
    EXPECT_EQ(0, std::memcmp(s1.payload.data(), p1.data(), p1.size()));
}

// [functional/error] A corrupted SectionFramer payload byte is caught by the
// section crc on read (crc range == [type][len][payload]).
TEST(SniiPrxPodTest, SectionFramerCrcCorruptionDetected) {
    const std::vector<uint8_t> payload_bytes = {0x10, 0x20, 0x30, 0x40, 0x50};
    ByteSink sink;
    SectionFramer::write(sink, /*type=*/4, Slice(payload_bytes));
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 5U);
    bytes[bytes.size() - 5] ^= 0xFF; // last payload byte, before the crc
    ByteSource src((Slice(bytes)));
    FramedSection out;
    Status s = SectionFramer::read(src, &out);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// [functional/boundary] BITPACK-RT: pfor_encode -> pfor_decode round-trips for
// every value width w in [0,32] at run lengths n = 1, 255, 256 (the kFrqBaseUnit
// boundary). Proves the bitpack reserve (capacity-only) leaves the packed bytes
// bit-exact and the decode path recovers every value.
TEST(SniiPrxPodTest, PforBitpackRoundTripAcrossWidths) {
    for (uint8_t w = 0; w <= 32; ++w) {
        for (size_t n : {size_t {1}, size_t {255}, size_t {256}}) {
            const std::vector<uint32_t> run = MakeWidthRun(w, n);
            ByteSink sink;
            pfor_encode(run.data(), n, &sink);
            std::vector<uint32_t> decoded(n, 0xFFFFFFFFU);
            ByteSource src(sink.view());
            ASSERT_TRUE(pfor_decode(&src, n, decoded.data()).ok())
                    << "w=" << static_cast<int>(w) << " n=" << n;
            EXPECT_TRUE(src.eof()) << "w=" << static_cast<int>(w) << " n=" << n;
            EXPECT_EQ(decoded, run) << "w=" << static_cast<int>(w) << " n=" << n;
        }
    }
}
