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
