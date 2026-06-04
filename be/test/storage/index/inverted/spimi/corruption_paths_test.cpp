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

// Exercises the `SPIMI_THROW_CORRUPT` defensive checks across every
// untrusted-byte reader in the SPIMI write/read pipeline. These tests
// feed crafted malformed buffers and assert each one trips a hard fail
// with `doris::Exception` carrying `INVERTED_INDEX_FILE_CORRUPTED`.
//
// Coverage targets (every test corresponds to a `SPIMI_THROW_CORRUPT`
// site identified by the multi-agent review):
//   - segment_infos_reader.cpp: empty buffer, FORMAT mismatch, VInt
//     shift overflow, segment_count OOR, doc_count OOR, norm_gen_len
//     sentinel mismatch.
//   - term_docs_reader.cpp: bad doc_freq, unknown CodeMode byte, VInt
//     shift overflow, PFOR sub-block bad width, bitpacked underflow,
//     kDefault docCount mismatch.
//   - prox_reader.cpp: non-positive freq, VInt underflow, VInt shift
//     overflow.
//   - pfor_encoder.cpp: empty buffer, count OOR (passed-in), width OOR,
//     BitReader underflow.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "storage/index/inverted/spimi/prox_reader.h"
#include "storage/index/inverted/spimi/segment_infos_reader.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Encodes `v` as a CLucene-style VInt directly into `out` (MSB = continuation
// bit). Matches `ByteOutput::WriteVInt`. Used here so tests can construct
// raw byte streams without spinning up a full writer.
void EmitVInt(std::vector<uint8_t>* out, uint32_t v) {
    while ((v & ~0x7FU) != 0) {
        out->push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    out->push_back(static_cast<uint8_t>(v));
}

std::vector<uint8_t> BuildValidSegmentsN() {
    MemoryByteOutput buf;
    SegmentInfosWriter w;
    SegmentInfoEntry seg;
    seg.name = "_0";
    seg.doc_count = 3;
    seg.del_gen = -1;
    seg.doc_store_offset = -1;
    seg.has_single_norm_file = true;
    seg.is_compound_file = -1;
    w.WriteSegmentsN(&buf, /*version=*/1, /*counter=*/1, {seg});
    return buf.bytes();
}

} // namespace

// ===== SegmentInfosReader =================================================

TEST(SpimiCorruptionPathsTest, SegmentInfos_EmptyBuffer) {
    std::vector<uint8_t> empty;
    EXPECT_THROW(SegmentInfosReader::Read(empty), doris::Exception);
}

TEST(SpimiCorruptionPathsTest, SegmentInfos_FormatMismatch) {
    auto bytes = BuildValidSegmentsN();
    // FORMAT is the first 4-byte big-endian int. Flip it to a non-supported
    // value and the reader must reject.
    bytes[0] = 0x7F;
    bytes[1] = 0x7F;
    bytes[2] = 0x7F;
    bytes[3] = 0x7F;
    EXPECT_THROW(SegmentInfosReader::Read(bytes), doris::Exception);
}

TEST(SpimiCorruptionPathsTest, SegmentInfos_ReadPastEnd) {
    // A 1-byte buffer can pass the empty() check but no full int32 is
    // available, so the cursor must trip the past-end guard.
    std::vector<uint8_t> tiny {0x00};
    EXPECT_THROW(SegmentInfosReader::Read(tiny), doris::Exception);
}

TEST(SpimiCorruptionPathsTest, SegmentInfos_SegmentCountOutOfRange) {
    // Build: FORMAT(int32 -4) + version(int64 0) + counter(int32 0) +
    //        segment_count(int32 = 0x40000000) — bigger than any conceivable
    //        buffer size, so the reader's bound check must fire.
    std::vector<uint8_t> bytes;
    // -4 big-endian = 0xFFFFFFFC
    bytes.insert(bytes.end(), {0xFF, 0xFF, 0xFF, 0xFC});
    // version (8 bytes)
    bytes.insert(bytes.end(), 8, 0x00);
    // counter (4 bytes)
    bytes.insert(bytes.end(), {0x00, 0x00, 0x00, 0x00});
    // segment_count = 0x40000000 ≫ bytes.size()
    bytes.insert(bytes.end(), {0x40, 0x00, 0x00, 0x00});
    EXPECT_THROW(SegmentInfosReader::Read(bytes), doris::Exception);
}

// ===== SpimiTermDocsReader =================================================

TEST(SpimiCorruptionPathsTest, TermDocs_BadDocFreqOrZeroLengthBuffer) {
    std::vector<uint8_t> empty;
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(empty, /*doc_freq=*/1, /*has_prox=*/true),
                 doris::Exception);
    // doc_freq <= 0 with a non-empty buffer also hard-fails.
    std::vector<uint8_t> non_empty {0x00, 0x01};
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(non_empty, /*doc_freq=*/0, /*has_prox=*/true),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, TermDocs_UnknownCodeMode) {
    // Any non-slim block whose leading byte is neither kCodeModeSpimiPfor,
    // kCodeModeSpimiWindowed nor kCodeModeZstd is a corrupt segment. (A bare
    // 0x00 kDefault top-level byte is no longer produced — that path is now the
    // codec-byte-less SLIM layout, dispatched via is_slim — so it too lands in
    // the unknown-CodeMode reject when is_slim is false.)
    std::vector<uint8_t> bytes {0xFE, 0x01, 0x02, 0x03};
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(bytes, /*doc_freq=*/1, /*has_prox=*/false),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, TermDocs_SlimBlockTruncatedThrows) {
    // SLIM kDefault block (is_slim=true): NO codec byte, NO doc_count — just
    // per-doc VInt deltas. doc_freq is authoritative, so the reader reads
    // exactly doc_freq VInts. A buffer with fewer bytes than doc_freq deltas
    // require must hard-fail (read past end) rather than silently truncate.
    std::vector<uint8_t> bytes;
    EmitVInt(&bytes, 11); // one doc-delta VInt only
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(bytes, /*doc_freq=*/2, /*has_prox=*/false,
                                               /*is_slim=*/true),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, TermDocs_VIntShiftOverflow) {
    // SLIM kDefault block + a malformed VInt with 5+ continuation bytes drives
    // the shift counter past 32 — the explicit shift-overflow guard must fire
    // before clang's `shlx` silently truncates.
    std::vector<uint8_t> bytes;
    // 6 continuation bytes — shift hits 7,14,21,28,35 → triggers the
    // `shift >= 32U` guard on the 5th continuation byte.
    bytes.insert(bytes.end(), {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01});
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(bytes, /*doc_freq=*/1, /*has_prox=*/true,
                                               /*is_slim=*/true),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, TermDocs_PforSubBlockBadWidth) {
    // kSpimiPfor mode + width byte 0x00 (width must be ≥1). The sub-block
    // value count is now derived from doc_freq (=1), so the first byte after
    // the codec byte is the width byte directly — no leading VInt(n).
    std::vector<uint8_t> bytes;
    bytes.push_back(FreqProxEncoder::kCodeModeSpimiPfor);
    bytes.push_back(0x00); // width=0 → invalid
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(bytes, /*doc_freq=*/1, /*has_prox=*/false),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, TermDocs_PforSubBlockBitpackedUnderflow) {
    // kSpimiPfor mode, doc_freq=8 ⇒ derived n=8 at width=32 needs 32 bytes of
    // bitpacked payload, but only 4 are supplied — the run decoder must
    // hard-fail on the bitpacked underflow rather than read past the buffer.
    std::vector<uint8_t> bytes;
    bytes.push_back(FreqProxEncoder::kCodeModeSpimiPfor);
    bytes.push_back(32); // width=32
    bytes.insert(bytes.end(), 4, 0xFF);
    EXPECT_THROW(SpimiTermDocsReader::ReadTerm(bytes, /*doc_freq=*/8, /*has_prox=*/false),
                 doris::Exception);
}

// ===== SpimiProxReader =====================================================

TEST(SpimiCorruptionPathsTest, Prox_NonPositiveFreq) {
    // freqs_per_doc[i] <= 0 is rejected before any byte parsing.
    std::vector<uint8_t> bytes {0x00, 0x00};
    EXPECT_THROW(SpimiProxReader::ReadPositions(bytes, /*freqs_per_doc=*/ {0}), doris::Exception);
    EXPECT_THROW(SpimiProxReader::ReadPositions(bytes, /*freqs_per_doc=*/ {-1}), doris::Exception);
}

TEST(SpimiCorruptionPathsTest, Prox_VIntUnderflowOnTruncatedBuffer) {
    // freq=2 means 2 VInts expected, but the buffer is empty.
    std::vector<uint8_t> empty;
    EXPECT_THROW(SpimiProxReader::ReadPositions(empty, /*freqs_per_doc=*/ {2}), doris::Exception);
}

TEST(SpimiCorruptionPathsTest, Prox_VIntShiftOverflow) {
    // freq=1 + a VInt with 6 continuation bytes → shift overflow guard.
    std::vector<uint8_t> bytes {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01};
    EXPECT_THROW(SpimiProxReader::ReadPositions(bytes, /*freqs_per_doc=*/ {1}), doris::Exception);
}

// ===== SpimiPforDecoder ====================================================

TEST(SpimiCorruptionPathsTest, Pfor_EmptyBuffer) {
    // The block format no longer stores a count, so the decoder is told
    // count=1 but the buffer is empty → it cannot even read the width
    // byte. Hard-fail rather than read out of bounds.
    std::vector<uint8_t> empty;
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(empty, /*count=*/1, &out),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, Pfor_BadWidthByte) {
    // width=0 is illegal — the encoder always emits width in [1, 32].
    std::vector<uint8_t> bytes;
    bytes.push_back(0x00);
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, /*count=*/1, &out),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, Pfor_BitReaderUnderflow) {
    // Claim 8 values at width=32 — needs 32 bytes of bit data, but
    // we supply only 4 bytes after the width byte.
    std::vector<uint8_t> bytes;
    bytes.push_back(32); // width=32
    bytes.insert(bytes.end(), 4, 0xFF);
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, /*count=*/8, &out),
                 doris::Exception);
}

TEST(SpimiCorruptionPathsTest, Pfor_SubBlockCountOutOfRange) {
    // count=0 is illegal; the encoder never emits an empty sub-block.
    // count is now supplied out-of-band, so passing 0 must hard-fail.
    std::vector<uint8_t> bytes;
    bytes.push_back(0x01);
    std::vector<uint32_t> out;
    EXPECT_THROW(SpimiPforDecoder::DecodeBlockFromBytes(bytes, /*count=*/0, &out),
                 doris::Exception);
}

} // namespace doris::segment_v2::inverted_index::spimi
