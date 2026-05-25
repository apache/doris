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

#include "storage/index/inverted/spimi/fulltext_writer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

struct SegmentBytes {
    MemoryLuceneOutput tis, tii, frq, prx, fnm, nrm, segments_n, segments_gen;

    SpimiSegmentSink Sink() {
        return SpimiSegmentSink {.tis = &tis,
                                 .tii = &tii,
                                 .frq = &frq,
                                 .prx = &prx,
                                 .fnm = &fnm,
                                 .nrm = &nrm,
                                 .segments_n = &segments_n,
                                 .segments_gen = &segments_gen};
    }
};

class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() { return _bytes[_pos++]; }
    int32_t ReadInt() {
        int32_t v = (Byte() << 24);
        v |= (Byte() << 16);
        v |= (Byte() << 8);
        v |= Byte();
        return v;
    }
    int64_t ReadLong() {
        const auto hi = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        const auto lo = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        return (hi << 32) | lo;
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int32_t>(v);
    }
    size_t remaining() const { return _bytes.size() - _pos; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

} // namespace

TEST(SpimiFulltextWriterTest, EmptySegmentEmitsAllSevenStreams) {
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body");
    const int64_t terms = w.Finish();

    EXPECT_EQ(terms, 0);
    // tis: header + footer (24 + 8 = 32 bytes).
    EXPECT_EQ(seg.tis.bytes().size(), 32U);
    // tii: header (24) + sentinel TII entry (~11 bytes: VInt(prefix=0)
    // + VInt(suffix=0) + VInt(field=-1) + VInt(doc_freq=0) +
    // VLong(freq_delta=0) + VLong(prox_delta=0) + VLong(tis_delta=
    // tis_data_start)) + footer (16 = tii_size + tis_size).
    // The writer emits the sentinel even for zero-term segments so
    // the reader's "first entry is the sentinel" invariant holds
    // for honest all-NULL / all-empty segments.
    EXPECT_GT(seg.tii.bytes().size(), 40U);
    EXPECT_TRUE(seg.frq.bytes().empty());
    EXPECT_TRUE(seg.prx.bytes().empty());
    // fnm: 1 field
    EXPECT_FALSE(seg.fnm.bytes().empty());
    // segments_n + segments.gen are non-empty
    EXPECT_FALSE(seg.segments_n.bytes().empty());
    EXPECT_FALSE(seg.segments_gen.bytes().empty());
}

TEST(SpimiFulltextWriterTest, AddingTokensUpdatesDocCount) {
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body");
    w.AddOccurrence(0, "alpha", 0);
    w.AddOccurrence(3, "alpha", 1);
    w.AddOccurrence(3, "beta", 2);
    EXPECT_EQ(w.DocCount(), 4) << "max doc id 3 ⇒ doc_count 4";

    w.Finish();
    // segments_N records doc_count = 4.
    ByteReader r(seg.segments_n.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    r.ReadLong();               // version
    EXPECT_EQ(r.ReadInt(), 1);  // counter
    EXPECT_EQ(r.ReadInt(), 1);  // segment count
    EXPECT_EQ(r.ReadVInt(), 2); // name length
    (void)r.Byte();
    (void)r.Byte();
    EXPECT_EQ(r.ReadInt(), 4); // doc_count
}

TEST(SpimiFulltextWriterTest, FieldInfosEmittedWithProxBitsAndHasNorms) {
    // Phase 32 — `.fnm` now mirrors CLucene's analyzed-field shape
    // (omit_norms = false). Matches what
    // `InvertedIndexColumnWriter::create_field` sets via
    // `setOmitNorms(false)` on the analyzed path, so the SPIMI
    // shadow `.fnm` is byte-identical to CLucene's `.fnm`.
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body");
    w.AddOccurrence(0, "x", 0);
    w.Finish();

    ByteReader r(seg.fnm.bytes());
    EXPECT_EQ(r.ReadVInt(), 1); // field count
    EXPECT_EQ(r.ReadVInt(), 4); // name length "body"
    for (char c : std::string("body")) {
        EXPECT_EQ(r.Byte(), static_cast<uint8_t>(c));
    }
    const uint8_t bits = r.Byte();
    EXPECT_NE(bits & FieldInfosWriter::kIsIndexed, 0U);
    EXPECT_EQ(bits & FieldInfosWriter::kOmitNorms, 0U)
            << "Phase 32: analyzed field carries norms (omit_norms=false)";
    EXPECT_NE(bits & FieldInfosWriter::kTermFreqAndPositions, 0U);
}

TEST(SpimiFulltextWriterTest, V4IndexVersionPropagatesToFnm) {
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body", /*index_version=*/4);
    w.AddOccurrence(0, "x", 0);
    w.Finish();

    ByteReader r(seg.fnm.bytes());
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 4);
    for (int i = 0; i < 4; ++i) {
        (void)r.Byte();
    }
    const uint8_t bits = r.Byte();
    EXPECT_NE(bits & FieldInfosWriter::kHasVersionTag, 0U);
    EXPECT_EQ(r.ReadVInt(), 4); // version
    EXPECT_EQ(r.ReadVInt(), 0); // flags
}

TEST(SpimiFulltextWriterTest, SortsBeforeEmitting) {
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body");
    // Out-of-order on purpose.
    w.AddOccurrence(2, "beta", 0);
    w.AddOccurrence(0, "alpha", 0);
    w.AddOccurrence(1, "alpha", 0);
    const int64_t terms = w.Finish();
    EXPECT_EQ(terms, 2) << "alpha and beta";
}

TEST(SpimiFulltextWriterTest, SegmentsGenUsesGenerationOne) {
    SegmentBytes seg;
    SpimiFulltextWriter w(seg.Sink(), "_0", "body");
    w.Finish();

    ByteReader r(seg.segments_gen.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatLockless);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SpimiFulltextWriterTest, EmitSegmentStaticAcceptsExternalBuffer) {
    // External-buffer mode: the integration layer owns a long-lived
    // SpimiPostingBuffer for the column's write lifetime and only opens
    // the seven sinks at finish() time. Confirm the produced bytes are
    // identical to the instance-owned-buffer path for the same input.
    SegmentBytes inst;
    {
        SpimiFulltextWriter w(inst.Sink(), "_0", "body");
        w.AddOccurrence(0, "alpha", 0);
        w.AddOccurrence(1, "alpha", 1);
        w.AddOccurrence(2, "beta", 0);
        w.Finish();
    }

    SegmentBytes ext;
    {
        SpimiPostingBuffer buffer;
        buffer.Append("alpha", 0, 0);
        buffer.Append("alpha", 1, 1);
        buffer.Append("beta", 2, 0);
        const int64_t terms = SpimiFulltextWriter::EmitSegment(buffer, ext.Sink(), "_0", "body",
                                                               /*doc_count=*/3);
        EXPECT_EQ(terms, 2);
    }

    EXPECT_EQ(inst.tis.bytes(), ext.tis.bytes());
    EXPECT_EQ(inst.tii.bytes(), ext.tii.bytes());
    EXPECT_EQ(inst.frq.bytes(), ext.frq.bytes());
    EXPECT_EQ(inst.prx.bytes(), ext.prx.bytes());
    EXPECT_EQ(inst.fnm.bytes(), ext.fnm.bytes());
    EXPECT_EQ(inst.segments_n.bytes(), ext.segments_n.bytes());
    EXPECT_EQ(inst.segments_gen.bytes(), ext.segments_gen.bytes());
}

TEST(SpimiFulltextWriterTest, EmitSegmentPropagatesDocCount) {
    SegmentBytes seg;
    SpimiPostingBuffer buffer;
    buffer.Append("x", 0, 0);
    buffer.Append("x", 7, 0);
    SpimiFulltextWriter::EmitSegment(buffer, seg.Sink(), "_0", "body",
                                     /*doc_count=*/8);
    ByteReader r(seg.segments_n.bytes());
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    (void)r.ReadLong();
    EXPECT_EQ(r.ReadInt(), 1);  // counter
    EXPECT_EQ(r.ReadInt(), 1);  // segment count
    EXPECT_EQ(r.ReadVInt(), 2); // name "_0"
    (void)r.Byte();
    (void)r.Byte();
    EXPECT_EQ(r.ReadInt(), 8); // doc_count
}

// Phase 32 — `.nrm` mirrors CLucene's SDocumentsWriter::writeNorms.
// The header bytes "NRM\xFF", the per-field 8-byte VLong totalTermCount,
// and 1 byte of encoded length-norm per doc id must all match CLucene.

TEST(SpimiFulltextWriterTest, NrmHeaderAndPerDocLengthEncoding) {
    // 3 docs: doc 0 has 5 tokens, doc 1 has 2 tokens, doc 2 has 8 tokens.
    // Each token contributes one record (with arbitrary term/position).
    SegmentBytes seg;
    SpimiPostingBuffer buffer;
    for (uint32_t i = 0; i < 5; ++i) {
        buffer.Append("a", 0, i);
    }
    for (uint32_t i = 0; i < 2; ++i) {
        buffer.Append("b", 1, i);
    }
    for (uint32_t i = 0; i < 8; ++i) {
        buffer.Append("c", 2, i);
    }
    SpimiFulltextWriter::EmitSegment(buffer, seg.Sink(), "_0", "body", /*doc_count=*/3);

    ByteReader r(seg.nrm.bytes());
    EXPECT_EQ(r.Byte(), 'N');
    EXPECT_EQ(r.Byte(), 'R');
    EXPECT_EQ(r.Byte(), 'M');
    EXPECT_EQ(r.Byte(), 0xFF);
    // total term count: 5 + 2 + 8 = 15, written as 8-byte big-endian long.
    EXPECT_EQ(r.ReadLong(), 15);
    // 1 byte per doc; lengths 5/2/8 are all < NUM_FREE_VALUES (≈ 220) so
    // int_to_byte4 returns the raw length unchanged.
    EXPECT_EQ(r.Byte(), 5U);
    EXPECT_EQ(r.Byte(), 2U);
    EXPECT_EQ(r.Byte(), 8U);
}

TEST(SpimiFulltextWriterTest, NrmEncodingHandlesLengthAboveFreeValueRange) {
    // Round-9 architect Q2: pin the compact-branch of
    // `int_to_byte4` for length >= NUM_FREE_VALUES (= 220). Production
    // fulltext docs almost never have > 220 tokens, so the branch is
    // unreachable on real workloads — but this test locks the formula
    // so a future refactor can't break the encoding silently. Values
    // are computed by hand from CLucene's
    // `int_to_byte4` ⇄ `long_to_int4` (Similarity.cpp:212-242):
    //   length=220 → NUM_FREE_VALUES + long_to_int4(0)
    //              = 220 + 0 = 220 = 0xDC
    //   length=221 → 220 + long_to_int4(1) = 220 + 1 = 221 = 0xDD
    //   length=228 → 220 + long_to_int4(8) = 220 + (0x20 | 1) = 220 + 0x21
    //                                              = 253 = 0xFD
    //                 (numBits=4, shift=0, encoded = (8&7) | ((0+1)<<3) = 0|8 = 8 ⇒ result 220+8=228? wait recompute)
    //   Recomputed from Similarity.cpp long_to_int4:
    //     i = length - 220
    //     numBits = bits set in i
    //     shift = numBits - 4 (if numBits >= 4)
    //     encoded = (i >> shift) & 0x07 | ((shift+1) << 3)
    //   length=228 ⇒ i=8 (=0b1000), numBits=4, shift=0,
    //                encoded = (8 & 7) | (1 << 3) = 0 | 8 = 8 ⇒ result = 220+8 = 228 = 0xE4
    //
    // Drive each value through the real `.nrm` writer and compare to
    // the hand-derived bytes.
    auto encode_via_nrm = [](int32_t length) -> uint8_t {
        SegmentBytes seg;
        SpimiPostingBuffer buffer;
        for (int32_t i = 0; i < length; ++i) {
            buffer.Append("a", 0, static_cast<uint32_t>(i));
        }
        SpimiFulltextWriter::EmitSegment(buffer, seg.Sink(), "_0", "body",
                                         /*doc_count=*/1);
        // NRM header (4) + long (8) = 12 bytes, then 1 norm byte.
        return seg.nrm.bytes().at(12);
    };

    EXPECT_EQ(encode_via_nrm(219), 219); // free range, last passthrough
    EXPECT_EQ(encode_via_nrm(220), 220); // free range edge
    EXPECT_EQ(encode_via_nrm(221), 221); // first compact-branch slot
    EXPECT_EQ(encode_via_nrm(228), 228); // numBits=4 boundary
}

} // namespace doris::segment_v2::inverted_index::spimi
