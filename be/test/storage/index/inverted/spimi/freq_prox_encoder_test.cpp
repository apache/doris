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

#include "storage/index/inverted/spimi/freq_prox_encoder.h"

#include <gtest/gtest.h>

#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size()) << "Read past end of buffer";
        return _bytes[_pos++];
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

TEST(FreqProxEncoderTest, SingleDocSingleFreqEncodesAsTaggedVInt) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx, /*skip_interval=*/16, /*max_skip_levels=*/10);

    e.StartTerm(1);
    e.StartDoc(5, /*freq=*/1);
    e.AddPosition(3);
    e.FinishDoc();
    const TermInfo info = e.FinishTerm();

    EXPECT_EQ(info.doc_freq, 1);
    EXPECT_EQ(info.freq_pointer, 0);
    EXPECT_EQ(info.prox_pointer, 0);
    EXPECT_EQ(info.skip_offset, 0) << "doc_freq < skip_interval ⇒ no skip data emitted";

    ByteReader fr(frq.bytes());
    // Each term's .frq block opens with a 2-byte codec prefix produced by
    // Doris's CLucene fork (`CodeMode` byte + VInt doc count); see
    // contrib/clucene/src/core/CLucene/index/SDocumentWriter.cpp
    // appendPostings line ~1330. SPIMI mirrors the same prefix so the
    // production reader picks the matching decode branch.
    EXPECT_EQ(fr.Byte(), 0x00) << "CodeMode::kDefault prefix";
    EXPECT_EQ(fr.ReadVInt(), 1) << "doc count VInt prefix";
    // Single doc: doc_delta = 5-0 = 5; freq == 1 ⇒ encoded as (5 << 1) | 1 = 11.
    EXPECT_EQ(fr.ReadVInt(), 11);
    EXPECT_EQ(fr.remaining(), 0U) << "No skip data appended for short term";

    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 3) << "position delta from 0";
    EXPECT_EQ(pr.remaining(), 0U);
}

TEST(FreqProxEncoderTest, MultiFreqDocEncodesAsTwoVInts) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx);

    e.StartTerm(1);
    e.StartDoc(4, /*freq=*/3);
    e.AddPosition(1);
    e.AddPosition(5);
    e.AddPosition(9);
    e.FinishDoc();
    (void)e.FinishTerm();

    ByteReader fr(frq.bytes());
    EXPECT_EQ(fr.Byte(), 0x00) << "CodeMode::kDefault prefix";
    EXPECT_EQ(fr.ReadVInt(), 1) << "doc count VInt prefix";
    EXPECT_EQ(fr.ReadVInt(), 8) << "doc_delta=4 << 1 = 8 (LSB clear ⇒ freq follows)";
    EXPECT_EQ(fr.ReadVInt(), 3) << "freq";

    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 1); // 1 - 0
    EXPECT_EQ(pr.ReadVInt(), 4); // 5 - 1
    EXPECT_EQ(pr.ReadVInt(), 4); // 9 - 5
}

TEST(FreqProxEncoderTest, MultipleDocsWithDocDeltas) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx);

    e.StartTerm(3);

    e.StartDoc(2, 1);
    e.AddPosition(0);
    e.FinishDoc();

    e.StartDoc(5, 2);
    e.AddPosition(1);
    e.AddPosition(7);
    e.FinishDoc();

    e.StartDoc(9, 1);
    e.AddPosition(4);
    e.FinishDoc();

    const TermInfo info = e.FinishTerm();
    EXPECT_EQ(info.doc_freq, 3);

    ByteReader fr(frq.bytes());
    EXPECT_EQ(fr.Byte(), 0x00) << "CodeMode::kDefault prefix";
    EXPECT_EQ(fr.ReadVInt(), 3) << "doc count VInt prefix";
    EXPECT_EQ(fr.ReadVInt(), (2 << 1) | 1); // doc 2, freq 1 (tagged)
    EXPECT_EQ(fr.ReadVInt(), (3 << 1));     // doc delta 3, freq 2 follows
    EXPECT_EQ(fr.ReadVInt(), 2);            // freq
    EXPECT_EQ(fr.ReadVInt(), (4 << 1) | 1); // doc delta 4, freq 1 (tagged)
    EXPECT_EQ(fr.remaining(), 0U);

    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 0);
    EXPECT_EQ(pr.ReadVInt(), 1);
    EXPECT_EQ(pr.ReadVInt(), 6);
    EXPECT_EQ(pr.ReadVInt(), 4);
    EXPECT_EQ(pr.remaining(), 0U);
}

TEST(FreqProxEncoderTest, ProxResetsPerDoc) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx);
    e.StartTerm(2);
    // doc 1, positions {2, 7}
    e.StartDoc(1, 2);
    e.AddPosition(2);
    e.AddPosition(7);
    e.FinishDoc();
    // doc 2, positions {3, 8} — note: 3 < 7 (previous doc's last pos), so
    // if the encoder didn't reset, the delta would be negative.
    e.StartDoc(2, 2);
    e.AddPosition(3);
    e.AddPosition(8);
    e.FinishDoc();
    (void)e.FinishTerm();

    ByteReader pr(prx.bytes());
    EXPECT_EQ(pr.Byte(), 0x00) << "prox block raw-mode header byte";
    EXPECT_EQ(pr.ReadVInt(), 2); // doc 1: 2 - 0
    EXPECT_EQ(pr.ReadVInt(), 5); // doc 1: 7 - 2
    EXPECT_EQ(pr.ReadVInt(), 3) << "doc 2: position delta reset to 0";
    EXPECT_EQ(pr.ReadVInt(), 5); // doc 2: 8 - 3
}

TEST(FreqProxEncoderTest, SkipListEmittedWhenDocFreqExceedsInterval) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    // Tiny skip_interval so we can hit boundaries quickly.
    FreqProxEncoder e(&frq, &prx, /*skip_interval=*/4, /*max_skip_levels=*/2);

    e.StartTerm(/*expected_doc_freq=*/8);
    for (int i = 0; i < 8; ++i) {
        e.StartDoc(i + 1, 1);
        e.AddPosition(i * 2);
        e.FinishDoc();
    }
    const TermInfo info = e.FinishTerm();
    EXPECT_EQ(info.doc_freq, 8);
    EXPECT_GT(info.skip_offset, 0)
            << "doc_freq >= skip_interval ⇒ skip data emitted, skip_offset > 0";
    EXPECT_LT(info.skip_offset, static_cast<int32_t>(frq.bytes().size()));
}

TEST(FreqProxEncoderTest, NoSkipListWhenDocFreqBelowInterval) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx, /*skip_interval=*/4, /*max_skip_levels=*/2);

    e.StartTerm(3);
    for (int i = 0; i < 3; ++i) {
        e.StartDoc(i + 1, 1);
        e.AddPosition(0);
        e.FinishDoc();
    }
    const TermInfo info = e.FinishTerm();
    EXPECT_EQ(info.skip_offset, 0);
}

TEST(FreqProxEncoderTest, MultipleTermsReuseTheEncoder) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx);

    // Term A.
    e.StartTerm(2);
    e.StartDoc(1, 1);
    e.AddPosition(0);
    e.FinishDoc();
    e.StartDoc(2, 1);
    e.AddPosition(0);
    e.FinishDoc();
    const TermInfo a = e.FinishTerm();
    EXPECT_EQ(a.freq_pointer, 0);
    EXPECT_EQ(a.prox_pointer, 0);

    // Term B — pointers reflect the bytes already written by term A.
    e.StartTerm(1);
    e.StartDoc(3, 1);
    e.AddPosition(0);
    e.FinishDoc();
    const TermInfo b = e.FinishTerm();
    EXPECT_GT(b.freq_pointer, a.freq_pointer);
    EXPECT_GT(b.prox_pointer, a.prox_pointer);
    // Term B occupies its own [CodeMode byte][VInt doc count][doc delta]
    // block — 3 bytes total for one doc — at the tail of the .frq stream.
    EXPECT_EQ(b.freq_pointer, static_cast<int64_t>(frq.bytes().size() - 3));
}

TEST(FreqProxEncoderTest, SkipOffsetMatchesFrqByteLayout) {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    FreqProxEncoder e(&frq, &prx, /*skip_interval=*/4, /*max_skip_levels=*/2);
    e.StartTerm(4);
    for (int i = 0; i < 4; ++i) {
        e.StartDoc(i + 1, 1);
        e.AddPosition(0);
        e.FinishDoc();
    }
    const TermInfo info = e.FinishTerm();
    // skip_offset is the byte length of the docs portion (i.e. the offset
    // from term start to where skip data begins). The total frq term bytes
    // equals skip_offset + skip_bytes.
    EXPECT_GT(info.skip_offset, 0);
    EXPECT_LE(static_cast<size_t>(info.skip_offset), frq.bytes().size());
}

} // namespace doris::segment_v2::inverted_index::spimi
