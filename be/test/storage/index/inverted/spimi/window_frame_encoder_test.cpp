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

#include "storage/index/inverted/spimi/window_frame_encoder.h"

#include <gtest/gtest.h>

#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/prox_reader.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// A synthetic term: absolute doc ids, per-doc freqs, and per-doc positions.
struct Term {
    std::vector<int32_t> docs;                    // ascending absolute doc ids
    std::vector<int32_t> freqs;                   // freq per doc (== positions.size())
    std::vector<std::vector<int32_t>> positions;  // ascending positions per doc
};

// Drives FreqProxEncoder in V4 windowed mode over `t`, returns the .frq / .prx
// bytes. has_prox controls whether freqs/positions are emitted.
void EncodeWindowed(const Term& t, bool has_prox, std::vector<uint8_t>* frq,
                    std::vector<uint8_t>* prx) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    // skip_interval=1 forces the per-term windowing gate to admit every df so
    // this isolation helper exercises the windowed encoder across df < unit /
    // single-window cases; the production gate (512) is unchanged.
    FreqProxEncoder enc(&frq_out, &prx_out, /*skip_interval=*/1, /*max_skip_levels=*/10,
                        /*omit_term_freq_and_positions=*/!has_prox, /*use_windowed=*/true);
    enc.StartTerm(static_cast<int32_t>(t.docs.size()));
    for (size_t i = 0; i < t.docs.size(); ++i) {
        const int32_t freq = has_prox ? t.freqs[i] : 1;
        enc.StartDoc(t.docs[i], freq);
        if (has_prox) {
            for (const int32_t p : t.positions[i]) {
                enc.AddPosition(p);
            }
        }
        enc.FinishDoc();
    }
    (void)enc.FinishTerm();
    *frq = frq_out.bytes();
    *prx = prx_out.bytes();
}

// Asserts the windowed .frq/.prx roundtrips byte-exact through BOTH readers.
void ExpectRoundtrip(const Term& t, bool has_prox) {
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
    EncodeWindowed(t, has_prox, &frq, &prx);

    const auto df = static_cast<int32_t>(t.docs.size());
    ASSERT_FALSE(frq.empty());
    ASSERT_EQ(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);

    // --- SpimiTermDocsReader path ---
    const auto pairs = SpimiTermDocsReader::ReadTerm(frq, df, has_prox);
    ASSERT_EQ(pairs.size(), static_cast<size_t>(df));
    for (int32_t i = 0; i < df; ++i) {
        EXPECT_EQ(pairs[i].first, t.docs[i]) << "doc id mismatch at " << i;
        const int32_t expect_freq = has_prox ? t.freqs[i] : 1;
        EXPECT_EQ(pairs[i].second, expect_freq) << "freq mismatch at " << i;
    }

    // --- PostingDecoder path (full decode incl. positions) ---
    const auto docs = PostingDecoder::Decode(frq.data(), frq.size(),
                                             has_prox ? prx.data() : nullptr,
                                             has_prox ? prx.size() : 0, df, has_prox);
    ASSERT_EQ(docs.size(), static_cast<size_t>(df));
    for (int32_t i = 0; i < df; ++i) {
        EXPECT_EQ(docs[i].doc_id, t.docs[i]);
        if (has_prox) {
            EXPECT_EQ(docs[i].freq, t.freqs[i]);
            ASSERT_EQ(docs[i].positions.size(), t.positions[i].size());
            for (size_t j = 0; j < t.positions[i].size(); ++j) {
                EXPECT_EQ(docs[i].positions[j], t.positions[i][j])
                        << "position mismatch doc " << i << " pos " << j;
            }
        }
    }

    // --- SpimiProxReader path ---
    if (has_prox) {
        ASSERT_FALSE(prx.empty());
        ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);
        std::vector<int32_t> freqs_per_doc(t.freqs.begin(), t.freqs.end());
        const auto positions = SpimiProxReader::ReadPositions(prx.data(), prx.size(), freqs_per_doc);
        ASSERT_EQ(positions.size(), static_cast<size_t>(df));
        for (int32_t i = 0; i < df; ++i) {
            ASSERT_EQ(positions[i].size(), t.positions[i].size());
            for (size_t j = 0; j < t.positions[i].size(); ++j) {
                EXPECT_EQ(positions[i][j], t.positions[i][j]);
            }
        }
    }
}

// Reads a VInt from `data` at `*pos`, advancing `*pos`. Mirrors
// ByteOutput::WriteVInt / ByteStream::ReadVInt (7-bit groups, MSB continuation).
uint32_t ReadVIntAt(const std::vector<uint8_t>& data, size_t* pos) {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        EXPECT_LT(*pos, data.size()) << "VInt ran past end of buffer";
        const uint8_t b = data[(*pos)++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
    }
    return v;
}

// One window's skip-table entry plus the resolved start position of its payload
// tuple (absolute byte offset into the .frq buffer).
struct SkipEntry {
    uint32_t doc_count = 0;
    uint32_t byte_offset = 0; // relative to the first payload tuple
    uint32_t min_docid = 0;
    uint32_t max_docid_delta = 0;
    size_t payload_abs_pos = 0; // absolute offset of this window's payload tuple
};

// Parses the V4 .frq header + per-window skip table out of `frq`, validating
// the structural invariants the prior interleave bug would have violated. Fills
// `*entries` with one SkipEntry per window and returns the parsed inner_mode.
// This is the metadata-level half of the regression: it proves each recorded
// win_byte_offset lands on a real per-window payload FRAME (a valid win_mode
// byte + uncomp length), and that the per-window doc range is self-consistent.
uint8_t ParseFrqSkipTable(const std::vector<uint8_t>& frq, std::vector<SkipEntry>* entries) {
    size_t pos = 0;
    EXPECT_EQ(frq.at(pos++), FreqProxEncoder::kCodeModeSpimiWindowed);
    const uint8_t inner_mode = frq.at(pos++);
    const uint32_t W = ReadVIntAt(frq, &pos);
    EXPECT_GE(W, 256u);
    const uint32_t num_windows = ReadVIntAt(frq, &pos);
    EXPECT_GE(num_windows, 1u);

    entries->clear();
    entries->resize(num_windows);
    for (uint32_t w = 0; w < num_windows; ++w) {
        (*entries)[w].doc_count = ReadVIntAt(frq, &pos);
        (*entries)[w].byte_offset = ReadVIntAt(frq, &pos);
        (*entries)[w].min_docid = ReadVIntAt(frq, &pos);
        (*entries)[w].max_docid_delta = ReadVIntAt(frq, &pos);
    }
    // The byte right after the whole skip table is the first payload tuple; all
    // recorded win_byte_offset values are relative to it.
    const size_t payloads_base = pos;
    for (uint32_t w = 0; w < num_windows; ++w) {
        (*entries)[w].payload_abs_pos = payloads_base + (*entries)[w].byte_offset;
    }
    return inner_mode;
}

// Asserts the skip table is internally consistent AND consistent with the
// actual decoded docs. `docs` are the ground-truth absolute doc ids.
void ExpectSkipTableSound(const std::vector<uint8_t>& frq, const std::vector<int32_t>& docs) {
    std::vector<SkipEntry> entries;
    ParseFrqSkipTable(frq, &entries);
    ASSERT_FALSE(entries.empty());

    // 1) win_byte_offset must be monotonically increasing and the first must be 0
    //    (offsets are relative to the first payload tuple).
    EXPECT_EQ(entries.front().byte_offset, 0u);
    for (size_t w = 1; w < entries.size(); ++w) {
        EXPECT_GT(entries[w].byte_offset, entries[w - 1].byte_offset)
                << "win_byte_offset not strictly increasing at window " << w;
    }

    // 2) Each recorded byte offset must START A VALID FRAME: a win_mode byte in
    //    {0 raw, 1 zstd} followed by a parseable uncomp-length VInt. (Under the
    //    prior interleave layout the recorded offsets would not land on real
    //    frame headers.)
    for (size_t w = 0; w < entries.size(); ++w) {
        size_t p = entries[w].payload_abs_pos;
        ASSERT_LT(p, frq.size()) << "payload offset past end, window " << w;
        const uint8_t win_mode = frq[p++];
        EXPECT_TRUE(win_mode == 0 || win_mode == 1)
                << "window " << w << " byte_offset does not start a valid frame; win_mode="
                << static_cast<int>(win_mode);
        const uint32_t uncomp = ReadVIntAt(frq, &p);
        EXPECT_GT(uncomp, 0u) << "window " << w << " inflated length must be > 0";
    }

    // 3) The per-window doc ranges must partition `docs` in order and each
    //    window's [min_docid, min_docid+max_docid_delta] must BOUND exactly the
    //    docs it covers.
    size_t doc_cursor = 0;
    uint64_t total = 0;
    for (size_t w = 0; w < entries.size(); ++w) {
        const uint32_t cnt = entries[w].doc_count;
        ASSERT_GT(cnt, 0u) << "window " << w << " doc_count must be > 0";
        ASSERT_LE(doc_cursor + cnt, docs.size());
        const int32_t first = docs[doc_cursor];
        const int32_t last = docs[doc_cursor + cnt - 1];
        EXPECT_EQ(entries[w].min_docid, static_cast<uint32_t>(first))
                << "window " << w << " min_docid does not match its first doc";
        EXPECT_EQ(entries[w].min_docid + entries[w].max_docid_delta,
                  static_cast<uint32_t>(last))
                << "window " << w << " max docid bound does not match its last doc";
        // Every doc in this window must fall within the recorded [min,max] bound.
        for (uint32_t i = 0; i < cnt; ++i) {
            const int32_t d = docs[doc_cursor + i];
            EXPECT_GE(static_cast<uint32_t>(d), entries[w].min_docid);
            EXPECT_LE(static_cast<uint32_t>(d), entries[w].min_docid + entries[w].max_docid_delta);
        }
        doc_cursor += cnt;
        total += cnt;
    }
    EXPECT_EQ(total, docs.size()) << "window doc counts must sum to df";
    EXPECT_EQ(doc_cursor, docs.size());
}

// Roundtrips AND validates the skip table for a has_prox term.
void ExpectRoundtripAndSkip(const Term& t, bool has_prox) {
    ExpectRoundtrip(t, has_prox);
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
    EncodeWindowed(t, has_prox, &frq, &prx);
    ExpectSkipTableSound(frq, t.docs);
}

// Builds a term of `df` docs with a fixed doc-id stride and `freq` positions
// per doc (positions = doc-local 0,1,2,...).
Term MakeTerm(int32_t df, int32_t stride, int32_t freq) {
    Term t;
    int32_t doc = 7; // non-zero first doc to exercise first-delta handling
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        t.freqs.push_back(freq);
        std::vector<int32_t> pos;
        for (int32_t p = 0; p < freq; ++p) {
            pos.push_back(p * 2);
        }
        t.positions.push_back(std::move(pos));
        doc += stride;
    }
    return t;
}

} // namespace

// df < 256 ⇒ single unit, single window, VInt inner mode, k clamped to 1.
TEST(WindowFrameEncoderTest, SingleDocDfBelowUnit) {
    ExpectRoundtrip(MakeTerm(/*df=*/1, /*stride=*/3, /*freq=*/2), /*has_prox=*/true);
}

TEST(WindowFrameEncoderTest, Df255SingleWindowVInt) {
    ExpectRoundtrip(MakeTerm(/*df=*/255, /*stride=*/2, /*freq=*/1), /*has_prox=*/true);
}

// df == 256 ⇒ exactly one finest unit, PFOR inner mode.
TEST(WindowFrameEncoderTest, Df256SingleUnitPfor) {
    ExpectRoundtrip(MakeTerm(/*df=*/256, /*stride=*/1, /*freq=*/3), /*has_prox=*/true);
}

// df spanning multiple units; the adaptive search must pick a valid W and the
// windows must compose part-wise (the prior interleave bug surfaced here).
TEST(WindowFrameEncoderTest, MultiUnitMultiWindow) {
    ExpectRoundtrip(MakeTerm(/*df=*/600, /*stride=*/2, /*freq=*/2), /*has_prox=*/true);
}

// df that forces k=4 (W=1024) granularity to be considered; doc-delta
// continuity across unit AND window boundaries is exercised.
TEST(WindowFrameEncoderTest, ManyUnitsContinuity) {
    ExpectRoundtrip(MakeTerm(/*df=*/2050, /*stride=*/1, /*freq=*/1), /*has_prox=*/true);
}

// Variable freqs (some 1, some large) exercise the patched-PFOR freq path and
// the position slicing per doc.
TEST(WindowFrameEncoderTest, VariableFreqsAndPositions) {
    Term t;
    int32_t doc = 1;
    for (int32_t i = 0; i < 700; ++i) {
        t.docs.push_back(doc);
        const int32_t freq = (i % 17 == 0) ? 40 : ((i % 3 == 0) ? 1 : 2);
        t.freqs.push_back(freq);
        std::vector<int32_t> pos;
        int32_t p = 0;
        for (int32_t k = 0; k < freq; ++k) {
            pos.push_back(p);
            p += 1 + (k % 4);
        }
        t.positions.push_back(std::move(pos));
        doc += 1 + (i % 5); // irregular gaps
    }
    ExpectRoundtrip(t, /*has_prox=*/true);
}

// has_prox = false: no freq region, no .prx block at all.
TEST(WindowFrameEncoderTest, NoProxDocIdsOnly) {
    ExpectRoundtrip(MakeTerm(/*df=*/600, /*stride=*/4, /*freq=*/1), /*has_prox=*/false);
}

TEST(WindowFrameEncoderTest, NoProxBelowUnit) {
    ExpectRoundtrip(MakeTerm(/*df=*/100, /*stride=*/4, /*freq=*/1), /*has_prox=*/false);
}

// Large, highly-compressible term to exercise the per-window ZSTD path.
TEST(WindowFrameEncoderTest, CompressibleLargeTerm) {
    ExpectRoundtrip(MakeTerm(/*df=*/4096, /*stride=*/1, /*freq=*/1), /*has_prox=*/true);
}

// Explicit doc-delta-continuity check: a 2-unit window must read all its
// doc-deltas as one PFOR run before the freq region (regression for the prior
// interleave bug). MakeTerm(df=512) yields exactly 2 units → one or two windows.
TEST(WindowFrameEncoderTest, TwoUnitWindowDocDeltaContinuity) {
    ExpectRoundtrip(MakeTerm(/*df=*/512, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// ---------------------------------------------------------------------------
// Composition-bug regression matrix. Every df at and around each unit/window
// boundary (256/512/1024/2048) is exercised so the part-wise window layout is
// validated across single-unit, multi-unit-single-window, and multi-window
// framings. Each case roundtrips byte-exact AND validates the per-window skip
// metadata (byte offsets start real frames; min/max docid bound each window).
//
// These are EXACTLY the cases the prior interleaved [ddA][fqA][ddB][fqB] layout
// would have failed: as soon as a window covers >= 2 finest units, the decoder
// reads all the window's doc-deltas as one PFOR run, then all its freqs; the
// interleaved layout made DecodePforRun run past the doc-deltas into a freq
// sub-block header → "PFOR sub-block count out of range".

// Just under / at / just over the first unit boundary (256). df<256 takes the
// VInt single-window path (the prior HANG case if k were 0); df>=256 PFOR.
TEST(WindowFrameEncoderTest, BoundaryDf255) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/255, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf256) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/256, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf257) {
    // 2 units (256 + 1) — first multi-unit case; part-wise composition required.
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/257, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// Around 512 (k=2 candidate, W=512).
TEST(WindowFrameEncoderTest, BoundaryDf511) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/511, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf512) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/512, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf513) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/513, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// Around 1024 (k=4 candidate, W=1024).
TEST(WindowFrameEncoderTest, BoundaryDf1023) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/1023, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf1024) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/1024, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf1025) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/1025, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// Around 2048 (k=8 candidate, W=2048 — the coarsest finest-window).
TEST(WindowFrameEncoderTest, BoundaryDf2047) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/2047, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf2048) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/2048, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, BoundaryDf2049) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/2049, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// Doc-id-only (omit_term_freq_and_positions) variants at the boundaries: the
// .frq carries only the doc-delta part (no freq region), so the part-wise
// composition for the doc-delta-only window must still be one continuous PFOR
// run across units. No .prx block is produced at all.
TEST(WindowFrameEncoderTest, OmitTfapDf257) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/257, /*stride=*/3, /*freq=*/1), /*has_prox=*/false);
}
TEST(WindowFrameEncoderTest, OmitTfapDf1025) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/1025, /*stride=*/2, /*freq=*/1), /*has_prox=*/false);
}
TEST(WindowFrameEncoderTest, OmitTfapDf2049) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/2049, /*stride=*/1, /*freq=*/1), /*has_prox=*/false);
}

// Very large df exercising the W=2048 multi-window framing (many windows, each
// covering up to 8 finest units). last_doc must thread across every window and
// the skip table must partition all 20000 docs.
TEST(WindowFrameEncoderTest, VeryLargeDfMultiWindow2048) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/20000, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, VeryLargeDfMultiWindow2048OmitTfap) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/20000, /*stride=*/2, /*freq=*/1), /*has_prox=*/false);
}

// Highly-compressible term: every doc-delta == 1 (dense, consecutive docs) and
// a single uniform freq. The adaptive search should find the whole-term framing
// cheapest (or a tied finer one within budget); ZSTD on the near-constant inner
// bytes makes the payload tiny. Roundtrip must still be byte-exact and the skip
// table sound regardless of which W wins.
TEST(WindowFrameEncoderTest, HighlyCompressibleUniform) {
    Term t;
    for (int32_t i = 0; i < 5000; ++i) {
        t.docs.push_back(i + 1); // delta == 1 throughout
        t.freqs.push_back(1);    // uniform freq
        t.positions.push_back({0});
    }
    ExpectRoundtripAndSkip(t, /*has_prox=*/true);
}

// Same dense/uniform shape but doc-id-only: confirms the compressible whole-term
// fallback also holds when there is no freq region.
TEST(WindowFrameEncoderTest, HighlyCompressibleUniformOmitTfap) {
    Term t;
    for (int32_t i = 0; i < 5000; ++i) {
        t.docs.push_back(i + 1);
        t.freqs.push_back(1);
        t.positions.push_back({});
    }
    ExpectRoundtripAndSkip(t, /*has_prox=*/false);
}

// df == 1 (and the hang-case df < 256) must complete without spinning and
// produce exactly one window. A test timeout guards the build; this asserting
// completion at all is the no-hang regression for the k=0 clamp.
TEST(WindowFrameEncoderTest, Df1NoHang) {
    ExpectRoundtripAndSkip(MakeTerm(/*df=*/1, /*stride=*/1, /*freq=*/1), /*has_prox=*/true);
}
TEST(WindowFrameEncoderTest, SmallDfNoHang) {
    for (int32_t df : {1, 2, 10, 100, 200, 254}) {
        ExpectRoundtripAndSkip(MakeTerm(df, /*stride=*/2, /*freq=*/1), /*has_prox=*/true);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
