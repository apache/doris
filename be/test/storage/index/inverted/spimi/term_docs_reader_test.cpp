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

#include "storage/index/inverted/spimi/term_docs_reader.h"

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// End-to-end fixture: drives `FreqProxEncoder` to write one term's
// `.frq` block into a MemoryByteOutput, then hands the bytes to
// `SpimiTermDocsReader::ReadTerm` and asserts the reconstructed
// (doc_id, freq) pairs.
struct WriteThenReadOneTerm {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    std::unique_ptr<FreqProxEncoder> enc;
    int32_t skip_interval;

    explicit WriteThenReadOneTerm(int32_t skip_interval = 16, bool omit_tfap = false)
            : skip_interval(skip_interval) {
        enc = std::make_unique<FreqProxEncoder>(&frq, &prx, skip_interval,
                                                /*max_skip_levels=*/4, omit_tfap);
    }

    std::vector<SpimiTermDocsReader::DocFreq> ReadDocs(
            const std::vector<std::pair<int32_t, int32_t>>& doc_freqs, bool has_prox) {
        const auto doc_freq = static_cast<int32_t>(doc_freqs.size());
        enc->StartTerm(doc_freq);
        for (const auto& [doc_id, freq] : doc_freqs) {
            enc->StartDoc(doc_id, freq);
            for (int32_t p = 0; p < freq; ++p) {
                enc->AddPosition(p); // dummy positions, prox not validated here
            }
            enc->FinishDoc();
        }
        (void)enc->FinishTerm();
        // is_slim mirrors the writer's kDefault dispatch (df < skip_interval):
        // such terms are the SLIM no-codec-byte layout.
        const bool is_slim = doc_freq < skip_interval;
        return SpimiTermDocsReader::ReadTerm(frq.bytes(), doc_freq, has_prox, is_slim);
    }
};

} // namespace

TEST(SpimiTermDocsReaderTest, RoundTripsKDefaultBlock) {
    // df=3 < skip_interval=16 ⇒ encoder picks kDefault. Reader must
    // recover (doc_id, freq) pairs exactly.
    WriteThenReadOneTerm fx;
    const std::vector<std::pair<int32_t, int32_t>> docs {{0, 1}, {5, 3}, {12, 1}};
    const auto got = fx.ReadDocs(docs, /*has_prox=*/true);
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0], (std::pair<int32_t, int32_t> {0, 1}));
    EXPECT_EQ(got[1], (std::pair<int32_t, int32_t> {5, 3}));
    EXPECT_EQ(got[2], (std::pair<int32_t, int32_t> {12, 1}));
}

TEST(SpimiTermDocsReaderTest, RoundTripsKDefaultBlockOmitTfap) {
    // omit_tfap mode: freq is always 1 from the reader's perspective.
    WriteThenReadOneTerm fx(/*skip_interval=*/16, /*omit_tfap=*/true);
    const std::vector<std::pair<int32_t, int32_t>> docs {{1, 1}, {4, 1}, {9, 1}};
    const auto got = fx.ReadDocs(docs, /*has_prox=*/false);
    ASSERT_EQ(got.size(), 3U);
    EXPECT_EQ(got[0], (std::pair<int32_t, int32_t> {1, 1}));
    EXPECT_EQ(got[1], (std::pair<int32_t, int32_t> {4, 1}));
    EXPECT_EQ(got[2], (std::pair<int32_t, int32_t> {9, 1}));
}

TEST(SpimiTermDocsReaderTest, RoundTripsKSpimiPforBlockAtBoundary) {
    // df == skip_interval ⇒ encoder switches to PFOR (per Phase 35's
    // `_use_pfor = (df >= skip_interval)` rule). The PFOR block flushes
    // 16 buffered (doc_delta, freq) pairs as one sub-block ≤ kBlockSize.
    WriteThenReadOneTerm fx(/*skip_interval=*/16);
    std::vector<std::pair<int32_t, int32_t>> docs;
    for (int i = 0; i < 16; ++i) {
        docs.emplace_back(i * 2, 1 + (i % 3)); // mixed freqs
    }
    const auto got = fx.ReadDocs(docs, /*has_prox=*/true);
    ASSERT_EQ(got.size(), 16U);
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i], docs[i]) << "PFOR mismatch at i=" << i;
    }
}

TEST(SpimiTermDocsReaderTest, RoundTripsLargePforTermAcrossSubBlocks) {
    // df = 300 ⇒ PFOR mode, 3 sub-blocks (128+128+44 docs). Verifies
    // multi-sub-block consumption logic in `DecodePforRun`.
    WriteThenReadOneTerm fx(/*skip_interval=*/16);
    std::vector<std::pair<int32_t, int32_t>> docs;
    std::mt19937 rng(0x7300U);
    for (int i = 0; i < 300; ++i) {
        const int32_t delta = 1 + (rng() % 8);
        const int32_t prev = docs.empty() ? -1 : docs.back().first;
        docs.emplace_back(prev + delta, 1);
    }
    const auto got = fx.ReadDocs(docs, /*has_prox=*/true);
    ASSERT_EQ(got.size(), docs.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i], docs[i]) << "i=" << i;
    }
}

TEST(SpimiTermDocsReaderTest, RoundTripsPforOmitTfap) {
    // PFOR mode + omit_tfap: only doc_deltas are PFOR-encoded; no
    // freqs in the .frq stream. Reader must return freq=1 for every
    // doc and not attempt to consume a non-existent freq run.
    WriteThenReadOneTerm fx(/*skip_interval=*/16, /*omit_tfap=*/true);
    std::vector<std::pair<int32_t, int32_t>> docs;
    for (int i = 0; i < 50; ++i) {
        docs.emplace_back(i * 3, 1);
    }
    const auto got = fx.ReadDocs(docs, /*has_prox=*/false);
    ASSERT_EQ(got.size(), 50U);
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].first, docs[i].first) << "i=" << i;
        EXPECT_EQ(got[i].second, 1) << "omit_tfap: freq always 1";
    }
}

TEST(SpimiTermDocsReaderTest, RandomizedRoundTripStress) {
    // 50 random terms with random df, mode, and shape. Each term
    // round-trips through the writer + reader.
    std::mt19937 rng(0xBADC0DEU);
    for (int trial = 0; trial < 50; ++trial) {
        const int32_t skip_interval = 4 + (rng() % 28); // 4..31
        const bool omit_tfap = (rng() % 2) != 0;
        const int32_t df = 1 + (rng() % 200); // 1..200, often crosses skip_interval
        WriteThenReadOneTerm fx(skip_interval, omit_tfap);
        std::vector<std::pair<int32_t, int32_t>> docs;
        int32_t cur_doc = -1;
        for (int32_t i = 0; i < df; ++i) {
            cur_doc += 1 + static_cast<int32_t>(rng() % 16);
            const int32_t freq = omit_tfap ? 1 : (1 + static_cast<int32_t>(rng() % 5));
            docs.emplace_back(cur_doc, freq);
        }
        const auto got = fx.ReadDocs(docs, /*has_prox=*/!omit_tfap);
        ASSERT_EQ(got.size(), docs.size()) << " trial=" << trial;
        for (size_t i = 0; i < got.size(); ++i) {
            EXPECT_EQ(got[i].first, docs[i].first)
                    << "trial=" << trial << " i=" << i << " skip=" << skip_interval
                    << " omit_tfap=" << omit_tfap;
            EXPECT_EQ(got[i].second, omit_tfap ? 1 : docs[i].second)
                    << "trial=" << trial << " i=" << i;
        }
    }
}

// === SLIM kDefault .frq format (df < skip_interval) ========================
// Proves the in-place format change: a df=1 kDefault term's external .frq block
// is now just the per-doc VInt(s) — NO leading codec byte and NO VInt(doc_count)
// — so the block is 1-3 bytes (the docid VInt) instead of the old 5. A term just
// below the skip_interval is still slim and round-trips; a term at/above the
// skip_interval takes the (unchanged) PFOR path with its codec byte preserved.

TEST(SpimiTermDocsReaderTest, SlimDf1BlockHasNoCodecByteOrDocCount) {
    // df=1, has_prox: the block is exactly the single doc-delta VInt. For doc 5,
    // freq 1 ⇒ (5 << 1) | 1 = 11 (a single VInt byte). The OLD format prepended
    // a codec byte (0x00) + a VInt(doc_count=1), making it 3 bytes; the SLIM
    // format drops both.
    WriteThenReadOneTerm fx(/*skip_interval=*/16);
    const auto got = fx.ReadDocs({{5, 1}}, /*has_prox=*/true);
    ASSERT_EQ(got.size(), 1U);
    EXPECT_EQ(got[0], (std::pair<int32_t, int32_t> {5, 1}));

    // Inspect the raw .frq bytes: exactly one byte, equal to the doc-delta VInt.
    ASSERT_EQ(fx.frq.bytes().size(), 1U) << "SLIM df=1 block is a single doc-delta VInt";
    EXPECT_EQ(fx.frq.bytes()[0], 11U) << "(5 << 1) | 1; no codec byte, no doc count";
}

TEST(SpimiTermDocsReaderTest, SlimBlockJustBelowSkipIntervalRoundTrips) {
    // df = skip_interval - 1 is the largest df that still takes the SLIM kDefault
    // path. It must round-trip exactly, and its block must NOT begin with any of
    // the codec-mode bytes (its first byte is a doc-delta VInt).
    constexpr int32_t kSkip = 16;
    WriteThenReadOneTerm fx(kSkip);
    std::vector<std::pair<int32_t, int32_t>> docs;
    for (int32_t i = 0; i < kSkip - 1; ++i) {
        docs.emplace_back(i * 3, 1 + (i % 2));
    }
    const auto got = fx.ReadDocs(docs, /*has_prox=*/true);
    ASSERT_EQ(got.size(), docs.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i], docs[i]) << "slim mismatch at i=" << i;
    }
    // First doc: delta 0, freq 1 ⇒ (0 << 1) | 1 = 1. Definitely not a PFOR
    // (0x05) / windowed (0x06) / ZSTD (0x80) codec byte.
    ASSERT_FALSE(fx.frq.bytes().empty());
    EXPECT_EQ(fx.frq.bytes()[0], 1U);
    EXPECT_NE(fx.frq.bytes()[0], FreqProxEncoder::kCodeModeSpimiPfor);
    EXPECT_NE(fx.frq.bytes()[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    EXPECT_NE(fx.frq.bytes()[0], FreqProxEncoder::kCodeModeZstd);
}

TEST(SpimiTermDocsReaderTest, AtSkipIntervalKeepsPforCodecByte) {
    // df == skip_interval takes the (UNCHANGED) PFOR path: its .frq block still
    // begins with the kCodeModeSpimiPfor codec byte. The is_slim hint is false
    // for such a term (df >= skip_interval), so ReadDocs reads via the
    // codec-byte dispatch and round-trips exactly.
    constexpr int32_t kSkip = 16;
    WriteThenReadOneTerm fx(kSkip);
    std::vector<std::pair<int32_t, int32_t>> docs;
    for (int32_t i = 0; i < kSkip; ++i) {
        docs.emplace_back(i * 2, 1 + (i % 3));
    }
    const auto got = fx.ReadDocs(docs, /*has_prox=*/true);
    ASSERT_EQ(got.size(), docs.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i], docs[i]) << "PFOR mismatch at i=" << i;
    }
    ASSERT_FALSE(fx.frq.bytes().empty());
    EXPECT_EQ(fx.frq.bytes()[0], FreqProxEncoder::kCodeModeSpimiPfor)
            << "df >= skip_interval keeps the codec byte (untouched by the slim change)";
}

} // namespace doris::segment_v2::inverted_index::spimi
