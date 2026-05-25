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

#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/byte_output.h"

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

    explicit WriteThenReadOneTerm(int32_t skip_interval = 16, bool omit_tfap = false) {
        enc = std::make_unique<FreqProxEncoder>(&frq, &prx, skip_interval,
                                                /*max_skip_levels=*/4, omit_tfap);
    }

    std::vector<SpimiTermDocsReader::DocFreq> ReadDocs(
            const std::vector<std::pair<int32_t, int32_t>>& doc_freqs, bool has_prox) {
        enc->StartTerm(static_cast<int32_t>(doc_freqs.size()));
        for (const auto& [doc_id, freq] : doc_freqs) {
            enc->StartDoc(doc_id, freq);
            for (int32_t p = 0; p < freq; ++p) {
                enc->AddPosition(p); // dummy positions, prox not validated here
            }
            enc->FinishDoc();
        }
        (void)enc->FinishTerm();
        return SpimiTermDocsReader::ReadTerm(frq.bytes(), static_cast<int32_t>(doc_freqs.size()),
                                             has_prox);
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

} // namespace doris::segment_v2::inverted_index::spimi
