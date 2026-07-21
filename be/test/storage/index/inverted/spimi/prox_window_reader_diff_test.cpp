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

// Broad differential sweep: lazy `.prx` positions == eager
// SpimiProxReader::ReadPositions for every doc, across a range of term sizes
// and shapes (compressible → ZSTD windows; incompressible → raw windows;
// single + multi window). Complements prox_window_reader_test.cpp by covering
// the ZSTD per-window branch and a wider df spectrum.

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/prox_reader.h"
#include "storage/index/inverted/spimi/prox_window_reader.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace {

struct Term {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions;
};

void EncodeWindowed(const Term& t, std::vector<uint8_t>* frq, std::vector<uint8_t>* prx) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    // skip_interval=1 forces windowing for every df so this isolation helper
    // covers the full df boundary matrix; production gate (512) is unchanged.
    FreqProxEncoder enc(&frq_out, &prx_out, /*skip_interval=*/1, /*max_skip_levels=*/10,
                        /*omit_term_freq_and_positions=*/false, /*use_windowed=*/true);
    enc.StartTerm(static_cast<int32_t>(t.docs.size()));
    for (size_t i = 0; i < t.docs.size(); ++i) {
        enc.StartDoc(t.docs[i], t.freqs[i]);
        for (const int32_t p : t.positions[i]) {
            enc.AddPosition(p);
        }
        enc.FinishDoc();
    }
    (void)enc.FinishTerm();
    *frq = frq_out.bytes();
    *prx = prx_out.bytes();
}

// Highly regular term: identical freq + identical positions per doc → the
// per-window payloads compress well, exercising the ZSTD window branch.
Term MakeCompressibleTerm(int32_t df, int32_t freq) {
    Term t;
    int32_t doc = 1;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        t.freqs.push_back(freq);
        std::vector<int32_t> pos;
        for (int32_t k = 0; k < freq; ++k) {
            pos.push_back(k * 2);
        }
        t.positions.push_back(std::move(pos));
        doc += 1; // dense, regular gaps
    }
    return t;
}

// Irregular term → raw windows.
Term MakeIncompressibleTerm(int32_t df, uint32_t seed) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> gap(1, 97);
    std::uniform_int_distribution<int32_t> fq(1, 12);
    int32_t doc = 5;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        const int32_t f = fq(rng);
        t.freqs.push_back(f);
        std::vector<int32_t> pos;
        int32_t pp = 0;
        std::uniform_int_distribution<int32_t> step(1, 13);
        for (int32_t k = 0; k < f; ++k) {
            pos.push_back(pp);
            pp += step(rng);
        }
        t.positions.push_back(std::move(pos));
        doc += gap(rng);
    }
    return t;
}

void ExpectMatchesEager(const Term& t) {
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = SpimiProxReader::ReadPositions(prx.data(), prx.size(), t.freqs);

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));

    int32_t i = 0;
    while (frq_lazy.next()) {
        const auto& got = prx_lazy.PositionsForDoc(frq_lazy.doc_index(), frq_lazy);
        const auto& want = oracle[static_cast<size_t>(i)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at " << i;
        for (size_t k = 0; k < want.size(); ++k) {
            EXPECT_EQ(got[k], want[k]) << "pos mismatch at doc " << i << " k " << k;
        }
        ++i;
    }
    EXPECT_EQ(i, df);
}

} // namespace

TEST(SpimiProxWindowReaderDiffTest, CompressibleTermsAcrossSizes) {
    for (int32_t df : {300, 600, 1200, 2400, 5000}) {
        SCOPED_TRACE("df=" + std::to_string(df));
        ExpectMatchesEager(MakeCompressibleTerm(df, /*freq=*/6));
    }
}

TEST(SpimiProxWindowReaderDiffTest, IncompressibleTermsAcrossSizes) {
    for (int32_t df : {300, 600, 1200, 2400, 5000}) {
        for (uint32_t seed : {3U, 17U, 41U}) {
            SCOPED_TRACE("df=" + std::to_string(df) + " seed=" + std::to_string(seed));
            ExpectMatchesEager(MakeIncompressibleTerm(df, seed));
        }
    }
}

TEST(SpimiProxWindowReaderDiffTest, BoundaryDocFreqs) {
    // Around the unit (256) and candidate-W (256/512/1024/2048) boundaries.
    for (int32_t df : {1, 2, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049}) {
        SCOPED_TRACE("df=" + std::to_string(df));
        ExpectMatchesEager(MakeIncompressibleTerm(df, /*seed=*/df + 1));
        ExpectMatchesEager(MakeCompressibleTerm(df, /*freq=*/4));
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
