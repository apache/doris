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

// Differential correctness for the LAZY window-addressed `.prx` reader
// (`SpimiWindowedTermPositions`). The oracle is the EAGER
// `SpimiProxReader::ReadPositions` over the whole-term `.prx` block; for every
// doc the lazy reader's per-doc position vector must be byte-identical to the
// oracle's, across raw + ZSTD + multi-window terms and several W.

#include "storage/index/inverted/spimi/prox_window_reader.h"

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/prox_reader.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace {

struct Term {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions;
};

// Encodes `t` in V4 windowed mode, returning BOTH the `.frq` and `.prx` bytes
// (a single term, so prox_pointer == 0).
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

// A term with `df` docs whose positions per doc == freq, irregular gaps/freqs
// so the adaptive encoder picks finer windowing as df grows.
Term MakeTerm(int32_t df, uint32_t seed) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> gap(1, 50);
    std::uniform_int_distribution<int32_t> fq(1, 9);
    int32_t doc = 3;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        const int32_t f = fq(rng);
        t.freqs.push_back(f);
        std::vector<int32_t> pos;
        int32_t pp = 0;
        for (int32_t k = 0; k < f; ++k) {
            pos.push_back(pp);
            pp += 1 + (k % 7);
        }
        t.positions.push_back(std::move(pos));
        doc += gap(rng);
    }
    return t;
}

// Builds the eager oracle: SpimiProxReader::ReadPositions over the whole `.prx`.
std::vector<std::vector<int32_t>> EagerOracle(const std::vector<uint8_t>& prx, const Term& t) {
    return SpimiProxReader::ReadPositions(prx.data(), prx.size(), t.freqs);
}

// Drives the lazy `.prx` reader for EVERY doc and asserts byte-identical
// positions vs the eager oracle. `frq` must be windowed (asserted).
void ExpectAllDocsMatchOracle(const Term& t) {
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    ASSERT_EQ(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);

    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = EagerOracle(prx, t);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    // `.frq` lazy reader provides the per-window doc partition + freqs.
    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));

    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, /*prox_pointer=*/0, frq_lazy));
    EXPECT_EQ(prx_lazy.windows_total(), frq_lazy.windows_total());

    // Walk forward through the `.frq` reader (mirrors a phrase scan) so the
    // covering `.frq` window is warm when positions are requested.
    int32_t i = 0;
    while (frq_lazy.next()) {
        const auto& got = prx_lazy.PositionsForDoc(frq_lazy.doc_index(), frq_lazy);
        const auto& want = oracle[static_cast<size_t>(i)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at doc_index " << i;
        for (size_t k = 0; k < want.size(); ++k) {
            EXPECT_EQ(got[k], want[k]) << "position mismatch at doc_index " << i << " pos " << k;
        }
        ++i;
    }
    EXPECT_EQ(i, df);
}

} // namespace

TEST(SpimiProxWindowReaderTest, SmallSingleWindowMatchesEager) {
    ExpectAllDocsMatchOracle(MakeTerm(/*df=*/40, /*seed=*/1));
}

TEST(SpimiProxWindowReaderTest, MultiWindowMatchesEager) {
    // df well over W ⇒ several windows.
    ExpectAllDocsMatchOracle(MakeTerm(/*df=*/4000, /*seed=*/7));
}

TEST(SpimiProxWindowReaderTest, VariousSizesMatchEager) {
    for (int32_t df : {1, 2, 255, 256, 257, 512, 1024, 2048, 3000}) {
        for (uint32_t seed : {11U, 23U}) {
            SCOPED_TRACE("df=" + std::to_string(df) + " seed=" + std::to_string(seed));
            ExpectAllDocsMatchOracle(MakeTerm(df, seed));
        }
    }
}

TEST(SpimiProxWindowReaderTest, RandomAccessOrderMatchesEager) {
    // Request positions out of doc order: the lazy reader must force the `.frq`
    // covering window decode for each doc's window independently.
    const Term t = MakeTerm(/*df=*/2000, /*seed=*/99);
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = EagerOracle(prx, t);

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));

    std::mt19937 rng(5);
    std::uniform_int_distribution<int32_t> pick(0, df - 1);
    for (int32_t trial = 0; trial < 500; ++trial) {
        const int32_t p = pick(rng);
        const auto& got = prx_lazy.PositionsForDoc(p, frq_lazy);
        const auto& want = oracle[static_cast<size_t>(p)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at doc_index " << p;
        for (size_t k = 0; k < want.size(); ++k) {
            EXPECT_EQ(got[k], want[k]) << "position mismatch at doc_index " << p;
        }
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
