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
//
// ===========================================================================
// VALIDATOR differential test for the window-addressed, range-fetched `.prx`
// positions reader (`SpimiWindowedTermPositions`).
//
// Invariant under test (correctness, NON-NEGOTIABLE): for EVERY doc of a term,
// the lazy reader's per-doc position vector is byte-identical to the eager
// `SpimiProxReader::ReadPositions` over the whole-term `.prx` block (the
// committed, trusted oracle).
//
// This file is independent of (and complements) prox_window_reader_diff_test
// and prox_window_reader_io_test. It explicitly nails the matrix the increment
// mandate calls out:
//   - df matrix: single-window, multi-window across the 512 / 1024 / 2048
//     candidate-W boundaries, and a large df=20000 term.
//   - per-doc freq shapes: freq==1 everywhere, high freq, and mixed.
//   - has_prox terms only (positions are meaningful).
//   - a doc whose positions span a window-internal boundary is exercised by
//     the high-freq shape (many positions per doc) at every doc index; the
//     FIRST and LAST doc of every window are checked explicitly.
//   - a `.prx` byte-fetch proxy (instrumented MemPostingStore): reading
//     positions for ~1% of a large term's docs fetches ~one `.prx` window vs
//     the whole `.prx`; the ratio is printed.
//
// Fixed seeds throughout; bounded loops; no sleeps. Oracle is the eager path,
// so any divergence (per-doc slicing, delta rebasing across windows, window
// alignment) fails LOUD.
// ===========================================================================

#include <gtest/gtest.h>

#include <cstdio>
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

// Encodes `t` in V4 windowed mode (use_windowed=true), returning BOTH the
// `.frq` and `.prx` bytes. A single term ⇒ prox_pointer == 0.
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

enum class FreqShape { kUnit, kHigh, kMixed };

// Builds a `df`-doc term with the requested per-doc freq shape and strictly
// ascending positions (delta encoding reset per doc, matching the encoder).
Term MakeTerm(int32_t df, uint32_t seed, FreqShape shape) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> gap(1, 50);
    std::uniform_int_distribution<int32_t> mixed_fq(1, 40);
    std::uniform_int_distribution<int32_t> step(1, 17);
    int32_t doc = 3;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        int32_t f = 0;
        switch (shape) {
        case FreqShape::kUnit:
            f = 1; // exactly one position per doc — minimal slicing
            break;
        case FreqShape::kHigh:
            // Large, varying-but-tall freq ⇒ each doc's positions span many
            // bytes inside its window (window-internal boundary coverage).
            f = 120 + (i % 90);
            break;
        case FreqShape::kMixed:
            f = mixed_fq(rng);
            break;
        }
        t.freqs.push_back(f);
        std::vector<int32_t> pos;
        pos.reserve(static_cast<size_t>(f));
        int32_t pp = 0;
        for (int32_t k = 0; k < f; ++k) {
            pos.push_back(pp);
            pp += step(rng); // strictly ascending
        }
        t.positions.push_back(std::move(pos));
        doc += gap(rng);
    }
    return t;
}

std::vector<std::vector<int32_t>> EagerOracle(const std::vector<uint8_t>& prx, const Term& t) {
    return SpimiProxReader::ReadPositions(prx.data(), prx.size(), t.freqs);
}

// Forward-scan check: walk the `.frq` lazy reader (mirrors a phrase scan) and
// assert lazy `.prx` positions == eager oracle for every doc.
void ExpectForwardScanMatchesEager(const Term& t) {
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    ASSERT_EQ(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);

    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = EagerOracle(prx, t);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, /*prox_pointer=*/0, frq_lazy));
    // `.prx` is framed independently of `.frq` now; window counts may differ.
    ASSERT_GE(prx_lazy.windows_total(), 1);

    int32_t i = 0;
    while (frq_lazy.next()) {
        const auto& got = prx_lazy.PositionsForDoc(frq_lazy.doc_index(), frq_lazy);
        const auto& want = oracle[static_cast<size_t>(i)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at doc_index " << i;
        for (size_t k = 0; k < want.size(); ++k) {
            ASSERT_EQ(got[k], want[k]) << "position mismatch at doc_index " << i << " pos " << k;
        }
        ++i;
    }
    EXPECT_EQ(i, df);
}

// Explicitly probes the FIRST and LAST doc of EVERY window (window-boundary
// docs) plus a deterministic interior sample, out of natural scan order, so
// per-window slicing and the doc-index→window mapping are checked at the seams.
void ExpectWindowBoundaryDocsMatchEager(const Term& t) {
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = EagerOracle(prx, t);

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));

    const int32_t nwin = frq_lazy.windows_total();
    ASSERT_GT(nwin, 0);
    std::vector<int32_t> probe;
    for (int32_t w = 0; w < nwin; ++w) {
        const int32_t start = frq_lazy.window_doc_index_start(w);
        const int32_t count = frq_lazy.window_doc_count(w);
        ASSERT_GT(count, 0);
        probe.push_back(start);             // first doc of window w
        probe.push_back(start + count - 1); // last doc of window w
        if (count > 2) {
            probe.push_back(start + count / 2); // an interior doc
        }
    }
    for (const int32_t p : probe) {
        ASSERT_GE(p, 0);
        ASSERT_LT(p, df);
        const auto& got = prx_lazy.PositionsForDoc(p, frq_lazy);
        const auto& want = oracle[static_cast<size_t>(p)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at boundary doc " << p;
        for (size_t k = 0; k < want.size(); ++k) {
            ASSERT_EQ(got[k], want[k]) << "position mismatch at boundary doc " << p << " pos " << k;
        }
    }
}

} // namespace

// --- df matrix across all freq shapes (single + multi window, the candidate-W
//     boundaries 512/1024/2048 and their +/-1 neighbours). ---
TEST(SpimiProxWindowReaderValidatorTest, DfMatrixForwardScanMatchesEager) {
    const std::vector<int32_t> dfs = {1,   2,    16,   255,  256,  257,  511,  512,
                                      513, 1023, 1024, 1025, 2047, 2048, 2049, 4000};
    for (const FreqShape shape : {FreqShape::kUnit, FreqShape::kHigh, FreqShape::kMixed}) {
        for (const int32_t df : dfs) {
            SCOPED_TRACE("df=" + std::to_string(df) +
                         " shape=" + std::to_string(static_cast<int>(shape)));
            ExpectForwardScanMatchesEager(
                    MakeTerm(df, /*seed=*/100u + static_cast<uint32_t>(df), shape));
        }
    }
}

// --- First/last doc of every window + interior, across the same matrix. The
//     high-freq shape guarantees docs whose positions span many bytes within
//     a window (window-internal boundary). ---
TEST(SpimiProxWindowReaderValidatorTest, WindowBoundaryDocsMatchEager) {
    const std::vector<int32_t> dfs = {256, 512, 1024, 2048, 3000};
    for (const FreqShape shape : {FreqShape::kUnit, FreqShape::kHigh, FreqShape::kMixed}) {
        for (const int32_t df : dfs) {
            SCOPED_TRACE("df=" + std::to_string(df) +
                         " shape=" + std::to_string(static_cast<int>(shape)));
            ExpectWindowBoundaryDocsMatchEager(
                    MakeTerm(df, /*seed=*/500u + static_cast<uint32_t>(df), shape));
        }
    }
}

// --- Large term (df=20000) across all freq shapes: forward scan + boundary
//     docs. Confirms the invariant holds at scale where the term spans many
//     windows. ---
TEST(SpimiProxWindowReaderValidatorTest, LargeTermDf20000MatchesEager) {
    for (const FreqShape shape : {FreqShape::kUnit, FreqShape::kHigh, FreqShape::kMixed}) {
        SCOPED_TRACE("shape=" + std::to_string(static_cast<int>(shape)));
        const Term t = MakeTerm(/*df=*/20000, /*seed=*/777u, shape);
        ExpectForwardScanMatchesEager(t);
        ExpectWindowBoundaryDocsMatchEager(t);
    }
}

// --- Out-of-order random access on a large multi-window term: each requested
//     doc's covering window is decoded independently and matches the oracle. ---
TEST(SpimiProxWindowReaderValidatorTest, RandomAccessMatchesEager) {
    const Term t = MakeTerm(/*df=*/8000, /*seed=*/4242u, FreqShape::kMixed);
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto oracle = EagerOracle(prx, t);

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    ASSERT_GT(frq_lazy.windows_total(), 1);
    MemPostingStore prx_store(prx.data(), prx.size());
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));

    std::mt19937 rng(31337u);
    std::uniform_int_distribution<int32_t> pick(0, df - 1);
    for (int32_t trial = 0; trial < 2000; ++trial) {
        const int32_t p = pick(rng);
        const auto& got = prx_lazy.PositionsForDoc(p, frq_lazy);
        const auto& want = oracle[static_cast<size_t>(p)];
        ASSERT_EQ(got.size(), want.size()) << "freq mismatch at doc " << p;
        for (size_t k = 0; k < want.size(); ++k) {
            ASSERT_EQ(got[k], want[k]) << "position mismatch at doc " << p << " pos " << k;
        }
    }
}

// --- BYTE-FETCH PROXY: reading positions for ~1% of a large term's docs must
//     fetch ~one `.prx` window worth of bytes — a small fraction of the whole
//     `.prx` — while a full sweep fetches the whole block. Prints the ratio. ---
TEST(SpimiProxWindowReaderValidatorTest, OnePercentAccessFetchesAboutOneWindow) {
    const Term t = MakeTerm(/*df=*/20000, /*seed=*/9001u, FreqShape::kMixed);
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);
    const auto df = static_cast<int32_t>(t.docs.size());

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    const int32_t nwin = frq_lazy.windows_total();
    ASSERT_GT(nwin, 4) << "need a many-window term to make the 1% proxy meaningful";

    MemPostingStore prx_store(prx.data(), prx.size());

    // Open must only probe headers, never inflate a payload.
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));
    const int64_t open_bytes = prx_store.bytes_read();
    EXPECT_LT(open_bytes, static_cast<int64_t>(prx.size())) << "Open must not slurp the whole .prx";
    EXPECT_EQ(prx_lazy.windows_inflated(), 0) << "Open inflates no window";

    // Read positions for ~1% of docs, all clustered inside ONE window (the
    // window covering the 1%-into-the-term region) — this models a selective
    // phrase candidate set landing within a window.
    prx_store.reset_counters();
    const int32_t target = df / 100; // ~1% in
    const int32_t w = frq_lazy.WindowIndexForDoc(target);
    const int32_t wstart = frq_lazy.window_doc_index_start(w);
    const int32_t wcount = frq_lazy.window_doc_count(w);
    const int32_t sample = std::min<int32_t>(df / 100 + 1, wcount); // ~1% of df, capped to window
    for (int32_t j = 0; j < sample; ++j) {
        (void)prx_lazy.PositionsForDoc(wstart + j, frq_lazy);
    }
    const int64_t one_window_bytes = prx_store.bytes_read();
    // `.prx` is framed independently of `.frq` now (coarser, config-sized windows),
    // so a sample drawn from one `.frq` window may straddle a `.prx` boundary —
    // a tight cluster still touches only a window or two.
    EXPECT_GE(prx_lazy.windows_inflated(), 1);
    EXPECT_LE(prx_lazy.windows_inflated(), 2) << "1% clustered access inflates few .prx windows";
    EXPECT_GT(one_window_bytes, 0);
    EXPECT_LT(one_window_bytes, static_cast<int64_t>(prx.size()))
            << "1% access must fetch far less than the whole .prx";
    // All reads stay within file bounds.
    for (const auto& [off, len] : prx_store.read_log()) {
        EXPECT_GE(off, 0);
        EXPECT_LE(off + static_cast<int64_t>(len), static_cast<int64_t>(prx.size()));
    }

    // Full sweep fetches the whole block (every window inflated once).
    prx_store.reset_counters();
    SpimiWindowedTermPositions prx_lazy_all;
    ASSERT_TRUE(prx_lazy_all.Open(&prx_store, 0, frq_lazy));
    prx_store.reset_counters();
    for (int32_t p = 0; p < df; ++p) {
        (void)prx_lazy_all.PositionsForDoc(p, frq_lazy);
    }
    const int64_t all_bytes = prx_store.bytes_read();
    EXPECT_EQ(prx_lazy_all.windows_inflated(), prx_lazy_all.windows_total())
            << "full sweep inflates every .prx window once";
    EXPECT_GT(all_bytes, one_window_bytes)
            << "full sweep must fetch strictly more than a single-window 1% access";

    const double ratio = static_cast<double>(one_window_bytes) / static_cast<double>(prx.size());
    std::printf(
            "[BYTE-FETCH PROXY] df=%d windows=%d .prx=%zu bytes | 1%%-in-window access "
            "(%d docs, 1 window) fetched %lld bytes (%.2f%% of .prx) | full sweep fetched "
            "%lld bytes (%d windows)\n",
            df, nwin, prx.size(), sample, static_cast<long long>(one_window_bytes), ratio * 100.0,
            static_cast<long long>(all_bytes), nwin);
    // A single window of a >4-window term is well under half the block.
    EXPECT_LT(ratio, 0.5)
            << "a 1% single-window access should fetch a small fraction of the whole .prx";
}

} // namespace doris::segment_v2::inverted_index::spimi
