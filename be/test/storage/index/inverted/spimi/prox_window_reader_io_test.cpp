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

// Byte-fetch proxy for the LAZY `.prx` reader, via MemPostingStore's
// read_count()/bytes_read()/read_log() instrumentation. A selective phrase-like
// access reading positions for docs confined to a FEW windows must fetch only
// those windows' bytes — strictly LESS than the whole `.prx` block — while a
// full sweep fetches ~the whole block (sanity upper bound).

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
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

Term MakeMultiWindowTerm(int32_t df, uint32_t seed) {
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

} // namespace

TEST(SpimiProxWindowReaderIoTest, SelectiveAccessFetchesFewerBytesThanWholeBlock) {
    const Term t = MakeMultiWindowTerm(/*df=*/4000, /*seed=*/7);
    std::vector<uint8_t> frq, prx;
    EncodeWindowed(t, &frq, &prx);
    ASSERT_EQ(prx[0], FreqProxEncoder::kProxWindowed);
    const auto df = static_cast<int32_t>(t.docs.size());

    SpimiWindowedTermDocs frq_lazy;
    ASSERT_TRUE(frq_lazy.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
    ASSERT_GT(frq_lazy.windows_total(), 1) << "need a multi-window term for the IO proxy";

    MemPostingStore prx_store(prx.data(), prx.size());

    // --- Open: only header + per-window payload-header probes (O(num_windows)
    //     small reads), NOT any inflated payload. ---
    SpimiWindowedTermPositions prx_lazy;
    ASSERT_TRUE(prx_lazy.Open(&prx_store, 0, frq_lazy));
    const int64_t open_bytes = prx_store.bytes_read();
    EXPECT_LT(open_bytes, static_cast<int64_t>(prx.size())) << "Open must not slurp the whole .prx";
    // Each window contributes a tiny (<=11 byte) header probe + the header
    // read; a generous bound is num_windows * 32 + header.
    EXPECT_LT(open_bytes, static_cast<int64_t>(frq_lazy.windows_total()) * 32 + 64)
            << "Open should only probe per-window headers";

    // --- Read positions for docs confined to ONE window (the first window's
    //     docs). bytes_read for the payload should be ~one window's span. ---
    prx_store.reset_counters();
    const int32_t w0_count = frq_lazy.window_doc_count(0);
    for (int32_t p = 0; p < w0_count; ++p) {
        (void)prx_lazy.PositionsForDoc(p, frq_lazy);
    }
    const int64_t one_window_bytes = prx_store.bytes_read();
    EXPECT_GT(one_window_bytes, 0);
    EXPECT_LT(one_window_bytes, static_cast<int64_t>(prx.size()))
            << "single-window access must fetch less than the whole .prx";
    EXPECT_EQ(prx_lazy.windows_inflated(), 1) << "exactly one window inflated";

    // All payload reads must fall within window 0's [payload_pos, +payload_len).
    // We can't see PrxWinEntry, but the read offsets must be < the second
    // window's start — bounded above by half the block for a 4000-doc term that
    // splits into many windows.
    for (const auto& [off, len] : prx_store.read_log()) {
        EXPECT_GE(off, 0);
        EXPECT_LE(off + static_cast<int64_t>(len), static_cast<int64_t>(prx.size()));
    }

    // --- Contrast: reading ALL docs fetches ~the whole block. ---
    prx_store.reset_counters();
    SpimiWindowedTermPositions prx_lazy_all;
    ASSERT_TRUE(prx_lazy_all.Open(&prx_store, 0, frq_lazy));
    for (int32_t p = 0; p < df; ++p) {
        (void)prx_lazy_all.PositionsForDoc(p, frq_lazy);
    }
    const int64_t all_bytes = prx_store.bytes_read();
    EXPECT_GT(all_bytes, one_window_bytes) << "full sweep must fetch strictly more than one window";
    EXPECT_EQ(prx_lazy_all.windows_inflated(), prx_lazy_all.windows_total())
            << "full sweep inflates every .prx window exactly once";
}

} // namespace doris::segment_v2::inverted_index::spimi
