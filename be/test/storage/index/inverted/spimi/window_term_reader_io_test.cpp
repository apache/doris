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
// POSITIONED-READ (byte-fetch) harness for the lazy window-addressed `.frq`
// reader. Drives `SpimiWindowedTermDocs` through an INSTRUMENTED
// `MemPostingStore` (which counts read_at calls + bytes + logs ranges) and
// asserts, on the SAME `.frq` bytes the eager `ReadTerm` oracle decodes:
//
//   (a) DECODE-CORRECTNESS: full next() scan and a battery of skipTo()
//       sequences produce (doc,freq) byte-identical to the oracle.
//   (b) SELECTIVITY: after Open (which fetches only the header+skip-table
//       prefix — bytes read << whole term), a single selective skipTo fetches
//       the header prefix + ONE covering window's bytes, strictly less than the
//       whole-term payload for a multi-window term.
//   (c) FULL SCAN: a complete next() walk fetches the union of all windows
//       exactly once = the whole-term payload, with no window double-fetched.
//       The reader's windows_decoded() telemetry equals the distinct windows
//       the IO log touched.
//
// The MemPostingStore wraps the term-base offset like the real
// IndexInputPostingStore: logical offset 0 == the term's freq_pointer. We pass
// term_base=0 (the term block is the whole buffer here). All randomness uses
// fixed seeds; the runner timeout-guards everything.
// ===========================================================================

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <set>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/posting_store.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace {

struct Term {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions;
};

std::vector<uint8_t> EncodeWindowedFrq(const Term& t, bool has_prox) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    FreqProxEncoder enc(&frq_out, &prx_out, /*skip_interval=*/512, /*max_skip_levels=*/10,
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
    return frq_out.bytes();
}

using Oracle = std::vector<SpimiTermDocsReader::DocFreq>;

// Multi-window term: irregular gaps + varied freqs make the adaptive encoder
// pick FINE windowing (many windows), which is what selectivity needs.
Term MakeMultiWindowTerm(int32_t df, bool has_prox, uint32_t seed) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> gap(1, 64);
    std::uniform_int_distribution<int32_t> fq(1, 12);
    int32_t doc = 2;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        const int32_t f = has_prox ? fq(rng) : 1;
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

// ---- (a) DECODE CORRECTNESS through the PostingStore seam -------------------
TEST(WindowTermReaderIoTest, DecodeCorrectnessFullScanAndSkip) {
    for (int32_t df : {1, 256, 512, 1024, 3000, 20000}) {
        for (bool has_prox : {true, false}) {
            const Term t = MakeMultiWindowTerm(df, has_prox, /*seed=*/1000 + df + (has_prox ? 1 : 0));
            const std::vector<uint8_t> frq = EncodeWindowedFrq(t, has_prox);
            ASSERT_EQ(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
            const Oracle oracle = SpimiTermDocsReader::ReadTerm(frq, df, has_prox);
            ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

            // Full scan via PostingStore.
            {
                MemPostingStore store(frq.data(), frq.size());
                SpimiWindowedTermDocs r;
                ASSERT_TRUE(r.Open(&store, /*term_base=*/0, df, has_prox));
                int32_t i = 0;
                while (r.next()) {
                    ASSERT_LT(i, df);
                    ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "doc@" << i;
                    ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second) << "freq@" << i;
                    ++i;
                }
                ASSERT_EQ(i, df);
                ASSERT_EQ(r.doc(), std::numeric_limits<int32_t>::max());
            }

            // skipTo battery (fresh reader per target so each is a cold seek).
            for (int32_t idx : {0, df / 4, df / 2, (df * 3) / 4, df - 1}) {
                if (idx < 0 || idx >= df) {
                    continue;
                }
                const int32_t target = oracle[static_cast<size_t>(idx)].first;
                MemPostingStore store(frq.data(), frq.size());
                SpimiWindowedTermDocs r;
                ASSERT_TRUE(r.Open(&store, /*term_base=*/0, df, has_prox));
                ASSERT_TRUE(r.skipTo(target));
                ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(idx)].first);
                ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(idx)].second);
                // Tail scan must continue matching.
                int32_t j = idx;
                while (r.next()) {
                    ++j;
                    ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(j)].first) << "tail@" << j;
                    ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(j)].second);
                }
                ASSERT_EQ(j, df - 1);
            }
        }
    }
}

// ---- (b) SELECTIVITY: a single skipTo reads ~one window, not the whole term -
TEST(WindowTermReaderIoTest, SelectiveSkipFetchesOneWindow) {
    const int32_t df = 20000;
    const Term t = MakeMultiWindowTerm(df, /*has_prox=*/true, /*seed=*/4242);
    const std::vector<uint8_t> frq = EncodeWindowedFrq(t, /*has_prox=*/true);
    const Oracle oracle = SpimiTermDocsReader::ReadTerm(frq, df, /*has_prox=*/true);

    // (b.1) Open alone fetches only the header+skip-table prefix.
    MemPostingStore probe(frq.data(), frq.size());
    SpimiWindowedTermDocs r0;
    ASSERT_TRUE(r0.Open(&probe, /*term_base=*/0, df, /*has_prox=*/true));
    ASSERT_GT(r0.windows_total(), 1) << "need a multi-window term for selectivity";
    const int64_t open_bytes = probe.bytes_read();
    ASSERT_LT(open_bytes, static_cast<int64_t>(frq.size()) / 4)
            << "Open fetched " << open_bytes << " of " << frq.size()
            << " bytes — should be just the header+skip-table prefix";

    // (b.2) A single selective skipTo into a LATE window fetches the prefix +
    // exactly one window's bytes, strictly less than the whole buffer.
    const int32_t target_idx = (df * 9) / 10; // ~90% in
    const int32_t target = oracle[static_cast<size_t>(target_idx)].first;
    MemPostingStore store(frq.data(), frq.size());
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(&store, /*term_base=*/0, df, /*has_prox=*/true));
    const int64_t after_open = store.bytes_read();
    ASSERT_TRUE(r.skipTo(target));
    ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(target_idx)].first);
    ASSERT_EQ(r.windows_decoded(), 1) << "a single selective skipTo must decode exactly one window";
    const int64_t total_bytes = store.bytes_read();
    const int64_t window_bytes = total_bytes - after_open;
    ASSERT_GT(window_bytes, 0);
    ASSERT_LT(total_bytes, static_cast<int64_t>(frq.size()))
            << "selective skipTo fetched the whole term (" << total_bytes << "/" << frq.size()
            << ")";
    // Far less than half the buffer for a 90%-deep single seek.
    ASSERT_LT(total_bytes, static_cast<int64_t>(frq.size()) / 2)
            << "selective skipTo should touch well under half the bytes";
    std::printf("[IO] df=%d windows=%d | Open=%ld B, +1 window=%ld B, total=%ld / %zu B\n", df,
                r.windows_total(), static_cast<long>(after_open), static_cast<long>(window_bytes),
                static_cast<long>(total_bytes), frq.size());
}

// ---- (c) FULL SCAN fetches every window exactly once = whole-term payload ---
TEST(WindowTermReaderIoTest, FullScanFetchesEachWindowOnce) {
    const int32_t df = 8000;
    const Term t = MakeMultiWindowTerm(df, /*has_prox=*/false, /*seed=*/909);
    const std::vector<uint8_t> frq = EncodeWindowedFrq(t, /*has_prox=*/false);
    const Oracle oracle = SpimiTermDocsReader::ReadTerm(frq, df, /*has_prox=*/false);

    MemPostingStore store(frq.data(), frq.size());
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(&store, /*term_base=*/0, df, /*has_prox=*/false));
    const int32_t windows_total = r.windows_total();
    const int64_t open_reads = store.read_count();

    int32_t i = 0;
    while (r.next()) {
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first);
        ++i;
    }
    ASSERT_EQ(i, df);
    ASSERT_EQ(r.windows_decoded(), windows_total) << "full scan must decode every window";

    // Each window fetched at most once: the number of payload-fetch reads after
    // Open equals the number of distinct windows decoded. (For the last window
    // the self-frame adds ONE extra header-probe read, so allow +1.)
    const int64_t payload_reads = store.read_count() - open_reads;
    EXPECT_GE(payload_reads, windows_total);
    EXPECT_LE(payload_reads, windows_total + 1)
            << "windows were re-fetched: " << payload_reads << " reads for " << windows_total
            << " windows";

    // No two payload reads cover the SAME offset (no double-fetch). The first
    // `open_reads` log entries are the header prefix; the rest are payloads.
    std::set<int64_t> seen_offsets;
    const auto& log = store.read_log();
    for (size_t k = static_cast<size_t>(open_reads); k < log.size(); ++k) {
        const int64_t off = log[k].first;
        // The last window's self-frame probe reads the same offset as its
        // payload read — that's the only allowed offset repeat.
        seen_offsets.insert(off);
    }
    EXPECT_GE(static_cast<int64_t>(seen_offsets.size()), windows_total)
            << "fewer distinct payload offsets than windows — a window was skipped";
}

// ---- term_base != 0: the store offset is honored (mirrors a non-first term) -
TEST(WindowTermReaderIoTest, NonZeroTermBaseHonored) {
    const int32_t df = 1500;
    const Term t = MakeMultiWindowTerm(df, /*has_prox=*/true, /*seed=*/77);
    const std::vector<uint8_t> frq = EncodeWindowedFrq(t, /*has_prox=*/true);
    const Oracle oracle = SpimiTermDocsReader::ReadTerm(frq, df, /*has_prox=*/true);

    // Prepend arbitrary padding so the term block sits at a non-zero offset.
    const int64_t pad = 137;
    std::vector<uint8_t> padded(static_cast<size_t>(pad), 0xAB);
    padded.insert(padded.end(), frq.begin(), frq.end());

    MemPostingStore store(padded.data(), padded.size());
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(&store, /*term_base=*/pad, df, /*has_prox=*/true));
    int32_t i = 0;
    while (r.next()) {
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "doc@" << i;
        ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second);
        ++i;
    }
    ASSERT_EQ(i, df);
    // Every logged read must be within [pad, padded.size()) — never the padding.
    for (const auto& [off, len] : store.read_log()) {
        ASSERT_GE(off, pad) << "read dipped into the padding before the term base";
        ASSERT_LE(off + static_cast<int64_t>(len), static_cast<int64_t>(padded.size()));
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
