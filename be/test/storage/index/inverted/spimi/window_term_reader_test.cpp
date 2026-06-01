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

// Differential correctness + decode-work telemetry for the LAZY
// window-addressed `.frq` reader (`SpimiWindowedTermDocs`). The oracle is
// the EAGER `SpimiTermDocsReader::ReadTerm`, which materializes the whole
// term; every next()/skipTo() sequence the lazy reader produces must be
// byte-identical to the subsequence the same sequence selects from the
// oracle vector.

#include "storage/index/inverted/spimi/window_term_reader.h"

#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace {

struct Term {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions;
};

// Encodes `t` in V4 windowed mode, returning the `.frq` bytes.
std::vector<uint8_t> EncodeWindowedFrq(const Term& t, bool has_prox) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    // skip_interval=1 forces the per-term windowing gate (df >= skip_interval)
    // to admit EVERY df, so this isolation helper exercises the windowed reader
    // across the full df boundary matrix (1, 256, 512, ...). The PRODUCTION gate
    // (skip_interval=512) is unchanged — that is verified in freq_prox_encoder_test.
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
    return frq_out.bytes();
}

// Encodes `t` in the LEGACY (non-windowed) mode, returning the `.frq` bytes.
std::vector<uint8_t> EncodeLegacyFrq(const Term& t, bool has_prox) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    FreqProxEncoder enc(&frq_out, &prx_out, /*skip_interval=*/512, /*max_skip_levels=*/10,
                        /*omit_term_freq_and_positions=*/!has_prox, /*use_windowed=*/false);
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

Term MakeTerm(int32_t df, int32_t stride, int32_t freq) {
    Term t;
    int32_t doc = 7;
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

// Builds a large term whose data is NOT highly compressible (irregular doc
// gaps + varied freqs), so the adaptive encoder picks a FINE windowing (many
// windows) rather than collapsing to a single whole-term window. This is the
// shape that makes the selective-decode win observable.
Term MakeMultiWindowTerm(int32_t df, bool has_prox, uint32_t seed) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> gap(1, 50);
    std::uniform_int_distribution<int32_t> fq(1, 9);
    int32_t doc = 3;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        const int32_t f = has_prox ? fq(rng) : 1;
        t.freqs.push_back(f);
        std::vector<int32_t> pos;
        int32_t pp = 0;
        for (int32_t k = 0; k < f; ++k) {
            pos.push_back(pp);
            pp += 1 + (k % 5);
        }
        t.positions.push_back(std::move(pos));
        doc += gap(rng);
    }
    return t;
}

using Oracle = std::vector<SpimiTermDocsReader::DocFreq>;

Oracle BuildOracle(const std::vector<uint8_t>& frq, int32_t df, bool has_prox) {
    return SpimiTermDocsReader::ReadTerm(frq, df, has_prox);
}

// --- A full next() scan must reproduce the oracle exactly, decoding EVERY
//     window (lazy must never decode MORE than eager). ---
void ExpectFullScanMatches(const Term& t, bool has_prox) {
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto frq = EncodeWindowedFrq(t, has_prox);
    ASSERT_EQ(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    const Oracle oracle = BuildOracle(frq, df, has_prox);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    EXPECT_EQ(r.doc(), -1) << "pre-start doc() must be -1";
    EXPECT_EQ(r.doc_index(), -1);

    int32_t i = 0;
    while (r.next()) {
        ASSERT_LT(i, df) << "lazy reader produced more docs than the oracle";
        EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "doc mismatch at " << i;
        EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second) << "freq mismatch at " << i;
        EXPECT_EQ(r.doc_index(), i);
        ++i;
    }
    EXPECT_EQ(i, df) << "lazy reader stopped early";
    EXPECT_EQ(r.doc(), std::numeric_limits<int32_t>::max()) << "exhausted doc() must be INT32_MAX";
    EXPECT_FALSE(r.next()) << "next() past end must stay false";
    // A full scan must touch every window — never more, never fewer.
    EXPECT_EQ(r.windows_decoded(), r.windows_total());
}

// --- A fresh-seek skipTo(target) must land on the first oracle doc >= target,
//     and report it byte-identically. Returns windows_decoded for the caller
//     to assert decode-work bounds. ---
int32_t ExpectSkipToMatches(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df,
                            bool has_prox, int32_t target) {
    SpimiWindowedTermDocs r;
    EXPECT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    // Find the oracle answer: first index with doc >= target.
    int32_t expect = df;
    for (int32_t i = 0; i < df; ++i) {
        if (oracle[static_cast<size_t>(i)].first >= target) {
            expect = i;
            break;
        }
    }
    const bool ok = r.skipTo(target);
    if (expect == df) {
        EXPECT_FALSE(ok) << "skipTo(" << target << ") should exhaust";
        EXPECT_EQ(r.doc(), std::numeric_limits<int32_t>::max());
        EXPECT_FALSE(r.next());
    } else {
        EXPECT_TRUE(ok) << "skipTo(" << target << ") should land on index " << expect;
        EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(expect)].first);
        EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(expect)].second);
        EXPECT_EQ(r.doc_index(), expect);
        // Continuing to scan from here must still match the oracle tail.
        int32_t i = expect;
        while (r.next()) {
            ++i;
            EXPECT_LT(i, df);
            if (i >= df) {
                break;
            }
            EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "tail mismatch at " << i;
            EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second);
        }
        EXPECT_EQ(i, df - 1);
    }
    return r.windows_decoded();
}

// Opens fresh, performs a SINGLE skipTo(target) (no tail scan), validates the
// landing doc against the oracle, and returns windows_decoded — the decode-work
// for exactly one selective seek. Separate from ExpectSkipToMatches (which scans
// the whole tail and therefore decodes every following window).
int32_t SingleSkipWindowsDecoded(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df,
                                 bool has_prox, int32_t target) {
    int32_t expect = df;
    for (int32_t i = 0; i < df; ++i) {
        if (oracle[static_cast<size_t>(i)].first >= target) {
            expect = i;
            break;
        }
    }
    SpimiWindowedTermDocs r;
    EXPECT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    const bool ok = r.skipTo(target);
    if (expect == df) {
        EXPECT_FALSE(ok);
        EXPECT_EQ(r.doc(), std::numeric_limits<int32_t>::max());
    } else {
        EXPECT_TRUE(ok);
        EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(expect)].first);
        EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(expect)].second);
        EXPECT_EQ(r.doc_index(), expect);
    }
    return r.windows_decoded();
}

// --- A randomized MONOTONIC skipTo/next sequence must match the oracle. ---
void ExpectRandomMonotonicSequence(const Term& t, bool has_prox, uint32_t seed) {
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto frq = EncodeWindowedFrq(t, has_prox);
    const Oracle oracle = BuildOracle(frq, df, has_prox);

    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));

    std::mt19937 rng(seed);
    const int32_t max_doc = oracle.back().first;
    std::uniform_int_distribution<int32_t> op(0, 2);
    std::uniform_int_distribution<int32_t> tgt(0, max_doc + 5);

    int32_t cursor = -1;     // oracle index of current position (-1 pre-start)
    int32_t last_target = 0; // monotonic skipTo target
    bool exhausted = false;
    for (int step = 0; step < 200 && !exhausted; ++step) {
        if (op(rng) == 0) {
            // next()
            const bool ok = r.next();
            if (cursor + 1 >= df) {
                EXPECT_FALSE(ok);
                exhausted = true;
            } else {
                ASSERT_TRUE(ok);
                ++cursor;
                EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(cursor)].first);
                EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(cursor)].second);
            }
        } else {
            // skipTo(monotonic target)
            int32_t target = tgt(rng);
            if (target < last_target) {
                target = last_target;
            }
            last_target = target;
            // Oracle: first index > cursor (or current if already >= target).
            int32_t expect = df;
            const int32_t from = (cursor < 0) ? 0 : cursor;
            for (int32_t i = from; i < df; ++i) {
                if (i < cursor) {
                    continue;
                }
                // If already positioned on a doc >= target, skipTo is a no-op.
                if (cursor >= 0 && oracle[static_cast<size_t>(cursor)].first >= target) {
                    expect = cursor;
                    break;
                }
                if (oracle[static_cast<size_t>(i)].first >= target && i > cursor) {
                    expect = i;
                    break;
                }
            }
            const bool ok = r.skipTo(target);
            if (expect == df) {
                EXPECT_FALSE(ok) << "step " << step << " skipTo(" << target << ")";
                exhausted = true;
            } else {
                ASSERT_TRUE(ok) << "step " << step << " skipTo(" << target << ")";
                cursor = expect;
                EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(cursor)].first)
                        << "step " << step << " skipTo(" << target << ")";
                EXPECT_EQ(r.freq(), oracle[static_cast<size_t>(cursor)].second);
            }
        }
    }
}

} // namespace

// ===========================================================================
// Full-scan differential across the unit/window boundary df matrix.
// ===========================================================================
TEST(WindowTermReaderTest, FullScanBoundaryMatrixHasProx) {
    for (int32_t df : {1, 100, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049}) {
        ExpectFullScanMatches(MakeTerm(df, /*stride=*/2, /*freq=*/3), /*has_prox=*/true);
    }
}

TEST(WindowTermReaderTest, FullScanBoundaryMatrixOmitTfap) {
    for (int32_t df : {1, 100, 255, 256, 257, 511, 512, 1024, 2049}) {
        ExpectFullScanMatches(MakeTerm(df, /*stride=*/3, /*freq=*/1), /*has_prox=*/false);
    }
}

TEST(WindowTermReaderTest, FullScanVariableFreqs) {
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
        doc += 1 + (i % 5);
    }
    ExpectFullScanMatches(t, /*has_prox=*/true);
}

// ===========================================================================
// skipTo differential + decode-work assertions on a large multi-window term.
// ===========================================================================
TEST(WindowTermReaderTest, SkipToSelectiveDecode) {
    // A large multi-window term. A fresh-seek skipTo to a target must decode
    // exactly ONE window (the covering one), regardless of how many windows the
    // term has — that is the selective-decode win.
    const Term t = MakeMultiWindowTerm(/*df=*/20000, /*has_prox=*/true, /*seed=*/7);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto frq = EncodeWindowedFrq(t, /*has_prox=*/true);
    const Oracle oracle = BuildOracle(frq, df, /*has_prox=*/true);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    // Sanity: more than one window exists, else "decode only one" is trivial.
    {
        SpimiWindowedTermDocs probe;
        ASSERT_TRUE(probe.Open(frq.data(), frq.size(), df, /*has_prox=*/true));
        ASSERT_GT(probe.windows_total(), 1);
    }

    const int32_t first_doc = oracle.front().first;
    const int32_t mid_doc = oracle[static_cast<size_t>(df / 2)].first;
    const int32_t last_doc = oracle.back().first;
    const int32_t late_doc = oracle[static_cast<size_t>(df - df / 100)].first;

    // DECODE-WORK: a SINGLE fresh-seek skipTo decodes exactly ONE covering
    // window (the selective-decode win) — independent of windows_total (~79).
    EXPECT_EQ(SingleSkipWindowsDecoded(frq, oracle, df, true, first_doc), 1);
    EXPECT_EQ(SingleSkipWindowsDecoded(frq, oracle, df, true, late_doc), 1);
    EXPECT_EQ(SingleSkipWindowsDecoded(frq, oracle, df, true, mid_doc), 1);
    // skipTo PAST the last doc → exhausts, decodes ZERO windows (pure skip-table).
    EXPECT_EQ(SingleSkipWindowsDecoded(frq, oracle, df, true, last_doc + 1), 0);
    EXPECT_EQ(SingleSkipWindowsDecoded(frq, oracle, df, true, last_doc), 1);

    // CORRECTNESS: the same skips, with a full oracle-matching tail scan, land
    // on and stream the right docs (scanning the tail decodes more windows,
    // which is expected; correctness — not decode count — is what's asserted).
    (void)ExpectSkipToMatches(frq, oracle, df, true, first_doc);
    (void)ExpectSkipToMatches(frq, oracle, df, true, late_doc);
    (void)ExpectSkipToMatches(frq, oracle, df, true, mid_doc);
    (void)ExpectSkipToMatches(frq, oracle, df, true, last_doc + 1);
    (void)ExpectSkipToMatches(frq, oracle, df, true, last_doc);
}

// k scattered skipTo targets touch at most k distinct windows (decode-work
// proportional to query selectivity, not term size).
TEST(WindowTermReaderTest, ScatteredSkipsBoundedDecode) {
    const Term t = MakeMultiWindowTerm(/*df=*/20000, /*has_prox=*/false, /*seed=*/11);
    const auto df = static_cast<int32_t>(t.docs.size());
    const auto frq = EncodeWindowedFrq(t, /*has_prox=*/false);
    const Oracle oracle = BuildOracle(frq, df, /*has_prox=*/false);

    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, /*has_prox=*/false));
    const int32_t total_windows = r.windows_total();

    // 5 monotonically increasing scattered targets.
    const std::vector<int32_t> idxs = {df / 20, df / 5, df / 2, (df * 3) / 4, df - 3};
    int32_t k = 0;
    for (int32_t idx : idxs) {
        const int32_t target = oracle[static_cast<size_t>(idx)].first;
        ASSERT_TRUE(r.skipTo(target));
        EXPECT_EQ(r.doc(), oracle[static_cast<size_t>(idx)].first);
        ++k;
    }
    EXPECT_LE(r.windows_decoded(), k) << "decoded more windows than targets";
    EXPECT_LT(r.windows_decoded(), total_windows) << "selective skips decoded the whole term";
}

// ===========================================================================
// Randomized monotonic next()/skipTo() sequences — the broad correctness net.
// ===========================================================================
TEST(WindowTermReaderTest, RandomMonotonicSequences) {
    for (uint32_t seed = 1; seed <= 8; ++seed) {
        ExpectRandomMonotonicSequence(MakeTerm(/*df=*/5000, /*stride=*/2, /*freq=*/2),
                                      /*has_prox=*/true, seed);
        ExpectRandomMonotonicSequence(MakeTerm(/*df=*/3000, /*stride=*/1, /*freq=*/1),
                                      /*has_prox=*/false, seed + 100);
        // Multi-window (many-window) terms: cross-window skipTo/next threading is
        // the case the lazy reader is built for, so exercise it heavily.
        ExpectRandomMonotonicSequence(MakeMultiWindowTerm(/*df=*/15000, /*has_prox=*/true, seed),
                                      /*has_prox=*/true, seed + 200);
        ExpectRandomMonotonicSequence(MakeMultiWindowTerm(/*df=*/15000, /*has_prox=*/false, seed),
                                      /*has_prox=*/false, seed + 300);
    }
}

// Irregular doc gaps (varied deltas) exercise the per-window min/max bounds and
// the within-window linear scan.
TEST(WindowTermReaderTest, RandomMonotonicIrregularGaps) {
    Term t;
    int32_t doc = 3;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> gap(1, 9);
    std::uniform_int_distribution<int32_t> fq(1, 5);
    for (int32_t i = 0; i < 4000; ++i) {
        t.docs.push_back(doc);
        const int32_t f = fq(rng);
        t.freqs.push_back(f);
        std::vector<int32_t> pos;
        for (int32_t p = 0; p < f; ++p) {
            pos.push_back(p);
        }
        t.positions.push_back(std::move(pos));
        doc += gap(rng);
    }
    ExpectFullScanMatches(t, /*has_prox=*/true);
    for (uint32_t seed = 1; seed <= 4; ++seed) {
        ExpectRandomMonotonicSequence(t, /*has_prox=*/true, seed);
    }
}

// ===========================================================================
// Fallback: Open must return false for a NON-windowed (.frq legacy) block so
// the caller falls back to the eager path. df < 256 in legacy mode is a
// kCodeModeDefault block (no skip table).
// ===========================================================================
TEST(WindowTermReaderTest, OpenReturnsFalseForLegacyBlock) {
    const Term small = MakeTerm(/*df=*/10, /*stride=*/2, /*freq=*/1);
    const auto frq = EncodeLegacyFrq(small, /*has_prox=*/true);
    ASSERT_NE(frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    SpimiWindowedTermDocs r;
    EXPECT_FALSE(r.Open(frq.data(), frq.size(), /*doc_freq=*/10, /*has_prox=*/true));

    // A large legacy term (PFOR mode) must also be rejected by Open.
    const Term big = MakeTerm(/*df=*/2000, /*stride=*/1, /*freq=*/1);
    const auto frq_big = EncodeLegacyFrq(big, /*has_prox=*/false);
    ASSERT_NE(frq_big[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    SpimiWindowedTermDocs r2;
    EXPECT_FALSE(r2.Open(frq_big.data(), frq_big.size(), /*doc_freq=*/2000, /*has_prox=*/false));
}

// df == 1 windowed term: single window, must complete without spinning and
// round-trip a single (doc, freq).
TEST(WindowTermReaderTest, SingleDocWindowed) {
    ExpectFullScanMatches(MakeTerm(/*df=*/1, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
    const auto frq = EncodeWindowedFrq(MakeTerm(1, 1, 2), /*has_prox=*/true);
    const Oracle oracle = BuildOracle(frq, 1, /*has_prox=*/true);
    // skipTo before/at/after the only doc.
    EXPECT_EQ(ExpectSkipToMatches(frq, oracle, 1, true, 0), 1);
    EXPECT_EQ(ExpectSkipToMatches(frq, oracle, 1, true, oracle[0].first), 1);
    EXPECT_EQ(ExpectSkipToMatches(frq, oracle, 1, true, oracle[0].first + 1), 0);
}

} // namespace doris::segment_v2::inverted_index::spimi
