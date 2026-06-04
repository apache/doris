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
// VALIDATOR differential harness (independent of the implementer's own
// window_term_reader_test.cpp). It proves that the LAZY window-addressed
// reader `SpimiWindowedTermDocs` produces results that are BYTE-IDENTICAL to
// materializing the WHOLE term eagerly with `SpimiTermDocsReader::ReadTerm`,
// across:
//
//   * the df matrix: single-window small terms; multi-window terms straddling
//     the 256/512/1024/2048 window boundaries; a very large df=20000 term; and
//     a highly-compressible uniform term (which the adaptive encoder collapses
//     to ONE whole-term window).
//
//   * many access patterns on EACH term: a full next() scan; skipTo to the
//     first / middle / last doc; skipTo past the end; repeated skipTo jumping
//     by 1% / 10% / 50% of df; skipTo to a docid that is NOT present (must land
//     on the next doc >= target); and a skipTo into a window FAR past the start
//     (proving the windows below the target are skipped, not decoded).
//
// Every assertion is differential: the lazy (doc, freq) at the landed index
// must equal oracle[index]. Positions are intentionally NOT part of the lazy
// reader (doc/freq-only this increment); the eager positions path is untouched
// so phrase queries stay byte-identical by construction — we additionally
// re-decode the prox stream eagerly and confirm it still matches the source
// term, to document that positions were not perturbed.
//
// An S3-ACCESS PROXY test prints, for a skipTo to ~1% of a large term's docs,
// how many WINDOWS and BYTES the lazy reader touches vs whole-term
// materialization (the read-amplification the format is designed to cut).
//
// All randomness uses fixed seeds. Everything is timeout-guarded by the runner.
// ===========================================================================

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "storage/index/inverted/spimi/prox_reader.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {
namespace {

struct Term {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions;
};

struct Encoded {
    std::vector<uint8_t> frq;
    std::vector<uint8_t> prx;
};

Encoded EncodeWindowed(const Term& t, bool has_prox) {
    MemoryByteOutput frq_out;
    MemoryByteOutput prx_out;
    // skip_interval=1 forces windowing for every df so this isolation helper
    // covers the full df boundary matrix; production gate (512) is unchanged.
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
    return Encoded {frq_out.bytes(), prx_out.bytes()};
}

using Oracle = std::vector<SpimiTermDocsReader::DocFreq>;

Oracle EagerWholeTerm(const std::vector<uint8_t>& frq, int32_t df, bool has_prox) {
    return SpimiTermDocsReader::ReadTerm(frq, df, has_prox);
}

// ---- Term shapes (fixed-seed) ---------------------------------------------

// Regular stride / constant freq.
Term MakeUniform(int32_t df, int32_t stride, int32_t freq) {
    Term t;
    int32_t doc = 5;
    for (int32_t i = 0; i < df; ++i) {
        t.docs.push_back(doc);
        t.freqs.push_back(freq);
        std::vector<int32_t> pos;
        for (int32_t p = 0; p < freq; ++p) {
            pos.push_back(p);
        }
        t.positions.push_back(std::move(pos));
        doc += stride;
    }
    return t;
}

// Irregular gaps + varied freqs => the adaptive encoder picks FINE windowing.
Term MakeIrregular(int32_t df, bool has_prox, uint32_t seed) {
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

// Doc-delta OUTLIER gaps: long stretches of tiny gaps (1..3 docs) punctuated by
// a handful of huge jumps. Within a 128-doc PFOR block this drives the
// SEEK-CRITICAL doc-delta patch path: the plain base width is forced wide by a
// few big-gap deltas, while the patched form packs the small deltas at a narrow
// base and splits the few large gaps into the trailer. The skipTo battery then
// proves the patched doc-delta blocks decode correctly with NO leading sub-block
// count (the trailer is found at the implicitly-derived block offset).
Term MakeOutlierGaps(int32_t df, bool has_prox, uint32_t seed) {
    Term t;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> small_gap(1, 3);
    std::uniform_int_distribution<int32_t> big_gap(50000, 200000);
    std::uniform_int_distribution<int32_t> fq(1, 4);
    // Place a few outliers per 128-doc block at irregular offsets so each
    // doc-delta PFOR block carries 1..3 huge gaps among ~125 tiny ones.
    int32_t doc = 7;
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
        // ~2 outliers per 128-block: trigger on a couple of fixed in-block offsets.
        const int32_t in_block = i % 128;
        const bool outlier = (in_block == 17 || in_block == 88);
        doc += outlier ? big_gap(rng) : small_gap(rng);
    }
    return t;
}

// ---- Skip-table parser (validator-side, for the S3 byte proxy) ------------
// Mirrors the SLIM format: after [outer mode][inner mode][VInt W]
// [VInt num_windows] comes the per-window skip table of 3 VInts/window —
// {VInt byte_offset_delta, VInt min_docid_delta, VInt max_docid_delta} —
// where byte_offset and min_docid are delta-coded (offset vs the previous
// window's payload size; min_docid vs the previous window's max_docid).
// win_doc_count is NOT stored (derived = min(W, remaining)). We running-sum the
// deltas back to absolute byte_offset (relative to the first payload tuple) and
// min/max_docid; each window's payload byte size is offset[w+1]-offset[w] (last
// window runs to end-of-buffer).
struct SkipTable {
    int32_t num_windows = 0;
    std::vector<int32_t> doc_count;
    std::vector<int32_t> byte_offset; // relative to payload base
    std::vector<int32_t> min_docid;
    std::vector<int32_t> max_docid;
    size_t payload_base = 0;
    size_t total_len = 0;
    // payload byte size of window w.
    int32_t window_bytes(int32_t w) const {
        const size_t end = (w + 1 < num_windows) ? (payload_base + byte_offset[w + 1]) : total_len;
        return static_cast<int32_t>(end - (payload_base + byte_offset[w]));
    }
    int32_t total_payload_bytes() const { return static_cast<int32_t>(total_len - payload_base); }
};

int32_t ReadVIntAt(const std::vector<uint8_t>& b, size_t& pos) {
    uint32_t v = 0;
    int shift = 0;
    while (true) {
        const uint8_t byte = b[pos++];
        v |= static_cast<uint32_t>(byte & 0x7FU) << shift;
        if ((byte & 0x80U) == 0) {
            break;
        }
        shift += 7;
    }
    return static_cast<int32_t>(v);
}

SkipTable ParseSkipTable(const std::vector<uint8_t>& frq) {
    SkipTable st;
    st.total_len = frq.size();
    size_t pos = 0;
    EXPECT_EQ(frq[pos++], FreqProxEncoder::kCodeModeSpimiWindowed);
    ++pos;                                  // inner mode
    const int32_t W = ReadVIntAt(frq, pos); // window doc-width (derives doc_count)
    st.num_windows = ReadVIntAt(frq, pos);
    int32_t byte_offset_abs = 0;
    int32_t prev_max_docid = 0;
    for (int32_t w = 0; w < st.num_windows; ++w) {
        const int32_t byte_offset_delta = ReadVIntAt(frq, pos);
        const int32_t min_docid_delta = ReadVIntAt(frq, pos);
        const int32_t dl = ReadVIntAt(frq, pos);
        byte_offset_abs += byte_offset_delta;
        const int32_t mn = prev_max_docid + min_docid_delta;
        st.doc_count.push_back(W); // derived; provisional W (unused downstream)
        st.byte_offset.push_back(byte_offset_abs);
        st.min_docid.push_back(mn);
        st.max_docid.push_back(mn + dl);
        prev_max_docid = mn + dl;
    }
    st.payload_base = pos;
    return st;
}

// ---- Differential drivers --------------------------------------------------

// Full next() scan: every (doc, freq, index) must equal the oracle, and a full
// scan must decode EVERY window (never more than eager, never fewer).
void DiffFullScan(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df,
                  bool has_prox) {
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    ASSERT_EQ(r.doc(), -1);
    ASSERT_EQ(r.doc_index(), -1);
    int32_t i = 0;
    while (r.next()) {
        ASSERT_LT(i, df) << "lazy produced more docs than oracle";
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "doc @ " << i;
        ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second) << "freq @ " << i;
        ASSERT_EQ(r.doc_index(), i);
        ++i;
    }
    ASSERT_EQ(i, df) << "lazy stopped early";
    ASSERT_EQ(r.doc(), std::numeric_limits<int32_t>::max());
    ASSERT_FALSE(r.next());
    ASSERT_EQ(r.windows_decoded(), r.windows_total()) << "full scan must touch every window";
}

// One fresh skipTo(target): land on the first oracle doc >= target, with
// byte-identical (doc, freq); then a tail scan must match the oracle tail.
void DiffSkipTo(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df, bool has_prox,
                int32_t target) {
    int32_t expect = df;
    for (int32_t i = 0; i < df; ++i) {
        if (oracle[static_cast<size_t>(i)].first >= target) {
            expect = i;
            break;
        }
    }
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    const bool ok = r.skipTo(target);
    if (expect == df) {
        ASSERT_FALSE(ok) << "skipTo(" << target << ") must exhaust";
        ASSERT_EQ(r.doc(), std::numeric_limits<int32_t>::max());
        ASSERT_FALSE(r.next());
        return;
    }
    ASSERT_TRUE(ok) << "skipTo(" << target << ") must land @ " << expect;
    ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(expect)].first);
    ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(expect)].second);
    ASSERT_EQ(r.doc_index(), expect);
    int32_t i = expect;
    while (r.next()) {
        ++i;
        ASSERT_LT(i, df);
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(i)].first) << "tail @ " << i;
        ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(i)].second);
    }
    ASSERT_EQ(i, df - 1);
}

// Repeated monotonic skipTo by a fixed stride (in oracle-index space). Each
// landing must equal the oracle, and the reader must never move backwards.
void DiffRepeatedSkipByStride(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df,
                              bool has_prox, int32_t index_stride) {
    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
    int32_t prev_doc = -1;
    for (int32_t idx = 0; idx < df; idx += index_stride) {
        const int32_t target = oracle[static_cast<size_t>(idx)].first;
        const bool ok = r.skipTo(target);
        ASSERT_TRUE(ok) << "skipTo idx=" << idx;
        // skipTo lands on the first doc >= target; target IS present at idx, so
        // it must land exactly there.
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(idx)].first) << "idx=" << idx;
        ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(idx)].second);
        ASSERT_EQ(r.doc_index(), idx);
        ASSERT_GE(r.doc(), prev_doc) << "moved backwards";
        prev_doc = r.doc();
    }
}

// skipTo to a docid that is NOT present must land on the next doc >= target.
// We target (oracle[idx].doc - 1), which (because docs are strictly ascending
// with gap >= 1) is either absent or equals oracle[idx-1]; either way the
// answer is the first doc >= it.
void DiffSkipToAbsent(const std::vector<uint8_t>& frq, const Oracle& oracle, int32_t df,
                      bool has_prox) {
    for (int32_t idx : {df / 4, df / 2, (df * 3) / 4, df - 1}) {
        if (idx <= 0) {
            continue;
        }
        const int32_t target = oracle[static_cast<size_t>(idx)].first - 1;
        int32_t expect = df;
        for (int32_t i = 0; i < df; ++i) {
            if (oracle[static_cast<size_t>(i)].first >= target) {
                expect = i;
                break;
            }
        }
        SpimiWindowedTermDocs r;
        ASSERT_TRUE(r.Open(frq.data(), frq.size(), df, has_prox));
        ASSERT_TRUE(r.skipTo(target));
        ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(expect)].first)
                << "absent target " << target << " must land on next >=";
        ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(expect)].second);
    }
}

// Run the FULL access-pattern battery against one term.
void RunBattery(const Term& t, bool has_prox) {
    const auto df = static_cast<int32_t>(t.docs.size());
    const Encoded enc = EncodeWindowed(t, has_prox);
    ASSERT_FALSE(enc.frq.empty());
    ASSERT_EQ(enc.frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    const Oracle oracle = EagerWholeTerm(enc.frq, df, has_prox);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    // 1. Full next() scan.
    DiffFullScan(enc.frq, oracle, df, has_prox);

    // 2. skipTo first / middle / last.
    DiffSkipTo(enc.frq, oracle, df, has_prox, oracle.front().first);
    DiffSkipTo(enc.frq, oracle, df, has_prox, oracle[static_cast<size_t>(df / 2)].first);
    DiffSkipTo(enc.frq, oracle, df, has_prox, oracle.back().first);

    // 3. skipTo past end.
    DiffSkipTo(enc.frq, oracle, df, has_prox, oracle.back().first + 1);
    DiffSkipTo(enc.frq, oracle, df, has_prox, std::numeric_limits<int32_t>::max());

    // 4. skipTo FAR past the start (into a late window) — proves windows below
    //    the target are skipped, not decoded. Assert decode-work <= 1 covering
    //    window for a single fresh skipTo into the last 1%.
    if (df >= 100) {
        const int32_t late = oracle[static_cast<size_t>(df - df / 100)].first;
        SpimiWindowedTermDocs r;
        ASSERT_TRUE(r.Open(enc.frq.data(), enc.frq.size(), df, has_prox));
        ASSERT_TRUE(r.skipTo(late));
        ASSERT_EQ(r.doc(), [&] {
            for (int32_t i = 0; i < df; ++i) {
                if (oracle[static_cast<size_t>(i)].first >= late) {
                    return oracle[static_cast<size_t>(i)].first;
                }
            }
            return -1;
        }());
        ASSERT_LE(r.windows_decoded(), 1)
                << "a single fresh skipTo into a late window decoded > 1 window";
    }

    // 5. Repeated skipTo jumping by 1% / 10% / 50% of df (in index space).
    for (int32_t pct : {1, 10, 50}) {
        const int32_t stride = std::max(1, (df * pct) / 100);
        DiffRepeatedSkipByStride(enc.frq, oracle, df, has_prox, stride);
    }

    // 6. skipTo to absent docids -> next >=.
    DiffSkipToAbsent(enc.frq, oracle, df, has_prox);
}

} // namespace

// ===========================================================================
// df matrix x access patterns.
// ===========================================================================
TEST(WindowTermReaderDiffTest, BoundaryMatrixHasProx) {
    for (int32_t df :
         {1, 2, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4096}) {
        RunBattery(MakeUniform(df, /*stride=*/2, /*freq=*/3), /*has_prox=*/true);
    }
}

TEST(WindowTermReaderDiffTest, BoundaryMatrixOmitTfap) {
    for (int32_t df : {1, 256, 512, 1024, 2048, 4096}) {
        RunBattery(MakeUniform(df, /*stride=*/3, /*freq=*/1), /*has_prox=*/false);
    }
}

TEST(WindowTermReaderDiffTest, MultiWindowIrregularHasProx) {
    RunBattery(MakeIrregular(/*df=*/3000, /*has_prox=*/true, /*seed=*/101), /*has_prox=*/true);
}

TEST(WindowTermReaderDiffTest, MultiWindowIrregularOmitTfap) {
    RunBattery(MakeIrregular(/*df=*/3000, /*has_prox=*/false, /*seed=*/202), /*has_prox=*/false);
}

// SEEK-CRITICAL doc-delta patch path. A term whose doc-deltas are mostly tiny
// gaps punctuated by a few huge jumps drives the patched-PFOR doc-delta
// encoding (allow_patch=true at window_frame_encoder.cpp EncodePforPart for
// PART_DD). The full skipTo/next battery proves those patched doc-delta blocks
// decode correctly through the lazy window reader — i.e. with NO leading
// sub-block count, the patch trailer is found at the implicitly-derived block
// offset and skipTo binary-search lands byte-identically to the eager oracle.
TEST(WindowTermReaderDiffTest, DocDeltaOutlierPatchSeekHasProx) {
    RunBattery(MakeOutlierGaps(/*df=*/3000, /*has_prox=*/true, /*seed=*/707), /*has_prox=*/true);
}

TEST(WindowTermReaderDiffTest, DocDeltaOutlierPatchSeekOmitTfap) {
    RunBattery(MakeOutlierGaps(/*df=*/3000, /*has_prox=*/false, /*seed=*/808), /*has_prox=*/false);
}

// Proves the doc-delta patch is actually ENGAGED for the outlier-gap term (not a
// vacuous pass of the battery above): a doc-delta PFOR block built from the
// outlier-gap deltas is STRICTLY SMALLER patched than plain, and round-trips.
// This is the byte-cost half of anchor (1) — the battery is the decode half.
TEST(WindowTermReaderDiffTest, DocDeltaOutlierPatchIsSmaller) {
    // Reconstruct one 128-doc-delta block exactly as the window encoder would:
    // the deltas of the first 128 docs of the outlier-gap term.
    const Term t = MakeOutlierGaps(/*df=*/256, /*has_prox=*/false, /*seed=*/909);
    std::vector<uint32_t> deltas;
    deltas.reserve(SpimiPforEncoder::kBlockSize);
    int32_t prev = 0;
    for (size_t i = 0; i < SpimiPforEncoder::kBlockSize; ++i) {
        deltas.push_back(static_cast<uint32_t>(t.docs[i] - prev));
        prev = t.docs[i];
    }
    const auto plain = SpimiPforEncoder::EncodeBlockToBytes(deltas, /*allow_patch=*/false);
    const auto patched = SpimiPforEncoder::EncodeBlockToBytes(deltas, /*allow_patch=*/true);
    ASSERT_GE(patched.size(), 1U);
    EXPECT_NE(patched[0] & 0x80U, 0U) << "doc-delta outlier block must set the 0x80 patch flag";
    EXPECT_EQ(plain[0] & 0x80U, 0U) << "plain doc-delta block must leave 0x80 clear";
    EXPECT_LT(patched.size(), plain.size())
            << "patched doc-delta encoding must be strictly smaller than allow_patch=false";
    // Round-trip the patched form (count supplied out-of-band, as in the run).
    std::vector<uint32_t> back;
    const size_t n = SpimiPforDecoder::DecodeBlockFromBytes(patched, deltas.size(), &back);
    ASSERT_EQ(n, deltas.size());
    EXPECT_EQ(back, deltas);
}

// Very large df=20000.
TEST(WindowTermReaderDiffTest, VeryLargeDf) {
    RunBattery(MakeIrregular(/*df=*/20000, /*has_prox=*/true, /*seed=*/303), /*has_prox=*/true);
    RunBattery(MakeIrregular(/*df=*/20000, /*has_prox=*/false, /*seed=*/404), /*has_prox=*/false);
}

// Highly-compressible uniform term: the adaptive encoder may collapse it to a
// SINGLE whole-term window. Lazy must STILL be byte-identical (full scan +
// skips), and a full scan must decode exactly that one window.
TEST(WindowTermReaderDiffTest, HighlyCompressibleUniform) {
    RunBattery(MakeUniform(/*df=*/20000, /*stride=*/1, /*freq=*/1), /*has_prox=*/false);
    RunBattery(MakeUniform(/*df=*/20000, /*stride=*/1, /*freq=*/2), /*has_prox=*/true);
}

// Positions are NOT lazified this increment; confirm the eager prox stream
// still decodes back to the SOURCE positions (the eager path was untouched), so
// phrase queries remain byte-identical by construction.
TEST(WindowTermReaderDiffTest, PositionsUnperturbedEager) {
    const Term t = MakeIrregular(/*df=*/2000, /*has_prox=*/true, /*seed=*/505);
    const Encoded enc = EncodeWindowed(t, /*has_prox=*/true);
    std::vector<int32_t> freqs_per_doc;
    freqs_per_doc.reserve(t.docs.size());
    for (size_t i = 0; i < t.docs.size(); ++i) {
        freqs_per_doc.push_back(t.freqs[i]);
    }
    const auto positions = SpimiProxReader::ReadPositions(enc.prx, freqs_per_doc);
    ASSERT_EQ(positions.size(), t.positions.size());
    for (size_t i = 0; i < t.positions.size(); ++i) {
        ASSERT_EQ(positions[i], t.positions[i]) << "position list diverged @ doc " << i;
    }
}

// ===========================================================================
// S3-ACCESS PROXY: for a skipTo to ~1% of a large term's docs, count the
// WINDOWS and BYTES the lazy reader touches vs decoding the WHOLE term, and
// PRINT the read-amplification reduction. The lazy reader is correct here
// (its landing == oracle) and demonstrably reads far less.
// ===========================================================================
TEST(WindowTermReaderDiffTest, S3AccessProxyDecodeWork) {
    const int32_t df = 20000;
    const Term t = MakeIrregular(df, /*has_prox=*/true, /*seed=*/606);
    const Encoded enc = EncodeWindowed(t, /*has_prox=*/true);
    ASSERT_EQ(enc.frq[0], FreqProxEncoder::kCodeModeSpimiWindowed);
    const Oracle oracle = EagerWholeTerm(enc.frq, df, /*has_prox=*/true);
    ASSERT_EQ(oracle.size(), static_cast<size_t>(df));

    const SkipTable st = ParseSkipTable(enc.frq);
    ASSERT_GT(st.num_windows, 1) << "need a multi-window term for the proxy to be meaningful";

    // Whole-term materialization touches EVERY window and EVERY payload byte.
    const int32_t whole_windows = st.num_windows;
    const int32_t whole_bytes = st.total_payload_bytes();

    // Target ~1% into the term.
    const int32_t target_idx = df / 100;
    const int32_t target = oracle[static_cast<size_t>(target_idx)].first;

    SpimiWindowedTermDocs r;
    ASSERT_TRUE(r.Open(enc.frq.data(), enc.frq.size(), df, /*has_prox=*/true));
    ASSERT_TRUE(r.skipTo(target));
    // Correctness: lands on the right doc.
    ASSERT_EQ(r.doc(), oracle[static_cast<size_t>(target_idx)].first);
    ASSERT_EQ(r.freq(), oracle[static_cast<size_t>(target_idx)].second);

    const int32_t lazy_windows = r.windows_decoded();
    ASSERT_EQ(lazy_windows, 1) << "a single selective skipTo must decode exactly one window";

    // Identify the covering window to report its byte size (read-amplification).
    int32_t cover_w = -1;
    for (int32_t w = 0; w < st.num_windows; ++w) {
        if (st.max_docid[w] >= target) {
            cover_w = w;
            break;
        }
    }
    ASSERT_GE(cover_w, 0);
    const int32_t lazy_bytes = st.window_bytes(cover_w);

    const double win_ratio =
            100.0 * static_cast<double>(lazy_windows) / static_cast<double>(whole_windows);
    const double byte_ratio =
            100.0 * static_cast<double>(lazy_bytes) / static_cast<double>(whole_bytes);

    std::printf(
            "\n[S3 PROXY] df=%d, windows_total=%d, payload_bytes_total=%d\n"
            "[S3 PROXY] skipTo ~1%% (target idx=%d, docid=%d) landed @ covering window %d\n"
            "[S3 PROXY]   WINDOWS decoded: lazy=%d  vs  whole-term=%d  (%.2f%%)\n"
            "[S3 PROXY]   BYTES   touched: lazy=%d  vs  whole-term=%d  (%.2f%%)\n"
            "[S3 PROXY]   => lazy avoids %d/%d windows and %d/%d payload bytes for this seek\n\n",
            df, whole_windows, whole_bytes, target_idx, target, cover_w, lazy_windows,
            whole_windows, win_ratio, lazy_bytes, whole_bytes, byte_ratio,
            whole_windows - lazy_windows, whole_windows, whole_bytes - lazy_bytes, whole_bytes);

    // The win must be real: far fewer windows and bytes than whole-term.
    EXPECT_LT(lazy_windows, whole_windows);
    EXPECT_LT(lazy_bytes, whole_bytes);
    EXPECT_LT(byte_ratio, 50.0) << "a 1% seek should touch well under half the payload bytes";

    // Now seek into a LATE window (~90% in) on a fresh reader: the covering
    // window must be well past window 0, proving all earlier windows are
    // genuinely SKIPPED (not decoded) — still exactly one window decoded.
    const int32_t deep_idx = (df * 9) / 10;
    const int32_t deep_target = oracle[static_cast<size_t>(deep_idx)].first;
    int32_t deep_cover = -1;
    for (int32_t w = 0; w < st.num_windows; ++w) {
        if (st.max_docid[w] >= deep_target) {
            deep_cover = w;
            break;
        }
    }
    ASSERT_GT(deep_cover, 0) << "deep seek's covering window should be past window 0";
    SpimiWindowedTermDocs r2;
    ASSERT_TRUE(r2.Open(enc.frq.data(), enc.frq.size(), df, /*has_prox=*/true));
    ASSERT_TRUE(r2.skipTo(deep_target));
    ASSERT_EQ(r2.doc(), oracle[static_cast<size_t>(deep_idx)].first);
    ASSERT_EQ(r2.windows_decoded(), 1)
            << "deep seek decoded " << r2.windows_decoded() << " windows; the " << deep_cover
            << " windows below the target must be skipped, not decoded";
    std::printf("[S3 PROXY] deep seek (~90%%, docid=%d) covered by window %d/%d, decoded=1\n\n",
                deep_target, deep_cover, whole_windows);
}

} // namespace doris::segment_v2::inverted_index::spimi
