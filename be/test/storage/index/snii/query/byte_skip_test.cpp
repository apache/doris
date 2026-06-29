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

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"
#include "snii/format/frq_prelude.h"
#include "snii/io/local_file.h"
#include "snii/io/metered_file_reader.h"
#include "snii/query/bm25_scorer.h"
#include "snii/query/phrase_query.h"
#include "snii/query/scoring_query.h"
#include "snii/query/term_query.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/reader/windowed_posting.h"
#include "snii/stats/snii_stats_provider.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

// PHASE B differential + byte-reduction test (design spec sections 1.4 & 2).
//
// Builds an index (~4000 docs, positions + scoring) with a HIGH-DF term spanning
// MANY .frq windows plus low/mid-df terms and a planted 5-term phrase led by the
// high-df term. Over a MeteredFileReader it asserts:
//   (a) term_query and phrase_query docids equal a brute-force ORACLE (unchanged
//       by the freq-skip optimization);
//   (b) a docid-only term_query on the high-df term requests STRICTLY FEWER .frq
//       bytes than the pre-PhaseB full-window path (sum of per-window frq_len),
//       because the freq region is skipped on the wire;
//   (c) scoring_query STILL reads the FULL windows (freq region present -> its
//       .frq request bytes match the full-window total, strictly above the
//       docid-only path) and returns the correct top-K.
using namespace snii;
using namespace snii::format;
using namespace snii::reader;
using namespace snii::writer;
using snii::query::Bm25Params;
using snii::query::ScoredDoc;
using snii::stats::SniiStatsProvider;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_byte_skip_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

constexpr uint32_t kDocCount = 4000;
constexpr uint32_t kHiGap = 1;      // aa_hi in (almost) every doc -> many windows
constexpr uint32_t kHiCount = 3600; // df of aa_hi (>>256 -> many windows)
// The 5-term phrase is planted only in the FIRST kPhraseSpan occurrences of
// aa_hi, so a low-df lead term concentrates candidates into the first windows.
constexpr uint32_t kPhraseSpan = 120;

// aa_hi appears with VARIED, large frequency so its per-window freq region is
// big in absolute terms -- large enough to span whole cache blocks the docs-only
// path never touches, making the freq-skip visible in remote_bytes (not just in
// the raw wire-byte total). Frequencies cycle through a wide range.
uint32_t HiFreq(uint32_t occ) {
    return 40U + (occ * 13U + 7U) % 200U;
}

struct Corpus {
    uint32_t doc_count = 0;
    std::vector<std::vector<std::string>> docs; // docs[d] = ordered tokens
    std::vector<uint64_t> doc_len;              // per-doc token count (for norms)

    bool phrase_in_doc(uint32_t d, const std::vector<std::string>& phrase) const {
        if (phrase.empty()) {
            return false;
        }
        const auto& toks = docs[d];
        if (toks.size() < phrase.size()) {
            return false;
        }
        for (size_t i = 0; i + phrase.size() <= toks.size(); ++i) {
            bool match = true;
            for (size_t k = 0; k < phrase.size(); ++k) {
                if (toks[i + k] != phrase[k]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    std::vector<uint32_t> term_oracle(const std::string& term) const {
        std::vector<uint32_t> out;
        for (uint32_t d = 0; d < doc_count; ++d) {
            const auto& toks = docs[d];
            if (std::ranges::find(toks, term) != toks.end()) {
                out.push_back(d);
            }
        }
        return out;
    }

    std::vector<uint32_t> phrase_oracle(const std::vector<std::string>& phrase) const {
        std::vector<uint32_t> out;
        for (uint32_t d = 0; d < doc_count; ++d) {
            if (phrase_in_doc(d, phrase)) {
                out.push_back(d);
            }
        }
        return out;
    }
};

Corpus BuildCorpus() {
    Corpus c;
    c.doc_count = kDocCount;
    c.docs.resize(c.doc_count);
    c.doc_len.assign(c.doc_count, 0);
    const std::vector<std::string> vocab = {"alpha", "bravo", "charlie", "delta"};

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& toks = c.docs[d];
        const bool is_hi = (d % kHiGap == 0) && (d / kHiGap < kHiCount);
        if (is_hi) {
            toks.emplace_back("aa_hi"); // high-df, position 0
        }
        const uint32_t occ = d / kHiGap; // aa_hi occurrence ordinal

        if (is_hi && occ < kPhraseSpan) {
            // 5-term phrase led by the high-df term, concentrated in early windows.
            toks.emplace_back("aa_quick");
            toks.emplace_back("aa_brown");
            toks.emplace_back("aa_fox");
            toks.emplace_back("aa_jumps");
        } else if (d % 11 == 0) {
            toks.emplace_back("aa_lazy");
            toks.emplace_back("aa_dog");
        }
        // Mid-df term: every 5th doc (varied freq via repetition for scoring).
        if (d % 5 == 0) {
            toks.emplace_back("aa_mid");
            if (d % 15 == 0) {
                toks.emplace_back("aa_mid");
            }
        }
        for (uint32_t k = 0; k < 2; ++k) {
            toks.push_back(vocab[(d + k) % vocab.size()]);
        }
        if (d % 97 == 0) {
            toks.emplace_back("aa_rare");
        }
        // Filler vocabulary to occupy the lexicographic tail blocks.
        char nm[16];
        std::snprintf(nm, sizeof(nm), "zz_%03u", d % 400);
        toks.emplace_back(nm);
        // Extra aa_hi repetitions AFTER position 0's phrase, giving aa_hi a varied,
        // larger freq so its per-window freq region carries real bytes. These trail
        // the phrase tokens so the consecutive phrase at position 0 is preserved.
        if (is_hi) {
            const uint32_t extra = HiFreq(occ) - 1U; // total freq = HiFreq(occ)
            for (uint32_t k = 0; k < extra; ++k) {
                toks.emplace_back("aa_hi");
            }
        }
        c.doc_len[d] = toks.size();
    }
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        const auto& toks = c.docs[d];
        for (uint32_t pos = 0; pos < toks.size(); ++pos) {
            buf.add_token(toks[pos], d, pos);
        }
    }
    std::vector<TermPostings> terms = buf.finalize_sorted();

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring; // positions (phrase) + norms (scoring)
    in.doc_count = c.doc_count;
    in.terms = std::move(terms);
    in.target_dict_block_bytes = 256;

    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = snii::query::encode_norm(c.doc_len[d]);
    }

    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    ASSERT_TRUE(cw.add_logical_index(in).ok());
    ASSERT_TRUE(cw.finish().ok());
}

// Grouped-block byte totals of a windowed term (design 1.6): docs = dd-block
// length (sum of per-window dd_disk_len); full = dd-block + freq-block lengths
// (what scoring reads). Also returns prelude_len and window count.
struct FrqByteTotals {
    uint64_t full = 0; // dd-block + freq-block on-disk bytes
    uint64_t docs = 0; // dd-block on-disk bytes (the contiguous docs-only run)
    uint64_t prelude_len = 0;
    uint32_t windows = 0;
};

FrqByteTotals MeasureFrqTotals(const LogicalIndexReader& idx, const std::string& term) {
    FrqByteTotals t;
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.enc, DictEntryEnc::kWindowed);
    t.prelude_len = entry.prelude_len;
    FrqPreludeReader prelude;
    EXPECT_TRUE(fetch_windowed_prelude(idx, entry, frq_base, &prelude).ok());
    t.windows = prelude.n_windows();
    t.docs = prelude.dd_block_len();
    t.full = prelude.dd_block_len() + prelude.freq_block_len();
    return t;
}

// Reference BM25 ranking computed directly from the corpus (df/idf/tf + norms).
std::vector<ScoredDoc> ReferenceRanking(const Corpus& c, const std::vector<std::string>& terms,
                                        uint32_t k, const Bm25Params& params) {
    uint64_t sum_ttf = 0;
    for (uint64_t dl : c.doc_len) {
        sum_ttf += dl;
    }
    const double avgdl = static_cast<double>(sum_ttf) / std::max<uint64_t>(1, c.doc_count);

    std::unordered_map<uint32_t, double> scores;
    for (const auto& term : terms) {
        std::map<uint32_t, uint32_t> plist; // docid -> freq
        for (uint32_t d = 0; d < c.doc_count; ++d) {
            uint32_t f = 0;
            for (const auto& t : c.docs[d]) {
                if (t == term) {
                    ++f;
                }
            }
            if (f > 0) {
                plist[d] = f;
            }
        }
        if (plist.empty()) {
            continue;
        }
        const uint64_t df = plist.size();
        const double idf =
                std::log(1.0 + (static_cast<double>(c.doc_count) - df + 0.5) / (df + 0.5));
        for (const auto& [docid, freq] : plist) {
            // Match the index's quantization (encode -> decode) so scores compare exact.
            const double dl = snii::query::decode_norm(snii::query::encode_norm(c.doc_len[docid]));
            const double denom = freq + params.k1 * (1.0 - params.b + params.b * dl / avgdl);
            scores[docid] += idf * (freq * (params.k1 + 1.0)) / denom;
        }
    }

    std::vector<ScoredDoc> all;
    all.reserve(scores.size());
    for (const auto& [docid, s] : scores) {
        all.push_back({docid, s});
    }
    std::ranges::sort(all, [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        return a.docid < b.docid;
    });
    if (all.size() > k) {
        all.resize(k);
    }
    return all;
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiByteSkip, DocidAndPhraseSkipFreqWhileScoringKeepsIt) {
    Corpus c = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    // Fine-grained cache block (< a window's freq region) so the per-window
    // freq-skip shows up in remote_bytes too: each window's freq region spans whole
    // cache blocks that the docs-only path never touches.
    io::MeteredFileReader metered(&local, /*block_size=*/64);

    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    ASSERT_EQ(idx.stats().doc_count, c.doc_count);

    // The high-df term must span many windows for the freq-skip to be meaningful.
    const FrqByteTotals hi = MeasureFrqTotals(idx, "aa_hi");
    ASSERT_GE(hi.windows, 4U) << "high-df term should span multiple windows";
    // PhaseA format invariant: the docs-only prefix is strictly smaller than the
    // full window total (the freq region occupies real bytes).
    ASSERT_LT(hi.docs, hi.full) << "docs-only prefix not smaller: docs=" << hi.docs
                                << " full=" << hi.full;

    // ---- (a) term_query / phrase_query docids == ORACLE (unchanged) -----------
    const std::vector<std::string> terms_to_check = {"aa_hi", "aa_mid", "aa_quick", "aa_lazy",
                                                     "aa_rare"};
    for (const auto& term : terms_to_check) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, term, &got).ok()) << term;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << term;
        EXPECT_EQ(got, c.term_oracle(term)) << "term_query oracle mismatch: " << term;
    }

    const std::vector<std::vector<std::string>> phrases = {
            {"aa_hi", "aa_quick", "aa_brown", "aa_fox", "aa_jumps"}, // 5-term, hi lead
            {"aa_hi", "aa_quick"},                                   // hi + mid
            {"aa_quick", "aa_brown", "aa_fox", "aa_jumps"},          // no high-df term
            {"aa_lazy", "aa_dog"},                                   // scattered phrase
            {"aa_brown", "aa_fox"},                                  // sub-phrase
            {"aa_fox", "aa_brown"},                                  // reversed -> absent
            {"aa_hi", "aa_lazy"},                                    // present terms, absent phrase
    };
    for (const auto& p : phrases) {
        std::string label;
        for (const auto& w : p) {
            label += w + " ";
        }
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::phrase_query(idx, p, &got).ok()) << label;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << label;
        EXPECT_EQ(got, c.phrase_oracle(p)) << "phrase oracle mismatch: [" << label << "]";
    }

    // ---- (b) docid-only term_query on aa_hi skips the freq region -------------
    // Format-level proof: the docs-only prefixes total far fewer bytes than the
    // full windows (the freq region is real and is what the wire-level skip drops).
    const uint64_t full_window_frq = hi.full; // pre-PhaseB .frq byte total
    EXPECT_LT(hi.docs * 100, full_window_frq * 85)
            << "freq-skip format reduction too small: docs=" << hi.docs
            << " full=" << full_window_frq;
    // Both A and B below share the SAME lookup + prelude overhead; the ONLY
    // difference is whether each window fetches its docs-only prefix or the full
    // window. So (B - A) isolates exactly the .frq freq-region bytes the docid-only
    // path skips. A and B are measured live over the metered reader.
    //
    // A = PhaseB docid-only term_query (want_freq=false everywhere).
    metered.reset_metrics();
    std::vector<uint32_t> hi_docs;
    ASSERT_TRUE(query::term_query(idx, "aa_hi", &hi_docs).ok());
    const io::IoMetrics a = metered.metrics();
    ASSERT_EQ(hi_docs, c.term_oracle("aa_hi"));

    // B = the pre-PhaseB full-window baseline: same lookup + prelude, then read
    // every window IN FULL (want_freq=true). This is exactly what term_query used
    // to fetch before the freq-skip landed.
    metered.reset_metrics();
    {
        DictEntry entry;
        uint64_t fb = 0, pb = 0;
        bool found = false;
        ASSERT_TRUE(idx.lookup("aa_hi", &found, &entry, &fb, &pb).ok());
        ASSERT_TRUE(found);
        DecodedPosting full_posting;
        ASSERT_TRUE(read_windowed_posting(idx, entry, fb, pb, /*want_positions=*/false,
                                          /*want_freq=*/true, &full_posting)
                            .ok());
        EXPECT_EQ(full_posting.docids, c.term_oracle("aa_hi"));
        EXPECT_EQ(full_posting.freqs.size(), full_posting.docids.size())
                << "want_freq=true must decode per-doc freqs";
    }
    const io::IoMetrics b = metered.metrics();

    // The docid-only path requests STRICTLY FEWER bytes on the wire (freq skipped).
    EXPECT_LT(a.total_request_bytes, b.total_request_bytes)
            << "docid-only did not skip freq bytes: a=" << a.total_request_bytes
            << " b=" << b.total_request_bytes;
    // The skipped amount equals the full-vs-docs .frq delta (sum(frq_len-frq_docs_len)).
    const uint64_t saved = b.total_request_bytes - a.total_request_bytes;
    EXPECT_EQ(saved, hi.full - hi.docs) << "skipped bytes != freq-region size: saved=" << saved
                                        << " freq_region=" << (hi.full - hi.docs);
    // remote_bytes (cache-block granular) is also strictly below the full-window
    // baseline: the freq region occupies whole cache blocks the docs path never hits.
    EXPECT_LT(a.remote_bytes, b.remote_bytes)
            << "docid-only remote_bytes not below full-window: a=" << a.remote_bytes
            << " b=" << b.remote_bytes;

    // ---- (c) scoring_query STILL reads full windows (freq present) ------------
    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());
    const Bm25Params params; // defaults
    const std::vector<std::string> score_terms = {"aa_hi", "aa_mid", "aa_rare"};
    const uint32_t kTopK = 10;

    std::vector<ScoredDoc> exhaustive, wand;
    ASSERT_TRUE(query::scoring_query_exhaustive(idx, stats, score_terms, kTopK, params, &exhaustive)
                        .ok());
    ASSERT_TRUE(query::scoring_query_wand(idx, stats, score_terms, kTopK, params, &wand).ok());

    const std::vector<ScoredDoc> ref = ReferenceRanking(c, score_terms, kTopK, params);
    ASSERT_EQ(exhaustive.size(), ref.size());
    for (size_t i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(exhaustive[i].docid, ref[i].docid) << "scoring docid mismatch at " << i;
        EXPECT_NEAR(exhaustive[i].score, ref[i].score, 1e-9) << "scoring score mismatch at " << i;
    }
    // WAND must equal the exhaustive ranking exactly (docid + score).
    ASSERT_EQ(wand.size(), exhaustive.size());
    for (size_t i = 0; i < wand.size(); ++i) {
        EXPECT_EQ(wand[i].docid, exhaustive[i].docid) << "wand vs exhaustive at " << i;
        EXPECT_NEAR(wand[i].score, exhaustive[i].score, 1e-9) << "wand vs exhaustive at " << i;
    }

    // Scoring reads the FULL .frq windows for the high-df term (freq region present),
    // so a single-term scoring query requests STRICTLY MORE bytes than the
    // docid-only term_query A (which skipped the freq region). Same lookup + .frq
    // section; scoring additionally pulls the freq region (and norms/prelude), so
    // its request total exceeds the docid-only path by at least the freq region.
    metered.reset_metrics();
    std::vector<ScoredDoc> hi_only;
    ASSERT_TRUE(
            query::scoring_query_exhaustive(idx, stats, {"aa_hi"}, kTopK, params, &hi_only).ok());
    const io::IoMetrics score_io = metered.metrics();
    const uint64_t score_frq_request = score_io.total_request_bytes;
    const uint64_t term_frq_request = a.total_request_bytes;
    EXPECT_GE(score_frq_request, term_frq_request + (hi.full - hi.docs))
            << "scoring did not read the full freq region: scoring=" << score_frq_request
            << " docid=" << term_frq_request << " freq_region=" << (hi.full - hi.docs);
    EXPECT_GT(score_frq_request, term_frq_request)
            << "scoring should read MORE .frq bytes than docid-only: scoring=" << score_frq_request
            << " docid=" << term_frq_request;

    std::remove(path.c_str());
}
