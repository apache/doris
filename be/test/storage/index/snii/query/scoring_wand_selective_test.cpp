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
#include <random>
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
#include "snii/query/scoring_query.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/reader/windowed_posting.h"
#include "snii/stats/snii_stats_provider.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

// Phase C differential test: scoring_query_wand_selective (block-max SELECTIVE
// FETCH) MUST return top-K (docid sequence AND scores within 1e-9) byte-identical
// to scoring_query_exhaustive AND scoring_query_wand AND an in-memory brute-force
// reference -- for MANY random queries, varied k (incl k=1), varied term sets
// (incl a high-df windowed term spanning many .frq windows), AND a corpus crafted
// to force SCORE TIES (uniform doc length). It additionally asserts the selective
// path fetches FEWER .frq windows / bytes than reading all windows for a small-k
// high-df query, WITHOUT ever changing the result.
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
    return "/tmp/snii_wand_sel_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// term -> (docid -> freq); plus per-doc length (token count) for norms.
struct Corpus {
    uint32_t doc_count = 0;
    std::map<std::string, std::map<uint32_t, uint32_t>> postings;
    std::vector<uint64_t> doc_len;
};

// A scoring corpus with a HIGH-DF term ("hi", df=doc_count -> windowed, spans
// many .frq windows), two MID-df windowed terms, and several low-df terms. Doc
// lengths VARY (so length-norm matters and scores spread out for the random
// queries), but every doc that carries "hi" carries exactly freq=1 of it.
Corpus MakeVariedCorpus(uint32_t doc_count) {
    Corpus c;
    c.doc_count = doc_count;
    c.doc_len.assign(doc_count, 0);
    auto add = [&](const std::string& t, uint32_t d, uint32_t f) {
        c.postings[t][d] += f;
        c.doc_len[d] += f;
    };
    // All queryable terms use an "aa_" prefix and a "zz_NNN" filler vocabulary fills
    // the lexicographic tail, so every real term sorts within the SampledTermIndex's
    // candidate range (the index samples per-block first terms; without tail filler
    // a term sorting last can fall outside the sampled range and miss on lookup).
    for (uint32_t d = 0; d < doc_count; ++d) {
        add("aa_hi", d, 1 + (d % 5)); // df=N, windowed, varied freq
        if (d % 2 == 0) {
            add("aa_evenmid", d, 1 + (d % 3));
        }
        if (d % 3 == 0) {
            add("aa_thirdmid", d, 1 + (d % 4));
        }
        if (d % 37 == 0) {
            add("aa_rarea", d, 2);
        }
        if (d % 53 == 0) {
            add("aa_rareb", d, 3);
        }
        if (d % 101 == 0) {
            add("aa_rarec", d, 1);
        }
        add("aa_pad" + std::to_string(d % 13), d, 1 + (d % 7)); // spreads dl widely
        char nm[16];
        std::snprintf(nm, sizeof(nm), "zz_%03u", d % 257); // tail filler vocabulary
        add(nm, d, 1);
    }
    return c;
}

// A TIE corpus: uniform doc length L=8 for every doc (so equal-freq docs of a
// term score identically -> ties at the top-K boundary). "hi"/"evenz" are high-df
// windowed terms with freq=1 everywhere -> massive ties broken only by docid.
Corpus MakeTieCorpus(uint32_t doc_count) {
    Corpus c;
    c.doc_count = doc_count;
    c.doc_len.assign(doc_count, 0);
    auto add = [&](const std::string& t, uint32_t d, uint32_t f) {
        c.postings[t][d] += f;
        c.doc_len[d] += f;
    };
    for (uint32_t d = 0; d < doc_count; ++d) {
        add("aa_hi", d, 1); // df=N, freq=1 everywhere -> ties
        if (d % 2 == 0) {
            add("aa_evenz", d, 1); // another high-df windowed term
        }
        if (d % 41 == 0) {
            add("aa_rarez", d, 1); // low-df, still freq=1
        }
        // unique pad keeps every dl == 8 exactly; named in the "m_" mid range so the
        // queryable "aa_" terms still sort within the sampled candidate range.
        add("mm_u" + std::to_string(d), d, 6);
    }
    return c;
}

// A DECAYING corpus engineered so block-max WAND genuinely SKIPS later windows of
// the high-df term "hi": "hi"'s in-doc freq decreases with docid (early docs have
// the highest tf) and doc length increases with docid (early docs are shortest,
// so least length-penalized). Thus EARLY windows hold the top scorers and fill the
// heap with a high theta, after which LATE windows' block-max (small tf, long dl)
// falls below theta and is provably skippable -- selective never fetches them.
Corpus MakeDecayCorpus(uint32_t doc_count) {
    Corpus c;
    c.doc_count = doc_count;
    c.doc_len.assign(doc_count, 0);
    auto add = [&](const std::string& t, uint32_t d, uint32_t f) {
        c.postings[t][d] += f;
        c.doc_len[d] += f;
    };
    for (uint32_t d = 0; d < doc_count; ++d) {
        // tf decays from ~20 (docid 0) down to 1 (last docids).
        const uint32_t band = d / 256; // one step per window-sized band
        const uint32_t tf = band < 20 ? (20 - band) : 1;
        add("aa_hi", d, tf);
        // Padding grows with docid so later docs are longer (higher length penalty).
        add("mm_pad" + std::to_string(d % 7), d, 1 + band);
    }
    return c;
}

SniiIndexInput ToInput(const Corpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring;
    in.doc_count = c.doc_count;
    in.target_dict_block_bytes = 256;
    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = snii::query::encode_norm(c.doc_len[d]);
    }
    for (const auto& [term, plist] : c.postings) {
        TermPostings tp;
        tp.term = term;
        for (const auto& [docid, freq] : plist) {
            tp.docids.push_back(docid);
            tp.freqs.push_back(freq);
            for (uint32_t k = 0; k < freq; ++k) {
                tp.positions_flat.push_back(k); // flat
            }
        }
        in.terms.push_back(std::move(tp));
    }
    return in;
}

std::vector<uint8_t> EncodeNorms(const Corpus& c) {
    std::vector<uint8_t> v(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        v[d] = snii::query::encode_norm(c.doc_len[d]);
    }
    return v;
}

// Independent brute-force BM25 ranking straight from the corpus.
std::vector<ScoredDoc> ReferenceRanking(const Corpus& c, const std::vector<uint8_t>& norms,
                                        const std::vector<std::string>& terms, uint32_t k,
                                        const Bm25Params& params) {
    uint64_t sum_ttf = 0;
    for (const auto& dl : c.doc_len) {
        sum_ttf += dl;
    }
    const double avgdl = static_cast<double>(sum_ttf) / std::max<uint64_t>(1, c.doc_count);
    std::unordered_map<uint32_t, double> scores;
    for (const auto& term : terms) {
        auto it = c.postings.find(term);
        if (it == c.postings.end()) {
            continue;
        }
        const uint64_t df = it->second.size();
        const double idf =
                std::log(1.0 + (static_cast<double>(c.doc_count) - df + 0.5) / (df + 0.5));
        for (const auto& [docid, freq] : it->second) {
            const double dl = snii::query::decode_norm(norms[docid]);
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

// Opens a built index file over a metered reader.
struct OpenedIndex {
    io::LocalFileReader inner;
    io::MeteredFileReader metered;
    SniiSegmentReader seg;
    LogicalIndexReader idx;
    SniiStatsProvider stats;
    explicit OpenedIndex(const std::string& path, size_t block_size) : metered(&inner, block_size) {
        EXPECT_TRUE(inner.open(path).ok());
    }
};

void BuildAndOpen(const Corpus& c, const std::string& path) {
    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    ASSERT_TRUE(cw.add_logical_index(ToInput(c)).ok());
    ASSERT_TRUE(cw.finish().ok());
}

// Counts the .frq window count of a windowed term (its full posting window span).
uint32_t WindowCount(const LogicalIndexReader& idx, const std::string& term) {
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    if (!found || entry.enc != DictEntryEnc::kWindowed) {
        return 0;
    }
    FrqPreludeReader prelude;
    EXPECT_TRUE(fetch_windowed_prelude(idx, entry, frq_base, &prelude).ok());
    return prelude.n_windows();
}

// Asserts two top-K vectors are BYTE-IDENTICAL: same length, same docid sequence,
// scores equal within tol. This is the hard soundness gate used for selective vs
// the eager WAND (both implement the documented (score desc, docid asc) order and
// the >= theta tie rule, so they must agree exactly, ties included).
void ExpectIdentical(const std::vector<ScoredDoc>& a, const std::vector<ScoredDoc>& b,
                     const std::string& label, double tol) {
    ASSERT_EQ(a.size(), b.size()) << label << " size";
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].docid, b[i].docid) << label << " docid rank " << i;
        EXPECT_NEAR(a[i].score, b[i].score, tol) << label << " score rank " << i;
    }
}

// Asserts a is a VALID top-K relative to oracle b, tolerant ONLY of the floating-
// point ULP tie-order ambiguity inherent to BM25 (two docs whose summed scores
// differ by < tol may legitimately swap order across summation orders; the
// exhaustive path sums per doc in unordered_map order, the WAND path in cursor
// order). It groups each vector into maximal tie bands (consecutive scores within
// tol) and requires:
//   - identical length and position-wise scores within tol (any real score /
//     ranking error shows here -- positions only slide WITHIN an equal band),
//   - the docid MULTISET of each tie band is identical (catches wrong / dropped
//     docids, including at the top-K boundary when the boundary is not in a band),
//   - a's docids are ascending within each band (a is the canonical winner).
void ExpectValidTopK(const std::vector<ScoredDoc>& a, const std::vector<ScoredDoc>& b,
                     const std::string& label, double tol) {
    ASSERT_EQ(a.size(), b.size()) << label << " size";
    size_t i = 0;
    while (i < a.size()) {
        size_t j = i + 1;
        while (j < a.size() && std::abs(a[j].score - a[i].score) <= tol) {
            ++j;
        }
        std::vector<uint32_t> ad, bd;
        for (size_t t = i; t < j; ++t) {
            EXPECT_NEAR(a[t].score, b[t].score, tol) << label << " band score " << t;
            ad.push_back(a[t].docid);
            bd.push_back(b[t].docid);
            if (t > i) {
                EXPECT_LT(a[t - 1].docid, a[t].docid)
                        << label << " a not docid-ascending in tie band at " << t;
            }
        }
        std::ranges::sort(ad);
        std::ranges::sort(bd);
        EXPECT_EQ(ad, bd) << label << " tie-band docid set differs near rank " << i;
        i = j;
    }
}

} // namespace

// Selective vs exhaustive / eager-WAND / brute-force over MANY random queries on a
// varied-length scoring corpus with a high-df windowed term spanning many windows.
// The hard gate: selective is BYTE-IDENTICAL to the eager WAND (same docids, same
// scores, ties included) -- selective only changes the bytes read. Against the
// exhaustive oracle and the brute-force reference it must be a VALID top-K (same
// scores, same docid sets per tie band); those two paths sum per-doc in a
// different order, so they may legitimately swap docids only WITHIN an equal-score
// tie band (BM25 floating-point ULP), which ExpectValidTopK tolerates while still
// catching any real ranking divergence.
TEST(SniiScoringWandSelective, MatchesExhaustiveOverRandomQueries) {
    const Corpus corpus = MakeVariedCorpus(4000);
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    const std::string path = TempPath();
    BuildAndOpen(corpus, path);

    OpenedIndex oi(path, /*block_size=*/4096);
    ASSERT_TRUE(SniiSegmentReader::open(&oi.metered, &oi.seg).ok());
    ASSERT_TRUE(oi.seg.open_index(1, "body", &oi.idx).ok());
    ASSERT_TRUE(SniiStatsProvider::open(&oi.idx, &oi.stats).ok());

    ASSERT_GE(WindowCount(oi.idx, "aa_hi"), 8U) << "aa_hi must span many windows";

    const Bm25Params params;
    const std::vector<std::string> vocab = {"aa_hi",    "aa_evenmid", "aa_thirdmid",
                                            "aa_rarea", "aa_rareb",   "aa_rarec",
                                            "aa_pad0",  "aa_pad7",    "aa_missing"};
    std::mt19937 rng(0xC0FFEEU);
    std::uniform_int_distribution<uint32_t> n_terms(1, 4);
    std::uniform_int_distribution<size_t> pick(0, vocab.size() - 1);
    const std::vector<uint32_t> ks = {1, 2, 3, 5, 10, 25, 100, 500};
    std::uniform_int_distribution<size_t> pick_k(0, ks.size() - 1);

    for (int iter = 0; iter < 300; ++iter) {
        // Distinct query terms (a query never repeats a term in practice).
        std::vector<std::string> terms;
        const uint32_t nt = n_terms(rng);
        for (uint32_t t = 0; t < nt; ++t) {
            const std::string cand = vocab[pick(rng)];
            if (std::ranges::find(terms, cand) == terms.end()) {
                terms.push_back(cand);
            }
        }
        if (terms.empty()) {
            continue;
        }
        const uint32_t k = ks[pick_k(rng)];

        std::vector<ScoredDoc> sel, ex, wa;
        ASSERT_TRUE(
                snii::query::scoring_query_wand_selective(oi.idx, oi.stats, terms, k, params, &sel)
                        .ok());
        ASSERT_TRUE(snii::query::scoring_query_exhaustive(oi.idx, oi.stats, terms, k, params, &ex)
                            .ok());
        ASSERT_TRUE(snii::query::scoring_query_wand(oi.idx, oi.stats, terms, k, params, &wa).ok());
        const std::vector<ScoredDoc> ref = ReferenceRanking(corpus, norms, terms, k, params);

        std::string label = "iter " + std::to_string(iter) + " k=" + std::to_string(k) + " terms:";
        for (const auto& t : terms) {
            label += " " + t;
        }

        // HARD GATE: selective == eager WAND exactly (docids + scores, ties included).
        ExpectIdentical(sel, wa, label + " [sel==wand]", 1e-9);
        // Selective is a valid top-K vs the exhaustive oracle and brute-force ref.
        ExpectValidTopK(sel, ex, label + " [sel~ex]", 1e-9);
        ExpectValidTopK(sel, ref, label + " [sel~ref]", 1e-9);
    }
    std::remove(path.c_str());
}

// Selective == exhaustive == wand == brute-force WITH SCORE TIES (uniform doc
// length) across many k, including k=1. Strict pruning that dropped ties would
// fail here; the >= theta tie rule must be preserved end to end.
TEST(SniiScoringWandSelective, MatchesExhaustiveWithTies) {
    const Corpus corpus = MakeTieCorpus(2400);
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    const std::string path = TempPath();
    BuildAndOpen(corpus, path);

    OpenedIndex oi(path, /*block_size=*/4096);
    ASSERT_TRUE(SniiSegmentReader::open(&oi.metered, &oi.seg).ok());
    ASSERT_TRUE(oi.seg.open_index(1, "body", &oi.idx).ok());
    ASSERT_TRUE(SniiStatsProvider::open(&oi.idx, &oi.stats).ok());
    ASSERT_GE(WindowCount(oi.idx, "aa_hi"), 4U);

    const Bm25Params params;
    auto check = [&](const std::vector<std::string>& terms, uint32_t k) {
        std::vector<ScoredDoc> sel, ex, wa;
        ASSERT_TRUE(
                snii::query::scoring_query_wand_selective(oi.idx, oi.stats, terms, k, params, &sel)
                        .ok());
        ASSERT_TRUE(snii::query::scoring_query_exhaustive(oi.idx, oi.stats, terms, k, params, &ex)
                            .ok());
        ASSERT_TRUE(snii::query::scoring_query_wand(oi.idx, oi.stats, terms, k, params, &wa).ok());
        const std::vector<ScoredDoc> ref = ReferenceRanking(corpus, norms, terms, k, params);
        std::string label = "ties terms[0]=" + terms[0] + " k=" + std::to_string(k);
        // Uniform doc length => EXACT ties (bitwise-equal scores): every path must
        // agree exactly, ordering equal-score docs by ascending docid (the >= theta
        // tie rule). This is the strongest possible selective==exhaustive gate.
        ExpectIdentical(sel, ex, label + " [sel==ex]", 1e-9);
        ExpectIdentical(sel, wa, label + " [sel==wand]", 1e-9);
        ExpectIdentical(sel, ref, label + " [sel==ref]", 1e-9);
    };
    for (uint32_t k : {1U, 2U, 3U, 5U, 25U, 200U, 1000U}) {
        check({"aa_hi"}, k);
    }
    for (uint32_t k : {1U, 4U, 10U, 100U, 800U}) {
        check({"aa_hi", "aa_evenz"}, k);
    }
    for (uint32_t k : {1U, 5U, 50U}) {
        check({"aa_hi", "aa_evenz", "aa_rarez"}, k);
    }
    std::remove(path.c_str());
}

// Selective fetch reads FEWER .frq windows / bytes than reading EVERY window of
// the high-df term, for a small-k single-high-df-term query -- with NO change in
// the result. The decay corpus puts the top scorers in the EARLY windows so the
// block-max of later windows provably falls below theta and they are skipped. We
// measure the selective path's remote_bytes / requested bytes against the eager
// full-posting read of "hi" through the same metered reader.
TEST(SniiScoringWandSelective, FetchesFewerWindowsForSmallKHighDf) {
    const Corpus corpus = MakeDecayCorpus(8000);
    const std::string path = TempPath();
    BuildAndOpen(corpus, path);

    // Fine-grained cache block: the high-df frq span is only a few KiB, so a coarse
    // 4096-byte block can round BOTH the selective and full reads up to the same
    // aligned block set even though their raw request sizes differ. A 512-byte block
    // keeps the block-aligned remote_bytes measure faithful to the genuine per-window
    // savings (independent of where the interleaved posting region happens to land).
    OpenedIndex oi(path, /*block_size=*/512);
    ASSERT_TRUE(SniiSegmentReader::open(&oi.metered, &oi.seg).ok());
    ASSERT_TRUE(oi.seg.open_index(1, "body", &oi.idx).ok());
    ASSERT_TRUE(SniiStatsProvider::open(&oi.idx, &oi.stats).ok());
    const uint32_t hi_windows = WindowCount(oi.idx, "aa_hi");
    ASSERT_GE(hi_windows, 16U) << "aa_hi must span many windows";

    const Bm25Params params;
    const std::vector<std::string> terms = {"aa_hi"};
    const uint32_t k = 5;

    // Selective path metrics (prelude + only the surviving windows fetched).
    oi.metered.reset_metrics();
    std::vector<ScoredDoc> sel;
    ASSERT_TRUE(snii::query::scoring_query_wand_selective(oi.idx, oi.stats, terms, k, params, &sel)
                        .ok());
    const io::IoMetrics sel_m = oi.metered.metrics();

    // Full-posting read of the same term (every window's full .frq), as the
    // "read all windows" baseline through the same metered reader.
    oi.metered.reset_metrics();
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    ASSERT_TRUE(oi.idx.lookup("aa_hi", &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    DecodedPosting dp;
    ASSERT_TRUE(read_windowed_posting(oi.idx, entry, frq_base, prx_base,
                                      /*want_positions=*/false, /*want_freq=*/true, &dp)
                        .ok());
    const io::IoMetrics full_m = oi.metered.metrics();

    // Result must be unchanged vs exhaustive (byte savings never change the answer).
    std::vector<ScoredDoc> ex;
    ASSERT_TRUE(
            snii::query::scoring_query_exhaustive(oi.idx, oi.stats, terms, k, params, &ex).ok());
    ExpectValidTopK(sel, ex, "decay small-k [sel~ex]", 1e-9);

    // Selective requests strictly fewer raw bytes than reading all windows: later
    // windows are skipped, not merely cached.
    EXPECT_LT(sel_m.total_request_bytes, full_m.total_request_bytes)
            << "sel=" << sel_m.total_request_bytes << " full=" << full_m.total_request_bytes;
    // And fewer remote bytes leave the wire.
    EXPECT_LT(sel_m.remote_bytes, full_m.remote_bytes)
            << "sel=" << sel_m.remote_bytes << " full=" << full_m.remote_bytes;

    std::remove(path.c_str());
}
