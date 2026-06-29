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

#include "snii/query/scoring_query.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "snii/format/format_constants.h"
#include "snii/io/local_file.h"
#include "snii/io/metered_file_reader.h"
#include "snii/query/bm25_scorer.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/stats/snii_stats_provider.h"
#include "snii/writer/logical_index_writer.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

using namespace snii;
using namespace snii::format;
using namespace snii::writer;
using snii::query::Bm25Params;
using snii::query::ScoredDoc;
using snii::stats::SniiStatsProvider;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_score_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// A small in-memory corpus: each doc is a bag of (term -> freq). Doc lengths vary
// so length normalization matters. "common" is a high-df term (~half the docs),
// "rare" is a low-df term.
struct Corpus {
    uint32_t doc_count = 0;
    // term -> (docid -> freq), docids ascending.
    std::map<std::string, std::map<uint32_t, uint32_t>> postings;
    std::vector<uint64_t> doc_len; // per-doc total token count
};

// Builds ~60 docs with varied lengths and a high-df + low-df term.
Corpus MakeCorpus() {
    Corpus c;
    c.doc_count = 60;
    c.doc_len.assign(c.doc_count, 0);

    auto add = [&](const std::string& term, uint32_t doc, uint32_t freq) {
        c.postings[term][doc] += freq;
        c.doc_len[doc] += freq;
    };

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        // "common": appears in even docs, freq varies 1..4.
        if (d % 2 == 0) {
            add("common", d, 1 + (d % 4));
        }
        // "rare": appears in only a few docs.
        if (d == 3 || d == 17 || d == 42) {
            add("rare", d, 2);
        }
        // "filler": gives docs varied lengths so dl differs widely.
        add("filler", d, 1 + (d % 7) * 3);
        // a unique padding token per doc to spread lengths further.
        add("pad" + std::to_string(d % 11), d, (d % 5) + 1);
    }
    return c;
}

// Converts the corpus into a sorted SniiIndexInput with encoded norms.
SniiIndexInput ToInput(const Corpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring;
    in.doc_count = c.doc_count;
    in.target_dict_block_bytes = 1; // one block per term

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

// Reference BM25 ranking computed directly from the corpus (same encode/decode).
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

} // namespace

// Helper that mirrors ToInput's encoding so the reference path can decode norms.
namespace {
std::vector<uint8_t> EncodeNorms(const Corpus& c) {
    std::vector<uint8_t> v(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        v[d] = snii::query::encode_norm(c.doc_len[d]);
    }
    return v;
}
} // namespace

// Fixture-free test: build, open, and compare.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiScoringQuery, ReferenceOracleAndWandEqualsExhaustive) {
    const Corpus corpus = MakeCorpus();
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    const std::string path = TempPath();

    // --- build the scoring index ---
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    // --- open via SniiSegmentReader over a MeteredFileReader ---
    io::LocalFileReader inner;
    ASSERT_TRUE(inner.open(path).ok());
    io::MeteredFileReader metered(&inner);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());

    // (c) SniiStatsProvider df / ttf / avgdl / encoded_norm match brute force.
    uint64_t sum_ttf = 0;
    for (const auto& dl : corpus.doc_len) {
        sum_ttf += dl;
    }
    EXPECT_EQ(stats.indexed_doc_count(), corpus.doc_count);
    EXPECT_EQ(stats.sum_total_term_freq(), sum_ttf);
    EXPECT_NEAR(stats.avgdl(), static_cast<double>(sum_ttf) / corpus.doc_count, 1e-9);

    for (const auto& [term, plist] : corpus.postings) {
        uint64_t df = 0, ttf = 0;
        ASSERT_TRUE(stats.doc_freq(term, &df).ok());
        ASSERT_TRUE(stats.total_term_freq(term, &ttf).ok());
        uint64_t exp_ttf = 0;
        for (const auto& [d, f] : plist) {
            exp_ttf += f;
        }
        EXPECT_EQ(df, plist.size()) << term;
        EXPECT_EQ(ttf, exp_ttf) << term;
    }
    for (uint32_t d = 0; d < corpus.doc_count; ++d) {
        uint8_t got = 0;
        ASSERT_TRUE(stats.encoded_norm(d, &got).ok());
        EXPECT_EQ(got, norms[d]) << "docid " << d;
    }

    // (a) single-term scoring_query top-K matches the reference.
    const Bm25Params params; // defaults k1=1.2, b=0.75
    const uint32_t k = 10;

    auto run_and_check = [&](const std::vector<std::string>& terms) {
        std::vector<ScoredDoc> reference = ReferenceRanking(corpus, norms, terms, k, params);
        std::vector<ScoredDoc> exhaustive;
        ASSERT_TRUE(snii::query::scoring_query_exhaustive(idx, stats, terms, k, params, &exhaustive)
                            .ok());
        std::vector<ScoredDoc> wand;
        ASSERT_TRUE(snii::query::scoring_query_wand(idx, stats, terms, k, params, &wand).ok());

        ASSERT_EQ(exhaustive.size(), reference.size());
        for (size_t i = 0; i < reference.size(); ++i) {
            EXPECT_EQ(exhaustive[i].docid, reference[i].docid) << "rank " << i;
            EXPECT_NEAR(exhaustive[i].score, reference[i].score, 1e-6) << "rank " << i;
        }
        // (b) WAND-pruned top-K equals the exhaustive top-K.
        ASSERT_EQ(wand.size(), exhaustive.size());
        for (size_t i = 0; i < wand.size(); ++i) {
            EXPECT_EQ(wand[i].docid, exhaustive[i].docid) << "wand rank " << i;
            EXPECT_NEAR(wand[i].score, exhaustive[i].score, 1e-6) << "wand rank " << i;
        }
    };

    run_and_check({"common"});
    run_and_check({"rare"});
    run_and_check({"common", "rare"});
    run_and_check({"common", "rare", "filler"});

    std::remove(path.c_str());
}

namespace {

// A corpus engineered to produce SCORE TIES at the top-k boundary and to drive
// the WINDOWED posting path: uniform doc length (so length-norm is constant) and
// high-df terms (df >= kSlimDfThreshold = 512 -> windowed pod_ref + frq_prelude).
// Every doc has the same length L=8, so docs sharing a term/freq score identically.
Corpus MakeWindowedTieCorpus() {
    Corpus c;
    c.doc_count = 700; // >= 512 so "anchor" becomes a windowed term
    c.doc_len.assign(c.doc_count, 0);
    auto add = [&](const std::string& term, uint32_t doc, uint32_t freq) {
        c.postings[term][doc] += freq;
        c.doc_len[doc] += freq;
    };
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        add("anchor", d, 1); // df=700 (windowed), freq=1 everywhere -> ties
        if (d % 2 == 0) {
            add("evenz", d, 1); // df=350 (windowed), another high-df term
        }
        add("u" + std::to_string(d), d, 6); // unique pad: keeps every dl == 8 exactly
    }
    return c;
}

} // namespace

// Differential: WAND top-k MUST equal exhaustive top-k EVEN with boundary ties and
// windowed (block-max) terms, across many k. Strict-'>' pruning would drop ties.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiScoringQuery, WandEqualsExhaustiveWithTiesAndWindowedTerms) {
    const Corpus corpus = MakeWindowedTieCorpus();
    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(ToInput(corpus)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }
    io::LocalFileReader inner;
    ASSERT_TRUE(inner.open(path).ok());
    io::MeteredFileReader metered(&inner);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());

    const Bm25Params params;
    const std::vector<uint8_t> norms = EncodeNorms(corpus);
    auto check = [&](const std::vector<std::string>& terms, uint32_t k) {
        std::vector<ScoredDoc> ex, wa;
        ASSERT_TRUE(scoring_query_exhaustive(idx, stats, terms, k, params, &ex).ok());
        ASSERT_TRUE(scoring_query_wand(idx, stats, terms, k, params, &wa).ok());
        const std::vector<ScoredDoc> ref = ReferenceRanking(corpus, norms, terms, k, params);
        ASSERT_EQ(wa.size(), ex.size());
        ASSERT_EQ(ex.size(), ref.size());
        for (size_t i = 0; i < ex.size(); ++i) {
            EXPECT_EQ(wa[i].docid, ex[i].docid)
                    << "terms[0]=" << terms[0] << " k=" << k << " i=" << i;
            EXPECT_EQ(ex[i].docid, ref[i].docid) << "ref k=" << k << " i=" << i;
            EXPECT_NEAR(wa[i].score, ex[i].score, 1e-9);
        }
    };
    // Single high-df term: all 700 docs tie -> top-k must be the k smallest docids.
    for (uint32_t k : {1U, 3U, 5U, 50U, 200U}) {
        check({"anchor"}, k);
    }
    // Two windowed terms: even docs score higher (two terms) -> ties within each tier.
    for (uint32_t k : {1U, 4U, 10U, 100U}) {
        check({"anchor", "evenz"}, k);
    }

    std::remove(path.c_str());
}
