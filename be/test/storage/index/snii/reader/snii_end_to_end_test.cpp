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
#include <cstdint>
#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/format/format_constants.h"
#include "snii/io/local_file.h"
#include "snii/io/metered_file_reader.h"
#include "snii/query/phrase_query.h"
#include "snii/query/term_query.h"
#include "snii/reader/logical_index_reader.h"
#include "snii/reader/snii_segment_reader.h"
#include "snii/writer/snii_compound_writer.h"
#include "snii/writer/spimi_term_buffer.h"

using namespace snii;
using namespace snii::format;
using namespace snii::reader;
using namespace snii::writer;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_e2e_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// Deterministic corpus oracle: term -> set<docid>, and doc -> token sequence.
struct Corpus {
    uint32_t doc_count = 0;
    std::vector<std::vector<std::string>> docs; // docs[d] = ordered token list
    std::map<std::string, std::set<uint32_t>> term_docs;

    // Does the phrase occur consecutively in doc d?
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

// Builds a deterministic corpus of 60 docs. Plants a HIGH-frequency term
// "the" in every doc (df=60, but we also force a high-df term via padding to
// exercise the windowed path), several low-df terms, and known phrases.
Corpus BuildCorpus() {
    Corpus c;
    c.doc_count = 60;
    c.docs.resize(c.doc_count);

    // Common base vocabulary cycled deterministically.
    const std::vector<std::string> vocab = {"alpha", "bravo",   "charlie", "delta",
                                            "echo",  "foxtrot", "golf",    "hotel"};

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& toks = c.docs[d];
        // "the" in every doc -> high df.
        toks.emplace_back("the");
        // A planted 5-term phrase in docs where d % 7 == 0.
        if (d % 7 == 0) {
            toks.emplace_back("quick");
            toks.emplace_back("brown");
            toks.emplace_back("fox");
            toks.emplace_back("jumps");
            toks.emplace_back("over");
        }
        // A planted 2-term phrase "lazy dog" in docs where d % 5 == 0.
        if (d % 5 == 0) {
            toks.emplace_back("lazy");
            toks.emplace_back("dog");
        }
        // Repeated-term phrase to verify execution de-duplicates reads without
        // weakening positional semantics.
        if (d % 13 == 0) {
            toks.emplace_back("repeat");
            toks.emplace_back("repeat");
        }
        // Cycle through vocab to give terms varied dfs.
        for (uint32_t k = 0; k < 4; ++k) {
            toks.push_back(vocab[(d + k) % vocab.size()]);
        }
        // A unique low-df marker per doc bucket.
        if (d % 11 == 0) {
            toks.emplace_back("rare");
        }
    }

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        for (const auto& t : c.docs[d]) {
            c.term_docs[t].insert(d);
        }
    }
    return c;
}

// Adds a synthetic high-df term spanning > kSlimDfThreshold docs to force the
// windowed pod_ref path. Uses an expanded doc space; the term appears in docs
// [0, df) at position 0. Updates the oracle.
void PlantHighDfTerm(Corpus* c, const std::string& term, uint32_t df) {
    // Expand the corpus to hold df docs if needed.
    if (df > c->doc_count) {
        uint32_t old = c->doc_count;
        c->doc_count = df;
        c->docs.resize(df);
        (void)old;
    }
    for (uint32_t d = 0; d < df; ++d) {
        c->docs[d].insert(c->docs[d].begin(), term); // position 0
        c->term_docs[term].insert(d);
    }
    // Recompute term_docs fully to keep positions/oracle consistent.
    c->term_docs.clear();
    for (uint32_t d = 0; d < c->doc_count; ++d) {
        for (const auto& t : c->docs[d]) {
            c->term_docs[t].insert(d);
        }
    }
}

// Feeds the corpus into a SpimiTermBuffer and writes a single-index container.
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
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = c.doc_count;
    in.terms = std::move(terms);
    // Small block target so we get multiple DICT blocks (exercises sampling).
    in.target_dict_block_bytes = 256;

    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    ASSERT_TRUE(cw.add_logical_index(in).ok());
    ASSERT_TRUE(cw.finish().ok());
}

std::vector<uint32_t> SetToVec(const std::set<uint32_t>& s) {
    // NOLINTNEXTLINE(modernize-return-braced-init-list)
    return std::vector<uint32_t>(s.begin(), s.end());
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiEndToEnd, TermAndPhraseAgainstOracle) {
    Corpus c = BuildCorpus();
    // Force a windowed (df >= 512) term to exercise the windowed pod_ref path.
    PlantHighDfTerm(&c, "ubiquitous", 600);

    const std::string path = TempPath();
    WriteCorpus(c, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local);

    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    EXPECT_EQ(seg.n_logical_indexes(), 1U);

    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    EXPECT_EQ(idx.stats().doc_count, c.doc_count);

    // Small DICT blocks are resident after open: exact lookup should not add
    // query path I/O once the index is open.
    metered.reset_metrics();
    {
        DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        bool found = false;
        ASSERT_TRUE(idx.lookup("quick", &found, &entry, &frq_base, &prx_base).ok());
        ASSERT_TRUE(found);
        EXPECT_EQ(metered.metrics().read_at_calls, 0U);
    }

    // ---- term_query: present terms match the oracle docid set exactly. ----
    std::vector<std::string> present_terms = {"the",   "quick", "brown",  "fox",
                                              "jumps", "over",  "lazy",   "dog",
                                              "rare",  "alpha", "repeat", "ubiquitous"};
    for (const auto& t : present_terms) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, t, &got).ok()) << t;
        std::vector<uint32_t> want = SetToVec(c.term_docs[t]);
        EXPECT_EQ(got, want) << "term=" << t;
    }

    // ---- term_query: absent terms -> empty. ----
    std::vector<std::string> absent_terms = {"nonexistent", "zzzz", "qqqq-absent"};
    for (const auto& t : absent_terms) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, t, &got).ok()) << t;
        EXPECT_TRUE(got.empty()) << "term=" << t;
    }

    // ---- phrase_query: present and absent phrases vs oracle. ----
    std::vector<std::vector<std::string>> phrases = {
            {"quick", "brown", "fox", "jumps", "over"}, // 5-term planted phrase
            {"lazy", "dog"},                            // 2-term planted phrase
            {"repeat", "repeat"},                       // repeated-term phrase
            {"brown", "fox"},                           // sub-phrase
            {"fox", "brown"},                           // reversed -> absent
            {"quick", "fox"},                           // non-consecutive -> absent
            {"the"},                                    // single-term phrase
            {"nope", "missing"},                        // absent terms -> empty
    };
    for (const auto& p : phrases) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::phrase_query(idx, p, &got).ok());
        std::vector<uint32_t> want = c.phrase_oracle(p);
        std::string label;
        for (const auto& w : p) {
            label += w + " ";
        }
        EXPECT_EQ(got, want) << "phrase=[" << label << "]";
    }

    // ---- metrics sanity: a fresh phrase query over windowed postings
    // plans/batches its reads. Small DICT blocks are resident and small postings
    // may be inline, so use the synthetic high-df term to force external posting
    // I/O.
    metered.reset_metrics();
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_query(idx, {"ubiquitous", "the"}, &got).ok());
    const auto& m = metered.metrics();
    EXPECT_GE(m.serial_rounds, 1U);
    EXPECT_GE(m.range_gets, 1U);
    // The planned batch should keep rounds small (resident DICT + batched
    // postings).
    EXPECT_LE(m.serial_rounds, 8U) << "phrase query did not batch its I/O";

    std::remove(path.c_str());
}
