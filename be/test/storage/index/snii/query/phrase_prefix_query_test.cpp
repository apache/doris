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
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_phrase_prefix_query_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

bool HasPrefix(const std::string& term, const std::string& prefix) {
    return term.size() >= prefix.size() && term.starts_with(prefix);
}

struct Corpus {
    std::vector<std::vector<std::string>> docs;

    std::vector<uint32_t> phrase_prefix_docs(const std::vector<std::string>& terms) const {
        std::vector<uint32_t> out;
        if (terms.empty()) {
            return out;
        }
        for (uint32_t d = 0; d < docs.size(); ++d) {
            const std::vector<std::string>& doc = docs[d];
            bool match = false;
            if (terms.size() == 1) {
                for (const std::string& term : doc) {
                    if (HasPrefix(term, terms.front())) {
                        match = true;
                        break;
                    }
                }
            } else if (doc.size() >= terms.size()) {
                for (size_t start = 0; start + terms.size() <= doc.size(); ++start) {
                    bool exact = true;
                    for (size_t i = 0; i + 1 < terms.size(); ++i) {
                        if (doc[start + i] != terms[i]) {
                            exact = false;
                            break;
                        }
                    }
                    if (exact && HasPrefix(doc[start + terms.size() - 1], terms.back())) {
                        match = true;
                        break;
                    }
                }
            }
            if (match) {
                out.push_back(d);
            }
        }
        return out;
    }

    // Truncation-aware oracle: reproduces the byte-exact max_expansions semantics
    // the query must honour -- enumerate the REAL tail terms sharing the prefix in
    // lexicographic (dict) order, keep only the first `max_expansions`, then match
    // exactly as phrase_prefix_docs but restricted to that surviving tail set.
    // (These corpora carry no hidden phrase-bigram terms, so the corpus vocabulary
    // with the prefix IS the index's real-term enumeration for it.)
    std::vector<uint32_t> phrase_prefix_docs_capped(const std::vector<std::string>& terms,
                                                    int32_t max_expansions) const {
        if (max_expansions <= 0 || terms.size() < 2) {
            return phrase_prefix_docs(terms);
        }
        std::set<std::string> vocab;
        for (const std::vector<std::string>& doc : docs) {
            for (const std::string& t : doc) {
                if (HasPrefix(t, terms.back())) {
                    vocab.insert(t);
                }
            }
        }
        std::set<std::string> allowed;
        int32_t taken = 0;
        for (const std::string& t : vocab) { // std::set iterates ascending (dict order)
            if (taken >= max_expansions) {
                break;
            }
            allowed.insert(t);
            ++taken;
        }
        std::vector<uint32_t> out;
        for (uint32_t d = 0; d < docs.size(); ++d) {
            const std::vector<std::string>& doc = docs[d];
            if (doc.size() < terms.size()) {
                continue;
            }
            bool match = false;
            for (size_t start = 0; start + terms.size() <= doc.size() && !match; ++start) {
                bool exact = true;
                for (size_t i = 0; i + 1 < terms.size(); ++i) {
                    if (doc[start + i] != terms[i]) {
                        exact = false;
                        break;
                    }
                }
                if (exact && allowed.contains(doc[start + terms.size() - 1])) {
                    match = true;
                }
            }
            if (match) {
                out.push_back(d);
            }
        }
        return out;
    }
};

Corpus BuildPhraseCorpus() {
    Corpus c;
    c.docs = {{"quick", "brown", "fox"}, {"quick", "blue", "fox"}, {"quick", "bronze", "fox"},
              {"slow", "brown", "fox"},  {"quick", "brownish"},    {"quick", "brown", "fossil"},
              {"quick", "brown", "fog"}, {"quick", "brown"},       {"brown", "fox", "quick"}};
    return c;
}

Corpus BuildWideTailCorpus() {
    Corpus c;
    c.docs.resize(96);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        c.docs[d].emplace_back(d == 0 ? "lead" : "other");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        c.docs[d].emplace_back(term);
    }
    return c;
}

Corpus BuildRepeatedExactCorpus() {
    Corpus c;
    c.docs = {{"x", "x", "brown"},
              {"x", "y", "brown"},
              {"x", "brown", "x"},
              {"x", "x", "bronze"},
              {"x", "x", "blue"}};
    return c;
}

Corpus BuildSharedExactWideTailCorpus() {
    Corpus c;
    c.docs.resize(768);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        c.docs[d].emplace_back("lead");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d);
        c.docs[d].emplace_back(term);
    }
    return c;
}

// Large corpus whose leading exact term and every tail expansion have high df, so
// their postings span MULTIPLE windows and the merge advances its cursors across
// window boundaries. Most docs match ("lead" @0 then a "res_" tail @1); a slice
// injects a filler token so the tail is not adjacent (must NOT match), and a slice
// omits the leading term (must NOT match) -- forcing the cross-window sweep to
// reject as well as accept.
Corpus BuildCrossWindowTailCorpus() {
    Corpus c;
    c.docs.resize(5000);
    const char* const tails[] = {"res_a", "res_b", "res_c"};
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        if (d % 50 == 7) {
            c.docs[d] = {"lead", "gap", tails[d % 3]}; // tail not adjacent to lead
        } else if (d % 50 == 11) {
            c.docs[d] = {"nolead", tails[d % 3]}; // leading term absent
        } else {
            c.docs[d] = {"lead", tails[d % 3]};
        }
    }
    return c;
}

// CJK / multi-byte corpus. Leading term and tail prefix are UTF-8; prefix testing
// is byte-wise, matching the index's dict enumeration order.
Corpus BuildCjkTailCorpus() {
    Corpus c;
    const char* const tails[] = {"\xE7\xBB\x93\xE6\x9E\x9C\xE7\x94\xB2",  // 结果甲
                                 "\xE7\xBB\x93\xE6\x9E\x9C\xE4\xB9\x99",  // 结果乙
                                 "\xE7\xBB\x93\xE6\x9E\x9C\xE4\xB8\x99"}; // 结果丙
    const std::string lead = "\xE8\xBF\x9E\xE6\x8E\xA5";                  // 连接
    c.docs.resize(120);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        if (d % 20 == 3) {
            c.docs[d] = {lead, "\xE9\x97\xB4\xE9\x9A\x94",
                         tails[d % 3]}; // 间隔 filler, not adjacent
        } else {
            c.docs[d] = {lead, tails[d % 3]};
        }
    }
    return c;
}

// Two leading exact terms that both occur but are NEVER adjacent, so the leading
// phrase conjunction yields an empty expected-position set -- the multi-tail
// branch's `expected.docs.empty()` early return -- even though the tail prefix
// expands to several real terms.
Corpus BuildEmptyExpectedCorpus() {
    Corpus c;
    c.docs = {{"alpha", "x", "beta", "res_a"},
              {"alpha", "y", "beta", "res_b"},
              {"beta", "alpha", "res_c"},
              {"alpha", "z", "beta"}};
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        const std::vector<std::string>& terms = c.docs[d];
        for (uint32_t pos = 0; pos < terms.size(); ++pos) {
            buf.add_token(terms[pos], d, pos);
        }
    }

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = doris::snii::format::IndexConfig::kDocsPositionsScoring;
    in.doc_count = static_cast<uint32_t>(c.docs.size());
    in.encoded_norms.assign(c.docs.size(), 1);
    in.terms = buf.finalize_sorted();
    in.target_dict_block_bytes = 2048;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(in).ok());
    ASSERT_TRUE(compound.finish().ok());
}

LogicalIndexReader OpenIndex(io::LocalFileReader* file, SniiSegmentReader* segment,
                             const std::string& path) {
    EXPECT_TRUE(file->open(path).ok());
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

LogicalIndexReader OpenMeteredIndex(io::MeteredFileReader* file, SniiSegmentReader* segment) {
    EXPECT_TRUE(SniiSegmentReader::open(file, segment).ok());
    LogicalIndexReader idx;
    EXPECT_TRUE(segment->open_index(1, "body", &idx).ok());
    return idx;
}

} // namespace

TEST(SniiPhrasePrefixQuery, MatchesPositionOracle) {
    const Corpus corpus = BuildPhraseCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::vector<std::string>> cases = {
            {"quick", "bro"},  {"quick", "brown", "fo"}, {"slow", "bro"},
            {"absent", "bro"}, {"quick", "missing"},     {"bro"}};
    for (const std::vector<std::string>& terms : cases) {
        std::vector<uint32_t> got;
        const Status st = query::phrase_prefix_query(idx, terms, &got);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(std::ranges::is_sorted(got));
        EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    }

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, WideTailPrefixAvoidsPerExpansionLookup) {
    const Corpus corpus = BuildWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    const Status st = query::phrase_prefix_query(idx, terms, &got);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    EXPECT_LT(metered.metrics().read_at_calls, corpus.docs.size() / 3)
            << "phrase_prefix_query must reuse PrefixHit entries, not lookup every tail term";

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, RepeatedExactTermsMatchPositionOracle) {
    const Corpus corpus = BuildRepeatedExactCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::vector<std::string>> cases = {
            {"x", "x", "br"}, {"x", "brown", "x"}, {"x", "x", "missing"}};
    for (const std::vector<std::string>& terms : cases) {
        std::vector<uint32_t> got;
        const Status st = query::phrase_prefix_query(idx, terms, &got);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    }

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, WideTailPrefixReusesExactTermPostingReads) {
    const Corpus corpus = BuildSharedExactWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenMeteredIndex(&metered, &segment);

    metered.reset_metrics();
    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    const Status st = query::phrase_prefix_query(idx, terms, &got);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    EXPECT_LT(metered.metrics().read_at_calls, corpus.docs.size() / 4)
            << "wide phrase-prefix must not re-read exact term postings for every tail hit";

    std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// Merged multi-tail path (CollectMergedTailMatches): the per-tail verify+union
// loop was replaced by a single batched, forward-merge sweep. Every case below
// asserts the merged result equals the independent position oracle -- i.e. is
// identical to the old per-tail semantics -- across the 0/1/many-expansion
// trichotomy, max_expansions truncation, an empty expected set, cross-window
// positions, CJK/unicode terms, and a df-pruned bigram segment.
// ---------------------------------------------------------------------------

namespace qinternal = doris::snii::query::internal;

// Zero expansions: the tail prefix matches no real term -> empty result. Also
// exercises the single-expansion (untouched) branch for comparison.
TEST(SniiPhrasePrefixMerge, ZeroAndSingleExpansionMatchOracle) {
    const Corpus corpus = BuildSharedExactWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> zero_terms = {"lead", "zzz"};
    std::vector<uint32_t> zero_got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, zero_terms, &zero_got).ok());
    EXPECT_TRUE(zero_got.empty());
    EXPECT_EQ(zero_got, corpus.phrase_prefix_docs(zero_terms));

    // Prefix "aa_000" matches exactly one full term -> single-tail branch.
    const std::vector<std::string> single_terms = {"lead", "aa_000"};
    std::vector<uint32_t> single_got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, single_terms, &single_got).ok());
    EXPECT_TRUE(std::ranges::is_sorted(single_got));
    EXPECT_EQ(single_got, corpus.phrase_prefix_docs(single_terms));

    std::remove(path.c_str());
}

// Many expansions spanning several resident-capped groups (768 tails, batch 32).
// The merged/grouped path must equal the full oracle and stay sorted.
TEST(SniiPhrasePrefixMerge, ManyExpansionGroupsMatchOracle) {
    const Corpus corpus = BuildSharedExactWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_TRUE(std::ranges::is_sorted(got));
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));
    // The expected-docid projection must still be built exactly once per query
    // (hoisted out of the per-tail loop), proving the multi-tail branch ran.
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_EQ(qinternal::query_test_counters().expected_docids_build, 1U);

    std::remove(path.c_str());
}

// max_expansions truncation is byte-exact: lexicographic order, real unigrams
// only, and independent of how the tails are later grouped for the merge.
TEST(SniiPhrasePrefixMerge, MaxExpansionsTruncationMatchesOracle) {
    const Corpus corpus = BuildSharedExactWideTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "aa_"};
    for (int32_t cap : {1, 10, 32, 50, 100, 768, 1000}) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got, cap).ok()) << "cap=" << cap;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << "cap=" << cap;
        EXPECT_EQ(got, corpus.phrase_prefix_docs_capped(terms, cap)) << "cap=" << cap;
    }

    std::remove(path.c_str());
}

// An empty expected set (two leading terms never adjacent) short-circuits the
// multi-tail branch to an empty result even though the tail prefix expands.
TEST(SniiPhrasePrefixMerge, EmptyExpectedSetReturnsEmpty) {
    const Corpus corpus = BuildEmptyExpectedCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"alpha", "beta", "res_"};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_TRUE(got.empty());
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));

    std::remove(path.c_str());
}

// Cross-window positions: high-df leading + tails span multiple windows, and the
// forward merge must accept adjacent matches and reject the non-adjacent /
// leading-absent injections while sweeping across window boundaries.
TEST(SniiPhrasePrefixMerge, CrossWindowPositionsMatchOracle) {
    const Corpus corpus = BuildCrossWindowTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "res_"};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_TRUE(std::ranges::is_sorted(got));
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));

    std::remove(path.c_str());
}

// CJK / multi-byte terms merge identically to the oracle.
TEST(SniiPhrasePrefixMerge, CjkUnicodeTailsMatchOracle) {
    const Corpus corpus = BuildCjkTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"\xE8\xBF\x9E\xE6\x8E\xA5",  // 连接
                                            "\xE7\xBB\x93\xE6\x9E\x9C"}; // 结果
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_TRUE(std::ranges::is_sorted(got));
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));

    std::remove(path.c_str());
}

// A df-pruned bigram segment must yield the SAME multi-tail phrase-prefix result
// as the unpruned and bigram-free segments: the merged path verifies against
// unigram positions only and NEVER consults (possibly incomplete) bigram
// postings. Query {"failed","ord"} expands to the unigram tails order/ordinal.
TEST(SniiPhrasePrefixMerge, BigramPrunedSegmentMatchesUnigramPath) {
    namespace snii_test = doris::snii::snii_test;
    const std::vector<std::string> terms = {"failed", "ord"};

    auto run = [&](bool include_bigrams, uint32_t prune_min_df) {
        snii_test::MemoryFile file;
        SniiSegmentReader segment;
        LogicalIndexReader idx;
        EXPECT_TRUE(
                snii_test::build_reader(&file, &segment, &idx, include_bigrams, prune_min_df).ok());
        std::vector<uint32_t> got;
        EXPECT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
        EXPECT_TRUE(std::ranges::is_sorted(got));
        return got;
    };

    const std::vector<uint32_t> baseline = run(/*include_bigrams=*/false, /*prune_min_df=*/0);
    const std::vector<uint32_t> unpruned = run(/*include_bigrams=*/true, /*prune_min_df=*/0);
    const std::vector<uint32_t> pruned = run(/*include_bigrams=*/true, /*prune_min_df=*/2);

    EXPECT_FALSE(baseline.empty()); // the query must actually match some docs
    EXPECT_EQ(unpruned, baseline) << "bigram presence must not change phrase-prefix result";
    EXPECT_EQ(pruned, baseline) << "bigram df-pruning must not change phrase-prefix result";
}

// A MULTI-leading-term phrase-prefix ("failed order ord*") must NOT take the
// 2-term bigram fast path: the bigram pair (order, ord_expansion) only proves
// the LAST two terms are adjacent, not that "failed order" precedes them. Using
// it would over-match docs that have "order ord..." without the leading
// "failed". The single-leading guard routes this to full position verification;
// the result must be identical whether or not the segment carries bigrams (and
// must equal the brute-force position oracle would -- here anchored by the
// no-bigram baseline, which is pure position verification).
TEST(SniiPhrasePrefixMerge, MultiLeadingTermIgnoresBigramFastPath) {
    namespace snii_test = doris::snii::snii_test;
    const std::vector<std::string> terms = {"failed", "order", "ord"};

    auto run = [&](bool include_bigrams, uint32_t prune_min_df) {
        snii_test::MemoryFile file;
        SniiSegmentReader segment;
        LogicalIndexReader idx;
        EXPECT_TRUE(
                snii_test::build_reader(&file, &segment, &idx, include_bigrams, prune_min_df).ok());
        std::vector<uint32_t> got;
        EXPECT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
        EXPECT_TRUE(std::ranges::is_sorted(got));
        return got;
    };

    const std::vector<uint32_t> baseline = run(/*include_bigrams=*/false, /*prune_min_df=*/0);
    const std::vector<uint32_t> unpruned = run(/*include_bigrams=*/true, /*prune_min_df=*/0);
    const std::vector<uint32_t> pruned = run(/*include_bigrams=*/true, /*prune_min_df=*/2);

    // "failed order ordinal" is adjacent in docs 5000/7000 (order@1 -> ordinal@2
    // there); the leading "failed order" must gate it -- bigram(order,ordinal)
    // alone (present in docs 5000/7000) must not add a doc lacking "failed order".
    EXPECT_EQ(unpruned, baseline) << "multi-leading phrase-prefix must not use the bigram pair";
    EXPECT_EQ(pruned, baseline) << "multi-leading phrase-prefix stays on the positions path";
}
