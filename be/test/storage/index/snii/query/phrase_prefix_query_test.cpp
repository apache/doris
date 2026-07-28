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
namespace snii_test = doris::snii::snii_test;

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

Corpus BuildWideNonAdjacentTailCorpus() {
    Corpus c;
    c.docs.resize(256);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d % 96);
        c.docs[d] = {"lead", "gap", term};
    }
    return c;
}

Corpus BuildLeadingPrefilterCorpus(uint32_t tail_docs) {
    DCHECK_LE(tail_docs, 1024U);
    Corpus c;
    c.docs.resize(1024);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        if (d < tail_docs) {
            char term[16];
            std::snprintf(term, sizeof(term), "aa_%03u", d % 64);
            c.docs[d] = {"lead", term};
        } else {
            c.docs[d] = {"lead", "other"};
        }
    }
    return c;
}

Corpus BuildNearEqualLeadingAndTailDfCorpus() {
    Corpus c;
    c.docs.resize(2048, {"other"});
    for (uint32_t d = 0; d < 256; ++d) {
        if (d == 255) {
            c.docs[d] = {"lead", "other"};
            continue;
        }
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d % 50);
        c.docs[d] = {"lead", term};
    }
    return c;
}

Corpus BuildDefaultExpansionPruneCorpus(uint32_t adjacent_docs) {
    Corpus c;
    c.docs.resize(1024);
    for (uint32_t d = 0; d < c.docs.size(); ++d) {
        char term[16];
        if (d < 256) {
            std::snprintf(term, sizeof(term), "aa_%03u", d % 32);
            c.docs[d] = d < adjacent_docs ? std::vector<std::string> {"lead", term}
                                          : std::vector<std::string> {"lead", "gap", term};
        } else {
            std::snprintf(term, sizeof(term), "aa_%03u", 32 + (d % 18));
            c.docs[d] = {"lead", "gap", term};
        }
    }
    return c;
}

Corpus BuildAllMatchedBeforeFinalGroupCorpus() {
    Corpus c;
    c.docs.resize(1025);
    for (uint32_t d = 0; d < 1024; ++d) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d % 32);
        c.docs[d] = {"lead", term};
    }
    c.docs.back() = {"other", "aa_032"};
    return c;
}

Corpus BuildAllMatchedAfterPartialGroupCorpus() {
    Corpus c;
    c.docs.resize(1376);
    for (uint32_t d = 0; d < 1024; ++d) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", d < 512 ? d % 32 : 32 + (d % 32));
        c.docs[d] = {"lead", term};
    }
    for (uint32_t tail = 64; tail < 416; ++tail) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", tail);
        c.docs[1024 + tail - 64] = {"other", term};
    }
    return c;
}

Corpus BuildCrossGroupDuplicateTailCorpus() {
    Corpus c;
    c.docs.resize(3);
    for (uint32_t tail = 0; tail < 65; ++tail) {
        c.docs[0].emplace_back("lead");
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", tail);
        c.docs[0].emplace_back(term);
    }
    c.docs[1] = {"lead", "gap", "aa_000"};
    c.docs[2] = {"other", "aa_064"};
    return c;
}

// The first 32-tail resident group matches doc 0. The final group contains
// exactly 32 real tails, but none of their postings intersects the expected
// leading-term doc set. collect_merged_tail_matches must still flush the match
// accumulated by the earlier group when it processes this empty final group.
Corpus BuildFinalGroupWithoutCandidatesCorpus() {
    Corpus c;
    c.docs.resize(64);
    for (uint32_t tail = 0; tail < c.docs.size(); ++tail) {
        char term[16];
        std::snprintf(term, sizeof(term), "aa_%03u", tail);
        c.docs[tail] = {tail == 0 ? "lead" : "other", term};
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
    uint64_t token_count = 0;
    for (const auto& term : in.terms) {
        token_count += term.positions_flat.size();
    }
    in.common_grams_metadata = snii_test::make_plain_scoring_metadata(in.doc_count, token_count);
    in.target_dict_block_bytes = 2048;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(in).ok());
    ASSERT_TRUE(compound.finish().ok());
}

void WritePostings(SpimiTermBuffer* buffer, uint32_t doc_count, const std::string& path) {
    SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = doris::snii::format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = doc_count;
    input.encoded_norms.assign(doc_count, 1);
    input.terms = buffer->finalize_sorted();
    uint64_t token_count = 0;
    for (const auto& term : input.terms) {
        token_count += term.positions_flat.size();
    }
    input.common_grams_metadata =
            snii_test::make_plain_scoring_metadata(input.doc_count, token_count);
    input.target_dict_block_bytes = 256;

    io::LocalFileWriter writer;
    ASSERT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    ASSERT_TRUE(compound.add_logical_index(input).ok());
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

TEST(SniiPhrasePrefixQuery, SegmentRelativeDfGatePrefiltersLeadingPositions) {
    const Corpus corpus = BuildLeadingPrefilterCorpus(/*tail_docs=*/128);
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    query::QueryProfile profile;
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got, &profile).ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs({"lead", "aa_"}));
    EXPECT_EQ(profile.phrase_query_stats.prefix_leading_candidate_docs, 128U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, HalfDfTailKeepsDirectLeadingDecode) {
    const Corpus corpus = BuildLeadingPrefilterCorpus(/*tail_docs=*/512);
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    query::QueryProfile profile;
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got, &profile).ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs({"lead", "aa_"}));
    EXPECT_EQ(profile.phrase_query_stats.prefix_leading_candidate_docs, 1024U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixQuery, NearEqualTailDfKeepsDirectLeadingDecode) {
    const Corpus corpus = BuildNearEqualLeadingAndTailDfCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    query::QueryProfile profile;
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got, &profile).ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs({"lead", "aa_"}));
    EXPECT_EQ(profile.phrase_query_stats.prefix_leading_candidate_docs, 256U);

    std::remove(path.c_str());
}

// ---------------------------------------------------------------------------
// Merged multi-tail path (collect_merged_tail_matches): the per-tail verify+union
// loop was replaced by a single batched, forward-merge sweep. Every case below
// asserts the merged result equals the independent position oracle -- i.e. is
// identical to the old per-tail semantics -- across the 0/1/many-expansion
// trichotomy, max_expansions truncation, an empty expected set, cross-window
// positions and CJK/unicode terms.
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

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<query::PhraseMatch> single_matches;
    ASSERT_TRUE(
            query::phrase_prefix_query_with_frequencies(idx, single_terms, &single_matches).ok());
    EXPECT_EQ(single_matches, (std::vector<query::PhraseMatch> {{.docid = 0, .frequency = 1}}));
    EXPECT_EQ(qinternal::query_test_counters().expected_docids_build, 0U);

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
    EXPECT_EQ(qinternal::query_test_counters().resolved_term_entry_copies, 0U);
    constexpr uint64_t baseline_group_visits = 768U * (768U / 32U);
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, baseline_group_visits);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, NonAdjacentTailGroupsScanStableCandidateSet) {
    const Corpus corpus = BuildWideNonAdjacentTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got).ok());
    EXPECT_TRUE(got.empty());
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, 256U * 3U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, DefaultExpansionCapKeepsSingleRemainingGroupDirect) {
    const Corpus corpus = BuildDefaultExpansionPruneCorpus(/*adjacent_docs=*/256);
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got,
                                           /*max_expansions=*/50)
                        .ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs_capped({"lead", "aa_"}, 50));
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, 1024U * 2U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, SparseFirstGroupMatchScansStableCandidateSet) {
    const Corpus corpus = BuildDefaultExpansionPruneCorpus(/*adjacent_docs=*/1);
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got,
                                           /*max_expansions=*/50)
                        .ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs_capped({"lead", "aa_"}, 50));
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, 1024U * 2U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, AllDocsMatchedBeforeFinalGroupStopsEarly) {
    const Corpus corpus = BuildAllMatchedBeforeFinalGroupCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got).ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs({"lead", "aa_"}));
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, 1024U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, AllDocsMatchedAfterPartialGroupEmitOnce) {
    const Corpus corpus = BuildAllMatchedAfterPartialGroupCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, {"lead", "aa_"}, &got).ok());
    EXPECT_EQ(got, corpus.phrase_prefix_docs({"lead", "aa_"}));
    EXPECT_EQ(qinternal::query_test_counters().prefix_expected_doc_visits, 1024U * 2U);

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, CrossGroupDuplicateMatchesEmitOnce) {
    const Corpus corpus = BuildCrossGroupDuplicateTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_EQ(got, (std::vector<uint32_t> {0}));
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixFrequency, CountsDistinctPhraseStartsAcrossTailGroups) {
    const Corpus corpus = BuildCrossGroupDuplicateTailCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "aa_"};
    for (const auto [cap, expected_frequency] :
         {std::pair {32, 32U}, std::pair {33, 33U}, std::pair {65, 65U}}) {
        std::vector<query::PhraseMatch> matches;
        ASSERT_TRUE(query::phrase_prefix_query_with_frequencies(idx, terms, &matches, nullptr, cap)
                            .ok())
                << "cap=" << cap;
        EXPECT_EQ(matches,
                  (std::vector<query::PhraseMatch> {
                          {.docid = 0, .frequency = static_cast<float>(expected_frequency)}}))
                << "cap=" << cap;
    }

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixFrequency, CountsStackedLeadingTermOccurrences) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.add_token("lead", 0, 0);
    buffer.add_token("lead", 0, 0);
    buffer.add_token("tail_a", 0, 1);

    const std::string path = TempPath();
    WritePostings(&buffer, 1, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    std::vector<query::PhraseMatch> matches;
    ASSERT_TRUE(query::phrase_prefix_query_with_frequencies(idx, {"lead", "tail"}, &matches).ok());
    EXPECT_EQ(matches, (std::vector<query::PhraseMatch> {{.docid = 0, .frequency = 2}}));

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixFrequency, CountsStackedLeadingTermWithSparseMiddleClause) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.add_token("lead", 0, 0);
    buffer.add_token("lead", 0, 0);
    buffer.add_token("middle", 0, 1);
    buffer.add_token("tail_a", 0, 2);
    for (uint32_t docid = 1; docid < 8; ++docid) {
        buffer.add_token("lead", docid, 0);
    }

    const std::string path = TempPath();
    WritePostings(&buffer, 8, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    std::vector<query::PhraseMatch> matches;
    ASSERT_TRUE(
            query::phrase_prefix_query_with_frequencies(idx, {"lead", "middle", "tail"}, &matches)
                    .ok());
    EXPECT_EQ(matches, (std::vector<query::PhraseMatch> {{.docid = 0, .frequency = 2}}));

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixFrequency, CountsOneOccurrenceAcrossSamePositionTailExpansions) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.add_token("lead", 0, 0);
    buffer.add_token("tail_a", 0, 1);
    buffer.add_token("tail_b", 0, 1);

    const std::string path = TempPath();
    WritePostings(&buffer, 1, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    std::vector<query::PhraseMatch> matches;
    ASSERT_TRUE(query::phrase_prefix_query_with_frequencies(idx, {"lead", "tail"}, &matches).ok());
    EXPECT_EQ(matches, (std::vector<query::PhraseMatch> {{.docid = 0, .frequency = 1}}));

    std::remove(path.c_str());
}

TEST(SniiPhrasePrefixMerge, EmptyFinalGroupEmitsMatchesFromEarlierGroup) {
    const Corpus corpus = BuildFinalGroupWithoutCandidatesCorpus();
    const std::string path = TempPath();
    WriteCorpus(corpus, path);

    io::LocalFileReader file;
    SniiSegmentReader segment;
    LogicalIndexReader idx = OpenIndex(&file, &segment, path);

    const std::vector<std::string> terms = {"lead", "aa_"};
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_prefix_query(idx, terms, &got).ok());
    EXPECT_EQ(got, (std::vector<uint32_t> {0}));
    EXPECT_EQ(got, corpus.phrase_prefix_docs(terms));

    std::vector<query::PhraseMatch> matches;
    ASSERT_TRUE(query::phrase_prefix_query_with_frequencies(idx, terms, &matches).ok());
    EXPECT_EQ(matches, (std::vector<query::PhraseMatch> {{.docid = 0, .frequency = 1}}));

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
