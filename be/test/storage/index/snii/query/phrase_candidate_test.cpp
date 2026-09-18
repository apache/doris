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

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "roaring/roaring.hh"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

// Phrase queries restricted to a scan candidate set must return exactly the
// unrestricted result intersected with the candidates, for every phrase shape,
// and must skip postings that cannot reach a candidate.
namespace doris::snii::query {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

constexpr uint32_t kDocCount = 24000;
constexpr int32_t kMaxExpansions = 50;

struct WeightedToken {
    const char* token;
    uint32_t weight;
};

// Weights make "alpha" and "beta" windowed (df far above kSlimDfThreshold) and
// keep "echo" and "ecru" rare, so candidates drive both windowed and slim postings
// and the "ec" prefix expands to a small tail union.
constexpr WeightedToken kVocabulary[] = {
        {.token = "alpha", .weight = 60}, {.token = "beta", .weight = 40},
        {.token = "bravo", .weight = 10}, {.token = "bronze", .weight = 10},
        {.token = "brown", .weight = 20}, {.token = "charlie", .weight = 30},
        {.token = "delta", .weight = 20}, {.token = "echo", .weight = 1},
        {.token = "ecru", .weight = 1}};

class SniiPhraseCandidateTest : public ::testing::Test {
protected:
    void SetUp() override {
        build_corpus();
        write_index();
    }

    void build_corpus() {
        std::mt19937 rng(20260918);
        std::vector<uint32_t> weights;
        for (const auto& entry : kVocabulary) {
            weights.push_back(entry.weight);
        }
        std::discrete_distribution<size_t> pick_token(weights.begin(), weights.end());
        std::uniform_int_distribution<uint32_t> pick_length(3, 8);
        _docs.resize(kDocCount);
        for (uint32_t docid = 0; docid < kDocCount; ++docid) {
            const uint32_t length = pick_length(rng);
            for (uint32_t position = 0; position < length; ++position) {
                _docs[docid].emplace_back(kVocabulary[pick_token(rng)].token);
            }
        }
    }

    void write_index() {
        std::map<std::string, std::vector<PostingDoc>> postings;
        for (uint32_t docid = 0; docid < kDocCount; ++docid) {
            for (uint32_t position = 0; position < _docs[docid].size(); ++position) {
                auto& docs = postings[_docs[docid][position]];
                if (docs.empty() || docs.back().docid != docid) {
                    docs.push_back({.docid = docid, .positions = {}});
                }
                docs.back().positions.push_back(position);
            }
        }
        writer::SniiIndexInput input;
        input.index_id = 1;
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = kDocCount;
        for (auto& [term, docs] : postings) {
            input.terms.push_back(make_term(term, std::move(docs)));
        }
        writer::SniiCompoundWriter writer(&_file);
        assert_ok(writer.add_logical_index(input));
        assert_ok(writer.finish());
        assert_ok(reader::SniiSegmentReader::open(&_file, &_segment));
        assert_ok(_segment.open_index(input.index_id, input.index_suffix, &_index));
    }

    uint32_t df(const std::string& term) const {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(_index.lookup(term, &found, &entry, &frq_base, &prx_base));
        EXPECT_TRUE(found) << term;
        return entry.df;
    }

    std::vector<uint32_t> exact_phrase_oracle(const std::vector<std::string>& phrase) const {
        std::vector<uint32_t> out;
        for (uint32_t docid = 0; docid < kDocCount; ++docid) {
            const auto& tokens = _docs[docid];
            for (size_t start = 0; start + phrase.size() <= tokens.size(); ++start) {
                if (std::equal(phrase.begin(), phrase.end(), tokens.begin() + start)) {
                    out.push_back(docid);
                    break;
                }
            }
        }
        return out;
    }

    MemoryFile _file;
    reader::SniiSegmentReader _segment;
    reader::LogicalIndexReader _index;
    std::vector<std::vector<std::string>> _docs;
};

roaring::Roaring sample_candidates(double ratio, uint32_t seed) {
    std::mt19937 rng(seed);
    std::bernoulli_distribution keep(ratio);
    roaring::Roaring candidates;
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        if (keep(rng)) {
            candidates.add(docid);
        }
    }
    return candidates;
}

roaring::Roaring docid_range(uint32_t begin, uint32_t end) {
    roaring::Roaring out;
    out.addRange(begin, end);
    return out;
}

// Candidate shapes: empty, single doc, a contiguous window-sized range, random
// sets from sparse to dense, and the full domain.
std::vector<roaring::Roaring> candidate_sets() {
    std::vector<roaring::Roaring> sets = {roaring::Roaring(), docid_range(12345, 12346),
                                          docid_range(5000, 5600)};
    uint32_t seed = 7;
    for (double ratio : {0.001, 0.01, 0.1, 0.3, 0.5}) {
        sets.push_back(sample_candidates(ratio, seed++));
    }
    sets.push_back(docid_range(0, kDocCount));
    return sets;
}

std::vector<uint32_t> intersect(const std::vector<uint32_t>& docids,
                                const roaring::Roaring& candidates) {
    std::vector<uint32_t> out;
    std::ranges::copy_if(docids, std::back_inserter(out),
                         [&](uint32_t docid) { return candidates.contains(docid); });
    return out;
}

std::vector<PhraseMatch> restrict_matches(const std::vector<PhraseMatch>& matches,
                                          const roaring::Roaring& candidates) {
    std::vector<PhraseMatch> out;
    std::ranges::copy_if(matches, std::back_inserter(out), [&](const PhraseMatch& match) {
        return candidates.contains(match.docid);
    });
    return out;
}

TEST_F(SniiPhraseCandidateTest, CorpusExercisesWindowedPostings) {
    EXPECT_GE(df("alpha"), format::kSlimDfThreshold);
    EXPECT_GE(df("beta"), format::kSlimDfThreshold);
    EXPECT_LT(df("echo"), df("beta"));

    std::vector<uint32_t> docids;
    assert_ok(phrase_query(_index, {"alpha", "beta"}, &docids));
    EXPECT_EQ(docids, exact_phrase_oracle({"alpha", "beta"}));
    EXPECT_GT(docids.size(), 1000U);
}

TEST_F(SniiPhraseCandidateTest, PhraseRestrictedEqualsUnrestrictedIntersection) {
    const std::vector<std::pair<std::vector<std::string>, PhraseQueryOptions>> cases = {
            {{"alpha", "beta"}, {}},
            {{"alpha", "beta", "alpha"}, {}},
            {{"echo", "alpha", "beta"}, {}},
            {{"alpha", "charlie"}, {.slop = 2, .ordered = false}},
            {{"alpha", "charlie", "beta"}, {.slop = 2, .ordered = true}}};
    for (const auto& [terms, options] : cases) {
        std::vector<uint32_t> unrestricted;
        assert_ok(phrase_query(_index, terms, &unrestricted, nullptr, options));
        for (const auto& candidates : candidate_sets()) {
            PhraseQueryOptions restricted_options = options;
            restricted_options.candidates = &candidates;
            std::vector<uint32_t> restricted;
            assert_ok(phrase_query(_index, terms, &restricted, nullptr, restricted_options));
            EXPECT_EQ(restricted, intersect(unrestricted, candidates))
                    << terms.front() << " slop=" << options.slop
                    << " candidates=" << candidates.cardinality();
        }
    }
}

TEST_F(SniiPhraseCandidateTest, PhraseFrequenciesRestrictedToCandidates) {
    const std::vector<std::pair<std::vector<std::string>, PhraseQueryOptions>> cases = {
            {{"alpha", "beta"}, {}}, {{"alpha", "charlie"}, {.slop = 2, .ordered = false}}};
    for (const auto& [terms, options] : cases) {
        std::vector<PhraseMatch> unrestricted;
        assert_ok(phrase_query_with_frequencies(_index, terms, &unrestricted, nullptr, options));
        for (const auto& candidates : candidate_sets()) {
            PhraseQueryOptions restricted_options = options;
            restricted_options.candidates = &candidates;
            std::vector<PhraseMatch> restricted;
            assert_ok(phrase_query_with_frequencies(_index, terms, &restricted, nullptr,
                                                    restricted_options));
            EXPECT_EQ(restricted, restrict_matches(unrestricted, candidates))
                    << terms.front() << " candidates=" << candidates.cardinality();
        }
    }
}

TEST_F(SniiPhraseCandidateTest, PhrasePrefixRestrictedEqualsUnrestrictedIntersection) {
    const std::vector<std::vector<std::string>> cases = {
            {"alpha", "br"}, {"alpha", "brow"}, {"charlie", "alpha", "b"}, {"echo", "br"}};
    for (const auto& terms : cases) {
        std::vector<uint32_t> unrestricted;
        assert_ok(phrase_prefix_query(_index, terms, &unrestricted, nullptr, kMaxExpansions));
        std::vector<PhraseMatch> unrestricted_matches;
        assert_ok(phrase_prefix_query_with_frequencies(_index, terms, &unrestricted_matches,
                                                       nullptr, kMaxExpansions));
        for (const auto& candidates : candidate_sets()) {
            std::vector<uint32_t> restricted;
            assert_ok(phrase_prefix_query(
                    _index, terms, &restricted, nullptr,
                    {.max_expansions = kMaxExpansions, .candidates = &candidates}));
            EXPECT_EQ(restricted, intersect(unrestricted, candidates))
                    << terms.front() << " candidates=" << candidates.cardinality();
            std::vector<PhraseMatch> restricted_matches;
            assert_ok(phrase_prefix_query_with_frequencies(
                    _index, terms, &restricted_matches, nullptr,
                    {.max_expansions = kMaxExpansions, .candidates = &candidates}));
            EXPECT_EQ(restricted_matches, restrict_matches(unrestricted_matches, candidates))
                    << terms.front() << " candidates=" << candidates.cardinality();
        }
    }
}

// Dense candidates must not disable the tail-union prefilter: with a rare tail the
// restricted phrase prefix decodes no more leading candidates than the full query.
TEST_F(SniiPhraseCandidateTest, DenseCandidatesKeepPrefixTailPrefilter) {
    const std::vector<std::string> terms = {"alpha", "ec"};
    ASSERT_LT((df("echo") + df("ecru")) * 8, df("alpha"));
    QueryProfile full_profile;
    std::vector<uint32_t> unrestricted;
    assert_ok(phrase_prefix_query(_index, terms, &unrestricted, &full_profile, kMaxExpansions));

    const roaring::Roaring candidates = sample_candidates(0.5, 99);
    QueryProfile restricted_profile;
    std::vector<uint32_t> restricted;
    assert_ok(phrase_prefix_query(_index, terms, &restricted, &restricted_profile,
                                  {.max_expansions = kMaxExpansions, .candidates = &candidates}));

    EXPECT_EQ(restricted, intersect(unrestricted, candidates));
    EXPECT_LE(restricted_profile.phrase_query_stats.prefix_leading_candidate_docs,
              full_profile.phrase_query_stats.prefix_leading_candidate_docs);
}

// A few candidates inside one window must not pull the windowed terms' other
// windows: the restricted query reads a small fraction of the bytes the full
// query reads. The restricted query runs first so it cannot profit from any
// block the full query would have loaded.
TEST_F(SniiPhraseCandidateTest, SelectiveCandidatesSkipWindowedPostings) {
    const std::vector<std::string> terms = {"alpha", "beta"};
    roaring::Roaring candidates;
    for (uint32_t docid = 1000; docid < 1100; docid += 7) {
        candidates.add(docid);
    }

    _file.clear_reads();
    std::vector<uint32_t> restricted;
    assert_ok(phrase_query(_index, terms, &restricted, nullptr, {.candidates = &candidates}));
    const size_t restricted_bytes = _file.read_bytes();

    _file.clear_reads();
    std::vector<uint32_t> unrestricted;
    assert_ok(phrase_query(_index, terms, &unrestricted, nullptr, {}));
    const size_t unrestricted_bytes = _file.read_bytes();

    EXPECT_EQ(restricted, intersect(unrestricted, candidates));
    EXPECT_LT(restricted_bytes * 4, unrestricted_bytes)
            << "restricted=" << restricted_bytes << " unrestricted=" << unrestricted_bytes;

    _file.clear_reads();
    std::vector<uint32_t> restricted_prefix;
    assert_ok(phrase_prefix_query(_index, {"alpha", "br"}, &restricted_prefix, nullptr,
                                  {.max_expansions = kMaxExpansions, .candidates = &candidates}));
    const size_t restricted_prefix_bytes = _file.read_bytes();
    _file.clear_reads();
    std::vector<uint32_t> unrestricted_prefix;
    assert_ok(phrase_prefix_query(_index, {"alpha", "br"}, &unrestricted_prefix, nullptr,
                                  kMaxExpansions));
    const size_t unrestricted_prefix_bytes = _file.read_bytes();

    EXPECT_EQ(restricted_prefix, intersect(unrestricted_prefix, candidates));
    EXPECT_LT(restricted_prefix_bytes * 2, unrestricted_prefix_bytes)
            << "restricted=" << restricted_prefix_bytes
            << " unrestricted=" << unrestricted_prefix_bytes;
}

} // namespace
} // namespace doris::snii::query
