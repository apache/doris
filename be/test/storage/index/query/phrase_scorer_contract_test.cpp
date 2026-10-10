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
#include <functional>
#include <memory>
#include <numeric>
#include <random>
#include <vector>

#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/postings/loaded_postings.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/query/phrase/phrase_verifier.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Positions of each distinct term in one document, counting how often the verifier reads them.
struct CountingTermPositions {
    Status operator()(size_t term, index_query::PhrasePositionSpan* span) {
        ++loads[term];
        *span = {positions[term].data(), positions[term].data() + positions[term].size()};
        return Status::OK();
    }

    std::vector<std::vector<uint32_t>> positions;
    std::vector<uint32_t> loads = std::vector<uint32_t>(positions.size());
};

TEST(PhraseVerifierTest, ReadsARepeatedTermOncePerDocument) {
    CountingTermPositions terms {.positions = {{3, 4}, {5}}};
    const std::vector<uint32_t> offsets {0, 1, 2};
    const std::vector<uint64_t> costs {1, 1, 1};
    index_query::PhraseVerifier verifier({0, 0, 1}, offsets, costs, 0, false);

    float frequency = 0.0F;
    ASSERT_TRUE(verifier.verify(std::ref(terms), true, &frequency).ok());
    EXPECT_FLOAT_EQ(frequency, 1.0F);
    EXPECT_EQ(terms.loads, (std::vector<uint32_t> {1, 1}));
}

TEST(PhraseVerifierTest, ReadsTheOtherClausesOnlyAfterTheCheapestPairMatches) {
    CountingTermPositions terms {.positions = {{1}, {2}, {7}, {9}}};
    const std::vector<uint32_t> offsets {0, 1, 2, 3};
    const std::vector<uint64_t> costs {50, 40, 2, 3};
    index_query::PhraseVerifier verifier({0, 1, 2, 3}, offsets, costs, 0, false);

    float frequency = 1.0F;
    ASSERT_TRUE(verifier.verify(std::ref(terms), true, &frequency).ok());
    EXPECT_FLOAT_EQ(frequency, 0.0F);
    EXPECT_EQ(terms.loads, (std::vector<uint32_t> {0, 0, 1, 1}));
}

// A stopword dropped before the first clause leaves offsets that do not start at zero.
TEST(PhraseVerifierTest, CountsOffsetsFromTheFirstClause) {
    CountingTermPositions terms {.positions = {{0}, {1}, {2}}};
    const std::vector<uint32_t> offsets {1, 2, 3};
    const std::vector<uint64_t> costs {1, 1, 1};
    index_query::PhraseVerifier verifier({0, 1, 2}, offsets, costs, 0, false);

    float frequency = 0.0F;
    ASSERT_TRUE(verifier.verify(std::ref(terms), true, &frequency).ok());
    EXPECT_FLOAT_EQ(frequency, 1.0F);
}

TEST(PhraseScorerContractTest, SlopAcceptsOneGapButNotTwoOrATransposition) {
    const std::vector<uint32_t> docs {0, 1, 2, 3};
    PostingsPtr left = std::make_shared<LoadedPostings>(
            docs, std::vector<std::vector<uint32_t>> {{1}, {1}, {3}, {1}});
    PostingsPtr right = std::make_shared<LoadedPostings>(
            docs, std::vector<std::vector<uint32_t>> {{3}, {4}, {2}, {2}});
    const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, left}, {1, right}};

    auto scorer = PhraseScorer<PostingsPtr>::create(terms, nullptr, {.slop = 1}, docs.size());
    std::vector<uint32_t> matches;
    while (scorer->doc() != TERMINATED) {
        matches.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(matches, (std::vector<uint32_t> {0, 3}));
}

TEST(PhraseScorerContractTest, RepeatedSourceNeedsDistinctOccurrences) {
    const std::vector<uint32_t> docs {0, 1};
    PostingsPtr repeated = std::make_shared<LoadedPostings>(
            docs, std::vector<std::vector<uint32_t>> {{4}, {4, 6}});
    const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, repeated}, {1, repeated}};

    auto scorer = PhraseScorer<PostingsPtr>::create(terms, nullptr, {.slop = 1}, docs.size());
    std::vector<uint32_t> matches;
    while (scorer->doc() != TERMINATED) {
        matches.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(matches, (std::vector<uint32_t> {1}));
}

TEST(PhraseScorerContractTest, FractionalPhraseFrequencyReachesBm25) {
    const std::vector<uint32_t> docs {0, 1};
    PostingsPtr left = std::make_shared<LoadedPostings>(
            docs, std::vector<std::vector<uint32_t>> {{1}, {1, 5}});
    PostingsPtr right = std::make_shared<LoadedPostings>(
            docs, std::vector<std::vector<uint32_t>> {{3}, {3, 7}});
    const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, left}, {1, right}};
    auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);

    auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, {.slop = 1}, docs.size());
    ASSERT_EQ(scorer->doc(), 0);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(0.5F, 1));
    ASSERT_EQ(scorer->advance(), 1);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(1.0F, 1));
    EXPECT_EQ(scorer->advance(), TERMINATED);
}

TEST(PhraseScorerContractTest, GappedOffsetsSurvivePostingCostSorting) {
    PostingsPtr first = std::make_shared<LoadedPostings>(
            std::vector<uint32_t> {0, 1, 2}, std::vector<std::vector<uint32_t>> {{10}, {10}, {10}});
    PostingsPtr second = std::make_shared<LoadedPostings>(
            std::vector<uint32_t> {1}, std::vector<std::vector<uint32_t>> {{12}});
    PostingsPtr third = std::make_shared<LoadedPostings>(
            std::vector<uint32_t> {0, 1}, std::vector<std::vector<uint32_t>> {{16}, {16}});
    const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, first}, {2, second}, {5, third}};
    auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);

    auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, {.slop = 1}, 3);
    ASSERT_EQ(scorer->doc(), 1);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(0.5F, 1));
    EXPECT_EQ(scorer->advance(), TERMINATED);
}

TEST(PhraseScorerContractTest, ExactFrequencyUsesFirstClauseMultiplicityAfterCostSorting) {
    PostingsPtr first = std::make_shared<LoadedPostings>(
            std::vector<uint32_t> {0, 1}, std::vector<std::vector<uint32_t>> {{1, 1}, {1}});
    PostingsPtr second = std::make_shared<LoadedPostings>(std::vector<uint32_t> {0},
                                                          std::vector<std::vector<uint32_t>> {{2}});
    const std::vector<std::pair<size_t, PostingsPtr>> terms {{0, first}, {1, second}};
    auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);

    auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, {}, 2);
    ASSERT_EQ(scorer->doc(), 0);
    EXPECT_FLOAT_EQ(scorer->score(), similarity->score(2.0F, 1));
    EXPECT_EQ(scorer->advance(), TERMINATED);
}

TEST(PhraseScorerContractTest, ExactFrequencyKeepsTheFirstClauseAtEveryCostRank) {
    const auto make_postings = [](uint32_t cost, uint32_t extra_doc,
                                  const std::vector<uint32_t>& positions) -> PostingsPtr {
        std::vector<uint32_t> docs {0};
        while (docs.size() < cost) {
            docs.push_back(extra_doc++);
        }
        return std::make_shared<LoadedPostings>(
                docs, std::vector<std::vector<uint32_t>>(docs.size(), positions));
    };
    for (uint32_t first_cost : {1, 2, 3}) {
        SCOPED_TRACE(first_cost);
        PostingsPtr first = make_postings(first_cost, 10, {1, 1, 5, 5, 5});
        PostingsPtr second = make_postings(first_cost == 1 ? 2 : 1, 20, {2, 6, 6, 6});
        PostingsPtr third = make_postings(first_cost == 3 ? 2 : 3, 30, {3, 7});
        const std::vector<std::pair<size_t, PostingsPtr>> terms {
                {0, first}, {1, second}, {2, third}};
        auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);

        auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, {}, 40);
        ASSERT_EQ(scorer->doc(), 0);
        EXPECT_FLOAT_EQ(scorer->score(), similarity->score(5.0F, 1));
        EXPECT_EQ(scorer->advance(), TERMINATED);
    }
}

TEST(PhraseScorerContractTest, ExactFrequencyMatchesPositionMembershipAcrossCostRanks) {
    constexpr uint32_t kDocs = 256;
    std::mt19937 random(137);
    std::vector<std::vector<std::vector<uint32_t>>> positions(3);
    for (uint32_t clause = 0; clause < 3; ++clause) {
        for (uint32_t doc = 0; doc < kDocs; ++doc) {
            std::vector<uint32_t> values;
            const uint32_t count = 1 + random() % 8;
            for (uint32_t i = 0; i < count; ++i) {
                values.push_back(1 + random() % 8 + clause);
            }
            std::ranges::sort(values);
            positions[clause].push_back(std::move(values));
        }
    }
    for (uint32_t first_rank = 0; first_rank < 3; ++first_rank) {
        SCOPED_TRACE(first_rank);
        std::vector<std::pair<size_t, PostingsPtr>> terms;
        for (uint32_t clause = 0; clause < 3; ++clause) {
            const uint32_t extra = (clause + first_rank) % 3;
            std::vector<uint32_t> docs(kDocs);
            std::iota(docs.begin(), docs.end(), 0);
            auto term_positions = positions[clause];
            for (uint32_t i = 0; i < extra; ++i) {
                docs.push_back(kDocs + clause * 3 + i);
                term_positions.push_back({1 + clause});
            }
            terms.emplace_back(clause, std::make_shared<LoadedPostings>(docs, term_positions));
        }
        auto similarity = std::make_shared<BM25Similarity>(2.0F, 8.0F);
        auto scorer = PhraseScorer<PostingsPtr>::create(terms, similarity, {}, kDocs + 9);
        for (uint32_t doc = 0; doc < kDocs; ++doc) {
            SCOPED_TRACE(doc);
            uint32_t frequency = 0;
            for (uint32_t position : positions[0][doc]) {
                const bool matched = std::ranges::find(positions[1][doc], position + 1) !=
                                             positions[1][doc].end() &&
                                     std::ranges::find(positions[2][doc], position + 2) !=
                                             positions[2][doc].end();
                frequency += static_cast<uint32_t>(matched);
            }
            if (frequency == 0) {
                continue;
            }
            ASSERT_EQ(scorer->doc(), doc);
            EXPECT_FLOAT_EQ(scorer->score(), similarity->score(static_cast<float>(frequency), 1));
            scorer->advance();
        }
        EXPECT_EQ(scorer->doc(), TERMINATED);
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
