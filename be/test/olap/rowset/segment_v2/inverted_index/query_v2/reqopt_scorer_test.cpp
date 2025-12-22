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

#include "olap/rowset/segment_v2/inverted_index/query_v2/reqopt_scorer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

namespace {

class VectorScorer final : public Scorer {
public:
    VectorScorer(std::vector<uint32_t> docs, float const_score = 1.0F, uint32_t size_hint_val = 0)
            : _docs(std::move(docs)), _const_score(const_score), _size_hint_val(size_hint_val) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            std::sort(_docs.begin(), _docs.end());
            _current_doc = _docs[0];
        }
        if (_size_hint_val == 0) {
            _size_hint_val = static_cast<uint32_t>(_docs.size());
        }
    }

    uint32_t advance() override {
        if (_docs.empty() || _index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        ++_index;
        if (_index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        _current_doc = _docs[_index];
        return _current_doc;
    }

    uint32_t seek(uint32_t target) override {
        while (_current_doc < target && _current_doc != TERMINATED) {
            advance();
        }
        return _current_doc;
    }

    uint32_t doc() const override { return _current_doc; }
    uint32_t size_hint() const override { return _size_hint_val; }
    float score() override { return _const_score; }

private:
    std::vector<uint32_t> _docs;
    float _const_score;
    size_t _index = 0;
    uint32_t _current_doc = TERMINATED;
    uint32_t _size_hint_val = 0;
};

std::vector<uint32_t> sample_with_seed(uint32_t max_doc, double probability, uint32_t seed) {
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::vector<uint32_t> result;
    for (uint32_t i = 0; i < max_doc; ++i) {
        if (dis(gen) < probability) {
            result.push_back(i);
        }
    }
    return result;
}

} // namespace

class RequiredOptionalScorerTest : public ::testing::Test {};

TEST_F(RequiredOptionalScorerTest, EmptyOptional) {
    std::vector<uint32_t> req_docs {1, 3, 7};
    auto req_scorer = std::make_shared<VectorScorer>(req_docs, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(req_docs, docs);
}

TEST_F(RequiredOptionalScorerTest, BasicScoring) {
    auto req_scorer =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 7, 8, 9, 10, 13, 15}, 1.0F);
    auto opt_scorer =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 7, 11, 12, 15}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(3u, scorer->advance());
    EXPECT_EQ(3u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(7u, scorer->advance());
    EXPECT_EQ(7u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(8u, scorer->advance());
    EXPECT_EQ(8u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(9u, scorer->advance());
    EXPECT_EQ(9u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(10u, scorer->advance());
    EXPECT_EQ(10u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(13u, scorer->advance());
    EXPECT_EQ(13u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(15u, scorer->advance());
    EXPECT_EQ(15u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, SeekFunctionality) {
    auto req_scorer =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 7, 8, 9, 10, 13, 15}, 1.0F);
    auto opt_scorer =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 7, 11, 12, 15}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    EXPECT_EQ(7u, scorer->seek(7));
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    EXPECT_EQ(13u, scorer->seek(12));
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
}

TEST_F(RequiredOptionalScorerTest, EmptyRequired) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(RequiredOptionalScorerTest, BothEmpty) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(RequiredOptionalScorerTest, NoIntersection) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 5, 7, 9}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 4, 6, 8, 10}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scores.push_back(scorer->score());
        scorer->advance();
    }

    std::vector<uint32_t> expected_docs {1, 3, 5, 7, 9};
    std::vector<float> expected_scores {1.0F, 1.0F, 1.0F, 1.0F, 1.0F};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores, scores);
}

TEST_F(RequiredOptionalScorerTest, FullIntersection) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 5}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 5}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scores.push_back(scorer->score());
        scorer->advance();
    }

    std::vector<uint32_t> expected_docs {1, 3, 5};
    std::vector<float> expected_scores {2.0F, 2.0F, 2.0F};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores, scores);
}

TEST_F(RequiredOptionalScorerTest, OptionalLargerThanRequired) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(10u, scorer->advance());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, DifferentScores) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}, 2.5F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15}, 1.5F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(2.5F, scorer->score());

    EXPECT_EQ(5u, scorer->advance());
    EXPECT_FLOAT_EQ(4.0F, scorer->score());

    EXPECT_EQ(10u, scorer->advance());
    EXPECT_FLOAT_EQ(4.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, ScoreCaching) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    scorer->advance();
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
}

TEST_F(RequiredOptionalScorerTest, AdvanceAfterTerminated) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, SeekAfterTerminated) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->seek(100));
}

TEST_F(RequiredOptionalScorerTest, SeekToCurrentDoc) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 15}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->seek(5));
    EXPECT_EQ(5u, scorer->seek(3));
}

TEST_F(RequiredOptionalScorerTest, SeekPastEnd) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(TERMINATED, scorer->seek(100));
}

TEST_F(RequiredOptionalScorerTest, SizeHintDelegation) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3}, 1.0F, 100);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2}, 1.0F, 50);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(100u, scorer->size_hint());
}

TEST_F(RequiredOptionalScorerTest, DocDoesNotAdvance) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->doc());

    scorer->advance();
    EXPECT_EQ(10u, scorer->doc());
    EXPECT_EQ(10u, scorer->doc());
}

TEST_F(RequiredOptionalScorerTest, DoNothingCombiner) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}, 2.5F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10}, 1.5F);
    auto combiner = std::make_shared<DoNothingCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(0.0F, scorer->score());

    EXPECT_EQ(5u, scorer->advance());
    EXPECT_FLOAT_EQ(0.0F, scorer->score());
}

TEST_F(RequiredOptionalScorerTest, LargeDocIds) {
    auto req_scorer = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {100000000, 200000000, 300000000}, 1.0F);
    auto opt_scorer =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {200000000, 400000000}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(100000000u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(200000000u, scorer->advance());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(300000000u, scorer->advance());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, RandomData) {
    auto req_docs = sample_with_seed(10000, 0.02, 1);
    auto opt_docs = sample_with_seed(10000, 0.02, 2);

    auto req_scorer = std::make_shared<VectorScorer>(req_docs, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(opt_docs, 1.0F);
    auto combiner = std::make_shared<DoNothingCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    std::vector<uint32_t> actual;
    while (scorer->doc() != TERMINATED) {
        actual.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(req_docs, actual);
}

TEST_F(RequiredOptionalScorerTest, RandomDataSeek) {
    auto req_docs = sample_with_seed(10000, 0.02, 1);
    auto opt_docs = sample_with_seed(10000, 0.02, 2);
    auto seek_targets = sample_with_seed(10000, 0.001, 3);

    for (uint32_t target : seek_targets) {
        auto req_scorer = std::make_shared<VectorScorer>(req_docs, 1.0F);
        auto opt_scorer = std::make_shared<VectorScorer>(opt_docs, 1.0F);
        auto combiner = std::make_shared<DoNothingCombiner>();

        auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                    std::move(combiner));

        auto it = std::lower_bound(req_docs.begin(), req_docs.end(), target);
        uint32_t expected_doc = (it == req_docs.end()) ? TERMINATED : *it;

        uint32_t actual_doc = scorer->seek(target);
        EXPECT_EQ(expected_doc, actual_doc) << "Failed for target: " << target;
    }
}

TEST_F(RequiredOptionalScorerTest, InterleavedAdvanceAndSeek) {
    auto req_scorer = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 20, 35, 50}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(5u, scorer->advance());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(15u, scorer->seek(12));
    EXPECT_FLOAT_EQ(1.0F, scorer->score());

    EXPECT_EQ(20u, scorer->advance());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(35u, scorer->seek(35));
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(40u, scorer->advance());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
}

TEST_F(RequiredOptionalScorerTest, SingleDocRequired) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {42}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {42}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(42u, scorer->doc());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, SingleDocRequiredNotMatched) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {42}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_EQ(42u, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(RequiredOptionalScorerTest, ScoreCacheResetOnAdvance) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    scorer->advance();
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
}

TEST_F(RequiredOptionalScorerTest, ScoreCacheResetOnSeek) {
    auto req_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}, 1.0F);
    auto opt_scorer = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 10}, 1.0F);
    auto combiner = std::make_shared<SumCombiner>();

    auto scorer = make_required_optional_scorer(std::move(req_scorer), std::move(opt_scorer),
                                                std::move(combiner));

    EXPECT_FLOAT_EQ(2.0F, scorer->score());
    scorer->seek(5);
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    scorer->seek(10);
    EXPECT_FLOAT_EQ(2.0F, scorer->score());
}

} // namespace doris::segment_v2::inverted_index::query_v2
