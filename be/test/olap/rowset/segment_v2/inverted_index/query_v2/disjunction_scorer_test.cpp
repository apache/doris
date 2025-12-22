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

#include "olap/rowset/segment_v2/inverted_index/query_v2/disjunction_scorer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

namespace {

class VectorScorer final : public Scorer {
public:
    VectorScorer(std::vector<uint32_t> docs, std::vector<float> scores = {},
                 uint32_t size_hint_val = 0)
            : _docs(std::move(docs)), _scores(std::move(scores)), _size_hint_val(size_hint_val) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            std::sort(_docs.begin(), _docs.end());
            _current_doc = _docs[0];
        }
        if (_scores.size() != _docs.size()) {
            _scores.resize(_docs.size(), 1.0F);
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

    float score() override {
        if (_index >= _scores.size()) {
            return 0.0F;
        }
        return _scores[_index];
    }

private:
    std::vector<uint32_t> _docs;
    std::vector<float> _scores;
    size_t _index = 0;
    uint32_t _current_doc = TERMINATED;
    uint32_t _size_hint_val = 0;
};

std::vector<uint32_t> compute_expected(const std::vector<std::vector<uint32_t>>& arrays,
                                       size_t pass_line) {
    std::map<uint32_t, size_t> counts;
    for (const auto& array : arrays) {
        for (uint32_t element : array) {
            counts[element]++;
        }
    }
    std::vector<uint32_t> result;
    for (const auto& [element, count] : counts) {
        if (count >= pass_line) {
            result.push_back(element);
        }
    }
    return result;
}

} // namespace

class DisjunctionScorerTest : public ::testing::Test {};

TEST_F(DisjunctionScorerTest, BasicDisjunctionMinMatch2) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3333, 100000000}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 100000000}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 100000000}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 2, 100000000};
    EXPECT_EQ(expected, docs);
}

TEST_F(DisjunctionScorerTest, BasicDisjunctionMinMatch3) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3333, 100000000}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 100000000}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 100000000}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 3);
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 100000000};
    EXPECT_EQ(expected, docs);
}

TEST_F(DisjunctionScorerTest, NoIntersection) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {8}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {3, 4, 0xC0FFEE}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 100000000}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(DisjunctionScorerTest, ScoreCalculation) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 4},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {1.0F, 1.0F}));

    auto combiner = std::make_shared<SumCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 3);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(5.0F, scorer->score());

    EXPECT_EQ(2u, scorer->advance());
    EXPECT_FLOAT_EQ(3.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(DisjunctionScorerTest, ScoreCalculationCornerCase) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3},
                                                     std::vector<float> {1.0F, 1.0F}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3},
                                                     std::vector<float> {1.0F, 1.0F}));

    auto combiner = std::make_shared<SumCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(3.0F, scorer->score());

    EXPECT_EQ(3u, scorer->advance());
    EXPECT_FLOAT_EQ(2.0F, scorer->score());

    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(DisjunctionScorerTest, EmptyScorers) {
    std::vector<ScorerPtr> scorers;
    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(DisjunctionScorerTest, MinMatchExceedsScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 5);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(DisjunctionScorerTest, AllEmptyScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(DisjunctionScorerTest, SeekFunctionality) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10, 20}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 15, 20}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10, 20}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(10u, scorer->seek(8));
    EXPECT_EQ(20u, scorer->seek(15));
    EXPECT_EQ(TERMINATED, scorer->seek(100));
}

TEST_F(DisjunctionScorerTest, SeekToCurrentDoc) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->seek(5));
    EXPECT_EQ(5u, scorer->seek(3));
}

TEST_F(DisjunctionScorerTest, SizeHint) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {}, 100));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {}, 50));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2},
                                                     std::vector<float> {}, 200));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(200u, scorer->size_hint());
}

TEST_F(DisjunctionScorerTest, AdvanceAfterTerminated) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(DisjunctionScorerTest, SeekAfterTerminated) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->seek(100));
}

TEST_F(DisjunctionScorerTest, NullScorersAreIgnored) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}));
    scorers.push_back(nullptr);
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10}));
    scorers.push_back(nullptr);

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 5, 10};
    EXPECT_EQ(expected, docs);
}

TEST_F(DisjunctionScorerTest, SingleDocMultipleScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {42}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {42}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {42}));
    scorers.push_back(std::make_shared<VectorScorer>(std::vector<uint32_t> {42}));

    auto combiner = std::make_shared<SumCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 3);
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(42u, scorer->doc());
    EXPECT_FLOAT_EQ(4.0F, scorer->score());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(DisjunctionScorerTest, LargeDataSet) {
    std::vector<std::vector<uint32_t>> data;
    for (int i = 0; i < 100; ++i) {
        std::vector<uint32_t> docs;
        for (uint32_t j = i; j < 10000; j += 100) {
            docs.push_back(j);
        }
        data.push_back(docs);
    }

    std::vector<ScorerPtr> scorers;
    for (const auto& docs : data) {
        scorers.push_back(std::make_shared<VectorScorer>(docs));
    }

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto scorer = make_disjunction(std::move(scorers), combiner, 2);
    ASSERT_NE(nullptr, scorer);

    auto expected = compute_expected(data, 2);
    std::vector<uint32_t> actual;
    while (scorer->doc() != TERMINATED) {
        actual.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(expected, actual);
}

} // namespace doris::segment_v2::inverted_index::query_v2