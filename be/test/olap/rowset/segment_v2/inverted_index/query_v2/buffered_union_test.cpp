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

#include "olap/rowset/segment_v2/inverted_index/query_v2/union/buffered_union.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

// HORIZON constant used in buffered_union.cpp
constexpr uint32_t HORIZON = 64 * 64; // 4096

// Mock Scorer for testing
class MockScorer : public Scorer {
public:
    MockScorer(std::vector<uint32_t> docs, std::vector<float> scores = {},
               uint32_t size_hint_val = 0)
            : _docs(std::move(docs)), _scores(std::move(scores)), _size_hint_val(size_hint_val) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            std::ranges::sort(_docs);
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
        if (_docs.empty() || _index >= _docs.size()) {
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        if (_current_doc >= target) {
            return _current_doc;
        }
        auto it = std::lower_bound(_docs.begin() + static_cast<ptrdiff_t>(_index), _docs.end(),
                                   target);
        if (it == _docs.end()) {
            _index = _docs.size();
            _current_doc = TERMINATED;
            return TERMINATED;
        }
        _index = static_cast<size_t>(it - _docs.begin());
        _current_doc = *it;
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

using MockScorerPtr = std::shared_ptr<MockScorer>;

} // anonymous namespace

class BufferedUnionTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    // Helper to collect all docs from a scorer
    std::vector<uint32_t> collect_all_docs(ScorerPtr scorer) {
        std::vector<uint32_t> result;
        while (scorer->doc() != TERMINATED) {
            result.push_back(scorer->doc());
            if (scorer->advance() == TERMINATED) {
                break;
            }
        }
        return result;
    }

    // Helper to collect docs and scores
    std::pair<std::vector<uint32_t>, std::vector<float>> collect_docs_and_scores(ScorerPtr scorer) {
        std::vector<uint32_t> docs;
        std::vector<float> scores;
        while (scorer->doc() != TERMINATED) {
            docs.push_back(scorer->doc());
            scores.push_back(scorer->score());
            if (scorer->advance() == TERMINATED) {
                break;
            }
        }
        return {docs, scores};
    }
};

TEST_F(BufferedUnionTest, MakeBufferedUnionWithEmptyScorers) {
    std::vector<ScorerPtr> scorers;
    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), TERMINATED);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
}

TEST_F(BufferedUnionTest, MakeBufferedUnionWithNullScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(nullptr);
    scorers.push_back(nullptr);

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

TEST_F(BufferedUnionTest, MakeBufferedUnionWithAllTerminatedScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

TEST_F(BufferedUnionTest, MakeBufferedUnionWithMixedNullAndValid) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(nullptr);
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10}));
    scorers.push_back(nullptr);
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 5, 10}));
}

TEST_F(BufferedUnionTest, MakeBufferedUnionWithDoNothingCombiner) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 3, 5}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 4, 6}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 2, 3, 4, 5, 6}));

    EXPECT_FLOAT_EQ(union_scorer->score(), 0.0F);
}

TEST_F(BufferedUnionTest, BasicAdvanceSingleScorer) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15, 20}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 1);
    EXPECT_EQ(union_scorer->advance(), 5);
    EXPECT_EQ(union_scorer->advance(), 10);
    EXPECT_EQ(union_scorer->advance(), 15);
    EXPECT_EQ(union_scorer->advance(), 20);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
}

TEST_F(BufferedUnionTest, BasicAdvanceTwoDisjointScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 3, 5}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 4, 6}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 2, 3, 4, 5, 6}));
}

TEST_F(BufferedUnionTest, BasicAdvanceTwoOverlappingScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 3, 5, 7}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {3, 5, 9}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 3, 5, 7, 9}));
}

TEST_F(BufferedUnionTest, BasicAdvanceMultipleScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 10, 20}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {5, 15, 25}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {3, 13, 23}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 3, 5, 10, 13, 15, 20, 23, 25}));
}

TEST_F(BufferedUnionTest, BasicAdvanceIdenticalScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto docs = collect_all_docs(union_scorer);
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 5, 10}));
}

TEST_F(BufferedUnionTest, SeekToExactDoc) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 1);
    EXPECT_EQ(union_scorer->seek(10), 10);
    EXPECT_EQ(union_scorer->doc(), 10);
}

TEST_F(BufferedUnionTest, SeekToNonExistentDoc) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 6, 12, 16}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 1);
    EXPECT_EQ(union_scorer->seek(7), 10);
    EXPECT_EQ(union_scorer->doc(), 10);
}

TEST_F(BufferedUnionTest, SeekBeyondAllDocs) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 6, 12}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->seek(100), TERMINATED);
    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

TEST_F(BufferedUnionTest, SeekToCurrentDoc) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 1);
    EXPECT_EQ(union_scorer->advance(), 5);
    EXPECT_EQ(union_scorer->doc(), 5);

    EXPECT_EQ(union_scorer->seek(5), 5);
    EXPECT_EQ(union_scorer->doc(), 5);
}

TEST_F(BufferedUnionTest, SeekBackwards) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->seek(10), 10);
    EXPECT_EQ(union_scorer->doc(), 10);

    EXPECT_EQ(union_scorer->seek(5), 10);
    EXPECT_EQ(union_scorer->doc(), 10);
}

TEST_F(BufferedUnionTest, SeekThenAdvance) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 5, 10, 15, 20}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 6, 12, 16, 22}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->seek(8), 10);
    EXPECT_EQ(union_scorer->advance(), 12);
    EXPECT_EQ(union_scorer->advance(), 15);
    EXPECT_EQ(union_scorer->advance(), 16);
}

TEST_F(BufferedUnionTest, SeekWithinBuffer) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs;
    for (uint32_t i = 0; i < 100; i += 5) {
        docs.push_back(i);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 0);
    EXPECT_EQ(union_scorer->seek(50), 50);
    EXPECT_EQ(union_scorer->doc(), 50);
    EXPECT_EQ(union_scorer->advance(), 55);
}

TEST_F(BufferedUnionTest, SeekOutsideBuffer) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs = {0, 100, 5000, 5100, 10000};
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 0);
    EXPECT_EQ(union_scorer->seek(5000), 5000);
    EXPECT_EQ(union_scorer->doc(), 5000);
    EXPECT_EQ(union_scorer->advance(), 5100);
}

TEST_F(BufferedUnionTest, SeekFarOutsideBufferToTerminated) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs = {0, 100, 200};
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->seek(100000), TERMINATED);
    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

TEST_F(BufferedUnionTest, CrossHorizonBoundary) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs;
    for (uint32_t i = 0; i <= HORIZON + 500; i += 100) {
        docs.push_back(i);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto collected = collect_all_docs(union_scorer);
    EXPECT_EQ(collected, docs);
}

TEST_F(BufferedUnionTest, DocAtExactHorizonBoundary) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs = {0, HORIZON - 1, HORIZON, HORIZON + 1, HORIZON * 2};
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto collected = collect_all_docs(union_scorer);
    EXPECT_EQ(collected, docs);
}

TEST_F(BufferedUnionTest, MultipleScorersAcrossHorizon) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {0, 1000, HORIZON + 100}));
    scorers.push_back(
            std::make_shared<MockScorer>(std::vector<uint32_t> {500, HORIZON - 1, HORIZON + 500}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto collected = collect_all_docs(union_scorer);
    std::vector<uint32_t> expected = {0, 500, 1000, HORIZON - 1, HORIZON + 100, HORIZON + 500};
    EXPECT_EQ(collected, expected);
}

TEST_F(BufferedUnionTest, ScoringWithSumCombiner) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 3, 5},
                                                   std::vector<float> {1.0F, 2.0F, 3.0F}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 3, 6},
                                                   std::vector<float> {0.5F, 1.5F, 2.5F}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto [docs, scores] = collect_docs_and_scores(union_scorer);

    std::vector<uint32_t> expected_docs = {1, 2, 3, 5, 6};
    std::vector<float> expected_scores = {1.0F, 0.5F, 3.5F, 3.0F, 2.5F};

    EXPECT_EQ(docs, expected_docs);
    ASSERT_EQ(scores.size(), expected_scores.size());
    for (size_t i = 0; i < scores.size(); ++i) {
        EXPECT_FLOAT_EQ(scores[i], expected_scores[i]);
    }
}

TEST_F(BufferedUnionTest, ScoringThreeOverlappingScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(
            std::make_shared<MockScorer>(std::vector<uint32_t> {5}, std::vector<float> {1.0F}));
    scorers.push_back(
            std::make_shared<MockScorer>(std::vector<uint32_t> {5}, std::vector<float> {2.0F}));
    scorers.push_back(
            std::make_shared<MockScorer>(std::vector<uint32_t> {5}, std::vector<float> {3.0F}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 5);
    EXPECT_FLOAT_EQ(union_scorer->score(), 6.0F);
}

TEST_F(BufferedUnionTest, ScoringWithDoNothingCombiner) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 3},
                                                   std::vector<float> {5.0F, 10.0F}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, 3},
                                                   std::vector<float> {3.0F, 7.0F}));

    auto combiner = std::make_shared<DoNothingCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    while (union_scorer->doc() != TERMINATED) {
        EXPECT_FLOAT_EQ(union_scorer->score(), 0.0F);
        union_scorer->advance();
    }
}

TEST_F(BufferedUnionTest, SizeHintReturnsMaxOfScorers) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, HORIZON + 100},
                                                   std::vector<float> {}, 10));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {2, HORIZON + 200},
                                                   std::vector<float> {}, 20));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {3, HORIZON + 300},
                                                   std::vector<float> {}, 15));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->size_hint(), 20);
}

TEST_F(BufferedUnionTest, SizeHintSingleScorer) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, HORIZON + 100},
                                                   std::vector<float> {}, 100));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->size_hint(), 100);
}

TEST_F(BufferedUnionTest, SingleDocScorer) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {42}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 42);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
}

TEST_F(BufferedUnionTest, LargeDocIds) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs = {1000000, 2000000, 3000000};
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto collected = collect_all_docs(union_scorer);
    EXPECT_EQ(collected, docs);
}

TEST_F(BufferedUnionTest, ConsecutiveDocIds) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs;
    for (uint32_t i = 0; i < 100; ++i) {
        docs.push_back(i);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    auto collected = collect_all_docs(union_scorer);
    EXPECT_EQ(collected, docs);
}

TEST_F(BufferedUnionTest, ManyScorersWithSingleDoc) {
    std::vector<ScorerPtr> scorers;
    for (uint32_t i = 0; i < 20; ++i) {
        scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {i * 10}));
    }

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 20; ++i) {
        expected.push_back(i * 10);
    }

    auto collected = collect_all_docs(union_scorer);
    EXPECT_EQ(collected, expected);
}

TEST_F(BufferedUnionTest, AllScorersPointToSameDoc) {
    std::vector<ScorerPtr> scorers;
    for (int i = 0; i < 10; ++i) {
        scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {100},
                                                       std::vector<float> {1.0F}));
    }

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 100);
    EXPECT_FLOAT_EQ(union_scorer->score(), 10.0F);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
}

TEST_F(BufferedUnionTest, RefillAfterAllScorersExhausted) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {1, 2}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 1);
    EXPECT_EQ(union_scorer->advance(), 2);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
    EXPECT_EQ(union_scorer->advance(), TERMINATED);
    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

TEST_F(BufferedUnionTest, LargeNumberOfDocs) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs;
    for (uint32_t i = 0; i < 10000; ++i) {
        docs.push_back(i * 2);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs));

    std::vector<uint32_t> docs2;
    for (uint32_t i = 0; i < 10000; ++i) {
        docs2.push_back(i * 2 + 1);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs2));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    uint32_t expected_doc = 0;
    while (union_scorer->doc() != TERMINATED) {
        EXPECT_EQ(union_scorer->doc(), expected_doc);
        union_scorer->advance();
        expected_doc++;
    }
    EXPECT_EQ(expected_doc, 20000u);
}

TEST_F(BufferedUnionTest, ManySmallScorers) {
    std::vector<ScorerPtr> scorers;
    for (uint32_t s = 0; s < 100; ++s) {
        std::vector<uint32_t> docs;
        for (uint32_t d = 0; d < 10; ++d) {
            docs.push_back(s * 100 + d * 10);
        }
        scorers.push_back(std::make_shared<MockScorer>(docs));
    }

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    uint32_t count = 0;
    while (union_scorer->doc() != TERMINATED) {
        count++;
        union_scorer->advance();
    }

    EXPECT_EQ(count, 1000u);
}

TEST_F(BufferedUnionTest, ComplexSeekAdvanceSequence) {
    std::vector<ScorerPtr> scorers;
    std::vector<uint32_t> docs;
    for (uint32_t i = 0; i < 200; i += 2) {
        docs.push_back(i);
    }
    scorers.push_back(std::make_shared<MockScorer>(docs));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    EXPECT_EQ(union_scorer->doc(), 0);
    EXPECT_EQ(union_scorer->advance(), 2);
    EXPECT_EQ(union_scorer->seek(50), 50);
    EXPECT_EQ(union_scorer->advance(), 52);
    EXPECT_EQ(union_scorer->seek(100), 100);
    EXPECT_EQ(union_scorer->seek(99), 100);
    EXPECT_EQ(union_scorer->advance(), 102);
    EXPECT_EQ(union_scorer->seek(1000), TERMINATED);
}

TEST_F(BufferedUnionTest, DocReturnsCorrectValue) {
    std::vector<ScorerPtr> scorers;
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {10, 20, 30}));
    scorers.push_back(std::make_shared<MockScorer>(std::vector<uint32_t> {15, 25, 35}));

    auto combiner = std::make_shared<SumCombiner>();
    auto union_scorer = make_buffered_union(scorers, combiner);

    std::vector<uint32_t> expected = {10, 15, 20, 25, 30, 35};
    for (uint32_t exp_doc : expected) {
        EXPECT_EQ(union_scorer->doc(), exp_doc);
        union_scorer->advance();
    }
    EXPECT_EQ(union_scorer->doc(), TERMINATED);
}

} // namespace doris::segment_v2::inverted_index::query_v2
