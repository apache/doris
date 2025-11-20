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

#include <memory>
#include <utility>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/score_combiner.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris {

using segment_v2::inverted_index::query_v2::BufferedUnion;
using segment_v2::inverted_index::query_v2::DoNothingCombinerPtr;
using segment_v2::inverted_index::query_v2::Scorer;
using segment_v2::inverted_index::query_v2::ScorerPtr;
using segment_v2::inverted_index::query_v2::SumCombinerPtr;
using segment_v2::inverted_index::query_v2::TERMINATED;
using segment_v2::inverted_index::query_v2::build;

class SimpleVectorScorer : public Scorer {
public:
    SimpleVectorScorer(std::vector<uint32_t> docs, std::vector<float> scores,
                       uint32_t size_hint = 0)
            : _docs(std::move(docs)), _scores(std::move(scores)) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            _current_doc = _docs[0];
        }
        if (_scores.size() != _docs.size()) {
            _scores.resize(_docs.size(), 0.0F);
        }
        if (size_hint == 0) {
            _size_hint = static_cast<uint32_t>(_docs.size());
        } else {
            _size_hint = size_hint;
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
        auto it = std::lower_bound(_docs.begin() + _index, _docs.end(), target);
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

    uint32_t size_hint() const override { return _size_hint; }

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
    uint32_t _size_hint = 0;
};

class BufferedUnionTest : public ::testing::Test {};

TEST_F(BufferedUnionTest, BasicBuildAndIterationWithScores) {
    auto scorer1 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {1, 3, 7},
                                                        std::vector<float> {0.5F, 1.0F, 2.0F});
    auto scorer2 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {2, 3, 8},
                                                        std::vector<float> {0.7F, 1.5F, 2.5F});
    auto empty_scorer =
            std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {}, std::vector<float> {});

    SumCombinerPtr combiner = std::make_shared<segment_v2::inverted_index::query_v2::SumCombiner>();
    std::vector<ScorerPtr> children {scorer1, scorer2, empty_scorer};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);

    EXPECT_EQ(0u, union_scorer->size_hint());

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (union_scorer->doc() != TERMINATED) {
        docs.push_back(union_scorer->doc());
        scores.push_back(union_scorer->score());
        if (union_scorer->advance() == TERMINATED) {
            break;
        }
    }

    ASSERT_EQ(docs.size(), scores.size());
    std::vector<uint32_t> expected_docs {1, 2, 3, 7, 8};
    std::vector<float> expected_scores {0.5F, 0.7F, 2.5F, 2.0F, 2.5F};

    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores.size(), scores.size());
    for (size_t i = 0; i < expected_scores.size(); ++i) {
        EXPECT_FLOAT_EQ(expected_scores[i], scores[i]);
    }

    EXPECT_EQ(TERMINATED, union_scorer->advance());
    EXPECT_EQ(TERMINATED, union_scorer->doc());
}

TEST_F(BufferedUnionTest, SeekWithinHorizon) {
    auto scorer1 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {1, 3, 7},
                                                        std::vector<float> {0.5F, 1.0F, 2.0F});
    auto scorer2 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {2, 3, 8},
                                                        std::vector<float> {0.7F, 1.5F, 2.5F});

    SumCombinerPtr combiner = std::make_shared<segment_v2::inverted_index::query_v2::SumCombiner>();
    std::vector<ScorerPtr> children {scorer1, scorer2};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);

    EXPECT_EQ(1u, union_scorer->doc());

    uint32_t d = union_scorer->seek(6);
    EXPECT_EQ(7u, d);
    EXPECT_FLOAT_EQ(2.0F, union_scorer->score());

    EXPECT_EQ(8u, union_scorer->advance());
    EXPECT_EQ(TERMINATED, union_scorer->advance());
    EXPECT_EQ(TERMINATED, union_scorer->doc());
}

TEST_F(BufferedUnionTest, SeekBeyondHorizon) {
    auto scorer1 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {5, 10, 20},
                                                        std::vector<float> {1.0F, 2.0F, 3.0F});
    auto scorer2 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {6, 30},
                                                        std::vector<float> {4.0F, 5.0F});

    SumCombinerPtr combiner = std::make_shared<segment_v2::inverted_index::query_v2::SumCombiner>();
    std::vector<ScorerPtr> children {scorer1, scorer2};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);

    EXPECT_EQ(5u, union_scorer->doc());

    uint32_t target = 5000;
    uint32_t d = union_scorer->seek(target);

    EXPECT_EQ(TERMINATED, d);
    EXPECT_EQ(TERMINATED, union_scorer->doc());
}

TEST_F(BufferedUnionTest, BuildWithDoNothingCombinerNoScores) {
    auto scorer1 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {1, 4},
                                                        std::vector<float> {10.0F, 20.0F});
    auto scorer2 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {2, 4},
                                                        std::vector<float> {30.0F, 40.0F});

    DoNothingCombinerPtr combiner =
            std::make_shared<segment_v2::inverted_index::query_v2::DoNothingCombiner>();
    std::vector<ScorerPtr> children {scorer1, scorer2};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (union_scorer->doc() != TERMINATED) {
        docs.push_back(union_scorer->doc());
        scores.push_back(union_scorer->score());
        if (union_scorer->advance() == TERMINATED) {
            break;
        }
    }

    std::vector<uint32_t> expected_docs {1, 2, 4};
    EXPECT_EQ(expected_docs, docs);
    for (float s : scores) {
        EXPECT_FLOAT_EQ(0.0F, s);
    }
}

TEST_F(BufferedUnionTest, BuildWithAllEmptyScorers) {
    auto empty1 =
            std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {}, std::vector<float> {});
    auto empty2 =
            std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {}, std::vector<float> {});

    SumCombinerPtr combiner = std::make_shared<segment_v2::inverted_index::query_v2::SumCombiner>();
    std::vector<ScorerPtr> children {empty1, empty2};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);
    EXPECT_EQ(TERMINATED, union_scorer->doc());
    EXPECT_EQ(0u, union_scorer->size_hint());
}

TEST_F(BufferedUnionTest, SeekOnAlreadyAheadDoc) {
    auto scorer1 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {10, 20},
                                                        std::vector<float> {1.0F, 2.0F});
    auto scorer2 = std::make_shared<SimpleVectorScorer>(std::vector<uint32_t> {15, 25},
                                                        std::vector<float> {3.0F, 4.0F});

    SumCombinerPtr combiner = std::make_shared<segment_v2::inverted_index::query_v2::SumCombiner>();
    std::vector<ScorerPtr> children {scorer1, scorer2};

    auto union_scorer = segment_v2::inverted_index::query_v2::build(children, combiner);
    ASSERT_NE(nullptr, union_scorer);

    EXPECT_EQ(10u, union_scorer->doc());
    uint32_t d = union_scorer->seek(5);
    EXPECT_EQ(10u, d);
}

} // namespace doris