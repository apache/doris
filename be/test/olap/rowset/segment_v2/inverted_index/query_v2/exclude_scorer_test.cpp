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

#include "olap/rowset/segment_v2/inverted_index/query_v2/exclude_scorer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <vector>

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

std::vector<uint32_t> compute_exclude(const std::vector<uint32_t>& underlying,
                                      const std::vector<uint32_t>& excluding) {
    std::set<uint32_t> exclude_set(excluding.begin(), excluding.end());
    std::vector<uint32_t> result;
    for (uint32_t doc : underlying) {
        if (exclude_set.find(doc) == exclude_set.end()) {
            result.push_back(doc);
        }
    }
    return result;
}

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

class ExcludeScorerTest : public ::testing::Test {};

TEST_F(ExcludeScorerTest, BasicExclude) {
    auto underlying =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 5, 8, 10, 15, 24});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 10, 16, 24});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {5, 8, 15};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, NoExclusions) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 2, 3, 4, 5};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, AllExcluded) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(ExcludeScorerTest, EmptyUnderlying) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(ExcludeScorerTest, BothEmpty) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(ExcludeScorerTest, NoIntersection) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 5, 7, 9});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 4, 6, 8, 10});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 3, 5, 7, 9};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, SeekBasic) {
    auto underlying =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 5, 8, 10, 15, 24});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 10, 16, 24});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(8u, scorer->seek(8));
    EXPECT_EQ(15u, scorer->seek(10));
    EXPECT_EQ(TERMINATED, scorer->seek(24));
}

TEST_F(ExcludeScorerTest, SeekToExcludedDoc) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15, 20, 25});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {10, 20});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(15u, scorer->seek(10));
    EXPECT_EQ(25u, scorer->seek(20));
}

TEST_F(ExcludeScorerTest, SeekToCurrentDoc) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->seek(5));
    EXPECT_EQ(5u, scorer->seek(3));
}

TEST_F(ExcludeScorerTest, SeekPastEnd) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->seek(100));
}

TEST_F(ExcludeScorerTest, AdvanceAfterTerminated) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(ExcludeScorerTest, SeekAfterTerminated) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->seek(50));
}

TEST_F(ExcludeScorerTest, ScoreDelegation) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10},
                                                     std::vector<float> {1.5F, 2.5F, 3.5F});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {5});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_FLOAT_EQ(1.5F, scorer->score());
    EXPECT_EQ(10u, scorer->advance());
    EXPECT_FLOAT_EQ(3.5F, scorer->score());
}

TEST_F(ExcludeScorerTest, SizeHintDelegation) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3},
                                                     std::vector<float> {}, 100);
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {2});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(100u, scorer->size_hint());
}

TEST_F(ExcludeScorerTest, SingleDocNotExcluded) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {42});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(42u, scorer->doc());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(ExcludeScorerTest, SingleDocExcluded) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {42});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {42});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(ExcludeScorerTest, FirstDocExcluded) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
}

TEST_F(ExcludeScorerTest, LastDocExcluded) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {10});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 5};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, ConsecutiveDocsExcluded) {
    auto underlying =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {3, 4, 5, 6, 7});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 2, 8, 9, 10};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, ExcludingHasExtraDocs) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 5, 6, 7, 10, 100});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {15};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, LargeDocIds) {
    auto underlying = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {100000000, 200000000, 300000000, 400000000});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {200000000, 400000000});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {100000000, 300000000};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, RandomData) {
    auto sample_underlying = sample_with_seed(10000, 0.1, 1);
    auto sample_excluding = sample_with_seed(10000, 0.05, 2);

    auto underlying = std::make_shared<VectorScorer>(sample_underlying);
    auto excluding = std::make_shared<VectorScorer>(sample_excluding);

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    auto expected = compute_exclude(sample_underlying, sample_excluding);
    std::vector<uint32_t> actual;
    while (scorer->doc() != TERMINATED) {
        actual.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(expected, actual);
}

TEST_F(ExcludeScorerTest, RandomDataSeek) {
    auto sample_underlying = sample_with_seed(10000, 0.1, 1);
    auto sample_excluding = sample_with_seed(10000, 0.05, 2);
    auto sample_seek_targets = sample_with_seed(10000, 0.005, 3);

    auto expected = compute_exclude(sample_underlying, sample_excluding);

    auto underlying = std::make_shared<VectorScorer>(sample_underlying);
    auto excluding = std::make_shared<VectorScorer>(sample_excluding);
    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    for (uint32_t target : sample_seek_targets) {
        auto it = std::lower_bound(expected.begin(), expected.end(), target);
        uint32_t expected_doc = (it == expected.end()) ? TERMINATED : *it;

        underlying = std::make_shared<VectorScorer>(sample_underlying);
        excluding = std::make_shared<VectorScorer>(sample_excluding);
        scorer = make_exclude(std::move(underlying), std::move(excluding));

        uint32_t actual_doc = scorer->seek(target);
        EXPECT_EQ(expected_doc, actual_doc) << "Failed for target: " << target;
    }
}

TEST_F(ExcludeScorerTest, InterleavedAdvanceAndSeek) {
    auto underlying = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 20, 35, 50});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(1u, scorer->doc());
    EXPECT_EQ(10u, scorer->advance());
    EXPECT_EQ(15u, scorer->seek(12));
    EXPECT_EQ(25u, scorer->advance());
    EXPECT_EQ(40u, scorer->seek(35));
    EXPECT_EQ(45u, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

TEST_F(ExcludeScorerTest, DocDoesNotAdvance) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10, 15});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->doc());
    EXPECT_EQ(5u, scorer->doc());

    scorer->advance();
    EXPECT_EQ(10u, scorer->doc());
    EXPECT_EQ(10u, scorer->doc());
}

TEST_F(ExcludeScorerTest, ExcludingLargerThanUnderlying) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 10});
    auto excluding = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(TERMINATED, scorer->doc());
}

TEST_F(ExcludeScorerTest, LargeDataSet) {
    std::vector<uint32_t> underlying_docs;
    std::vector<uint32_t> excluding_docs;

    for (uint32_t i = 0; i < 100000; i += 10) {
        underlying_docs.push_back(i);
    }

    for (uint32_t i = 0; i < 100000; i += 30) {
        excluding_docs.push_back(i);
    }

    auto underlying = std::make_shared<VectorScorer>(underlying_docs);
    auto excluding = std::make_shared<VectorScorer>(excluding_docs);

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    auto expected = compute_exclude(underlying_docs, excluding_docs);
    std::vector<uint32_t> actual;
    while (scorer->doc() != TERMINATED) {
        actual.push_back(scorer->doc());
        scorer->advance();
    }

    EXPECT_EQ(expected, actual);
}

TEST_F(ExcludeScorerTest, AlternatingExcluded) {
    auto underlying =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 4, 6, 8, 10});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {1, 3, 5, 7, 9};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, IsWithinBehavior) {
    auto underlying = std::make_shared<VectorScorer>(std::vector<uint32_t> {100, 200, 300});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {50, 100, 150, 200, 250});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    std::vector<uint32_t> docs;
    while (scorer->doc() != TERMINATED) {
        docs.push_back(scorer->doc());
        scorer->advance();
    }

    std::vector<uint32_t> expected {300};
    EXPECT_EQ(expected, docs);
}

TEST_F(ExcludeScorerTest, SeekExcludingCatchUp) {
    auto underlying = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {10, 20, 30, 40, 50, 60, 70, 80, 90, 100});
    auto excluding = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 15, 25, 50, 75, 100});

    auto scorer = make_exclude(std::move(underlying), std::move(excluding));
    ASSERT_NE(nullptr, scorer);

    EXPECT_EQ(10u, scorer->doc());
    EXPECT_EQ(60u, scorer->seek(50));
    EXPECT_EQ(70u, scorer->advance());
    EXPECT_EQ(80u, scorer->advance());
    EXPECT_EQ(90u, scorer->advance());
    EXPECT_EQ(TERMINATED, scorer->advance());
}

} // namespace doris::segment_v2::inverted_index::query_v2
