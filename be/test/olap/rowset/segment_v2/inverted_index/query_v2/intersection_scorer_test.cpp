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

#include "olap/rowset/segment_v2/inverted_index/query_v2/intersection_scorer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

namespace doris {

using segment_v2::inverted_index::query_v2::AndNotScorer;
using segment_v2::inverted_index::query_v2::NullBitmapResolver;
using segment_v2::inverted_index::query_v2::Scorer;
using segment_v2::inverted_index::query_v2::ScorerPtr;
using segment_v2::inverted_index::query_v2::TERMINATED;

namespace {

class DummyResolver final : public NullBitmapResolver {
public:
    DummyResolver() = default;
    ~DummyResolver() override = default;

    segment_v2::IndexIterator* iterator_for(const Scorer& /*scorer*/,
                                            const std::string& /*logical_field*/) const override {
        return nullptr;
    }
};

class VectorScorer final : public Scorer {
public:
    VectorScorer(std::vector<uint32_t> docs, std::vector<float> scores,
                 std::vector<uint32_t> null_docs = {}, uint32_t size_hint = 0)
            : _docs(std::move(docs)), _scores(std::move(scores)), _size_hint(size_hint) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            _current_doc = _docs[0];
        }
        if (_scores.size() != _docs.size()) {
            _scores.resize(_docs.size(), 0.0F);
        }
        for (auto doc : null_docs) {
            _null_bitmap.add(doc);
        }
        if (_size_hint == 0) {
            _size_hint = static_cast<uint32_t>(_docs.size());
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

    bool has_null_bitmap(const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return !_null_bitmap.isEmpty();
    }

    const roaring::Roaring* get_null_bitmap(
            const NullBitmapResolver* /*resolver*/ = nullptr) override {
        return _null_bitmap.isEmpty() ? nullptr : &_null_bitmap;
    }

private:
    std::vector<uint32_t> _docs;
    std::vector<float> _scores;
    size_t _index = 0;
    uint32_t _current_doc = TERMINATED;
    uint32_t _size_hint = 0;
    roaring::Roaring _null_bitmap;
};

} // namespace

class IntersectionScorerTest : public ::testing::Test {};

TEST_F(IntersectionScorerTest, AllTermsMatchWithScoring) {
    auto scorer1 = std::make_shared<VectorScorer>(std::vector<uint32_t> {3, 5, 9},
                                                  std::vector<float> {1.0F, 2.0F, 3.0F});
    auto scorer2 = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 8, 9},
                                                  std::vector<float> {0.5F, 1.5F, 2.5F, 3.5F});
    auto scorer3 = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 9},
                                                  std::vector<float> {4.0F, 5.0F});

    std::vector<ScorerPtr> children {scorer1, scorer2, scorer3};
    auto and_scorer = segment_v2::inverted_index::query_v2::intersection_scorer_build(
            std::move(children), true, nullptr);
    ASSERT_NE(nullptr, and_scorer);

    EXPECT_EQ(2u, and_scorer->size_hint());

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (and_scorer->doc() != TERMINATED) {
        docs.push_back(and_scorer->doc());
        scores.push_back(and_scorer->score());
        if (and_scorer->advance() == TERMINATED) {
            break;
        }
    }

    std::vector<uint32_t> expected_docs {5, 9};
    std::vector<float> expected_scores {7.5F, 11.5F};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores, scores);
    EXPECT_EQ(TERMINATED, and_scorer->advance());
    EXPECT_EQ(TERMINATED, and_scorer->doc());
}

TEST_F(IntersectionScorerTest, SeekAndNoScoring) {
    auto scorer1 = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 6, 10},
                                                  std::vector<float> {1.0F, 1.0F, 1.0F});
    auto scorer2 = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 6, 10},
                                                  std::vector<float> {0.2F, 0.2F, 0.2F});

    std::vector<ScorerPtr> children {scorer1, scorer2};
    auto and_scorer = segment_v2::inverted_index::query_v2::intersection_scorer_build(
            std::move(children), false, nullptr);
    ASSERT_NE(nullptr, and_scorer);

    EXPECT_EQ(6u, and_scorer->doc());
    EXPECT_FLOAT_EQ(0.0F, and_scorer->score());

    EXPECT_EQ(10u, and_scorer->seek(7));
    EXPECT_FLOAT_EQ(0.0F, and_scorer->score());

    EXPECT_EQ(TERMINATED, and_scorer->advance());
    EXPECT_EQ(TERMINATED, and_scorer->doc());
}

TEST_F(IntersectionScorerTest, NullBitmapPropagation) {
    DummyResolver resolver;
    auto scorer1 = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 6},
                                                  std::vector<float> {1.0F, 1.0F},
                                                  std::vector<uint32_t> {4});
    auto scorer2 = std::make_shared<VectorScorer>(std::vector<uint32_t> {4, 6},
                                                  std::vector<float> {1.5F, 1.5F});
    auto scorer3 = std::make_shared<VectorScorer>(std::vector<uint32_t> {4, 6},
                                                  std::vector<float> {2.0F, 2.0F});

    std::vector<ScorerPtr> children {scorer1, scorer2, scorer3};
    auto and_scorer = segment_v2::inverted_index::query_v2::intersection_scorer_build(
            std::move(children), false, &resolver);
    ASSERT_NE(nullptr, and_scorer);

    EXPECT_EQ(6u, and_scorer->doc());
    EXPECT_TRUE(and_scorer->has_null_bitmap());
    const auto* null_bitmap = and_scorer->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(4));
    EXPECT_FALSE(null_bitmap->contains(6));
}

TEST_F(IntersectionScorerTest, AndNotScorerRespectsTrueAndNullExcludes) {
    DummyResolver resolver;
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 4, 6},
                                                  std::vector<float> {0.5F, 1.5F, 2.5F});
    auto exclude_true =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {4}, std::vector<float> {0.0F});
    auto exclude_null = std::make_shared<VectorScorer>(
            std::vector<uint32_t> {}, std::vector<float> {}, std::vector<uint32_t> {6});

    std::vector<ScorerPtr> excludes {exclude_true, exclude_null};
    auto and_not = std::make_shared<AndNotScorer>(include, std::move(excludes), &resolver);
    ASSERT_NE(nullptr, and_not);

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (and_not->doc() != TERMINATED) {
        docs.push_back(and_not->doc());
        scores.push_back(and_not->score());
        if (and_not->advance() == TERMINATED) {
            break;
        }
    }

    std::vector<uint32_t> expected_docs {2};
    std::vector<float> expected_scores {0.5F};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores, scores);

    EXPECT_TRUE(and_not->has_null_bitmap());
    const auto* null_bitmap = and_not->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(6));
    EXPECT_FALSE(null_bitmap->contains(4));

    EXPECT_EQ(TERMINATED, and_not->advance());
    EXPECT_EQ(TERMINATED, and_not->doc());
    EXPECT_EQ(include->size_hint(), and_not->size_hint());
}

} // namespace doris
