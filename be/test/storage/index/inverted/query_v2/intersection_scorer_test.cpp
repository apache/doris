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

#include "storage/index/inverted/query_v2/intersection_scorer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/inverted/query_v2/exclude_scorer.h"

namespace doris {

using segment_v2::inverted_index::query_v2::make_exclude;
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

// --- ExcludeScorer with null bitmap tests ---
// These tests verify that the enhanced ExcludeScorer correctly implements
// SQL three-valued logic: NOT(NULL) = NULL, keeping lazy seek-based
// exclusion while adding O(1) null bitmap awareness.

TEST_F(IntersectionScorerTest, ExcludeScorerRespectsTrueAndNullExcludes) {
    DummyResolver resolver;
    // Include docs: {2, 4, 6}
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {2, 4, 6},
                                                  std::vector<float> {0.5F, 1.5F, 2.5F});
    // Exclude scorer: TRUE docs {4} (lazy seek-based exclusion)
    auto exclude =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {4}, std::vector<float> {0.0F});
    // Pre-collected null bitmap from exclude scorers: {6}
    roaring::Roaring exclude_null;
    exclude_null.add(6);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);
    ASSERT_NE(nullptr, result);

    std::vector<uint32_t> docs;
    std::vector<float> scores;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        scores.push_back(result->score());
        if (result->advance() == TERMINATED) {
            break;
        }
    }

    // Doc 4 is TRUE-excluded, doc 6 is NULL-excluded
    std::vector<uint32_t> expected_docs {2};
    std::vector<float> expected_scores {0.5F};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_EQ(expected_scores, scores);

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(6));
    EXPECT_FALSE(null_bitmap->contains(4));

    EXPECT_EQ(TERMINATED, result->advance());
    EXPECT_EQ(TERMINATED, result->doc());
    EXPECT_EQ(include->size_hint(), result->size_hint());
}

TEST_F(IntersectionScorerTest, ExcludeScorerAllExcludesAreNull) {
    // When all exclude docs are NULL (not TRUE), those docs should appear
    // in the null bitmap rather than being excluded from the result set.
    DummyResolver resolver;
    // Include docs: 1, 2, 3, 4, 5
    auto include =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5},
                                           std::vector<float> {1.0F, 2.0F, 3.0F, 4.0F, 5.0F});
    // No TRUE exclude docs
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, std::vector<float> {});
    // NULL exclude: {2, 4}
    roaring::Roaring exclude_null;
    exclude_null.add(2);
    exclude_null.add(4);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);
    ASSERT_NE(nullptr, result);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    // Docs 2 and 4 are null-excluded (go to null bitmap), rest pass through
    std::vector<uint32_t> expected_docs {1, 3, 5};
    EXPECT_EQ(expected_docs, docs);

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(2));
    EXPECT_TRUE(null_bitmap->contains(4));
    EXPECT_FALSE(null_bitmap->contains(1));
    EXPECT_FALSE(null_bitmap->contains(3));
    EXPECT_FALSE(null_bitmap->contains(5));
}

TEST_F(IntersectionScorerTest, ExcludeScorerNoResolver) {
    // Without a resolver, null bitmaps from include are not inherited,
    // but pre-collected exclude_null is still effective.
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3},
                                                  std::vector<float> {1.0F, 2.0F, 3.0F});
    // TRUE exclude: {2}
    auto exclude =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {2}, std::vector<float> {0.0F});
    // NULL exclude: {3} (pre-collected, works even without resolver)
    roaring::Roaring exclude_null;
    exclude_null.add(3);

    auto result = make_exclude(include, exclude, std::move(exclude_null), nullptr);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    // Doc 2 is TRUE-excluded, doc 3 is NULL-excluded
    std::vector<uint32_t> expected_docs {1};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(3));
}

TEST_F(IntersectionScorerTest, ExcludeScorerEmptyExcludes) {
    // No excludes: all include docs should pass through with no null bitmap.
    DummyResolver resolver;
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10},
                                                  std::vector<float> {1.0F, 2.0F, 3.0F});
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {}, std::vector<float> {});
    roaring::Roaring exclude_null;

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    std::vector<uint32_t> expected_docs {1, 5, 10};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_FALSE(result->has_null_bitmap());
}

TEST_F(IntersectionScorerTest, ExcludeScorerMultipleExcludesWithNulls) {
    // Simulates multiple exclude scorers that were unioned.
    // TRUE docs {3, 6} from union, NULL docs {5, 7} from pre-collected null bitmaps.
    DummyResolver resolver;
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5, 6, 7, 8},
                                                  std::vector<float> {1, 2, 3, 4, 5, 6, 7, 8});
    // Unioned TRUE exclude: {3, 6}
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {3, 6},
                                                  std::vector<float> {0.0F, 0.0F});
    // Pre-collected NULL exclude: {5, 7}
    roaring::Roaring exclude_null;
    exclude_null.add(5);
    exclude_null.add(7);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    // TRUE-excluded: 3, 6
    // NULL-excluded: 5, 7 (go to null bitmap)
    // Remaining: 1, 2, 4, 8
    std::vector<uint32_t> expected_docs {1, 2, 4, 8};
    EXPECT_EQ(expected_docs, docs);

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(5));
    EXPECT_TRUE(null_bitmap->contains(7));
    EXPECT_FALSE(null_bitmap->contains(3));
    EXPECT_FALSE(null_bitmap->contains(6));
}

TEST_F(IntersectionScorerTest, ExcludeScorerTrueOverridesNull) {
    DummyResolver resolver;
    auto include =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5},
                                           std::vector<float> {1.0F, 2.0F, 3.0F, 4.0F, 5.0F});
    // Unioned TRUE exclude: {3, 4}
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {3, 4},
                                                  std::vector<float> {0.0F, 0.0F});
    // Pre-collected NULL exclude: {3, 4} (union of both excluders' null bitmaps)
    roaring::Roaring exclude_null;
    exclude_null.add(3);
    exclude_null.add(4);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    // Docs 3 and 4 are NULL-excluded (exclude_null check comes first)
    std::vector<uint32_t> expected_docs {1, 2, 5};
    EXPECT_EQ(expected_docs, docs);

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(3));
    EXPECT_TRUE(null_bitmap->contains(4));
}

TEST_F(IntersectionScorerTest, ExcludeScorerSeekWithNullExclusion) {
    // Test seek operations when exclude docs have null entries.
    DummyResolver resolver;
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 5, 10, 15, 20, 25, 30},
                                                  std::vector<float> {1, 2, 3, 4, 5, 6, 7});
    // TRUE exclude: {5, 20}
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {5, 20},
                                                  std::vector<float> {0.0F, 0.0F});
    // NULL exclude: {10, 25}
    roaring::Roaring exclude_null;
    exclude_null.add(10);
    exclude_null.add(25);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    // First doc should be 1 (not excluded)
    EXPECT_EQ(1u, result->doc());

    // Seek to 10 → doc 10 is null-excluded, should skip to 15
    EXPECT_EQ(15u, result->seek(10));

    // Seek to 20 → doc 20 is TRUE-excluded, should skip to 30
    // (25 is null-excluded)
    EXPECT_EQ(30u, result->seek(20));

    EXPECT_EQ(TERMINATED, result->advance());

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(10));
    EXPECT_TRUE(null_bitmap->contains(25));
}

TEST_F(IntersectionScorerTest, ExcludeScorerIncludeHasNullBitmap) {
    // When the include scorer has a null bitmap, it should be inherited.
    DummyResolver resolver;
    // Include scorer has null docs {8}
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3, 5, 7},
                                                  std::vector<float> {1.0F, 2.0F, 3.0F, 4.0F},
                                                  std::vector<uint32_t> {8});
    // TRUE exclude: {3}
    auto exclude =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {3}, std::vector<float> {0.0F});
    // NULL exclude: {5}
    roaring::Roaring exclude_null;
    exclude_null.add(5);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    // 3 is TRUE-excluded, 5 is NULL-excluded
    std::vector<uint32_t> expected_docs {1, 7};
    EXPECT_EQ(expected_docs, docs);

    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    // Include's null doc 8 should be inherited
    EXPECT_TRUE(null_bitmap->contains(8));
    // Exclude's null doc 5 should also be in null bitmap
    EXPECT_TRUE(null_bitmap->contains(5));
}

TEST_F(IntersectionScorerTest, ExcludeScorerAllDocsExcluded) {
    // All include docs are either TRUE-excluded or NULL-excluded.
    DummyResolver resolver;
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3},
                                                  std::vector<float> {1.0F, 2.0F, 3.0F});
    // TRUE exclude: {1, 3}
    auto exclude = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 3},
                                                  std::vector<float> {0.0F, 0.0F});
    // NULL exclude: {2}
    roaring::Roaring exclude_null;
    exclude_null.add(2);

    auto result = make_exclude(include, exclude, std::move(exclude_null), &resolver);

    EXPECT_EQ(TERMINATED, result->doc());

    // Doc 2 should be in null bitmap (NULL-excluded but no TRUE match)
    EXPECT_TRUE(result->has_null_bitmap());
    const auto* null_bitmap = result->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    EXPECT_TRUE(null_bitmap->contains(2));
    EXPECT_FALSE(null_bitmap->contains(1));
    EXPECT_FALSE(null_bitmap->contains(3));
}

TEST_F(IntersectionScorerTest, ExcludeScorerNoNullBitmapWhenEmpty) {
    // When exclude_null is empty and include has no null bitmap,
    // ExcludeScorer should behave exactly like the original (no null awareness).
    auto include = std::make_shared<VectorScorer>(std::vector<uint32_t> {1, 2, 3, 4, 5},
                                                  std::vector<float> {1, 2, 3, 4, 5});
    auto exclude =
            std::make_shared<VectorScorer>(std::vector<uint32_t> {3}, std::vector<float> {0.0F});
    roaring::Roaring exclude_null;

    auto result = make_exclude(include, exclude, std::move(exclude_null));

    std::vector<uint32_t> docs;
    while (result->doc() != TERMINATED) {
        docs.push_back(result->doc());
        result->advance();
    }

    std::vector<uint32_t> expected_docs {1, 2, 4, 5};
    EXPECT_EQ(expected_docs, docs);
    EXPECT_FALSE(result->has_null_bitmap());
}

} // namespace doris
