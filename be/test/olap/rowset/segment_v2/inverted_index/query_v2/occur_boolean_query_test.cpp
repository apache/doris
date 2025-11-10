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

#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_query.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

class MockScorer : public Scorer {
public:
    MockScorer(std::vector<uint32_t> docs, float score_val = 1.0F)
            : _docs(std::move(docs)), _score_val(score_val) {
        if (_docs.empty()) {
            _current_doc = TERMINATED;
        } else {
            std::ranges::sort(_docs);
            _current_doc = _docs[0];
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
    uint32_t size_hint() const override { return static_cast<uint32_t>(_docs.size()); }
    float score() override { return _score_val; }

private:
    std::vector<uint32_t> _docs;
    size_t _index = 0;
    uint32_t _current_doc = TERMINATED;
    float _score_val = 1.0F;
};

class MockWeight : public Weight {
public:
    explicit MockWeight(std::vector<uint32_t> docs, float score_val = 1.0F)
            : _docs(std::move(docs)), _score_val(score_val) {}

    ScorerPtr scorer(const QueryExecutionContext& /*context*/) override {
        return std::make_shared<MockScorer>(_docs, _score_val);
    }

private:
    std::vector<uint32_t> _docs;
    float _score_val;
};

class MockQuery : public Query {
public:
    explicit MockQuery(std::vector<uint32_t> docs, float score_val = 1.0F)
            : _docs(std::move(docs)), _score_val(score_val) {}

    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<MockWeight>(_docs, _score_val);
    }

private:
    std::vector<uint32_t> _docs;
    float _score_val;
};

} // anonymous namespace

class OccurBooleanQueryTest : public testing::Test {
protected:
    QueryExecutionContext _ctx;

public:
    std::vector<uint32_t> collect_docs(ScorerPtr scorer) {
        std::vector<uint32_t> result;
        uint32_t doc = scorer->doc();
        while (doc != TERMINATED) {
            result.push_back(doc);
            doc = scorer->advance();
        }
        return result;
    }

    std::set<uint32_t> to_set(const std::vector<uint32_t>& v) {
        return std::set<uint32_t>(v.begin(), v.end());
    }

    std::vector<uint32_t> set_union(const std::vector<uint32_t>& a,
                                    const std::vector<uint32_t>& b) {
        std::set<uint32_t> result;
        result.insert(a.begin(), a.end());
        result.insert(b.begin(), b.end());
        return std::vector<uint32_t>(result.begin(), result.end());
    }

    std::vector<uint32_t> set_intersection(const std::vector<uint32_t>& a,
                                           const std::vector<uint32_t>& b) {
        std::set<uint32_t> sa(a.begin(), a.end());
        std::set<uint32_t> sb(b.begin(), b.end());
        std::vector<uint32_t> result;
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::back_inserter(result));
        return result;
    }

    std::vector<uint32_t> set_difference(const std::vector<uint32_t>& a,
                                         const std::vector<uint32_t>& b) {
        std::set<uint32_t> sa(a.begin(), a.end());
        std::set<uint32_t> sb(b.begin(), b.end());
        std::vector<uint32_t> result;
        std::set_difference(sa.begin(), sa.end(), sb.begin(), sb.end(), std::back_inserter(result));
        return result;
    }

    std::vector<uint32_t> generate_random_docs(size_t count, uint32_t max_doc, uint32_t seed) {
        std::mt19937 gen(seed);
        std::uniform_int_distribution<uint32_t> dis(0, max_doc - 1);
        std::set<uint32_t> doc_set;
        while (doc_set.size() < count) {
            doc_set.insert(dis(gen));
        }
        return std::vector<uint32_t>(doc_set.begin(), doc_set.end());
    }

    std::vector<uint32_t> generate_range_docs(uint32_t start, uint32_t end, uint32_t step = 1) {
        std::vector<uint32_t> result;
        for (uint32_t i = start; i < end; i += step) {
            result.push_back(i);
        }
        return result;
    }
};

TEST_F(OccurBooleanQueryTest, EmptyQuery) {
    std::vector<std::pair<Occur, QueryPtr>> clauses;
    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);

    EXPECT_EQ(scorer->doc(), TERMINATED);
}

TEST_F(OccurBooleanQueryTest, SingleMustClause) {
    auto docs = generate_range_docs(0, 100, 2);
    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, docs);
}

TEST_F(OccurBooleanQueryTest, SingleShouldClause) {
    auto docs = generate_range_docs(0, 100, 3);
    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, docs);
}

TEST_F(OccurBooleanQueryTest, SingleMustNotClauseReturnsEmpty) {
    auto docs = generate_range_docs(0, 100);
    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);

    EXPECT_EQ(scorer->doc(), TERMINATED);
}

TEST_F(OccurBooleanQueryTest, TwoMustClausesIntersection) {
    auto docs1 = generate_range_docs(0, 1000, 2);
    auto docs2 = generate_range_docs(0, 1000, 3);
    auto expected = set_intersection(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, TwoShouldClausesUnion) {
    auto docs1 = generate_range_docs(0, 500, 2);
    auto docs2 = generate_range_docs(250, 750, 2);
    auto expected = set_union(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, MustWithMustNotExclusion) {
    auto must_docs = generate_range_docs(0, 1000);
    auto must_not_docs = generate_range_docs(0, 1000, 3);
    auto expected = set_difference(must_docs, must_not_docs);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, ShouldWithMustNotExclusion) {
    auto should_docs = generate_range_docs(0, 1000, 2);
    auto must_not_docs = generate_range_docs(0, 500);
    auto expected = set_difference(should_docs, must_not_docs);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, MustAndShouldCombined) {
    auto must_docs = generate_range_docs(0, 500);
    auto should_docs = generate_range_docs(250, 750);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, must_docs);
}

TEST_F(OccurBooleanQueryTest, MultipleMustClausesIntersection) {
    auto docs1 = generate_range_docs(0, 10000, 2);
    auto docs2 = generate_range_docs(0, 10000, 3);
    auto docs3 = generate_range_docs(0, 10000, 5);
    auto expected = set_intersection(set_intersection(docs1, docs2), docs3);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs3));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, MultipleShouldClausesUnion) {
    auto docs1 = generate_range_docs(0, 3000, 7);
    auto docs2 = generate_range_docs(1000, 4000, 11);
    auto docs3 = generate_range_docs(2000, 5000, 13);
    auto expected = set_union(set_union(docs1, docs2), docs3);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs2));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs3));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, ComplexMustShouldMustNot) {
    auto must_docs1 = generate_range_docs(0, 2000);
    auto must_docs2 = generate_range_docs(500, 2500);
    auto should_docs = generate_range_docs(0, 3000, 3);
    auto must_not_docs = generate_range_docs(1000, 1500);

    auto must_intersection = set_intersection(must_docs1, must_docs2);
    auto expected = set_difference(must_intersection, must_not_docs);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs2));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, LargeScaleIntersection) {
    auto docs1 = generate_random_docs(5000, 100000, 42);
    auto docs2 = generate_random_docs(5000, 100000, 123);
    auto expected = set_intersection(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, LargeScaleUnion) {
    auto docs1 = generate_random_docs(3000, 50000, 1);
    auto docs2 = generate_random_docs(3000, 50000, 2);
    auto docs3 = generate_random_docs(3000, 50000, 3);
    auto expected = set_union(set_union(docs1, docs2), docs3);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs2));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs3));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, LargeScaleExclusion) {
    auto must_docs = generate_range_docs(0, 50000);
    auto must_not_docs = generate_random_docs(10000, 50000, 999);
    auto expected = set_difference(must_docs, must_not_docs);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, DisjointMustClausesEmpty) {
    auto docs1 = generate_range_docs(0, 100);
    auto docs2 = generate_range_docs(200, 300);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_TRUE(result.empty());
}

TEST_F(OccurBooleanQueryTest, MustNotExcludesAllMust) {
    auto must_docs = generate_range_docs(0, 100);
    auto must_not_docs = generate_range_docs(0, 200);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_TRUE(result.empty());
}

TEST_F(OccurBooleanQueryTest, EmptyMustClause) {
    std::vector<uint32_t> empty_docs;
    auto docs2 = generate_range_docs(0, 100);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(empty_docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_TRUE(result.empty());
}

TEST_F(OccurBooleanQueryTest, EmptyShouldClause) {
    std::vector<uint32_t> empty_docs;
    auto docs2 = generate_range_docs(0, 100);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(empty_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, docs2);
}

TEST_F(OccurBooleanQueryTest, ScoringEnabled) {
    auto docs1 = generate_range_docs(0, 100, 2);
    auto docs2 = generate_range_docs(0, 100, 3);
    auto overlap = set_intersection(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs1, 1.0F));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs2, 2.0F));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(true);
    auto scorer = weight->scorer(_ctx);

    std::set<uint32_t> overlap_set(overlap.begin(), overlap.end());
    bool found_overlap_with_higher_score = false;
    bool found_single_match = false;

    uint32_t doc = scorer->seek(0);
    while (doc != TERMINATED) {
        float s = scorer->score();
        if (overlap_set.count(doc) > 0) {
            EXPECT_FLOAT_EQ(s, 3.0F);
            found_overlap_with_higher_score = true;
        } else {
            EXPECT_TRUE(s == 1.0F || s == 2.0F);
            found_single_match = true;
        }
        doc = scorer->advance();
    }

    EXPECT_TRUE(found_overlap_with_higher_score);
    EXPECT_TRUE(found_single_match);
}

TEST_F(OccurBooleanQueryTest, ManyMustClausesStress) {
    std::vector<std::vector<uint32_t>> doc_sets;
    for (int i = 0; i < 10; ++i) {
        doc_sets.push_back(generate_range_docs(0, 10000, i + 2));
    }

    auto expected = doc_sets[0];
    for (size_t i = 1; i < doc_sets.size(); ++i) {
        expected = set_intersection(expected, doc_sets[i]);
    }

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    for (const auto& docs : doc_sets) {
        clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs));
    }

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, ManyShouldClausesStress) {
    std::vector<std::vector<uint32_t>> doc_sets;
    for (int i = 0; i < 20; ++i) {
        doc_sets.push_back(generate_random_docs(500, 20000, i * 100));
    }

    std::set<uint32_t> expected_set;
    for (const auto& docs : doc_sets) {
        expected_set.insert(docs.begin(), docs.end());
    }

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    for (const auto& docs : doc_sets) {
        clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs));
    }

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected_set.size());
    EXPECT_EQ(to_set(result), expected_set);
}

TEST_F(OccurBooleanQueryTest, MultipleMustNotClauses) {
    auto must_docs = generate_range_docs(0, 5000);
    auto must_not_docs1 = generate_range_docs(0, 1000);
    auto must_not_docs2 = generate_range_docs(2000, 3000);
    auto must_not_docs3 = generate_range_docs(4000, 5000);

    auto expected = set_difference(must_docs, must_not_docs1);
    expected = set_difference(expected, must_not_docs2);
    expected = set_difference(expected, must_not_docs3);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs1));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs2));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs3));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), expected.size());
    EXPECT_EQ(to_set(result), to_set(expected));
}

TEST_F(OccurBooleanQueryTest, SeekOperations) {
    auto docs1 = generate_range_docs(0, 10000, 2);
    auto docs2 = generate_range_docs(0, 10000, 3);
    auto expected = set_intersection(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);

    EXPECT_EQ(scorer->seek(100), 102);
    EXPECT_EQ(scorer->seek(500), 504);
    EXPECT_EQ(scorer->seek(1000), 1002);

    uint32_t current = scorer->doc();
    while (current != TERMINATED && current < 5000) {
        current = scorer->advance();
    }

    EXPECT_EQ(scorer->seek(6000), 6000);
    EXPECT_EQ(scorer->seek(9999), TERMINATED);
}

TEST_F(OccurBooleanQueryTest, IdenticalDocSets) {
    auto docs = generate_range_docs(0, 1000);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, docs);
}

TEST_F(OccurBooleanQueryTest, OverlappingShouldClauses) {
    auto docs = generate_range_docs(0, 100);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, docs);
}

TEST_F(OccurBooleanQueryTest, SparseDocIds) {
    std::vector<uint32_t> docs1 = {0, 10000, 20000, 30000, 40000};
    std::vector<uint32_t> docs2 = {0, 5000, 10000, 15000, 20000, 25000, 30000};
    auto expected = set_intersection(docs1, docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, OnlyMustNotClausesEmpty) {
    auto docs1 = generate_range_docs(0, 100);
    auto docs2 = generate_range_docs(50, 150);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(docs1));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(docs2));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);

    EXPECT_EQ(scorer->doc(), TERMINATED);
}

struct ExpectedToken {
    std::string term;
    int32_t pos = 0;

    bool operator==(const ExpectedToken& other) const {
        return term == other.term && pos == other.pos;
    }
};

std::vector<ExpectedToken> tokenize1(const CustomAnalyzerPtr& custom_analyzer,
                                     const std::string line) {
    std::vector<ExpectedToken> results;
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(line.data(), line.size(), false);
    auto* token_stream = custom_analyzer->reusableTokenStream(L"", reader);
    token_stream->reset();
    Token t;
    while (token_stream->next(&t)) {
        results.emplace_back(std::string(t.termBuffer<char>(), t.termLength<char>()),
                             t.getPositionIncrement());
    }
    return results;
}

// 测试用例 1：构建索引（导入数据）
TEST_F(OccurBooleanQueryTest, BuildIndex) {
    getchar();

    std::string name = "name";
    std::string path = "/mnt/disk3/yangsiyu/wikipedia_index_data";

    std::vector<std::string> lines;
    std::ifstream ifs("/mnt/disk3/yangsiyu/wikipedia/wikipedia.json000");
    std::string line;
    while (getline(ifs, line)) {
        lines.emplace_back(line);
    }
    ifs.close();

    std::cout << "lines size: " << lines.size() << std::endl;

    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("basic", {});
    builder.add_token_filter_config("lowercase", {});
    auto custom_analyzer_config = builder.build();

    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    {
        lucene::index::IndexWriter indexwriter(path.c_str(), custom_analyzer.get(), true);
        indexwriter.setRAMBufferSizeMB(512);
        indexwriter.setMaxFieldLength(0x7FFFFFFFL);
        indexwriter.setMergeFactor(1000000000);
        indexwriter.setUseCompoundFile(false);

        auto reader = std::make_shared<lucene::util::SStringReader<char>>();

        lucene::document::Document doc;
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name = std::wstring(name.begin(), name.end());
        auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        field->setOmitNorms(false);
        field->setIndexVersion(IndexVersion::kV4);
        doc.add(*field);

        for (int32_t j = 0; j < 1; j++) {
            for (size_t k = 0; k < lines.size(); k++) {
                reader->init(lines[k].data(), lines[k].size(), false);
                auto* stream = custom_analyzer->reusableTokenStream(field->name(), reader);
                field->setValue(stream);

                indexwriter.addDocument(&doc);
            }
        }

        indexwriter.close();
    }

    std::cout << "Index built successfully!" << std::endl;
}

// 测试用例 2：测试单个词（详细输出所有行）
TEST_F(OccurBooleanQueryTest, TestSingleTermDetailed) {
    std::string name = "name";
    std::string path = "/mnt/disk3/yangsiyu/wikipedia_index_data";

    std::cout << "\n=== Detailed Verification for Single Term ===" << std::endl;

    auto* reader = lucene::index::IndexReader::open(path.c_str());

    // Specify term to search for
    auto field_name_w = std::wstring(name.begin(), name.end());
    std::wstring search_term = L"easy";

    lucene::index::Term term(field_name_w.c_str(), search_term.c_str());
    std::wcout << L"Testing term: " << search_term << std::endl;

    // Get term statistics for BM25
    int32_t numDocs = reader->numDocs();
    int32_t docFreq = reader->docFreq(&term);

    // Calculate IDF: log(1 + (N - n + 0.5) / (n + 0.5))
    float idf = std::log(1.0F + (static_cast<float>(numDocs) - docFreq + 0.5F) / (docFreq + 0.5F));

    // 从索引中获取真实的 avgdl (total_term_count / numDocs)
    float avgdl = 0.0F;
    auto sumTermFreq = reader->sumTotalTermFreq(field_name_w.c_str());
    if (sumTermFreq.has_value() && numDocs > 0) {
        avgdl = static_cast<float>(sumTermFreq.value()) / static_cast<float>(numDocs);
    }
    std::cout << "SumTermFreq: "
              << (sumTermFreq.has_value() ? std::to_string(sumTermFreq.value()) : "N/A")
              << ", NumDocs: " << numDocs << ", avgdl: " << avgdl << std::endl;
    std::cout << "NumDocs: " << numDocs << ", DocFreq: " << docFreq << ", IDF: " << idf
              << std::endl;

    // Create BM25Similarity with real avgdl from index
    auto bm25 = std::make_shared<BM25Similarity>(idf, avgdl);

    // Get posting list with block reading, enable load_stats to get norm values
    auto* rawTermPos = reader->termPositions(&term, true);
    TermPositionsPtr termPosPtr(rawTermPos);

    // 使用新的构造函数，传入 enable_scoring=true 和 similarity
    SegmentPostings postings(std::move(termPosPtr), true, bm25);

    float maxScore = 0.0F;
    int32_t maxScoreDocId = -1;
    int32_t docCount = 0;
    int32_t violationCount = 0;
    int32_t noSkipCount = 0;
    int32_t okCount = 0;

    // Block 级别统计（使用 block_id 跟踪）
    std::set<int64_t> blocksWithViolation;
    int64_t maxBlockId = 0;

    // 限制文档数量，避免处理时间过长
    const int32_t MAX_DOCS = 100000;

    std::cout << "\n=== Processing documents (max " << MAX_DOCS << ") ===" << std::endl;

    uint32_t docId = postings.doc();
    while (docId != TERMINATED && docCount < MAX_DOCS) {
        int32_t freq = postings.freq();
        int32_t normEncoded = postings.norm();

        // Calculate BM25 score
        float score = bm25->score(static_cast<float>(freq), normEncoded);

        // 获取 block_max_score 和 block_id
        float blockMaxScore = postings.block_max_score();
        int64_t blockId = postings.block_id();
        maxBlockId = std::max(maxBlockId, blockId);

        // 判断状态
        if (blockMaxScore == SegmentPostings::MAX_SCORE) {
            noSkipCount++;
        } else if (score <= blockMaxScore + 0.001F) {
            okCount++;
        } else {
            violationCount++;
            blocksWithViolation.insert(blockId);
        }

        if (score > maxScore) {
            maxScore = score;
            maxScoreDocId = docId;
        }

        docCount++;
        docId = postings.advance();
    }

    std::cout << "\n=== Final Summary ===" << std::endl;
    std::cout << "Total docs processed: " << docCount << std::endl;
    std::cout << "Total docFreq: " << docFreq << std::endl;
    std::cout << "Total blocks: " << maxBlockId << std::endl;
    std::cout << "Global Max Score: " << maxScore << " (DocID: " << maxScoreDocId << ")"
              << std::endl;
    std::cout << "OK count: " << okCount << std::endl;
    std::cout << "NO_SKIP count: " << noSkipCount << std::endl;
    std::cout << "Violations (docs): " << violationCount << std::endl;
    std::cout << "Blocks with violations: " << blocksWithViolation.size() << std::endl;

    // 先清理资源，再检查断言
    reader->close();
    _CLLDELETE(reader);

    // 计算违规比例
    float docViolationRate =
            docCount > 0 ? (static_cast<float>(violationCount) / docCount * 100.0F) : 0.0F;
    float blockViolationRate =
            maxBlockId > 0 ? (static_cast<float>(blocksWithViolation.size()) / maxBlockId * 100.0F)
                           : 0.0F;
    std::cout << "Doc violation rate: " << docViolationRate << "%" << std::endl;
    std::cout << "Block violation rate: " << blockViolationRate << "%" << std::endl;
}

// 测试用例 3：测试所有词，输出每个 term 的 Block violation rate
TEST_F(OccurBooleanQueryTest, TestAllTerms) {
    std::string name = "name";
    std::string path = "/mnt/disk3/yangsiyu/wikipedia_index_data";

    std::cout << "\n=== Block Violation Rate Analysis for All Terms ===" << std::endl;

    auto* reader = lucene::index::IndexReader::open(path.c_str());
    auto field_name_w = std::wstring(name.begin(), name.end());

    int32_t numDocs = reader->numDocs();

    // 从索引中获取真实的 avgdl (total_term_count / numDocs)
    float avgdl = 128.0F; // 默认值
    auto sumTermFreq = reader->sumTotalTermFreq(field_name_w.c_str());
    if (sumTermFreq.has_value() && numDocs > 0) {
        avgdl = static_cast<float>(sumTermFreq.value()) / static_cast<float>(numDocs);
    }
    std::cout << "SumTermFreq: "
              << (sumTermFreq.has_value() ? std::to_string(sumTermFreq.value()) : "N/A")
              << ", NumDocs: " << numDocs << ", avgdl: " << avgdl << std::endl;

    // Get all terms in the field
    lucene::index::Term fieldTerm(field_name_w.c_str(), L"");
    auto* termEnum = reader->terms(&fieldTerm);

    int32_t totalTerms = 0;
    int32_t testedTerms = 0;
    int32_t termsWithViolations = 0;

    // 全局统计
    int64_t globalTotalBlocks = 0;
    int64_t globalBlocksWithViolation = 0;
    int64_t globalTotalDocs = 0;
    int64_t globalDocsWithViolation = 0;

    // 先输出表头，只有当有 violation 时才会输出数据行
    bool headerPrinted = false;

    // Iterate through all terms
    while (termEnum->next()) {
        lucene::index::Term* currentTerm = termEnum->term();

        // Only process terms from our field
        if (wcscmp(currentTerm->field(), field_name_w.c_str()) != 0) {
            _CLDECDELETE(currentTerm);
            break;
        }

        totalTerms++;
        std::wstring termText = currentTerm->text();

        // Get term statistics
        int32_t docFreq = reader->docFreq(currentTerm);

        // 不再跳过低频词，测试所有词
        testedTerms++;

        try {
            // Calculate IDF for BM25
            float idf = std::log(1.0F +
                                 (static_cast<float>(numDocs) - docFreq + 0.5F) / (docFreq + 0.5F));
            auto bm25 = std::make_shared<BM25Similarity>(idf, avgdl);

            // Get posting list
            auto* rawTermPos = reader->termPositions(currentTerm, true);
            if (rawTermPos != nullptr) {
                TermPositionsPtr termPosPtr(rawTermPos);
                SegmentPostings postings(std::move(termPosPtr), true, bm25);

                int32_t docCount = 0;
                int32_t violationCount = 0;
                int32_t noSkipCount = 0;
                int32_t okCount = 0;

                // Block 级别统计（使用 block_id 跟踪）
                std::set<int64_t> blocksWithViolation;
                int64_t maxBlockId = 0;

                // 跟踪最大误差（实际 score - 跳表 block_max_score）
                float maxScoreGap = 0.0F;

                uint32_t docId = postings.doc();
                while (docId != TERMINATED) {
                    int32_t freq = postings.freq();
                    int32_t normEncoded = postings.norm();

                    // Calculate BM25 score
                    float score = bm25->score(static_cast<float>(freq), normEncoded);

                    // 获取 block_max_score 和 block_id
                    float blockMaxScore = postings.block_max_score();
                    int64_t blockId = postings.block_id();
                    maxBlockId = std::max(maxBlockId, blockId);

                    // 判断状态
                    if (blockMaxScore == SegmentPostings::MAX_SCORE) {
                        noSkipCount++;
                    } else if (score <= blockMaxScore + 0.001F) {
                        okCount++;
                    } else {
                        violationCount++;
                        blocksWithViolation.insert(blockId);
                        // 计算误差：实际 score - 跳表 block_max_score
                        float gap = score - blockMaxScore;
                        if (gap > maxScoreGap) {
                            maxScoreGap = gap;
                        }
                    }

                    docCount++;
                    docId = postings.advance();
                }
                (void)noSkipCount;
                (void)okCount;

                // 计算该 term 的 block violation rate
                float blockViolationRate = 0.0F;
                if (maxBlockId > 0) {
                    blockViolationRate = static_cast<float>(blocksWithViolation.size()) /
                                         static_cast<float>(maxBlockId) * 100.0F;
                }

                // 更新全局统计
                globalTotalBlocks += maxBlockId;
                globalBlocksWithViolation += blocksWithViolation.size();
                globalTotalDocs += docCount;
                globalDocsWithViolation += violationCount;

                // 只输出有 violation 的 term
                if (!blocksWithViolation.empty()) {
                    termsWithViolations++;

                    // 第一次有 violation 时打印表头
                    if (!headerPrinted) {
                        std::cout << "\n--- Terms with Block Violations ---" << std::endl;
                        std::cout << std::left << std::setw(30) << "Term" << std::setw(12)
                                  << "DocFreq" << std::setw(12) << "Blocks" << std::setw(12)
                                  << "ViolBlocks" << std::setw(12) << "ViolRate(%)" << std::setw(12)
                                  << "MaxScoreGap" << std::endl;
                        std::cout << std::string(90, '-') << std::endl;
                        headerPrinted = true;
                    }

                    // 转换 wstring 为 string 用于输出
                    std::string termStr(termText.begin(), termText.end());
                    if (termStr.length() > 28) {
                        termStr = termStr.substr(0, 25) + "...";
                    }

                    std::cout << std::left << std::setw(30) << termStr << std::setw(12) << docFreq
                              << std::setw(12) << maxBlockId << std::setw(12)
                              << blocksWithViolation.size() << std::setw(12) << std::fixed
                              << std::setprecision(2) << blockViolationRate << std::setw(12)
                              << std::setprecision(4) << maxScoreGap << std::endl;
                }
            }

        } catch (const std::exception& e) {
            std::wcerr << L"[ERROR] Term '" << termText << L"': " << e.what() << std::endl;
        } catch (...) {
            std::wcerr << L"[ERROR] Term '" << termText << L"': Unknown exception" << std::endl;
        }

        _CLDECDELETE(currentTerm);
    }

    termEnum->close();
    _CLDELETE(termEnum);

    // Print summary
    std::cout << "\n=== Global Summary ===" << std::endl;
    std::cout << "Total terms in field: " << totalTerms << std::endl;
    std::cout << "Terms tested: " << testedTerms << std::endl;
    std::cout << "Terms with violations: " << termsWithViolations << std::endl;
    std::cout << "Total blocks: " << globalTotalBlocks << std::endl;
    std::cout << "Blocks with violations: " << globalBlocksWithViolation << std::endl;
    std::cout << "Total docs: " << globalTotalDocs << std::endl;
    std::cout << "Docs with violations: " << globalDocsWithViolation << std::endl;

    // 计算全局违规比例
    float globalBlockViolationRate = 0.0F;
    float globalDocViolationRate = 0.0F;
    if (globalTotalBlocks > 0) {
        globalBlockViolationRate = static_cast<float>(globalBlocksWithViolation) /
                                   static_cast<float>(globalTotalBlocks) * 100.0F;
    }
    if (globalTotalDocs > 0) {
        globalDocViolationRate = static_cast<float>(globalDocsWithViolation) /
                                 static_cast<float>(globalTotalDocs) * 100.0F;
    }

    std::cout << "\n=== Violation Rates ===" << std::endl;
    std::cout << "Global Block Violation Rate: " << std::fixed << std::setprecision(4)
              << globalBlockViolationRate << "%" << std::endl;
    std::cout << "Global Doc Violation Rate: " << std::fixed << std::setprecision(4)
              << globalDocViolationRate << "%" << std::endl;

    if (termsWithViolations == 0) {
        std::cout << "\n[PASS] No violations found in any term!" << std::endl;
    }

    reader->close();
    _CLLDELETE(reader);
}

} // namespace doris::segment_v2::inverted_index::query_v2
