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

#include "olap/rowset/segment_v2/inverted_index/query_v2/all_query/all_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/occur_boolean_weight.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

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

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchExceedsShouldClausesReturnsEmpty) {
    {
        auto must_docs1 = generate_range_docs(0, 100);
        auto must_docs2 = generate_range_docs(50, 150);
        std::vector<std::pair<Occur, QueryPtr>> clauses;
        clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs1));
        clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs2));

        OccurBooleanQuery query(std::move(clauses), 2);
        auto weight = query.weight(false);
        auto scorer = weight->scorer(_ctx);

        EXPECT_EQ(scorer->doc(), TERMINATED);
    }

    {
        auto must_docs = generate_range_docs(0, 100);
        auto should_docs = generate_range_docs(0, 100);
        std::vector<std::pair<Occur, QueryPtr>> clauses;
        clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
        clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));

        OccurBooleanQuery query(std::move(clauses), 2);
        auto weight = query.weight(false);
        auto scorer = weight->scorer(_ctx);

        EXPECT_EQ(scorer->doc(), TERMINATED);
    }

    {
        auto should_docs1 = generate_range_docs(0, 100);
        auto should_docs2 = generate_range_docs(50, 150);
        auto expected = set_intersection(should_docs1, should_docs2);

        std::vector<std::pair<Occur, QueryPtr>> clauses;
        clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs1));
        clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs2));

        OccurBooleanQuery query(std::move(clauses), 2);
        auto weight = query.weight(false);
        auto scorer = weight->scorer(_ctx);
        auto result = collect_docs(scorer);

        EXPECT_EQ(result.size(), expected.size());
        EXPECT_EQ(to_set(result), to_set(expected));
    }

    {
        auto must_docs = generate_range_docs(0, 100);
        auto must_not_docs = generate_range_docs(50, 150);

        std::vector<std::pair<Occur, QueryPtr>> clauses;
        clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
        clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

        OccurBooleanQuery query(std::move(clauses), 2);
        auto weight = query.weight(false);
        auto scorer = weight->scorer(_ctx);

        EXPECT_EQ(scorer->doc(), TERMINATED);
    }
}

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchZeroWithNoShouldClausesReturnsIgnored) {
    auto must_docs1 = generate_range_docs(0, 100);
    auto must_docs2 = generate_range_docs(50, 150);
    auto expected = set_intersection(must_docs1, must_docs2);

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs1));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs2));

    OccurBooleanQuery query(std::move(clauses), 0);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchEqualsNumShouldWithMustClause) {
    auto must_docs = std::vector<uint32_t> {10, 20};
    auto should1_docs = std::vector<uint32_t> {10, 20, 30, 100};
    auto should2_docs = std::vector<uint32_t> {10, 20, 30, 200};
    auto expected = std::vector<uint32_t> {10, 20};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs));

    OccurBooleanQuery query(std::move(clauses), 2);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchEqualsNumShouldWithMultipleMustClauses) {
    auto must1_docs = std::vector<uint32_t> {10, 20, 30, 40, 50};
    auto must2_docs = std::vector<uint32_t> {10, 20, 30, 60, 70};
    auto should1_docs = std::vector<uint32_t> {10, 20, 30, 100};
    auto should2_docs = std::vector<uint32_t> {10, 20, 30, 200};
    auto should3_docs = std::vector<uint32_t> {10, 20, 30, 300};
    auto expected = std::vector<uint32_t> {10, 20, 30};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must1_docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must2_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should3_docs));

    OccurBooleanQuery query(std::move(clauses), 3);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchEqualsNumShouldOnlyShould) {
    auto should1_docs = std::vector<uint32_t> {10, 20, 30, 40};
    auto should2_docs = std::vector<uint32_t> {20, 30, 40, 50};
    auto should3_docs = std::vector<uint32_t> {30, 40, 50, 60};
    auto expected = std::vector<uint32_t> {30, 40};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should3_docs));

    OccurBooleanQuery query(std::move(clauses), 3);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, MinimumShouldMatchEqualsNumShouldWithMustNot) {
    auto must_docs = std::vector<uint32_t> {10, 20, 30, 40, 50};
    auto should1_docs = std::vector<uint32_t> {10, 20, 30, 100};
    auto should2_docs = std::vector<uint32_t> {10, 20, 30, 200};
    auto must_not_docs = std::vector<uint32_t> {20, 100, 200};
    auto expected = std::vector<uint32_t> {10, 30};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs));
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses), 2);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, AllQueryWithMustClause) {
    _ctx.segment_num_rows = 100;

    auto must_docs = std::vector<uint32_t> {10, 20, 30, 40, 50};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, must_docs);
}

TEST_F(OccurBooleanQueryTest, AllQueryWithShouldClause) {
    _ctx.segment_num_rows = 50;

    auto should_docs = std::vector<uint32_t> {10, 20, 30};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), 50);
    EXPECT_EQ(result.front(), 0);
    EXPECT_EQ(result.back(), 49);
}

TEST_F(OccurBooleanQueryTest, AllQueryWithMustNotClause) {
    _ctx.segment_num_rows = 100;

    auto must_not_docs = std::vector<uint32_t> {10, 20, 30, 40, 50};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());
    clauses.emplace_back(Occur::MUST_NOT, std::make_shared<MockQuery>(must_not_docs));

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), 95);
    for (uint32_t doc : must_not_docs) {
        EXPECT_TRUE(std::find(result.begin(), result.end(), doc) == result.end());
    }
}

TEST_F(OccurBooleanQueryTest, MultipleAllQueriesWithMust) {
    _ctx.segment_num_rows = 100;

    auto must_docs = std::vector<uint32_t> {5, 15, 25, 35, 45};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result, must_docs);
}

TEST_F(OccurBooleanQueryTest, AllQueryOnlyMust) {
    _ctx.segment_num_rows = 50;

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), 50);
    for (uint32_t i = 0; i < 50; ++i) {
        EXPECT_EQ(result[i], i);
    }
}

TEST_F(OccurBooleanQueryTest, AllQueryWithMustAndShouldMinMatch) {
    _ctx.segment_num_rows = 100;

    auto must_docs = std::vector<uint32_t> {10, 20, 30, 40, 50};
    auto should1_docs = std::vector<uint32_t> {10, 20, 30};
    auto should2_docs = std::vector<uint32_t> {10, 20, 40};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs));
    clauses.emplace_back(Occur::MUST, std::make_shared<AllQuery>());
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs));

    OccurBooleanQuery query(std::move(clauses), 2);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    auto expected = std::vector<uint32_t> {10, 20};
    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, ScoringWithMinimumShouldMatchEqualsNumShould) {
    auto must_docs = std::vector<uint32_t> {10, 20, 30};
    auto should1_docs = std::vector<uint32_t> {10, 20, 30, 100};
    auto should2_docs = std::vector<uint32_t> {10, 20, 30, 200};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::MUST, std::make_shared<MockQuery>(must_docs, 1.0F));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should1_docs, 2.0F));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should2_docs, 3.0F));

    OccurBooleanQuery query(std::move(clauses), 2);
    auto weight = query.weight(true);
    auto scorer = weight->scorer(_ctx);

    std::vector<uint32_t> result;
    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        result.push_back(doc);
        float s = scorer->score();
        EXPECT_FLOAT_EQ(s, 6.0F);
        doc = scorer->advance();
    }

    auto expected = std::vector<uint32_t> {10, 20, 30};
    EXPECT_EQ(result, expected);
}

TEST_F(OccurBooleanQueryTest, ShouldOnlyWithAllQueryMinShouldMatch) {
    _ctx.segment_num_rows = 50;

    auto should_docs = std::vector<uint32_t> {10, 20, 30, 40, 45};

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD, std::make_shared<MockQuery>(should_docs));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses), 2);
    auto weight = query.weight(false);
    auto scorer = weight->scorer(_ctx);
    auto result = collect_docs(scorer);

    EXPECT_EQ(result.size(), 5);
    EXPECT_EQ(result, should_docs);
}

TEST_F(OccurBooleanQueryTest, ShouldOnlyAllQueryScoring) {
    _ctx.segment_num_rows = 10;

    std::vector<std::pair<Occur, QueryPtr>> clauses;
    clauses.emplace_back(Occur::SHOULD,
                         std::make_shared<MockQuery>(std::vector<uint32_t> {1, 2}, 2.0F));
    clauses.emplace_back(Occur::SHOULD, std::make_shared<AllQuery>());

    OccurBooleanQuery query(std::move(clauses));
    auto weight = query.weight(true);
    auto scorer = weight->scorer(_ctx);

    uint32_t doc = scorer->doc();
    while (doc != TERMINATED) {
        float s = scorer->score();
        if (doc == 1 || doc == 2) {
            EXPECT_FLOAT_EQ(s, 3.0F);
        } else {
            EXPECT_FLOAT_EQ(s, 1.0F);
        }
        doc = scorer->advance();
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
