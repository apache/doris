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

#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/be_mock_util.h"
#include "olap/collection_similarity.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/similarity/similarity.h"
#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_position_iterator.h"

using namespace doris;
using namespace doris::segment_v2;

class MockSimilarity : public doris::segment_v2::Similarity {
public:
    MockSimilarity(float score_value) : _score_value(score_value) {}

    MOCK_FUNCTION void for_one_term(const IndexQueryContextPtr& context,
                                    const std::wstring& field_name,
                                    const std::wstring& term) override {}

    MOCK_FUNCTION float score(float freq, int64_t encoded_norm) override { return _score_value; }

private:
    float _score_value;
};

class MockTermIterator : public TermIterator {
public:
    MockTermIterator(int32_t freq_val, int32_t norm_val) : _freq(freq_val), _norm(norm_val) {}

    int32_t freq() const override { return _freq; }
    int32_t norm() const override { return _norm; }

private:
    int32_t _freq;
    int32_t _norm;
};

class MockTermPositionsIterator : public TermPositionsIterator {
public:
    MockTermPositionsIterator(int32_t freq_val, int32_t norm_val)
            : _freq(freq_val), _norm(norm_val) {}

    int32_t freq() const override { return _freq; }
    int32_t norm() const override { return _norm; }

private:
    int32_t _freq;
    int32_t _norm;
};

class QueryHelperTest : public ::testing::Test {
protected:
    void SetUp() override {
        _context = std::make_shared<IndexQueryContext>();
        _context->collection_similarity = std::make_shared<CollectionSimilarity>();
    }

    IndexQueryContextPtr _context;
};

TEST_F(QueryHelperTest, CollectWithTermIterators) {
    std::vector<SimilarityPtr> similarities;
    similarities.push_back(std::make_unique<MockSimilarity>(1.5f));
    similarities.push_back(std::make_unique<MockSimilarity>(2.0f));

    std::vector<TermIterPtr> iterators;
    iterators.push_back(std::make_shared<MockTermIterator>(3, 100));
    iterators.push_back(std::make_shared<MockTermIterator>(5, 200));

    int32_t doc = 42;

    QueryHelper::collect(_context, similarities, iterators, doc);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 1);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(doc) > 0);
}

TEST_F(QueryHelperTest, CollectWithEmptyTermIterators) {
    std::vector<SimilarityPtr> similarities;
    std::vector<TermIterPtr> iterators;

    int32_t doc = 42;

    QueryHelper::collect(_context, similarities, iterators, doc);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 0);
}

TEST_F(QueryHelperTest, CollectWithDISIContainingTermPositionsIter) {
    std::vector<SimilarityPtr> similarities;
    similarities.push_back(std::make_unique<MockSimilarity>(2.5f));
    similarities.push_back(std::make_unique<MockSimilarity>(3.0f));

    std::vector<DISI> iterators;
    iterators.push_back(std::make_shared<MockTermPositionsIterator>(4, 150));
    iterators.push_back(std::make_shared<MockTermPositionsIterator>(6, 250));

    int32_t doc = 123;

    QueryHelper::collect(_context, similarities, iterators, doc);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 1);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(doc) > 0);
}

TEST_F(QueryHelperTest, CollectWithDISIContainingNonTermPositionsIter) {
    std::vector<SimilarityPtr> similarities;
    similarities.push_back(std::make_unique<MockSimilarity>(1.0f));

    std::vector<DISI> iterators;
    iterators.push_back(std::make_shared<inverted_index::MockIterator>());

    int32_t doc = 456;

    QueryHelper::collect(_context, similarities, iterators, doc);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 0);
}

TEST_F(QueryHelperTest, CollectWithEmptyDISI) {
    std::vector<SimilarityPtr> similarities;
    std::vector<DISI> iterators;

    int32_t doc = 789;

    QueryHelper::collect(_context, similarities, iterators, doc);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 0);
}

TEST_F(QueryHelperTest, CollectMany) {
    SimilarityPtr similarity = std::make_unique<MockSimilarity>(4.0f);

    DocRange doc_range;
    doc_range.type_ = DocRangeType::kMany;
    doc_range.doc_many_size_ = 3;

    std::vector<uint32_t> docs = {10, 20, 30};
    std::vector<uint32_t> freqs = {2, 3, 4};
    std::vector<uint32_t> norms = {100, 200, 300};

    doc_range.doc_many = &docs;
    doc_range.freq_many = &freqs;
    doc_range.norm_many = &norms;

    QueryHelper::collect_many(_context, similarity, doc_range);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 3);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(10) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(20) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(30) > 0);
}

TEST_F(QueryHelperTest, CollectManyWithZeroSize) {
    SimilarityPtr similarity = std::make_unique<MockSimilarity>(4.0f);

    DocRange doc_range;
    doc_range.type_ = DocRangeType::kMany;
    doc_range.doc_many_size_ = 0;

    QueryHelper::collect_many(_context, similarity, doc_range);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 0);
}

TEST_F(QueryHelperTest, CollectRange) {
    SimilarityPtr similarity = std::make_unique<MockSimilarity>(5.0f);

    DocRange doc_range;
    doc_range.type_ = DocRangeType::kRange;
    doc_range.doc_range = std::make_pair(100, 105);

    std::vector<uint32_t> freqs = {1, 2, 3, 4, 5};
    std::vector<uint32_t> norms = {50, 60, 70, 80, 90};

    doc_range.freq_many = &freqs;
    doc_range.norm_many = &norms;

    QueryHelper::collect_range(_context, similarity, doc_range);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 5);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(100) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(101) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(102) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(103) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(104) > 0);
}

TEST_F(QueryHelperTest, CollectRangeWithZeroSize) {
    SimilarityPtr similarity = std::make_unique<MockSimilarity>(5.0f);

    DocRange doc_range;
    doc_range.type_ = DocRangeType::kRange;
    doc_range.doc_range = std::make_pair(200, 200);

    std::vector<uint32_t> freqs;
    std::vector<uint32_t> norms;

    doc_range.freq_many = &freqs;
    doc_range.norm_many = &norms;

    QueryHelper::collect_range(_context, similarity, doc_range);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 0);
}

TEST_F(QueryHelperTest, CollectMixedScenario) {
    std::vector<SimilarityPtr> similarities;
    similarities.push_back(std::make_unique<MockSimilarity>(1.0f));

    std::vector<TermIterPtr> iterators;
    iterators.push_back(std::make_shared<MockTermIterator>(1, 50));

    QueryHelper::collect(_context, similarities, iterators, 1000);

    SimilarityPtr similarity_for_many = std::make_unique<MockSimilarity>(2.0f);
    DocRange doc_range_many;
    doc_range_many.type_ = DocRangeType::kMany;
    doc_range_many.doc_many_size_ = 2;

    std::vector<uint32_t> docs_many = {1001, 1002};
    std::vector<uint32_t> freqs_many = {2, 3};
    std::vector<uint32_t> norms_many = {100, 150};

    doc_range_many.doc_many = &docs_many;
    doc_range_many.freq_many = &freqs_many;
    doc_range_many.norm_many = &norms_many;

    QueryHelper::collect_many(_context, similarity_for_many, doc_range_many);

    SimilarityPtr similarity_for_range = std::make_unique<MockSimilarity>(3.0f);
    DocRange doc_range_range;
    doc_range_range.type_ = DocRangeType::kRange;
    doc_range_range.doc_range = std::make_pair(2000, 2002);

    std::vector<uint32_t> freqs_range = {4, 5};
    std::vector<uint32_t> norms_range = {200, 250};

    doc_range_range.freq_many = &freqs_range;
    doc_range_range.norm_many = &norms_range;

    QueryHelper::collect_range(_context, similarity_for_range, doc_range_range);

    ASSERT_EQ(_context->collection_similarity->_bm25_scores.size(), 5);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(1000) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(1001) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(1002) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(2000) > 0);
    ASSERT_TRUE(_context->collection_similarity->_bm25_scores.count(2001) > 0);
}