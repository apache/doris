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

#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/be_mock_util.h"
#include "olap/collection_statistics.h"
#include "olap/rowset/segment_v2/index_query_context.h"

using namespace doris;
using namespace doris::segment_v2;

// Mock CollectionStatistics class for testing
class MockCollectionStatistics : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& lucene_col_name,
                               const std::wstring& term) override {
        return mock_idf_;
    }

    float get_or_calculate_avg_dl(const std::wstring& lucene_col_name) override {
        return mock_avg_dl_;
    }

    void set_mock_idf(float idf) { mock_idf_ = idf; }
    void set_mock_avg_dl(float avg_dl) { mock_avg_dl_ = avg_dl; }

private:
    float mock_idf_ = 2.5f;
    float mock_avg_dl_ = 10.0f;
};

class BM25SimilarityTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_stats_ = std::make_shared<MockCollectionStatistics>();
        context_ = std::make_shared<IndexQueryContext>();
        context_->collection_statistics = mock_stats_;
        similarity_ = std::make_unique<BM25Similarity>();
    }

    std::shared_ptr<MockCollectionStatistics> mock_stats_;
    std::shared_ptr<IndexQueryContext> context_;
    std::unique_ptr<BM25Similarity> similarity_;
};

TEST_F(BM25SimilarityTest, ConstructorTest) {
    BM25Similarity sim;
    ASSERT_EQ(sim._cache.size(), 256);

    ASSERT_FLOAT_EQ(sim._boost, 1.0f);
    ASSERT_FLOAT_EQ(sim._k1, 1.2f);
    ASSERT_FLOAT_EQ(sim._b, 0.75f);
    ASSERT_FLOAT_EQ(sim._idf, 0.0f);
    ASSERT_FLOAT_EQ(sim._avgdl, 0.0f);
    ASSERT_FLOAT_EQ(sim._weight, 1.0f);
}

TEST_F(BM25SimilarityTest, ForOneTermTest) {
    mock_stats_->set_mock_idf(2.5f);
    mock_stats_->set_mock_avg_dl(10.0f);

    std::wstring field_name = L"test_field";
    std::wstring term = L"test_term";

    similarity_->for_one_term(context_, field_name, term);

    ASSERT_FLOAT_EQ(similarity_->_idf, 2.5f);
    ASSERT_FLOAT_EQ(similarity_->_avgdl, 10.0f);
    ASSERT_FLOAT_EQ(similarity_->_weight, 1.0f * 2.5f * (1.2f + 1.0f)); // boost * idf * (k1 + 1)

    ASSERT_GT(similarity_->_cache[0], 0.0f);
    ASSERT_GT(similarity_->_cache[255], 0.0f);
}

TEST_F(BM25SimilarityTest, ScoreTest) {
    mock_stats_->set_mock_idf(2.0f);
    mock_stats_->set_mock_avg_dl(5.0f);

    similarity_->for_one_term(context_, L"field", L"term");

    float score1 = similarity_->score(1.0f, 0);
    float score2 = similarity_->score(2.0f, 0);
    float score3 = similarity_->score(1.0f, 128);

    ASSERT_GT(score1, 0.0f);
    ASSERT_GT(score2, score1);
    ASSERT_NE(score1, score3);
}

TEST_F(BM25SimilarityTest, ScoreEdgeCasesTest) {
    mock_stats_->set_mock_idf(1.0f);
    mock_stats_->set_mock_avg_dl(1.0f);

    similarity_->for_one_term(context_, L"field", L"term");

    float score_zero = similarity_->score(0.0f, 0);
    ASSERT_FLOAT_EQ(score_zero, 0.0f);

    float score_high = similarity_->score(1000.0f, 0);
    ASSERT_GT(score_high, 0.0f);

    float score_max_norm = similarity_->score(1.0f, 255);
    ASSERT_GT(score_max_norm, 0.0f);
}

TEST_F(BM25SimilarityTest, Int4EncodingTest) {
    std::vector<int32_t> values = {
            0,         1,         16,        128,        1430,       10000,    65535,
            100000,    1000000,   2000000,   5000000,    10000000,   16777215, 50000000,
            100000000, 268435455, 500000000, 1000000000, 2147483647, 8388608,  16777216};

    std::vector<int32_t> expected_restored = {
            0,        1,         16,        128,       1304,       9240,     61464,
            98328,    983064,    1966104,   4718616,   9437208,    15728664, 46137368,
            92274712, 251658264, 469762072, 939524120, 2013265944, 7864344,  15728664};

    for (size_t i = 0; i < values.size(); ++i) {
        uint8_t c = doris::segment_v2::BM25Similarity::int_to_byte4(values[i]);
        int32_t restored = doris::segment_v2::BM25Similarity::byte4_to_int(c);
        ASSERT_EQ(restored, expected_restored[i]);
    }
}

TEST_F(BM25SimilarityTest, Int4EncodingEdgeCasesTest) {
    ASSERT_EQ(BM25Similarity::byte4_to_int(BM25Similarity::int_to_byte4(0)), 0);

    int32_t max_val = std::numeric_limits<int32_t>::max();
    uint8_t encoded = BM25Similarity::int_to_byte4(max_val);
    int32_t decoded = BM25Similarity::byte4_to_int(encoded);
    ASSERT_GT(decoded, 0);

    int32_t boundary = 255 - static_cast<int>(BM25Similarity::MAX_INT4);
    uint8_t encoded_boundary = BM25Similarity::int_to_byte4(boundary - 1);
    uint8_t encoded_after = BM25Similarity::int_to_byte4(boundary + 1);

    ASSERT_LT(encoded_boundary, boundary);
    ASSERT_GE(encoded_after, boundary);
}

TEST_F(BM25SimilarityTest, Int4EncodingExceptionTest) {
    ASSERT_THROW(BM25Similarity::int_to_byte4(-1), Exception);
    ASSERT_THROW(BM25Similarity::int_to_byte4(std::numeric_limits<int32_t>::min()), Exception);
}

TEST_F(BM25SimilarityTest, NumberOfLeadingZerosTest) {
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(0), 64);

    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(1), 63);
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(2), 62);
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(4), 61);
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(8), 60);

    uint64_t max_val = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(max_val), 0);

    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(0x8000000000000000ULL), 0);
    ASSERT_EQ(BM25Similarity::number_of_leading_zeros(0x4000000000000000ULL), 1);
}

TEST_F(BM25SimilarityTest, LongToInt4Test) {
    for (uint64_t i = 0; i < 16; ++i) {
        ASSERT_EQ(BM25Similarity::long_to_int4(i), i);
    }

    ASSERT_EQ(BM25Similarity::long_to_int4(16), 16);
    ASSERT_NE(BM25Similarity::long_to_int4(1000), 1000);

    uint32_t result = BM25Similarity::long_to_int4(15);
    ASSERT_EQ(result, 15);
}

TEST_F(BM25SimilarityTest, Int4ToLongTest) {
    for (uint32_t i = 0; i < 8; ++i) {
        ASSERT_EQ(BM25Similarity::int4_to_long(i), i);
    }

    uint64_t original = 1000;
    uint32_t encoded = BM25Similarity::long_to_int4(original);
    uint64_t decoded = BM25Similarity::int4_to_long(encoded);

    ASSERT_GT(decoded, 0);
}

TEST_F(BM25SimilarityTest, RoundTripInt4Test) {
    std::vector<uint64_t> test_values = {
            0,       1,
            15,      16,
            100,     1000,
            10000,   100000,
            1000000, static_cast<uint64_t>(std::numeric_limits<int32_t>::max())};

    for (uint64_t val : test_values) {
        uint32_t encoded = BM25Similarity::long_to_int4(val);
        uint64_t decoded = BM25Similarity::int4_to_long(encoded);

        if (val < 16) {
            ASSERT_EQ(decoded, val);
        } else {
            ASSERT_GT(decoded, 0);
        }
    }
}

TEST_F(BM25SimilarityTest, LengthTableInitializationTest) {
    ASSERT_EQ(BM25Similarity::LENGTH_TABLE.size(), 256);

    ASSERT_EQ(BM25Similarity::LENGTH_TABLE[0], 0);
    ASSERT_GT(BM25Similarity::LENGTH_TABLE[255], 0);

    for (size_t i = 0; i < BM25Similarity::LENGTH_TABLE.size(); ++i) {
        ASSERT_GE(BM25Similarity::LENGTH_TABLE[i], 0.0f);
    }
}

TEST_F(BM25SimilarityTest, DifferentParametersTest) {
    mock_stats_->set_mock_idf(1.0f);
    mock_stats_->set_mock_avg_dl(1.0f);

    BM25Similarity sim1;
    sim1._boost = 2.0f;
    sim1.for_one_term(context_, L"field", L"term");

    BM25Similarity sim2;
    sim2._boost = 0.5f;
    sim2.for_one_term(context_, L"field", L"term");

    float score1 = sim1.score(1.0f, 0);
    float score2 = sim2.score(1.0f, 0);

    ASSERT_GT(score1, score2);
}

TEST_F(BM25SimilarityTest, CacheConsistencyTest) {
    mock_stats_->set_mock_idf(2.0f);
    mock_stats_->set_mock_avg_dl(8.0f);

    similarity_->for_one_term(context_, L"field", L"term");

    for (int i = 0; i < 256; ++i) {
        float expected =
                1.0f / (similarity_->_k1 *
                        ((1 - similarity_->_b) +
                         similarity_->_b * BM25Similarity::LENGTH_TABLE[i] / similarity_->_avgdl));
        ASSERT_FLOAT_EQ(similarity_->_cache[i], expected);
    }
}
