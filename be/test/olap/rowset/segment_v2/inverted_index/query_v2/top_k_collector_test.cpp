
#include "olap/rowset/segment_v2/inverted_index/query_v2/collect/top_k_collector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <random>

namespace doris::segment_v2::inverted_index::query_v2 {

TEST(TopKCollectorTest, TestTieBreaking) {
    {
        TopKCollector collector(1);

        collector.collect(100, 5.0);
        ASSERT_EQ(collector.size(), 1);
        ASSERT_EQ(collector.threshold(), 5.0);

        collector.collect(99, 5.0);

        auto result = collector.into_sorted_vec();
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(result[0].doc_id, 99);
        EXPECT_EQ(result[0].score, 5.0);
    }

    {
        TopKCollector collector(2);

        collector.collect(100, 5.0);
        collector.collect(101, 5.0);

        collector.collect(99, 5.0);

        auto result = collector.into_sorted_vec();
        ASSERT_EQ(result.size(), 2);
        EXPECT_EQ(result[0].doc_id, 99);
        EXPECT_EQ(result[1].doc_id, 100);
    }
}

TEST(TopKCollectorTest, TestBasicCollection) {
    TopKCollector collector(3);

    collector.collect(1, 1.0);
    collector.collect(2, 2.0);
    collector.collect(3, 3.0);
    collector.collect(4, 4.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 3);

    EXPECT_EQ(result[0].doc_id, 4);
    EXPECT_EQ(result[0].score, 4.0);

    EXPECT_EQ(result[1].doc_id, 3);
    EXPECT_EQ(result[1].score, 3.0);

    EXPECT_EQ(result[2].doc_id, 2);
    EXPECT_EQ(result[2].score, 2.0);
}

TEST(TopKCollectorTest, TestThresholdPruning) {
    TopKCollector collector(2);

    collector.collect(1, 5.0);
    collector.collect(2, 6.0);
    EXPECT_EQ(collector.threshold(), 5.0);

    float new_threshold = collector.collect(3, 4.0);
    EXPECT_EQ(new_threshold, 5.0);

    new_threshold = collector.collect(4, 7.0);
    EXPECT_EQ(new_threshold, 5.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0].doc_id, 4);
    EXPECT_EQ(result[1].doc_id, 2);
}

TEST(TopKCollectorTest, TestK1) {
    TopKCollector collector(1);

    collector.collect(10, 1.0);
    EXPECT_EQ(collector.threshold(), 1.0);

    collector.collect(20, 0.5);
    collector.collect(30, 2.0);
    EXPECT_EQ(collector.threshold(), 2.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].doc_id, 30);
}

TEST(TopKCollectorTest, TestLargeK) {
    TopKCollector collector(100);

    for (uint32_t i = 0; i < 50; i++) {
        collector.collect(i, static_cast<float>(i));
    }

    EXPECT_EQ(collector.size(), 50);
    EXPECT_EQ(collector.threshold(), -std::numeric_limits<float>::infinity());

    for (uint32_t i = 50; i < 100; i++) {
        collector.collect(i, static_cast<float>(i));
    }
    EXPECT_EQ(collector.threshold(), 0.0);

    for (uint32_t i = 100; i < 150; i++) {
        collector.collect(i, static_cast<float>(i));
    }

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 100);
    EXPECT_EQ(result[0].doc_id, 149);
    EXPECT_EQ(result[99].doc_id, 50);
}

TEST(TopKCollectorTest, TestBufferTruncation) {
    TopKCollector collector(3);

    collector.collect(1, 1.0);
    collector.collect(2, 2.0);
    collector.collect(3, 3.0);
    collector.collect(4, 4.0);
    collector.collect(5, 5.0);
    collector.collect(6, 6.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0].score, 6.0);
    EXPECT_EQ(result[1].score, 5.0);
    EXPECT_EQ(result[2].score, 4.0);
}

TEST(TopKCollectorTest, TestEmptyCollector) {
    TopKCollector collector(5);

    auto result = collector.into_sorted_vec();
    EXPECT_TRUE(result.empty());
}

TEST(TopKCollectorTest, TestFewerThanK) {
    TopKCollector collector(10);

    collector.collect(1, 3.0);
    collector.collect(2, 1.0);
    collector.collect(3, 2.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0].doc_id, 1);
    EXPECT_EQ(result[1].doc_id, 3);
    EXPECT_EQ(result[2].doc_id, 2);
}

TEST(TopKCollectorTest, TestNegativeScores) {
    TopKCollector collector(3);

    collector.collect(1, -1.0);
    collector.collect(2, -2.0);
    collector.collect(3, -0.5);
    collector.collect(4, -3.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0].doc_id, 3);
    EXPECT_EQ(result[1].doc_id, 1);
    EXPECT_EQ(result[2].doc_id, 2);
}

TEST(TopKCollectorTest, TestAllSameScore) {
    TopKCollector collector(3);

    collector.collect(5, 1.0);
    collector.collect(3, 1.0);
    collector.collect(7, 1.0);
    collector.collect(1, 1.0);

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0].doc_id, 1);
    EXPECT_EQ(result[1].doc_id, 3);
    EXPECT_EQ(result[2].doc_id, 5);
}

std::vector<ScoredDoc> compute_expected_topk(std::vector<ScoredDoc>& docs, size_t k) {
    std::sort(docs.begin(), docs.end(), ScoredDocByScoreDesc {});
    docs.resize(std::min(docs.size(), k));
    return docs;
}

TEST(TopKCollectorTest, StressRandomScores1M) {
    constexpr size_t N = 1000000;
    constexpr size_t K = 100;

    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist(0.0f, 1000.0f);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = dist(rng);
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id) << "Mismatch at position " << i;
        EXPECT_FLOAT_EQ(result[i].score, expected[i].score) << "Mismatch at position " << i;
    }
}

TEST(TopKCollectorTest, StressAscendingOrder500K) {
    constexpr size_t N = 500000;
    constexpr size_t K = 1000;

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = static_cast<float>(i);
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    EXPECT_EQ(result[0].doc_id, N - 1);
    EXPECT_EQ(result[K - 1].doc_id, N - K);

    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id);
    }
}

TEST(TopKCollectorTest, StressDescendingOrder500K) {
    constexpr size_t N = 500000;
    constexpr size_t K = 1000;

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = static_cast<float>(N - i);
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    EXPECT_EQ(result[0].doc_id, 0);
    EXPECT_EQ(result[K - 1].doc_id, K - 1);

    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id);
    }
}

TEST(TopKCollectorTest, StressManyDuplicateScores) {
    constexpr size_t N = 100000;
    constexpr size_t K = 500;
    constexpr int NUM_DISTINCT_SCORES = 100;

    std::mt19937 rng(123);
    std::uniform_int_distribution<int> score_dist(0, NUM_DISTINCT_SCORES - 1);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = static_cast<float>(score_dist(rng));
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id) << "Mismatch at position " << i;
        EXPECT_FLOAT_EQ(result[i].score, expected[i].score);
    }
}

TEST(TopKCollectorTest, StressAllSameScore) {
    constexpr size_t N = 50000;
    constexpr size_t K = 1000;
    constexpr float SCORE = 42.0f;

    std::mt19937 rng(456);
    std::vector<uint32_t> doc_ids(N);
    std::iota(doc_ids.begin(), doc_ids.end(), 0);
    std::shuffle(doc_ids.begin(), doc_ids.end(), rng);

    TopKCollector collector(K);
    for (uint32_t doc_id : doc_ids) {
        collector.collect(doc_id, SCORE);
    }

    auto result = collector.into_sorted_vec();
    ASSERT_EQ(result.size(), K);

    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, i) << "Expected doc_id " << i << " at position " << i;
        EXPECT_FLOAT_EQ(result[i].score, SCORE);
    }
}

TEST(TopKCollectorTest, StressMultipleTruncations) {
    constexpr size_t K = 100;
    constexpr size_t N = K * 50;

    std::mt19937 rng(789);
    std::uniform_real_distribution<float> dist(0.0f, 10000.0f);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = dist(rng);
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id);
        EXPECT_FLOAT_EQ(result[i].score, expected[i].score);
    }
}

TEST(TopKCollectorTest, StressZipfDistribution) {
    constexpr size_t N = 500000;
    constexpr size_t K = 100;

    std::mt19937 rng(999);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float base_score = 1.0f / (static_cast<float>(i % 10000) + 1.0f);
        float noise = static_cast<float>(rng() % 1000) / 1000000.0f;
        float score = base_score + noise;

        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id) << "Mismatch at position " << i;
    }
}

TEST(TopKCollectorTest, StressSmallKLargeN) {
    constexpr size_t N = 1000000;
    constexpr size_t K = 10;

    std::mt19937 rng(111);
    std::uniform_real_distribution<float> dist(0.0f, 1.0f);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score = dist(rng);
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id);
        EXPECT_FLOAT_EQ(result[i].score, expected[i].score);
    }
}

TEST(TopKCollectorTest, StressBimodalDistribution) {
    constexpr size_t N = 200000;
    constexpr size_t K = 500;

    std::mt19937 rng(222);

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < N; i++) {
        float score;
        if (i % 2 == 0) {
            score = static_cast<float>(rng() % 1000) / 100.0f;
        } else {
            score = 90.0f + static_cast<float>(rng() % 1000) / 100.0f;
        }
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id);
    }

    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id % 2, 1) << "Expected odd doc_id at position " << i;
    }
}

TEST(TopKCollectorTest, StressThresholdBoundary) {
    constexpr size_t K = 100;
    constexpr size_t N = 10000;
    constexpr float BASE_SCORE = 50.0f;

    TopKCollector collector(K);
    std::vector<ScoredDoc> all_docs;
    all_docs.reserve(N);

    for (uint32_t i = 0; i < K; i++) {
        collector.collect(i, BASE_SCORE);
        all_docs.emplace_back(i, BASE_SCORE);
    }

    for (uint32_t i = K; i < N; i++) {
        float score = (i % 2 == 0) ? BASE_SCORE : BASE_SCORE + 0.001f;
        collector.collect(i, score);
        all_docs.emplace_back(i, score);
    }

    auto result = collector.into_sorted_vec();
    auto expected = compute_expected_topk(all_docs, K);

    ASSERT_EQ(result.size(), K);
    for (size_t i = 0; i < K; i++) {
        EXPECT_EQ(result[i].doc_id, expected[i].doc_id) << "Mismatch at position " << i;
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
