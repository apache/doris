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

#include "olap/collection_similarity.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris {

class CollectionSimilarityTest : public ::testing::Test {
protected:
    void SetUp() override { similarity = std::make_unique<CollectionSimilarity>(); }

    void TearDown() override { similarity.reset(); }

    std::unique_ptr<CollectionSimilarity> similarity;

    roaring::Roaring create_bitmap(const std::vector<uint32_t>& row_ids) {
        roaring::Roaring bitmap;
        for (uint32_t id : row_ids) {
            bitmap.add(id);
        }
        return bitmap;
    }

    void verify_scores(const vectorized::IColumn::MutablePtr& scores,
                       const std::vector<float>& expected_scores) {
        ASSERT_EQ(scores->size(), expected_scores.size());

        auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
        ASSERT_NE(nullable_column, nullptr);

        const auto* float_column = dynamic_cast<const vectorized::ColumnFloat32*>(
                &nullable_column->get_nested_column());
        ASSERT_NE(float_column, nullptr);

        const auto& data = float_column->get_data();
        for (size_t i = 0; i < expected_scores.size(); ++i) {
            EXPECT_FLOAT_EQ(data[i], expected_scores[i]);
        }
    }

    void verify_row_ids(const std::unique_ptr<std::vector<uint64_t>>& row_ids,
                        const std::vector<uint64_t>& expected_row_ids) {
        ASSERT_EQ(row_ids->size(), expected_row_ids.size());
        for (size_t i = 0; i < expected_row_ids.size(); ++i) {
            EXPECT_EQ((*row_ids)[i], expected_row_ids[i]);
        }
    }
};

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresDescendingTest) {
    similarity->collect(1, 0.4f);
    similarity->collect(2, 0.9f);
    similarity->collect(3, 0.1f);
    similarity->collect(4, 0.7f);
    similarity->collect(5, 0.6f);
    similarity->collect(6, 0.8f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5, 6});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 3);

    verify_scores(scores, {0.9f, 0.8f, 0.7f});
    verify_row_ids(row_ids, {2, 6, 4});

    EXPECT_TRUE(bitmap.contains(2));
    EXPECT_TRUE(bitmap.contains(6));
    EXPECT_TRUE(bitmap.contains(4));
    EXPECT_FALSE(bitmap.contains(1));
    EXPECT_FALSE(bitmap.contains(3));
    EXPECT_FALSE(bitmap.contains(5));
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresAscendingTest) {
    similarity->collect(10, 0.8f);
    similarity->collect(20, 0.2f);
    similarity->collect(30, 0.6f);
    similarity->collect(40, 0.1f);
    similarity->collect(50, 0.9f);

    roaring::Roaring bitmap = create_bitmap({10, 20, 30, 40, 50});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::ASC, 3);

    verify_scores(scores, {0.1f, 0.2f, 0.6f});
    verify_row_ids(row_ids, {40, 20, 30});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresZeroTopKTest) {
    similarity->collect(1, 0.5f);
    similarity->collect(2, 0.8f);
    similarity->collect(3, 0.3f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 0);

    EXPECT_EQ(scores->size(), 0);
    EXPECT_EQ(row_ids->size(), 0);
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresNegativeTopKTest) {
    similarity->collect(1, 0.5f);
    similarity->collect(2, 0.8f);

    roaring::Roaring bitmap = create_bitmap({1, 2});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 0);

    EXPECT_EQ(scores->size(), 0);
    EXPECT_EQ(row_ids->size(), 0);
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresTopKLargerThanDataTest) {
    similarity->collect(1, 0.4f);
    similarity->collect(2, 0.7f);

    roaring::Roaring bitmap = create_bitmap({1, 2});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 10);

    verify_scores(scores, {0.7f, 0.4f});
    verify_row_ids(row_ids, {2, 1});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresFallbackTest) {
    similarity->collect(1, 0.5f);
    similarity->collect(3, 0.8f);
    similarity->collect(5, 0.2f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5, 6});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 6);

    verify_scores(scores, {0.8f, 0.5f, 0.2f, 0.0f, 0.0f, 0.0f});
    verify_row_ids(row_ids, {3, 1, 5, 2, 4, 6});
}

TEST_F(CollectionSimilarityTest, IdenticalScoresSortingTest) {
    similarity->collect(10, 0.5f);
    similarity->collect(20, 0.5f);
    similarity->collect(30, 0.5f);
    similarity->collect(40, 0.5f);

    roaring::Roaring bitmap = create_bitmap({10, 20, 30, 40});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 2);

    EXPECT_EQ(scores->size(), 2);
    EXPECT_EQ(row_ids->size(), 2);

    verify_scores(scores, {0.5f, 0.5f});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresBasicTest) {
    similarity->collect(1, 0.4f);
    similarity->collect(2, 0.9f);
    similarity->collect(3, 0.1f);
    similarity->collect(5, 0.7f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5, 6});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), 6);
    EXPECT_EQ(row_ids->size(), 6);

    verify_scores(scores, {0.4f, 0.9f, 0.1f, 0.0f, 0.7f, 0.0f});

    verify_row_ids(row_ids, {1, 2, 3, 4, 5, 6});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresEmptyBitmapTest) {
    similarity->collect(10, 0.8f);
    similarity->collect(20, 0.2f);
    similarity->collect(30, 0.6f);

    roaring::Roaring bitmap;
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), 0);
    EXPECT_EQ(row_ids->size(), 0);
}

// Tests for ScoreRangeFilter

TEST_F(CollectionSimilarityTest, GetBm25ScoresWithFilterGTTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);
    similarity->collect(3, 0.9f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4}); // 4 has score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.5);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    verify_scores(scores, {0.6f, 0.9f});
    verify_row_ids(row_ids, {2, 3});
    EXPECT_EQ(bitmap.cardinality(), 2);
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresWithFilterGETest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.5f);
    similarity->collect(3, 0.9f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GE, 0.5);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    verify_scores(scores, {0.5f, 0.9f});
    verify_row_ids(row_ids, {2, 3});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresWithFilterZeroThresholdTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3}); // 3 has score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    // GT 0: should exclude score=0
    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.0);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    verify_scores(scores, {0.3f, 0.6f});
    verify_row_ids(row_ids, {1, 2});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresWithFilterGEZeroTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3}); // 3 has score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    // GE 0: should include score=0
    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GE, 0.0);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    verify_scores(scores, {0.3f, 0.6f, 0.0f});
    verify_row_ids(row_ids, {1, 2, 3});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresWithFilterDescTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);
    similarity->collect(3, 0.9f);
    similarity->collect(4, 0.4f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5}); // 5 has score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.35);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 3, filter);

    // Only 0.9, 0.6, 0.4 pass filter (> 0.35), top 3 DESC
    verify_scores(scores, {0.9f, 0.6f, 0.4f});
    verify_row_ids(row_ids, {3, 2, 4});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresWithFilterAscTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);
    similarity->collect(3, 0.9f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5}); // 4,5 have score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    // GE 0: includes zeros, ASC order puts zeros first
    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GE, 0.0);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::ASC, 3, filter);

    // ASC: zeros first, then lowest scores
    verify_scores(scores, {0.0f, 0.0f, 0.3f});
    verify_row_ids(row_ids, {4, 5, 1});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresWithFilterExcludeZerosAscTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.6f);
    similarity->collect(3, 0.9f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5}); // 4,5 have score 0
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    // GT 0: excludes zeros
    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.0);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::ASC, 3, filter);

    verify_scores(scores, {0.3f, 0.6f, 0.9f});
    verify_row_ids(row_ids, {1, 2, 3});
}

TEST_F(CollectionSimilarityTest, GetTopnBm25ScoresWithFilterAllFilteredTest) {
    similarity->collect(1, 0.3f);
    similarity->collect(2, 0.4f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    // Filter threshold too high, all filtered out
    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.5);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 3, filter);

    EXPECT_EQ(scores->size(), 0);
    EXPECT_EQ(row_ids->size(), 0);
}

TEST_F(CollectionSimilarityTest, LargeDataGetBm25ScoresBasicTest) {
    constexpr size_t NUM_ROWS = 100000;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), NUM_ROWS);
    EXPECT_EQ(row_ids->size(), NUM_ROWS);
    EXPECT_EQ(bitmap.cardinality(), NUM_ROWS);
}

TEST_F(CollectionSimilarityTest, LargeDataGetBm25ScoresWithGTFilterTest) {
    constexpr size_t NUM_ROWS = 100000;
    constexpr double THRESHOLD = 0.5;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, THRESHOLD);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    size_t expected_count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        if (score > THRESHOLD) {
            expected_count++;
        }
    }

    EXPECT_EQ(scores->size(), expected_count);
    EXPECT_EQ(row_ids->size(), expected_count);
    EXPECT_EQ(bitmap.cardinality(), expected_count);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_GT(data[i], THRESHOLD);
    }
}

TEST_F(CollectionSimilarityTest, LargeDataGetBm25ScoresWithGEFilterTest) {
    constexpr size_t NUM_ROWS = 100000;
    constexpr double THRESHOLD = 0.5;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GE, THRESHOLD);
    similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

    size_t expected_count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        if (score >= THRESHOLD) {
            expected_count++;
        }
    }

    EXPECT_EQ(scores->size(), expected_count);
    EXPECT_EQ(row_ids->size(), expected_count);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_GE(data[i], THRESHOLD);
    }
}

TEST_F(CollectionSimilarityTest, LargeDataTopNDescTest) {
    constexpr size_t NUM_ROWS = 100000;
    constexpr size_t TOP_K = 100;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, TOP_K);

    EXPECT_EQ(scores->size(), TOP_K);
    EXPECT_EQ(row_ids->size(), TOP_K);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();

    for (size_t i = 1; i < data.size(); ++i) {
        EXPECT_GE(data[i - 1], data[i]) << "DESC order violated at index " << i;
    }

    float expected_max = static_cast<float>(NUM_ROWS - 1) / static_cast<float>(NUM_ROWS);
    EXPECT_FLOAT_EQ(data[0], expected_max);
}

TEST_F(CollectionSimilarityTest, LargeDataTopNAscTest) {
    constexpr size_t NUM_ROWS = 100000;
    constexpr size_t TOP_K = 100;

    for (size_t i = 1; i <= NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS + 1);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 1; i <= NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::ASC, TOP_K);

    EXPECT_EQ(scores->size(), TOP_K);
    EXPECT_EQ(row_ids->size(), TOP_K);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();

    for (size_t i = 1; i < data.size(); ++i) {
        EXPECT_LE(data[i - 1], data[i]) << "ASC order violated at index " << i;
    }

    float expected_min = 1.0f / static_cast<float>(NUM_ROWS + 1);
    EXPECT_FLOAT_EQ(data[0], expected_min);
}

TEST_F(CollectionSimilarityTest, LargeDataTopNDescWithFilterTest) {
    constexpr size_t NUM_ROWS = 100000;
    constexpr size_t TOP_K = 50;
    constexpr double THRESHOLD = 0.8;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, THRESHOLD);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, TOP_K, filter);

    EXPECT_EQ(scores->size(), TOP_K);
    EXPECT_EQ(row_ids->size(), TOP_K);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();

    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_GT(data[i], THRESHOLD);
    }
    for (size_t i = 1; i < data.size(); ++i) {
        EXPECT_GE(data[i - 1], data[i]) << "DESC order violated at index " << i;
    }
}

TEST_F(CollectionSimilarityTest, LargeDataTopKExceedsFilteredCountTest) {
    constexpr size_t NUM_ROWS = 10000;
    constexpr size_t TOP_K = 5000;
    constexpr double THRESHOLD = 0.9;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, THRESHOLD);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, TOP_K, filter);

    size_t expected_count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        if (score > THRESHOLD) {
            expected_count++;
        }
    }

    EXPECT_EQ(scores->size(), expected_count);
    EXPECT_EQ(row_ids->size(), expected_count);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_GT(data[i], THRESHOLD);
    }
}

TEST_F(CollectionSimilarityTest, LargeDataSparseBitmapTest) {
    constexpr size_t NUM_SCORED_ROWS = 1000;
    constexpr size_t BITMAP_SIZE = 100000;
    constexpr size_t TOP_K = 100;

    for (size_t i = 0; i < NUM_SCORED_ROWS; ++i) {
        float score = static_cast<float>(i + 1) / static_cast<float>(NUM_SCORED_ROWS);
        similarity->collect(static_cast<uint32_t>(i * 100), score); // 稀疏分布
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(BITMAP_SIZE);
    for (size_t i = 0; i < BITMAP_SIZE; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, 0.0);
    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, TOP_K, filter);

    EXPECT_EQ(scores->size(), TOP_K);
    EXPECT_EQ(row_ids->size(), TOP_K);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_GT(data[i], 0.0f);
    }
}

TEST_F(CollectionSimilarityTest, LargeDataScoreAccumulationTest) {
    constexpr size_t NUM_ROWS = 10000;
    constexpr size_t ACCUMULATIONS_PER_ROW = 5;

    for (size_t acc = 0; acc < ACCUMULATIONS_PER_ROW; ++acc) {
        for (size_t i = 0; i < NUM_ROWS; ++i) {
            float score = 0.1f;
            similarity->collect(static_cast<uint32_t>(i), score);
        }
    }

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), NUM_ROWS);

    auto* nullable_column = dynamic_cast<vectorized::ColumnNullable*>(scores.get());
    ASSERT_NE(nullable_column, nullptr);
    const auto* float_column =
            dynamic_cast<const vectorized::ColumnFloat32*>(&nullable_column->get_nested_column());
    ASSERT_NE(float_column, nullptr);
    const auto& data = float_column->get_data();

    float expected_score = 0.1f * ACCUMULATIONS_PER_ROW;
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_FLOAT_EQ(data[i], expected_score);
    }
}

TEST_F(CollectionSimilarityTest, LargeDataBoundaryThresholdTest) {
    constexpr size_t NUM_ROWS = 10000;
    constexpr double THRESHOLD = 0.5;

    for (size_t i = 0; i < NUM_ROWS; ++i) {
        float score = static_cast<float>(i) / static_cast<float>(NUM_ROWS);
        similarity->collect(static_cast<uint32_t>(i), score);
    }

    similarity->collect(static_cast<uint32_t>(NUM_ROWS), static_cast<float>(THRESHOLD));

    std::vector<uint32_t> all_ids;
    all_ids.reserve(NUM_ROWS + 1);
    for (size_t i = 0; i <= NUM_ROWS; ++i) {
        all_ids.push_back(static_cast<uint32_t>(i));
    }
    roaring::Roaring bitmap = create_bitmap(all_ids);

    {
        vectorized::IColumn::MutablePtr scores;
        std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();
        auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GT, THRESHOLD);
        similarity->get_bm25_scores(&bitmap, scores, row_ids, filter);

        bool found_threshold = false;
        for (size_t i = 0; i < row_ids->size(); ++i) {
            if ((*row_ids)[i] == NUM_ROWS) {
                found_threshold = true;
                break;
            }
        }
        EXPECT_FALSE(found_threshold) << "GT filter should not include score == threshold";
    }

    {
        vectorized::IColumn::MutablePtr scores;
        std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();
        roaring::Roaring bitmap2 = create_bitmap(all_ids);
        auto filter = std::make_shared<ScoreRangeFilter>(TExprOpcode::GE, THRESHOLD);
        similarity->get_bm25_scores(&bitmap2, scores, row_ids, filter);

        bool found_threshold = false;
        for (size_t i = 0; i < row_ids->size(); ++i) {
            if ((*row_ids)[i] == NUM_ROWS) {
                found_threshold = true;
                break;
            }
        }
        EXPECT_TRUE(found_threshold) << "GE filter should include score == threshold";
    }
}

} // namespace doris