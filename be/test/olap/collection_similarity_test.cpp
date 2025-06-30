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
        auto* float_column = dynamic_cast<vectorized::ColumnFloat32*>(scores.get());
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

TEST_F(CollectionSimilarityTest, ConstructorInitializationTest) {
    auto new_similarity = std::make_unique<CollectionSimilarity>();

    roaring::Roaring bitmap = create_bitmap({1});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    new_similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {0.0f});
    verify_row_ids(row_ids, {1});
}

TEST_F(CollectionSimilarityTest, CollectBasicFunctionalityTest) {
    similarity->collect(10, 1.5f);
    similarity->collect(20, 2.5f);
    similarity->collect(30, 0.8f);

    roaring::Roaring bitmap = create_bitmap({10, 20, 30});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {1.5f, 2.5f, 0.8f});
    verify_row_ids(row_ids, {10, 20, 30});
}

TEST_F(CollectionSimilarityTest, CollectAccumulationTest) {
    similarity->collect(100, 0.3f);
    similarity->collect(100, 0.7f);
    similarity->collect(100, 0.2f);
    similarity->collect(200, 1.0f);
    similarity->collect(200, 0.5f);

    roaring::Roaring bitmap = create_bitmap({100, 200});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {1.2f, 1.5f});
    verify_row_ids(row_ids, {100, 200});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresEmptyCollectionTest) {
    roaring::Roaring bitmap = create_bitmap({5, 15, 25, 35});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {0.0f, 0.0f, 0.0f, 0.0f});
    verify_row_ids(row_ids, {5, 15, 25, 35});
}

TEST_F(CollectionSimilarityTest, GetBm25ScoresPartialCoverageTest) {
    similarity->collect(2, 0.9f);
    similarity->collect(6, 1.8f);
    similarity->collect(10, 0.4f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 6, 8, 10, 12});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {0.0f, 0.9f, 0.0f, 1.8f, 0.0f, 0.4f, 0.0f});
    verify_row_ids(row_ids, {1, 2, 3, 6, 8, 10, 12});
}

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

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, -5);

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

    similarity->get_topn_bm25_scores(&bitmap, scores, row_ids, OrderType::DESC, 3);

    verify_scores(scores, {0.5f, 0.0f, 0.8f, 0.0f, 0.2f, 0.0f});
    verify_row_ids(row_ids, {1, 2, 3, 4, 5, 6});
}

TEST_F(CollectionSimilarityTest, LargeDatasetPerformanceTest) {
    const int num_rows = 1000;
    std::vector<uint32_t> row_ids;

    for (int i = 0; i < num_rows; ++i) {
        float score = static_cast<float>(i % 100) / 100.0f;
        similarity->collect(i, score);
        row_ids.push_back(i);
    }

    roaring::Roaring bitmap = create_bitmap(row_ids);
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> result_row_ids =
            std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, result_row_ids);

    EXPECT_EQ(scores->size(), num_rows);
    EXPECT_EQ(result_row_ids->size(), num_rows);

    vectorized::IColumn::MutablePtr top_scores;
    std::unique_ptr<std::vector<uint64_t>> top_row_ids = std::make_unique<std::vector<uint64_t>>();
    roaring::Roaring bitmap_copy = bitmap;

    similarity->get_topn_bm25_scores(&bitmap_copy, top_scores, top_row_ids, OrderType::DESC, 50);

    EXPECT_EQ(top_scores->size(), 50);
    EXPECT_EQ(top_row_ids->size(), 50);
}

TEST_F(CollectionSimilarityTest, NegativeScoresHandlingTest) {
    similarity->collect(1, -0.8f);
    similarity->collect(2, 0.5f);
    similarity->collect(3, -0.3f);
    similarity->collect(4, 0.0f);
    similarity->collect(5, -1.2f);

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {-0.8f, 0.5f, -0.3f, 0.0f, -1.2f});

    vectorized::IColumn::MutablePtr top_scores;
    std::unique_ptr<std::vector<uint64_t>> top_row_ids = std::make_unique<std::vector<uint64_t>>();
    roaring::Roaring bitmap_copy = bitmap;

    similarity->get_topn_bm25_scores(&bitmap_copy, top_scores, top_row_ids, OrderType::DESC, 3);

    verify_scores(top_scores, {0.5f, 0.0f, -0.3f});
    verify_row_ids(top_row_ids, {2, 4, 3});
}

TEST_F(CollectionSimilarityTest, SingleRowEdgeCaseTest) {
    similarity->collect(999, 3.14f);

    roaring::Roaring bitmap = create_bitmap({999});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    verify_scores(scores, {3.14f});
    verify_row_ids(row_ids, {999});

    vectorized::IColumn::MutablePtr top_scores;
    std::unique_ptr<std::vector<uint64_t>> top_row_ids = std::make_unique<std::vector<uint64_t>>();
    roaring::Roaring bitmap_copy = bitmap;

    similarity->get_topn_bm25_scores(&bitmap_copy, top_scores, top_row_ids, OrderType::ASC, 1);

    verify_scores(top_scores, {3.14f});
    verify_row_ids(top_row_ids, {999});
}

TEST_F(CollectionSimilarityTest, EmptyBitmapTest) {
    similarity->collect(1, 1.0f);
    similarity->collect(2, 2.0f);

    roaring::Roaring bitmap;
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), 0);
    EXPECT_EQ(row_ids->size(), 0);
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

    auto* float_column = dynamic_cast<vectorized::ColumnFloat32*>(scores.get());
    const auto& data = float_column->get_data();
    EXPECT_FLOAT_EQ(data[0], 0.5f);
    EXPECT_FLOAT_EQ(data[1], 0.5f);
}

TEST_F(CollectionSimilarityTest, ExtremeFloatValuesTest) {
    similarity->collect(1, std::numeric_limits<float>::max());
    similarity->collect(2, std::numeric_limits<float>::min());
    similarity->collect(3, std::numeric_limits<float>::infinity());
    similarity->collect(4, -std::numeric_limits<float>::infinity());
    similarity->collect(5, std::numeric_limits<float>::quiet_NaN());

    roaring::Roaring bitmap = create_bitmap({1, 2, 3, 4, 5});
    vectorized::IColumn::MutablePtr scores;
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    similarity->get_bm25_scores(&bitmap, scores, row_ids);

    EXPECT_EQ(scores->size(), 5);
    EXPECT_EQ(row_ids->size(), 5);

    auto* float_column = dynamic_cast<vectorized::ColumnFloat32*>(scores.get());
    const auto& data = float_column->get_data();

    EXPECT_EQ(data[0], std::numeric_limits<float>::max());
    EXPECT_EQ(data[1], std::numeric_limits<float>::min());
    EXPECT_EQ(data[2], std::numeric_limits<float>::infinity());
    EXPECT_EQ(data[3], -std::numeric_limits<float>::infinity());
    EXPECT_TRUE(std::isnan(data[4]));
}

} // namespace doris