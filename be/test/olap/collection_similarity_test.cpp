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

} // namespace doris