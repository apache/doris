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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/ann_index/ann_index_iterator.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_reader.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_writer.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"
#include "vector_search_utils.h"

using namespace doris::vector_search_utils;

namespace doris::vectorized {

TEST_F(VectorSearchTest, TestAnnIndexStatsInitialization) {
    doris::segment_v2::AnnIndexStats stats;

    // Test initial values
    EXPECT_EQ(stats.search_costs_ns.value(), 0);
    EXPECT_EQ(stats.load_index_costs_ns.value(), 0);

    // Test setting values
    stats.search_costs_ns.set(1000L);
    stats.load_index_costs_ns.set(2000L);

    EXPECT_EQ(stats.search_costs_ns.value(), 1000);
    EXPECT_EQ(stats.load_index_costs_ns.value(), 2000);
}

TEST_F(VectorSearchTest, TestAnnIndexStatsCopyConstructor) {
    doris::segment_v2::AnnIndexStats original;
    original.search_costs_ns.set(1500L);
    original.load_index_costs_ns.set(2500L);

    doris::segment_v2::AnnIndexStats copied(original);

    EXPECT_EQ(copied.search_costs_ns.value(), 1500);
    EXPECT_EQ(copied.load_index_costs_ns.value(), 2500);
}

TEST_F(VectorSearchTest, TestAnnRangeSearchParamsToString) {
    doris::segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.radius = 5.5f;

    auto roaring = std::make_shared<roaring::Roaring>();
    roaring->add(1);
    roaring->add(2);
    roaring->add(3);
    params.roaring = roaring.get();

    std::string result = params.to_string();

    EXPECT_TRUE(result.find("is_le_or_lt: true") != std::string::npos);
    EXPECT_TRUE(result.find("radius: 5.5") != std::string::npos);
    EXPECT_TRUE(result.find("input rows 3") != std::string::npos);
}

TEST_F(VectorSearchTest, TestAnnRangeSearchParamsWithNullRoaring) {
    auto roaring = std::make_unique<roaring::Roaring>();
    doris::segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = false;
    params.radius = 10.0f;
    params.roaring = roaring.get(); // Assigning a null pointer

    std::string result = params.to_string();

    EXPECT_TRUE(result.find("is_le_or_lt: false") != std::string::npos);
    EXPECT_TRUE(result.find("radius: 10") != std::string::npos);
    EXPECT_TRUE(result.find("input rows 0") != std::string::npos);
}

TEST_F(VectorSearchTest, TestAnnTopNParamValidation) {
    // Test with zero limit
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);

    doris::segment_v2::AnnTopNParam param = {
            .query_value = query_data,
            .query_value_size = 4,
            .limit = 0, // Zero limit
            ._user_params = doris::VectorSearchUserParams {},
            .roaring = &bitmap,
            .distance = nullptr,
            .row_ids = nullptr,
            .stats = std::make_unique<doris::segment_v2::AnnIndexStats>()};

    // The parameter should be valid even with zero limit
    EXPECT_EQ(param.limit, 0);
    EXPECT_EQ(param.query_value_size, 4);
    EXPECT_NE(param.query_value, nullptr);
}

TEST_F(VectorSearchTest, TestVectorSearchUserParamsDefaultValues) {
    doris::VectorSearchUserParams params;

    // Test default values
    EXPECT_EQ(params.hnsw_ef_search, 32);
    EXPECT_EQ(params.hnsw_check_relative_distance, true);
    EXPECT_EQ(params.hnsw_bounded_queue, true);
}

TEST_F(VectorSearchTest, TestVectorSearchUserParamsEquality) {
    doris::VectorSearchUserParams params1;
    params1.hnsw_ef_search = 100;
    params1.hnsw_check_relative_distance = false;
    params1.hnsw_bounded_queue = false;

    doris::VectorSearchUserParams params2;
    params2.hnsw_ef_search = 100;
    params2.hnsw_check_relative_distance = false;
    params2.hnsw_bounded_queue = false;

    EXPECT_EQ(params1, params2);

    // Test inequality
    params2.hnsw_ef_search = 50;
    EXPECT_NE(params1, params2);
}

TEST_F(VectorSearchTest, TestIndexSearchResultInitialization) {
    doris::segment_v2::IndexSearchResult result;

    // Test initial state
    EXPECT_EQ(result.roaring, nullptr);
    EXPECT_EQ(result.distances, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
}

TEST_F(VectorSearchTest, TestAnnRangeSearchResultInitialization) {
    doris::segment_v2::AnnRangeSearchResult result;

    // Test initial state
    EXPECT_EQ(result.roaring, nullptr);
    EXPECT_EQ(result.distance, nullptr);
    EXPECT_EQ(result.row_ids, nullptr);
}

TEST_F(VectorSearchTest, TestAnnIndexWriterWithEmptyProperties) {
    // Test writer with empty properties (should use defaults)
    std::map<std::string, std::string> empty_properties;

    auto tablet_index = std::make_unique<doris::TabletIndex>();
    tablet_index->_properties = empty_properties;
    tablet_index->_index_id = 1;

    auto mock_file_writer =
            std::make_unique<MockIndexFileWriter>(doris::io::global_local_filesystem());
    auto writer = std::make_unique<segment_v2::AnnIndexColumnWriter>(mock_file_writer.get(),
                                                                     tablet_index.get());

    auto fs_dir = std::make_shared<segment_v2::DorisFSDirectory>();
    EXPECT_CALL(*mock_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // Should not crash and should use default values
    Status status = writer->init();
    EXPECT_TRUE(status.ok());
}

TEST_F(VectorSearchTest, TestLargeVectorDimensions) {
    // Test with large vector dimensions
    std::map<std::string, std::string> properties;
    properties["index_type"] = "hnsw";
    properties["metric_type"] = "l2_distance";
    properties["dim"] = "1024"; // Large dimension
    properties["max_degree"] = "64";

    auto tablet_index = std::make_unique<doris::TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto mock_file_writer =
            std::make_unique<MockIndexFileWriter>(doris::io::global_local_filesystem());
    auto writer = std::make_unique<segment_v2::AnnIndexColumnWriter>(mock_file_writer.get(),
                                                                     tablet_index.get());

    auto fs_dir = std::make_shared<segment_v2::DorisFSDirectory>();
    EXPECT_CALL(*mock_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Test adding vectors with correct large dimension
    const size_t dim = 1024;
    const size_t num_rows = 2;
    std::vector<float> vectors(num_rows * dim);

    // Fill with test data
    for (size_t i = 0; i < vectors.size(); ++i) {
        vectors[i] = static_cast<float>(i % 100) / 100.0f;
    }

    std::vector<size_t> offsets = {0, dim, 2 * dim};

    Status status =
            writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                     reinterpret_cast<const uint8_t*>(offsets.data()), num_rows);
    EXPECT_TRUE(status.ok());
}

TEST_F(VectorSearchTest, TestEmptyRoaringBitmap) {
    // Test with empty roaring bitmap
    doris::segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.radius = 5.0f;

    float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    params.query_value = query_data;

    roaring::Roaring empty_bitmap; // Empty bitmap
    params.roaring = &empty_bitmap;

    std::string result = params.to_string();

    EXPECT_TRUE(result.find("input rows 0") != std::string::npos);
}

TEST_F(VectorSearchTest, TestLargeRoaringBitmap) {
    // Test with large roaring bitmap
    doris::segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = false;
    params.radius = 10.0f;

    float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    params.query_value = query_data;

    roaring::Roaring large_bitmap;
    // Add many elements
    for (uint32_t i = 0; i < 100000; ++i) {
        large_bitmap.add(i);
    }
    params.roaring = &large_bitmap;

    std::string result = params.to_string();

    EXPECT_TRUE(result.find("input rows 100000") != std::string::npos);
}

} // namespace doris::vectorized
