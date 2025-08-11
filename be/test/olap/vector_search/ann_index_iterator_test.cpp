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

#include "olap/rowset/segment_v2/ann_index/ann_index_iterator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "olap/rowset/segment_v2/ann_index/ann_index_reader.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"
#include "vector_search_utils.h"

using namespace doris::vector_search_utils;

namespace doris::segment_v2 {

class AnnIndexIteratorTest : public doris::vectorized::VectorSearchTest {
protected:
    void SetUp() override {
        doris::vectorized::VectorSearchTest::SetUp();

        // Create test index properties
        _properties["index_type"] = "hnsw";
        _properties["metric_type"] = "l2_distance";
        _properties["dim"] = "4";
        _properties["max_degree"] = "16";

        // Create tablet index
        _tablet_index = std::make_unique<doris::TabletIndex>();
        _tablet_index->_properties = _properties;
        _tablet_index->_index_id = 1;
        _tablet_index->_index_name = "test_ann_index";

        // Create mock index file reader
        _mock_index_file_reader = std::make_shared<MockIndexFileReader>();

        // Create ann index reader
        _ann_reader =
                std::make_shared<AnnIndexReader>(_tablet_index.get(), _mock_index_file_reader);
    }

    void TearDown() override { doris::vectorized::VectorSearchTest::TearDown(); }

    std::map<std::string, std::string> _properties;
    std::unique_ptr<doris::TabletIndex> _tablet_index;
    std::shared_ptr<MockIndexFileReader> _mock_index_file_reader;
    std::shared_ptr<AnnIndexReader> _ann_reader;
};

TEST_F(AnnIndexIteratorTest, TestConstructor) {
    auto iterator = std::make_unique<AnnIndexIterator>(_ann_reader);
    EXPECT_NE(iterator, nullptr);
}

TEST_F(AnnIndexIteratorTest, TestConstructorWithNullReader) {
    auto iterator = std::make_unique<AnnIndexIterator>(nullptr);
    EXPECT_NE(iterator, nullptr);
}

TEST_F(AnnIndexIteratorTest, TestReadFromIndexWithNullParam) {
    auto iterator = std::make_unique<AnnIndexIterator>(_ann_reader);

    // Test with null parameter - this should trigger the null check
    IndexParam param = static_cast<doris::segment_v2::AnnTopNParam*>(nullptr);
    auto status = iterator->read_from_index(param);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<doris::ErrorCode::INDEX_INVALID_PARAMETERS>());
    EXPECT_TRUE(status.msg().find("a_param is null") != std::string::npos);
}

TEST_F(AnnIndexIteratorTest, TestReadFromIndexWithValidParam) {
    auto iterator = std::make_unique<AnnIndexIterator>(_ann_reader);

    // Set up the reader's _vector_index
    auto doris_faiss_vector_index = std::make_unique<FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(AnnIndexMetric::L2);

    FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->set_build_params(build_params);

    _ann_reader->_vector_index = std::move(doris_faiss_vector_index);

    // Create valid AnnTopNParam
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    doris::segment_v2::AnnTopNParam ann_param = {
            .query_value = query_data,
            .query_value_size = 4,
            .limit = 10,
            ._user_params = doris::VectorSearchUserParams {},
            .roaring = &bitmap,
            .distance = nullptr,
            .row_ids = nullptr,
            .stats = std::make_unique<doris::segment_v2::AnnIndexStats>()};

    IndexParam param = &ann_param;

    auto status = iterator->read_from_index(param);

    // The query might succeed or fail depending on the internal index state,
    // but it should not crash and should handle the parameter correctly
    if (status.ok()) {
        EXPECT_NE(ann_param.distance, nullptr);
        EXPECT_NE(ann_param.row_ids, nullptr);
    }
}

TEST_F(AnnIndexIteratorTest, TestRangeSearchWithNullReader) {
    auto iterator = std::make_unique<AnnIndexIterator>(nullptr);

    doris::segment_v2::AnnRangeSearchParams params;
    doris::VectorSearchUserParams user_params;
    doris::segment_v2::AnnRangeSearchResult result;
    auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();

    auto status = iterator->range_search(params, user_params, &result, stats.get());

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<doris::ErrorCode::INDEX_INVALID_PARAMETERS>());
    EXPECT_TRUE(status.msg().find("_ann_reader is null") != std::string::npos);
}

TEST_F(AnnIndexIteratorTest, TestRangeSearchWithValidReader) {
    auto iterator = std::make_unique<AnnIndexIterator>(_ann_reader);

    // Set up the reader's _vector_index
    auto doris_faiss_vector_index = std::make_unique<FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(AnnIndexMetric::L2);

    FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->set_build_params(build_params);

    _ann_reader->_vector_index = std::move(doris_faiss_vector_index);

    // Create range search parameters
    float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    doris::segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.radius = 5.0f;
    params.query_value = query_data;
    params.roaring = &bitmap;

    doris::VectorSearchUserParams user_params;
    user_params.hnsw_ef_search = 50;
    user_params.hnsw_check_relative_distance = true;
    user_params.hnsw_bounded_queue = true;

    doris::segment_v2::AnnRangeSearchResult result;
    auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();

    auto status = iterator->range_search(params, user_params, &result, stats.get());

    // The range search might succeed or fail depending on the internal index state,
    // but it should not crash
    if (status.ok()) {
        EXPECT_NE(result.roaring, nullptr);
    }
}

TEST_F(AnnIndexIteratorTest, TestRangeSearchWithDifferentParameters) {
    auto iterator = std::make_unique<AnnIndexIterator>(_ann_reader);

    // Set up the reader's _vector_index
    auto doris_faiss_vector_index = std::make_unique<FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(AnnIndexMetric::L2);

    FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->set_build_params(build_params);

    _ann_reader->_vector_index = std::move(doris_faiss_vector_index);

    // Test different parameter combinations
    std::vector<std::tuple<bool, float, int>> test_cases = {
            {true, 1.0f, 10},   // is_le_or_lt=true, small radius, small ef_search
            {false, 5.0f, 50},  // is_le_or_lt=false, medium radius, medium ef_search
            {true, 10.0f, 100}, // is_le_or_lt=true, large radius, large ef_search
    };

    for (const auto& [is_le_or_lt, radius, ef_search] : test_cases) {
        float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
        roaring::Roaring bitmap;
        bitmap.add(1);
        bitmap.add(2);
        bitmap.add(3);

        doris::segment_v2::AnnRangeSearchParams params;
        params.is_le_or_lt = is_le_or_lt;
        params.radius = radius;
        params.query_value = query_data;
        params.roaring = &bitmap;

        doris::VectorSearchUserParams user_params;
        user_params.hnsw_ef_search = ef_search;
        user_params.hnsw_check_relative_distance = false;
        user_params.hnsw_bounded_queue = false;

        doris::segment_v2::AnnRangeSearchResult result;
        auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();

        auto status = iterator->range_search(params, user_params, &result, stats.get());

        // Should not crash regardless of success/failure
        if (status.ok()) {
            EXPECT_NE(result.roaring, nullptr);
        }
    }
}

TEST_F(AnnIndexIteratorTest, TestWithInnerProductMetric) {
    // Test with inner product metric
    auto properties = _properties;
    properties["metric_type"] = "inner_product";

    auto tablet_index = std::make_unique<doris::TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto ann_reader = std::make_shared<AnnIndexReader>(tablet_index.get(), _mock_index_file_reader);
    auto iterator = std::make_unique<AnnIndexIterator>(ann_reader);

    // Set up the reader's _vector_index with IP metric
    auto doris_faiss_vector_index = std::make_unique<FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(AnnIndexMetric::IP);

    FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = FaissBuildParameter::MetricType::IP;
    doris_faiss_vector_index->set_build_params(build_params);

    ann_reader->_vector_index = std::move(doris_faiss_vector_index);

    // Test read_from_index with IP metric
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    doris::segment_v2::AnnTopNParam ann_param = {
            .query_value = query_data,
            .query_value_size = 4,
            .limit = 10,
            ._user_params = doris::VectorSearchUserParams {},
            .roaring = &bitmap,
            .distance = nullptr,
            .row_ids = nullptr,
            .stats = std::make_unique<doris::segment_v2::AnnIndexStats>()};

    IndexParam param = &ann_param;

    auto status = iterator->read_from_index(param);

    // Should not crash regardless of success/failure
    if (status.ok()) {
        EXPECT_NE(ann_param.distance, nullptr);
        EXPECT_NE(ann_param.row_ids, nullptr);
    }
}

TEST_F(AnnIndexIteratorTest, TestSuccessfulWorkflow) {
    // Test a complete successful workflow with mock
    auto mock_iterator = std::make_unique<MockAnnIndexIterator>();

    // Create test data
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    doris::segment_v2::AnnTopNParam ann_param = {
            .query_value = query_data,
            .query_value_size = 4,
            .limit = 10,
            ._user_params = doris::VectorSearchUserParams {},
            .roaring = &bitmap,
            .distance = nullptr,
            .row_ids = nullptr,
            .stats = std::make_unique<doris::segment_v2::AnnIndexStats>()};

    IndexParam param = &ann_param;

    // Mock successful read_from_index
    EXPECT_CALL(*mock_iterator, read_from_index(testing::_))
            .WillOnce(testing::Return(doris::Status::OK()));

    auto status = mock_iterator->read_from_index(param);
    EXPECT_TRUE(status.ok());

    // Mock successful range_search
    doris::segment_v2::AnnRangeSearchParams range_params;
    doris::VectorSearchUserParams user_params;
    doris::segment_v2::AnnRangeSearchResult result;
    auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();

    EXPECT_CALL(*mock_iterator, range_search(testing::_, testing::_, testing::_, testing::_))
            .WillOnce(testing::Return(doris::Status::OK()));

    status = mock_iterator->range_search(range_params, user_params, &result, stats.get());
    EXPECT_TRUE(status.ok());
}

} // namespace doris::segment_v2
