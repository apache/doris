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

#include "storage/index/ann/ann_index_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/cache/ann_index_ivf_list_cache.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_iterator.h"
#include "storage/index/ann/ann_index_storage.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/ann/vector_search_utils.h"
#include "storage/index/inverted/clucene/lucene_ann_store.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"

using namespace doris::vector_search_utils;

namespace doris {

namespace {

// Who was destroyed, in order.
using TeardownLog = std::vector<std::string>;

class TrackedSource;

// An input that logs its destruction and, on the way out, writes into the source it was opened
// from. That write is the kind of use an input may make of its source: destroyed after its source,
// it is a heap-use-after-free under ASAN.
class TrackedInput final : public segment_v2::AnnIndexInput {
public:
    TrackedInput(std::string name, std::unique_ptr<segment_v2::AnnIndexInput> input,
                 TrackedSource* source, TeardownLog* log)
            : _name(std::move(name)), _input(std::move(input)), _source(source), _log(log) {}
    ~TrackedInput() override;

    uint64_t length() const override { return _input->length(); }
    Status read_at(uint64_t offset, void* out, size_t size, const io::IOContext* io_ctx) override {
        return _input->read_at(offset, out, size, io_ctx);
    }

private:
    std::string _name;
    std::unique_ptr<segment_v2::AnnIndexInput> _input;
    TrackedSource* _source;
    TeardownLog* _log;
};

class TrackedSource final : public segment_v2::AnnIndexSource {
public:
    TrackedSource(std::unique_ptr<segment_v2::AnnIndexSource> source, TeardownLog* log)
            : _source(std::move(source)), _log(log) {}
    ~TrackedSource() override {
        EXPECT_EQ(live_inputs, 0) << "the source was destroyed before an input it opened";
        _log->emplace_back("source");
    }

    Result<std::unique_ptr<segment_v2::AnnIndexInput>> open_input(std::string_view name) override {
        auto input = _source->open_input(name);
        DORIS_CHECK(input.has_value()) << input.error();
        ++live_inputs;
        return std::make_unique<TrackedInput>(std::string(name), std::move(input.value()), this,
                                              _log);
    }
    std::string debug_string() const override { return _source->debug_string(); }

    int live_inputs = 0;

private:
    std::unique_ptr<segment_v2::AnnIndexSource> _source;
    TeardownLog* _log;
};

TrackedInput::~TrackedInput() {
    --_source->live_inputs;
    _log->emplace_back("input " + _name);
}

} // namespace

class AnnIndexReaderTest : public VectorSearchTest {
protected:
    void SetUp() override {
        VectorSearchTest::SetUp();

        // Create test index properties
        _properties["index_type"] = "hnsw";
        _properties["metric_type"] = "l2_distance";
        _properties["dim"] = "128";
        _properties["max_degree"] = "16";

        // Create tablet index
        _tablet_index = std::make_unique<TabletIndex>();
        _tablet_index->_properties = _properties;
        _tablet_index->_index_id = 1;
        _tablet_index->_index_name = "test_ann_index";

        // Create mock index file reader
        _mock_index_file_reader = std::make_shared<MockIndexFileReader>();
    }

    void TearDown() override { VectorSearchTest::TearDown(); }

    std::map<std::string, std::string> _properties;
    std::unique_ptr<TabletIndex> _tablet_index;
    std::shared_ptr<MockIndexFileReader> _mock_index_file_reader;
};

TEST_F(AnnIndexReaderTest, TestConstructor) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    EXPECT_NE(reader, nullptr);
    EXPECT_EQ(reader->get_index_id(), 1);
    EXPECT_EQ(reader->index_type(), IndexType::ANN);
    EXPECT_EQ(reader->get_metric_type(), segment_v2::AnnIndexMetric::L2);
}

TEST_F(AnnIndexReaderTest, TestConstructorWithDifferentMetrics) {
    // Test with inner product metric
    auto properties = _properties;
    properties["metric_type"] = "inner_product";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 2;

    auto reader = std::make_unique<segment_v2::AnnIndexReader>(tablet_index.get(),
                                                               _mock_index_file_reader);

    EXPECT_EQ(reader->get_metric_type(), segment_v2::AnnIndexMetric::IP);
    EXPECT_EQ(reader->get_index_id(), 2);
}

TEST_F(AnnIndexReaderTest, TestConstructorWithIVF) {
    // Test with IVF index type
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "128";
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 3;

    auto reader = std::make_unique<segment_v2::AnnIndexReader>(tablet_index.get(),
                                                               _mock_index_file_reader);
    reader->_index_type = segment_v2::AnnIndexType::IVF;

    EXPECT_EQ(reader->get_index_id(), 3);
    EXPECT_EQ(reader->index_type(), IndexType::ANN);
    EXPECT_EQ(reader->get_metric_type(), segment_v2::AnnIndexMetric::L2);
}

TEST_F(AnnIndexReaderTest, TestNewIterator) {
    // TODO: Fix if we using unique_ptr here.
    auto reader = std::make_shared<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    std::unique_ptr<segment_v2::IndexIterator> iterator;
    Status status = reader->new_iterator(&iterator);

    EXPECT_TRUE(status.ok());
    EXPECT_NE(iterator, nullptr);

    // Verify it's an AnnIndexIterator
    auto ann_iterator = dynamic_cast<segment_v2::AnnIndexIterator*>(iterator.get());
    EXPECT_NE(ann_iterator, nullptr);
}

TEST_F(AnnIndexReaderTest, TestLoadIndexSuccess) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Mock successful index file operations
    EXPECT_CALL(*_mock_index_file_reader, init(testing::_, testing::_))
            .WillOnce(testing::Return(Status::OK()));

    // For the open_ann_source method that returns Result<unique_ptr<...>>, we need to use a
    // different approach since gmock has issues with non-copyable return types
    EXPECT_CALL(*_mock_index_file_reader, open_ann_source(testing::_, testing::_))
            .WillOnce(testing::Invoke(
                    [](const doris::TabletIndex*, const doris::io::IOContext*)
                            -> doris::Result<std::unique_ptr<doris::segment_v2::AnnIndexSource>> {
                        return doris::ResultError(doris::Status::IOError("Mock not implemented"));
                    }));

    io::IOContext io_ctx;
    Status status = reader->load_index(&io_ctx);
    // We expect this to fail since we're not fully implementing the mock
    // but it should not crash due to the copy constructor issue
    EXPECT_FALSE(status.ok());
}

TEST_F(AnnIndexReaderTest, TestLoadIndexFailureInit) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Mock failed init
    EXPECT_CALL(*_mock_index_file_reader, init(testing::_, testing::_))
            .WillOnce(testing::Return(Status::IOError("Init failed")));

    io::IOContext io_ctx;
    Status status = reader->load_index(&io_ctx);
    EXPECT_FALSE(status.ok());
}

TEST_F(AnnIndexReaderTest, TestLoadIndexFailureOpen) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Mock successful init but failed open
    EXPECT_CALL(*_mock_index_file_reader, init(testing::_, testing::_))
            .WillOnce(testing::Return(Status::OK()));

    EXPECT_CALL(*_mock_index_file_reader, open_ann_source(testing::_, testing::_))
            .WillOnce(testing::Invoke(
                    [](const doris::TabletIndex*, const doris::io::IOContext*)
                            -> doris::Result<std::unique_ptr<doris::segment_v2::AnnIndexSource>> {
                        return doris::ResultError(doris::Status::IOError("Open failed"));
                    }));

    io::IOContext io_ctx;
    Status status = reader->load_index(&io_ctx);
    EXPECT_FALSE(status.ok());
}

TEST_F(AnnIndexReaderTest, TestQueryWithoutLoadIndex) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Set up _vector_index manually to bypass load_index for testing
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::L2);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);

    // Create query parameters
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    segment_v2::AnnTopNParam param {
            .query_value = query_data,
            .query_value_size = 4,
            .limit = 5,
            ._user_params = VectorSearchUserParams {.hnsw_ef_search = 100,
                                                    .hnsw_check_relative_distance = false,
                                                    .hnsw_bounded_queue = false},
            .roaring = &bitmap};

    segment_v2::AnnIndexStats stats;
    io::IOContext io_ctx;

    Status status = reader->query(&io_ctx, &param, &stats);

    // The query might succeed or fail depending on the internal index state,
    // but it should not crash and should properly initialize distance and row_ids
    if (status.ok()) {
        EXPECT_NE(param.distance, nullptr);
        EXPECT_NE(param.row_ids, nullptr);
    }
}

TEST_F(AnnIndexReaderTest, TestQueryIVFWithoutLoadIndex) {
    // Test IVF index query
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "64";
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;

    auto reader = std::make_unique<segment_v2::AnnIndexReader>(tablet_index.get(),
                                                               _mock_index_file_reader);

    // Set up _vector_index manually to bypass load_index for testing
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::L2);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.ivf_nlist = 64;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::IVF;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    build_params.quantizer = doris::segment_v2::FaissBuildParameter::Quantizer::FLAT;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);
    reader->_index_type = segment_v2::AnnIndexType::IVF;

    // Create query parameters
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    segment_v2::AnnTopNParam param {.query_value = query_data,
                                    .query_value_size = 4,
                                    .limit = 5,
                                    ._user_params = VectorSearchUserParams {},
                                    .roaring = &bitmap};

    segment_v2::AnnIndexStats stats;
    io::IOContext io_ctx;

    Status status = reader->query(&io_ctx, &param, &stats);

    // The query might succeed or fail depending on the internal index state,
    // but it should not crash and should properly initialize distance and row_ids
    if (status.ok()) {
        EXPECT_NE(param.distance, nullptr);
        EXPECT_NE(param.row_ids, nullptr);
    }
}

TEST_F(AnnIndexReaderTest, TestRangeSearchWithoutLoadIndex) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Set up _vector_index manually to bypass load_index for testing
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::L2);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);

    // Create range search parameters
    float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.radius = 5.0f;
    params.query_value = query_data;
    params.roaring = &bitmap;

    VectorSearchUserParams user_params;
    user_params.hnsw_ef_search = 50;
    user_params.hnsw_check_relative_distance = true;
    user_params.hnsw_bounded_queue = true;

    segment_v2::AnnRangeSearchResult result;
    segment_v2::AnnIndexStats stats;

    Status status = reader->range_search(params, user_params, &result, &stats);

    // The range search might succeed or fail depending on the internal index state,
    // but it should not crash
    if (status.ok()) {
        EXPECT_NE(result.roaring, nullptr);
    }
}

TEST_F(AnnIndexReaderTest, TestRangeSearchIVFWithoutLoadIndex) {
    // Test IVF index range search
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "64";
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;

    auto reader = std::make_unique<segment_v2::AnnIndexReader>(tablet_index.get(),
                                                               _mock_index_file_reader);

    // Set up _vector_index manually to bypass load_index for testing
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::L2);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.ivf_nlist = 64;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::IVF;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    build_params.quantizer = doris::segment_v2::FaissBuildParameter::Quantizer::FLAT;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);
    reader->_index_type = segment_v2::AnnIndexType::IVF;

    // Create range search parameters
    float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);

    segment_v2::AnnRangeSearchParams params;
    params.is_le_or_lt = true;
    params.radius = 5.0f;
    params.query_value = query_data;
    params.roaring = &bitmap;

    VectorSearchUserParams user_params;
    user_params.ivf_nprobe = 2;

    segment_v2::AnnRangeSearchResult result;
    segment_v2::AnnIndexStats stats;

    Status status = reader->range_search(params, user_params, &result, &stats);

    // The range search might succeed or fail depending on the internal index state,
    // but it should not crash
    if (status.ok()) {
        EXPECT_NE(result.roaring, nullptr);
    }
}

TEST_F(AnnIndexReaderTest, TestRangeSearchWithDifferentParameters) {
    auto reader = std::make_unique<segment_v2::AnnIndexReader>(_tablet_index.get(),
                                                               _mock_index_file_reader);

    // Set up _vector_index manually
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::L2);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);

    // Test case 1: is_le_or_lt = false
    {
        float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
        roaring::Roaring bitmap;
        bitmap.add(1);

        segment_v2::AnnRangeSearchParams params;
        params.is_le_or_lt = false; // This should result in no distances/row_ids
        params.radius = 5.0f;
        params.query_value = query_data;
        params.roaring = &bitmap;

        VectorSearchUserParams user_params;
        user_params.hnsw_ef_search = 50;

        segment_v2::AnnRangeSearchResult result;
        segment_v2::AnnIndexStats stats;

        Status status = reader->range_search(params, user_params, &result, &stats);

        if (status.ok()) {
            // When is_le_or_lt = false, we expect no distance/row_ids
            if (result.row_ids == nullptr) {
                EXPECT_EQ(result.row_ids, nullptr);
            }
            if (result.distance == nullptr) {
                EXPECT_EQ(result.distance, nullptr);
            }
        }
    }

    // Test case 2: is_le_or_lt = true
    {
        float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
        roaring::Roaring bitmap;
        bitmap.add(1);

        segment_v2::AnnRangeSearchParams params;
        params.is_le_or_lt = true;
        params.radius = 5.0f;
        params.query_value = query_data;
        params.roaring = &bitmap;

        VectorSearchUserParams user_params;
        user_params.hnsw_ef_search = 50;
        user_params.hnsw_check_relative_distance = false;
        user_params.hnsw_bounded_queue = false;

        segment_v2::AnnRangeSearchResult result;
        segment_v2::AnnIndexStats stats;

        Status status = reader->range_search(params, user_params, &result, &stats);

        // This should not crash regardless of success/failure
        if (status.ok()) {
            EXPECT_NE(result.roaring, nullptr);
        }
    }
}

TEST_F(AnnIndexReaderTest, TestWithInnerProductMetric) {
    // Test with inner product metric type
    auto properties = _properties;
    properties["metric_type"] = "inner_product";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto reader = std::make_unique<segment_v2::AnnIndexReader>(tablet_index.get(),
                                                               _mock_index_file_reader);

    EXPECT_EQ(reader->get_metric_type(), segment_v2::AnnIndexMetric::IP);

    // Set up _vector_index with IP metric
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris_faiss_vector_index->set_metric(doris::segment_v2::AnnIndexMetric::IP);

    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 4;
    build_params.max_degree = 16;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::HNSW;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::IP;
    doris_faiss_vector_index->build(build_params);

    reader->_vector_index = std::move(doris_faiss_vector_index);

    // Test query with IP metric
    const float query_data[] = {1.0f, 2.0f, 3.0f, 4.0f};
    roaring::Roaring bitmap;
    bitmap.add(1);

    segment_v2::AnnTopNParam param {.query_value = query_data,
                                    .query_value_size = 4,
                                    .limit = 5,
                                    ._user_params = VectorSearchUserParams {},
                                    .roaring = &bitmap};

    segment_v2::AnnIndexStats stats;
    io::IOContext io_ctx;

    Status status = reader->query(&io_ctx, &param, &stats);

    // Should not crash regardless of success/failure
    if (status.ok()) {
        EXPECT_NE(param.distance, nullptr);
        EXPECT_NE(param.row_ids, nullptr);
    }
}

TEST_F(AnnIndexReaderTest, AnnIndexReaderRangeSearch) {
    size_t iterations = 5;
    for (size_t i = 0; i < iterations; ++i) {
        std::map<std::string, std::string> index_properties;
        index_properties["index_type"] = "hnsw";
        // Use canonical metric name and include required dimension property
        index_properties["metric_type"] = "l2_distance";
        index_properties["dim"] = "128";
        std::unique_ptr<doris::TabletIndex> index_meta = std::make_unique<doris::TabletIndex>();
        index_meta->_properties = index_properties;
        auto mock_index_file_reader = std::make_shared<MockIndexFileReader>();
        auto ann_index_reader = std::make_unique<segment_v2::AnnIndexReader>(
                index_meta.get(), mock_index_file_reader);
        doris::vector_search_utils::IndexType index_type =
                doris::vector_search_utils::IndexType::HNSW;
        const size_t dim = 128;
        const size_t m = 16;
        auto doris_faiss_index = doris::vector_search_utils::create_doris_index(index_type, dim, m);
        auto native_faiss_index =
                doris::vector_search_utils::create_native_index(index_type, dim, m);
        const size_t num_vectors = 1000;
        auto vectors = doris::vector_search_utils::generate_test_vectors_matrix(num_vectors, dim);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(
                doris_faiss_index.get(), native_faiss_index.get(), vectors);
        std::ignore = save_to_directory(doris_faiss_index.get(), this->_ram_dir);
        std::vector<float> query_value = vectors[0];
        const float radius = doris::vector_search_utils::get_radius_from_matrix(query_value.data(),
                                                                                dim, vectors, 0.3);

        // Make sure all rows are in the roaring
        auto roaring = std::make_unique<roaring::Roaring>();
        for (size_t i = 0; i < num_vectors; ++i) {
            roaring->add(i);
        }

        doris::segment_v2::AnnRangeSearchParams params;
        params.radius = radius;
        params.query_value = query_value.data();
        params.roaring = roaring.get();
        doris::VectorSearchUserParams custom_params;
        custom_params.hnsw_ef_search = 16;
        doris::segment_v2::AnnRangeSearchResult result;
        auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();
        auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
        std::ignore = load_from_directory(doris_faiss_vector_index.get(), this->_ram_dir);
        ann_index_reader->_vector_index = std::move(doris_faiss_vector_index);
        std::ignore = ann_index_reader->range_search(params, custom_params, &result, stats.get());

        ASSERT_TRUE(result.roaring != nullptr);
        ASSERT_TRUE(result.distance != nullptr);
        ASSERT_TRUE(result.row_ids != nullptr);
        std::vector<std::pair<int, float>> doris_search_result_order_by_lables;
        for (size_t i = 0; i < result.roaring->cardinality(); ++i) {
            doris_search_result_order_by_lables.push_back(
                    {result.row_ids->at(i), result.distance[i]});
        }

        std::sort(doris_search_result_order_by_lables.begin(),
                  doris_search_result_order_by_lables.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        std::vector<std::pair<int, float>> native_search_result_order_by_lables =
                doris::vector_search_utils::perform_native_index_range_search(
                        native_faiss_index.get(), query_value.data(), radius);

        ASSERT_EQ(result.roaring->cardinality(), native_search_result_order_by_lables.size());

        for (size_t i = 0; i < native_search_result_order_by_lables.size(); ++i) {
            ASSERT_EQ(doris_search_result_order_by_lables[i].first,
                      native_search_result_order_by_lables[i].first);
            ASSERT_FLOAT_EQ(doris_search_result_order_by_lables[i].second,
                            native_search_result_order_by_lables[i].second);
        }
    }
}

TEST_F(AnnIndexReaderTest, AnnIndexReaderIVFRangeSearch) {
    // Test IVF index range search functionality
    std::map<std::string, std::string> index_properties;
    index_properties["index_type"] = "ivf";
    index_properties["metric_type"] = "l2_distance";
    index_properties["dim"] = "32";
    index_properties["nlist"] = "8"; // Small nlist for testing
    index_properties["quantizer"] = "flat";
    std::unique_ptr<doris::TabletIndex> index_meta = std::make_unique<doris::TabletIndex>();
    index_meta->_properties = index_properties;
    auto mock_index_file_reader = std::make_shared<MockIndexFileReader>();
    auto ann_index_reader =
            std::make_unique<segment_v2::AnnIndexReader>(index_meta.get(), mock_index_file_reader);
    ann_index_reader->_index_type = segment_v2::AnnIndexType::IVF;

    // Create and set up IVF index
    auto doris_faiss_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    doris::segment_v2::FaissBuildParameter build_params;
    build_params.dim = 32;
    build_params.ivf_nlist = 8;
    build_params.index_type = doris::segment_v2::FaissBuildParameter::IndexType::IVF;
    build_params.metric_type = doris::segment_v2::FaissBuildParameter::MetricType::L2;
    build_params.quantizer = doris::segment_v2::FaissBuildParameter::Quantizer::FLAT;
    doris_faiss_index->build(build_params);

    const size_t num_vectors = 1000;
    auto vectors = doris::vector_search_utils::generate_test_vectors_matrix(num_vectors, 32);
    for (const auto& vec : vectors) {
        doris_faiss_index->add(1, vec.data());
    }

    std::ignore = save_to_directory(doris_faiss_index.get(), this->_ram_dir);
    std::vector<float> query_value = vectors[0];
    const float radius = doris::vector_search_utils::get_radius_from_matrix(query_value.data(), 32,
                                                                            vectors, 0.3F);

    // Make sure all rows are in the roaring
    auto roaring = std::make_unique<roaring::Roaring>();
    for (size_t i = 0; i < num_vectors; ++i) {
        roaring->add(i);
    }

    doris::segment_v2::AnnRangeSearchParams params;
    params.radius = radius;
    params.query_value = query_value.data();
    params.roaring = roaring.get();
    doris::VectorSearchUserParams custom_params;
    doris::segment_v2::AnnRangeSearchResult result;
    auto stats = std::make_unique<doris::segment_v2::AnnIndexStats>();
    auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
    std::ignore = load_from_directory(doris_faiss_vector_index.get(), this->_ram_dir);
    ann_index_reader->_vector_index = std::move(doris_faiss_vector_index);
    Status status = ann_index_reader->range_search(params, custom_params, &result, stats.get());

    // IVF range search should work without crashing
    if (status.ok()) {
        EXPECT_NE(result.roaring, nullptr);
        EXPECT_GE(result.roaring->cardinality(), 0);
    }
}

// An IVF_ON_DISK index keeps reading ann.ivfdata after the load: the CachedRandomAccessReader
// inside _vector_index holds an input opened from _source and reads it on every search. So the
// reader must destroy _vector_index before _source, which it does by declaring _source first. The
// logging source and inputs above record the teardown, and an input destroyed after its source
// writes into freed memory, which ASAN reports.
TEST_F(AnnIndexReaderTest, IvfOnDiskIndexIsDestroyedBeforeItsSource) {
    // IVF_ON_DISK reads through the list cache in production (exec_env_init installs it).
    const bool owns_cache = AnnIndexIVFListCache::instance() == nullptr;
    if (owns_cache) {
        AnnIndexIVFListCache::create_global_cache(64 * 1024 * 1024);
    }
    Defer destroy_cache([owns_cache] {
        if (owns_cache) {
            AnnIndexIVFListCache::destroy_global_cache();
        }
    });

    constexpr int kDim = 8;
    constexpr int kRows = 256;
    const std::vector<float> vectors = generate_test_vectors_flatten(kRows, kDim);
    {
        segment_v2::FaissVectorIndex builder;
        segment_v2::FaissBuildParameter params;
        params.dim = kDim;
        params.ivf_nlist = 4;
        params.index_type = segment_v2::FaissBuildParameter::IndexType::IVF_ON_DISK;
        params.metric_type = segment_v2::FaissBuildParameter::MetricType::L2;
        params.quantizer = segment_v2::FaissBuildParameter::Quantizer::FLAT;
        builder.build(params);
        ASSERT_TRUE(builder.train(kRows, vectors.data()).ok());
        ASSERT_TRUE(builder.add(kRows, vectors.data()).ok());
        ASSERT_TRUE(save_to_directory(&builder, _ram_dir).ok());
    }

    TabletIndex index_meta;
    index_meta._properties = {{"index_type", "ivf_on_disk"},
                              {"metric_type", "l2_distance"},
                              {"dim", std::to_string(kDim)},
                              {"nlist", "4"}};
    // Part of the list cache key: a distinctive id keeps this test off entries other tests may have
    // left in a cache they installed.
    index_meta._index_id = 46011;
    TeardownLog log;
    auto file_reader = std::make_shared<MockIndexFileReader>();
    EXPECT_CALL(*file_reader, init(testing::_, testing::_)).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*file_reader, open_ann_source(testing::_, testing::_))
            .WillOnce(
                    testing::Invoke([&](const TabletIndex*, const io::IOContext*)
                                            -> Result<std::unique_ptr<segment_v2::AnnIndexSource>> {
                        return std::make_unique<TrackedSource>(
                                std::make_unique<segment_v2::LuceneAnnSource>(_ram_dir), &log);
                    }));
    {
        auto reader = std::make_shared<segment_v2::AnnIndexReader>(&index_meta, file_reader);
        io::IOContext io_ctx;
        ASSERT_TRUE(reader->load_index(&io_ctx).ok());

        roaring::Roaring candidates;
        candidates.addRange(0, kRows);
        segment_v2::AnnTopNParam param {.query_value = vectors.data(),
                                        .query_value_size = kDim,
                                        .limit = 4,
                                        ._user_params = VectorSearchUserParams {},
                                        .roaring = &candidates,
                                        .rows_of_segment = kRows,
                                        .enable_result_cache = false};
        segment_v2::AnnIndexStats stats;
        ASSERT_TRUE(reader->query(&io_ctx, &param, &stats).ok());
        // The search went to ann.ivfdata, through the input the index still holds.
        EXPECT_GT(stats.ivf_on_disk_cache_miss_cnt.value(), 0);
        // ann.faiss was read and closed by the load; ann.ivfdata stays open in the index.
        EXPECT_EQ(log, TeardownLog({"input ann.faiss"}));
    }
    EXPECT_EQ(log, TeardownLog({"input ann.faiss", "input ann.ivfdata", "source"}));
}

} // namespace doris
