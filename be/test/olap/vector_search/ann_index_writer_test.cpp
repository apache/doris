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

#include "olap/rowset/segment_v2/ann_index/ann_index_writer.h"

#include <CLucene/store/RAMDirectory.h>
#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "runtime/collection_value.h"
#include "vector_search_utils.h"

using namespace doris::vector_search_utils;

namespace doris::segment_v2 {

class AnnIndexWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure ExecEnv has a valid tmp dir for IndexFileWriter (prevents nullptr deref)
        if (ExecEnv::GetInstance()->get_tmp_file_dirs() == nullptr) {
            const std::string tmp_dir = "./ut_dir/tmp_vector_search";
            (void)doris::io::global_local_filesystem()->delete_directory(tmp_dir);
            (void)doris::io::global_local_filesystem()->create_directory(tmp_dir);
            std::vector<doris::StorePath> paths;
            paths.emplace_back(tmp_dir, -1);
            auto tmp_file_dirs = std::make_unique<doris::segment_v2::TmpFileDirs>(paths);
            ASSERT_TRUE(tmp_file_dirs->init().ok());
            ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        }

        // Create RAM directory for testing
        _ram_dir = std::make_shared<lucene::store::RAMDirectory>();

        // Create test index properties
        _properties["index_type"] = "hnsw";
        _properties["metric_type"] = "l2_distance";
        _properties["dim"] = "4";
        _properties["max_degree"] = "16";

        // Create tablet index
        _tablet_index = std::make_unique<TabletIndex>();
        _tablet_index->_properties = _properties;
        _tablet_index->_index_id = 1;
        _tablet_index->_index_name = "test_ann_index";

        // Create mock index file writer
        _index_file_writer =
                std::make_unique<MockIndexFileWriter>(doris::io::global_local_filesystem());
    }

    void TearDown() override {}

    std::shared_ptr<lucene::store::RAMDirectory> _ram_dir;
    std::map<std::string, std::string> _properties;
    std::unique_ptr<TabletIndex> _tablet_index;
    std::unique_ptr<MockIndexFileWriter> _index_file_writer;
};

TEST_F(AnnIndexWriterTest, TestConstructorAndDestructor) {
    // Test constructor
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());
    EXPECT_NE(writer, nullptr);

    // Destructor should be called automatically when writer goes out of scope
}

TEST_F(AnnIndexWriterTest, TestInitWithDifferentProperties) {
    // Test with different index types and parameters
    std::vector<std::map<std::string, std::string>> test_properties = {
            {{"index_type", "hnsw"},
             {"metric_type", "inner_product"},
             {"dim", "8"},
             {"max_degree", "32"}},
            {{"index_type", "hnsw"},
             {"metric_type", "l2_distance"},
             {"dim", "128"},
             {"max_degree", "64"}},
            // Test with default values (missing properties)
            {{"index_type", "hnsw"}},
            {}};

    for (const auto& props : test_properties) {
        auto tablet_index = std::make_unique<TabletIndex>();
        tablet_index->_properties = props;
        tablet_index->_index_id = 1;

        auto writer = std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

        auto fs_dir = std::make_shared<DorisFSDirectory>();
        EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

        Status status = writer->init();
        EXPECT_TRUE(status.ok());
    }
}

TEST_F(AnnIndexWriterTest, TestAddArrayValuesSuccess) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Prepare test data
    const size_t dim = 4;
    const size_t num_rows = 3;
    std::vector<float> vectors = {
            1.0f, 2.0f,  3.0f,  4.0f, // Row 0
            5.0f, 6.0f,  7.0f,  8.0f, // Row 1
            9.0f, 10.0f, 11.0f, 12.0f // Row 2
    };

    std::vector<size_t> offsets = {0, 4, 8, 12}; // Each row has 4 elements

    Status status =
            writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                     reinterpret_cast<const uint8_t*>(offsets.data()), num_rows);
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestAddArrayValuesEmptyRows) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Test with zero rows
    Status status = writer->add_array_values(sizeof(float), nullptr, nullptr, nullptr, 0);
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestAddArrayValuesWrongDimension) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Prepare test data with wrong dimension (expected 4, providing 3)
    const size_t num_rows = 2;
    std::vector<float> vectors = {
            1.0f, 2.0f, 3.0f, // Row 0: 3 elements (wrong)
            4.0f, 5.0f, 6.0f  // Row 1: 3 elements (wrong)
    };

    std::vector<size_t> offsets = {0, 3, 6}; // Each row has 3 elements instead of 4

    Status status =
            writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                     reinterpret_cast<const uint8_t*>(offsets.data()), num_rows);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>());
}

TEST_F(AnnIndexWriterTest, TestAddArrayValuesWithCollectionValue) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // This should return an error as ANN index doesn't support nullable columns
    Status status = writer->add_array_values(sizeof(float), nullptr, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST_F(AnnIndexWriterTest, TestAddValues) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // This method currently returns OK without doing anything
    Status status = writer->add_values("test", nullptr, 0);
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestAddNulls) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // This should return an error as ANN index doesn't support nullable columns
    Status status = writer->add_nulls(10);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST_F(AnnIndexWriterTest, TestAddArrayNulls) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // This should return an error as ANN index doesn't support nullable columns
    Status status = writer->add_array_nulls(nullptr, 10);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST_F(AnnIndexWriterTest, TestSize) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Size method currently returns 0
    EXPECT_EQ(writer->size(), 0);
}

TEST_F(AnnIndexWriterTest, TestCloseOnError) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    // close_on_error should not crash
    writer->close_on_error();
}

TEST_F(AnnIndexWriterTest, TestFinish) {
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());

    // Add some test data before finishing
    const size_t dim = 4;
    const size_t num_rows = 2;
    std::vector<float> vectors = {
            1.0f, 2.0f, 3.0f, 4.0f, // Row 0
            5.0f, 6.0f, 7.0f, 8.0f  // Row 1
    };

    std::vector<size_t> offsets = {0, 4, 8};

    ASSERT_TRUE(writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                         reinterpret_cast<const uint8_t*>(offsets.data()), num_rows)
                        .ok());

    // Finish should save the index
    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestFullWorkflow) {
    // Test a complete workflow: init -> add_data -> finish
    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // 1. Initialize
    ASSERT_TRUE(writer->init().ok());

    // 2. Add multiple batches of data
    const size_t dim = 4;

    // Batch 1
    {
        const size_t num_rows = 2;
        std::vector<float> vectors = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
        std::vector<size_t> offsets = {0, 4, 8};

        ASSERT_TRUE(writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                             reinterpret_cast<const uint8_t*>(offsets.data()),
                                             num_rows)
                            .ok());
    }

    // Batch 2
    {
        const size_t num_rows = 3;
        std::vector<float> vectors = {9.0f,  10.0f, 11.0f, 12.0f, 13.0f, 14.0f,
                                      15.0f, 16.0f, 17.0f, 18.0f, 19.0f, 20.0f};
        std::vector<size_t> offsets = {0, 4, 8, 12};

        ASSERT_TRUE(writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                             reinterpret_cast<const uint8_t*>(offsets.data()),
                                             num_rows)
                            .ok());
    }

    // 3. Finish
    ASSERT_TRUE(writer->finish().ok());
}

TEST_F(AnnIndexWriterTest, TestInvalidIndexType) {
    // Test with invalid index type
    auto properties = _properties;
    properties["index_type"] = "invalid_type";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // This should throw an exception due to invalid index type
    EXPECT_THROW(writer->init(), doris::Exception);
}

TEST_F(AnnIndexWriterTest, TestInvalidMetricType) {
    // Test with invalid metric type
    auto properties = _properties;
    properties["metric_type"] = "invalid_metric";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), tablet_index.get());

    auto fs_dir = std::make_shared<DorisFSDirectory>();
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // This should throw an exception due to invalid metric type
    EXPECT_THROW(writer->init(), doris::Exception);
}

} // namespace doris::segment_v2
