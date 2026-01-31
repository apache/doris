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

#include "olap/field.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "runtime/collection_value.h"
#include "vector_search_utils.h"

using namespace doris::vector_search_utils;

namespace doris::segment_v2 {

class MockVectorIndex : public VectorIndex {
public:
    MockVectorIndex() { _dimension = 4; } // Set dimension for test
    MOCK_METHOD(doris::Status, train, (vectorized::Int64 n, const float* vec), (override));
    MOCK_METHOD(doris::Status, add, (vectorized::Int64 n, const float* vec), (override));
    MOCK_METHOD(doris::Status, ann_topn_search,
                (const float* query_vec, int k, const segment_v2::IndexSearchParameters& params,
                 segment_v2::IndexSearchResult& result),
                (override));
    MOCK_METHOD(doris::Status, range_search,
                (const float* query_vec, const float& radius,
                 const segment_v2::IndexSearchParameters& params,
                 segment_v2::IndexSearchResult& result),
                (override));
    MOCK_METHOD(doris::Status, save, (lucene::store::Directory * dir), (override));
    MOCK_METHOD(doris::Status, load, (lucene::store::Directory * dir), (override));
    MOCK_METHOD(vectorized::Int64, get_min_train_rows, (), (const, override));
};

class TestAnnIndexColumnWriter : public AnnIndexColumnWriter {
public:
    TestAnnIndexColumnWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta)
            : AnnIndexColumnWriter(index_file_writer, index_meta) {}

    void set_vector_index(std::shared_ptr<VectorIndex> index) { _vector_index = index; }
    void set_need_save_index(bool value) { _need_save_index = value; }
};

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
        _tablet_index->_index_type = IndexType::ANN;
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
            {{"index_type", "ivf"},
             {"metric_type", "l2_distance"},
             {"dim", "8"},
             {"nlist", "128"},
             {"quantizer", "flat"}},
            {{"index_type", "ivf"},
             {"metric_type", "inner_product"},
             {"dim", "128"},
             {"nlist", "512"},
             {"quantizer", "sq4"}},
            {{"index_type", "ivf"},
             {"metric_type", "l2_distance"},
             {"dim", "64"},
             {"nlist", "256"},
             {"quantizer", "pq"},
             {"pq_m", "4"},
             {"pq_nbits", "8"}},
            // Test with default values (missing properties)
            {{"index_type", "hnsw"}},
            {{"index_type", "ivf"}},
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
    tablet_index->_index_type = IndexType::ANN;
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), tablet_index.get());

    auto fs_dir = std::make_shared<DorisFSDirectory>();
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // This should throw an exception due to invalid metric type
    EXPECT_THROW(writer->init(), doris::Exception);
}

TEST_F(AnnIndexWriterTest, TestAddMoreThanChunkSize) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(1)
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, train(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    // CHUNK_SIZE = 10
    const size_t dim = 4;

    {
        const size_t num_rows = 6;
        std::vector<float> vectors = {
                1.0f,  2.0f,  3.0f,  4.0f,  // Row 0
                5.0f,  6.0f,  7.0f,  8.0f,  // Row 1
                9.0f,  10.0f, 11.0f, 12.0f, // Row 2
                13.0f, 14.0f, 15.0f, 16.0f, // Row 3
                17.0f, 18.0f, 19.0f, 20.0f, // Row 4
                21.0f, 22.0f, 23.0f, 24.0f  // Row 5
        };
        std::vector<size_t> offsets = {0, 4, 8, 12, 16, 20, 24};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    {
        const size_t num_rows = 6;
        std::vector<float> vectors = {
                25.0f, 26.0f, 27.0f, 28.0f, // Row 6
                29.0f, 30.0f, 31.0f, 32.0f, // Row 7
                33.0f, 34.0f, 35.0f, 36.0f, // Row 8
                37.0f, 38.0f, 39.0f, 40.0f, // Row 9
                41.0f, 42.0f, 43.0f, 44.0f, // Row 10
                45.0f, 46.0f, 47.0f, 48.0f  // Row 11
        };
        std::vector<size_t> offsets = {0, 4, 8, 12, 16, 20, 24};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestCreateFromIndexColumnWriter) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    tablet_schema->init_from_pb(tablet_schema_pb);

    TabletColumn array_column;
    array_column.set_name("arr1");
    array_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array_column.set_length(0);
    array_column.set_index_length(0);
    array_column.set_is_nullable(false);

    TabletColumn child_column;
    child_column.set_name("arr_sub_float");
    child_column.set_type(FieldType::OLAP_FIELD_TYPE_FLOAT);
    child_column.set_length(INT_MAX);
    array_column.add_sub_column(child_column);
    tablet_schema->append_column(array_column);

    // Get field for array column
    std::unique_ptr<Field> field(FieldFactory::create(array_column));
    ASSERT_NE(field.get(), nullptr);

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    // Create column writer
    std::unique_ptr<IndexColumnWriter> column_writer;
    auto status = IndexColumnWriter::create(field.get(), &column_writer, _index_file_writer.get(),
                                            _tablet_index.get());
    EXPECT_TRUE(status.ok());

    // Prepare test data
    const size_t num_rows = 3;
    std::vector<float> vectors = {
            1.0f, 2.0f,  3.0f,  4.0f, // Row 0
            5.0f, 6.0f,  7.0f,  8.0f, // Row 1
            9.0f, 10.0f, 11.0f, 12.0f // Row 2
    };

    std::vector<size_t> offsets = {0, 4, 8, 12}; // Each row has 4 elements

    status = column_writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                             reinterpret_cast<const uint8_t*>(offsets.data()),
                                             num_rows);
    EXPECT_TRUE(status.ok());

    ASSERT_TRUE(column_writer->finish().ok());
}

TEST_F(AnnIndexWriterTest, TestAddArrayValuesIVF) {
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "3";
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer =
            std::make_unique<AnnIndexColumnWriter>(_index_file_writer.get(), tablet_index.get());

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

TEST_F(AnnIndexWriterTest, TestAddMoreThanChunkSizeIVF) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "2";
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(1)
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, train(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    // CHUNK_SIZE = 10
    const size_t dim = 4;

    {
        const size_t num_rows = 6;
        std::vector<float> vectors = {
                1.0f,  2.0f,  3.0f,  4.0f,  // Row 0
                5.0f,  6.0f,  7.0f,  8.0f,  // Row 1
                9.0f,  10.0f, 11.0f, 12.0f, // Row 2
                13.0f, 14.0f, 15.0f, 16.0f, // Row 3
                17.0f, 18.0f, 19.0f, 20.0f, // Row 4
                21.0f, 22.0f, 23.0f, 24.0f  // Row 5
        };
        std::vector<size_t> offsets = {0, 4, 8, 12, 16, 20, 24};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    {
        const size_t num_rows = 6;
        std::vector<float> vectors = {
                25.0f, 26.0f, 27.0f, 28.0f, // Row 6
                29.0f, 30.0f, 31.0f, 32.0f, // Row 7
                33.0f, 34.0f, 35.0f, 36.0f, // Row 8
                37.0f, 38.0f, 39.0f, 40.0f, // Row 9
                41.0f, 42.0f, 43.0f, 44.0f, // Row 10
                45.0f, 46.0f, 47.0f, 48.0f  // Row 11
        };
        std::vector<size_t> offsets = {0, 4, 8, 12, 16, 20, 24};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestSkipTrainWhenRemainderLessThanNlist) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "5"; // Set nlist to 5
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // CHUNK_SIZE = 10, nlist = 5
    // Add 12 rows: first 10 will be trained/added in one batch, remaining 2 < 5
    // Since we have trained data before (_need_save_index = true), we should add the remaining 2 rows and save
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(5));
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(1)
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add 12 rows total
    {
        const size_t num_rows = 10;
        std::vector<float> vectors(10 * 4);
        for (size_t i = 0; i < 10 * 4; ++i) {
            vectors[i] = static_cast<float>(i);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * 4);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Add 2 more rows
    {
        const size_t num_rows = 2;
        std::vector<float> vectors = {
                40.0f, 41.0f, 42.0f, 43.0f, // Row 10
                44.0f, 45.0f, 46.0f, 47.0f  // Row 11
        };
        std::vector<size_t> offsets = {0, 4, 8};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestLargeDataVolumeWithRemainderSkip) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "3"; // Set nlist to 3
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // CHUNK_SIZE = 10, nlist = 3
    // Add 23 rows: 2 full chunks of 10, remaining 3 == nlist, so train remaining
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(3));
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, train(3, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(3, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add 3 batches: 10 + 10 + 3 = 23 rows
    for (int batch = 0; batch < 2; ++batch) {
        const size_t num_rows = 10;
        std::vector<float> vectors(10 * 4);
        for (size_t i = 0; i < 10 * 4; ++i) {
            vectors[i] = static_cast<float>(batch * 40 + i);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * 4);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Add remaining 3 rows
    {
        const size_t num_rows = 3;
        std::vector<float> vectors = {
                80.0f, 81.0f, 82.0f, 83.0f, // Row 20
                84.0f, 85.0f, 86.0f, 87.0f, // Row 21
                88.0f, 89.0f, 90.0f, 91.0f  // Row 22
        };
        std::vector<size_t> offsets = {0, 4, 8, 12};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestLargeDataVolumeSkipRemainder) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "4"; // Set nlist to 4
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // CHUNK_SIZE = 10, nlist = 4
    // Add 22 rows: 2 full chunks of 10, remaining 2 < 4
    // Since we have trained data before (_need_save_index = true), we should add the remaining 2 rows and save
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(4));
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_))
            .Times(2)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add 2 batches of 10 rows
    for (int batch = 0; batch < 2; ++batch) {
        const size_t num_rows = 10;
        std::vector<float> vectors(10 * 4);
        for (size_t i = 0; i < 10 * 4; ++i) {
            vectors[i] = static_cast<float>(batch * 40 + i);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * 4);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Add remaining 2 rows
    {
        const size_t num_rows = 2;
        std::vector<float> vectors = {
                80.0f, 81.0f, 82.0f, 83.0f, // Row 20
                84.0f, 85.0f, 86.0f, 87.0f  // Row 21
        };
        std::vector<size_t> offsets = {0, 4, 8};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestSkipIndexWhenTotalRowsLessThanNlist) {
    auto mock_index = std::make_shared<MockVectorIndex>();
    auto properties = _properties;
    properties["index_type"] = "ivf";
    properties["nlist"] = "5"; // Set nlist to 5
    properties["quantizer"] = "flat";

    auto tablet_index = std::make_unique<TabletIndex>();
    tablet_index->_properties = properties;
    tablet_index->_index_id = 1;

    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);
    writer->set_need_save_index(false); // No previous training, so should skip entirely

    // Add only 3 rows, which is less than nlist (5)
    // Since no data was trained before (_need_save_index = false), we should skip index building entirely
    // No train, add, or save should be called
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(5));
    EXPECT_CALL(*mock_index, train(testing::_, testing::_)).Times(0);
    EXPECT_CALL(*mock_index, add(testing::_, testing::_)).Times(0);
    EXPECT_CALL(*mock_index, save(testing::_)).Times(0);

    const size_t dim = 4;

    // Add 3 rows
    {
        const size_t num_rows = 3;
        std::vector<float> vectors = {
                1.0f, 2.0f,  3.0f,  4.0f, // Row 0
                5.0f, 6.0f,  7.0f,  8.0f, // Row 1
                9.0f, 10.0f, 11.0f, 12.0f // Row 2
        };
        std::vector<size_t> offsets = {0, 4, 8, 12};

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestPQMinTrainRows) {
    // Test that PQ quantizer requires sufficient training data
    // PQ with pq_m=2, pq_nbits=8 requires at least 256 * 2^8 * 2 = 256 * 256 * 2 = 131072 training vectors
    // But nlist=10, so the effective minimum should be max(10, 131072) = 131072

    auto mock_index = std::make_shared<MockVectorIndex>();
    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // Set up expectations: PQ should require 131072 training vectors minimum
    // Since we only provide 1000 vectors, which is less than 131072, training will happen in batches
    // but finish() will skip saving since remaining data is insufficient
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(131072));
    // 1000 vectors will be processed in 100 batches of 10 vectors each
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(100)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_))
            .Times(100)
            .WillRepeatedly(testing::Return(Status::OK()));
    // Since we have trained data in batches, the index will be saved even though total data is insufficient
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add only 1000 rows, which is less than the required 131072
    {
        const size_t num_rows = 1000;
        std::vector<float> vectors(num_rows * dim);
        for (size_t i = 0; i < num_rows * dim; ++i) {
            vectors[i] = static_cast<float>(i % 100);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * dim);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Finish should skip index building due to insufficient training data
    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestSQMinTrainRows) {
    // Test that SQ quantizer requires sufficient training data
    // SQ requires at least nlist * 2 = 10 * 2 = 20 training vectors

    auto mock_index = std::make_shared<MockVectorIndex>();
    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // Set up expectations: SQ should require at least 20 training vectors
    // Since we only provide 15 vectors, training will happen in batches but finish() will skip saving
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(20));
    // 15 vectors will be processed in 1 batch of 10 vectors and remaining 5 vectors
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(1)
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(5, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    // Since we have trained data, the index will be saved even though total data is insufficient
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add only 15 rows, which is less than the required 20
    {
        const size_t num_rows = 15;
        std::vector<float> vectors(num_rows * dim);
        for (size_t i = 0; i < num_rows * dim; ++i) {
            vectors[i] = static_cast<float>(i % 100);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * dim);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Finish should skip index building due to insufficient training data
    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

TEST_F(AnnIndexWriterTest, TestPQWithSufficientData) {
    // Test that PQ works when sufficient training data is provided

    auto mock_index = std::make_shared<MockVectorIndex>();
    auto writer = std::make_unique<TestAnnIndexColumnWriter>(_index_file_writer.get(),
                                                             _tablet_index.get());

    auto fs_dir = std::make_shared<DorisRAMFSDirectory>();
    fs_dir->init(doris::io::global_local_filesystem(), "./ut_dir/tmp_vector_search", nullptr);
    EXPECT_CALL(*_index_file_writer, open(testing::_)).WillOnce(testing::Return(fs_dir));

    ASSERT_TRUE(writer->init().ok());
    writer->set_vector_index(mock_index);

    // PQ requires 131072 minimum, but we'll provide exactly that amount
    EXPECT_CALL(*mock_index, get_min_train_rows()).WillRepeatedly(testing::Return(131072));
    // Since we provide exactly 131072 vectors, they will be trained and added in chunks
    // Each chunk is 10 vectors, so we expect 13107 train calls and 13107 add calls for full chunks
    EXPECT_CALL(*mock_index, train(10, testing::_))
            .Times(13107)
            .WillRepeatedly(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, add(10, testing::_))
            .Times(13107)
            .WillRepeatedly(testing::Return(Status::OK()));
    // The remaining 2 vectors will be added without training since min_train_rows > 2
    EXPECT_CALL(*mock_index, add(2, testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*mock_index, save(testing::_)).Times(1).WillOnce(testing::Return(Status::OK()));

    const size_t dim = 4;

    // Add exactly 131072 rows
    {
        const size_t num_rows = 131072;
        std::vector<float> vectors(num_rows * dim);
        for (size_t i = 0; i < num_rows * dim; ++i) {
            vectors[i] = static_cast<float>(i % 100);
        }
        std::vector<size_t> offsets;
        for (size_t i = 0; i <= num_rows; ++i) {
            offsets.push_back(i * dim);
        }

        Status status = writer->add_array_values(sizeof(float), vectors.data(), nullptr,
                                                 reinterpret_cast<const uint8_t*>(offsets.data()),
                                                 num_rows);
        EXPECT_TRUE(status.ok());
    }

    // Finish should successfully build the index
    Status status = writer->finish();
    EXPECT_TRUE(status.ok());
}

} // namespace doris::segment_v2
