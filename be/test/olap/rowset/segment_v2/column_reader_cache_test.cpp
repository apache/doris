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
#include "olap/rowset/segment_v2/column_reader_cache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"
#include "mock/mock_segment.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/tablet_schema.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

// Mock classes for testing
class MockColumnReader : public ColumnReader {
public:
    MockColumnReader(int32_t col_uid, FieldType type = FieldType::OLAP_FIELD_TYPE_INT)
            : _col_uid(col_uid), _type(type) {}

    FieldType get_meta_type() override { return _type; }
    int32_t get_col_uid() const { return _col_uid; }

private:
    int32_t _col_uid;
    FieldType _type;
};

class ColumnReaderCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up test configuration
        config::max_segment_partial_column_cache_size = 3;

        // Create mock segment
        _mock_segment = std::make_unique<StrictMock<MockSegment>>();

        // Create cache
        _cache = std::make_unique<ColumnReaderCache>(_mock_segment.get());

        // Set up basic mock expectations
        setup_basic_mocks();
    }

    void TearDown() override {
        _cache.reset();
        _mock_segment.reset();
    }

    void setup_basic_mocks() {
        // Set up file reader
        io::FileReaderSPtr file_reader;
        EXPECT_CALL(*_mock_segment, file_reader()).WillRepeatedly(Return(file_reader));

        // Set up num rows
        EXPECT_CALL(*_mock_segment, num_rows()).WillRepeatedly(Return(1000));

        // mock _get_segment_footer
        EXPECT_CALL(*_mock_segment, _get_segment_footer(_, _))
                .WillRepeatedly(
                        testing::Invoke([this](std::shared_ptr<SegmentFooterPB>& footer_pb_shared,
                                               OlapReaderStatistics*) {
                            if (_mock_segment->_footer) {
                                footer_pb_shared = _mock_segment->_footer;
                                return Status::OK();
                            }
                            return Status::NotFound("Footer not set");
                        }));
    }

    void setup_column_uid_mapping(int32_t col_uid, int32_t footer_ordinal) {
        _mock_segment->add_column_uid_mapping(col_uid, footer_ordinal);
    }

    void setup_segment_footer(const std::vector<ColumnMetaPB>& columns) {
        auto footer = std::make_shared<SegmentFooterPB>();
        for (const auto& col : columns) {
            *footer->add_columns() = col;
        }
        _mock_segment->set_footer(footer);
    }

    std::unique_ptr<StrictMock<MockSegment>> _mock_segment;
    std::unique_ptr<ColumnReaderCache> _cache;
    OlapReaderStatistics _stats;
};

// Test basic cache functionality
TEST_F(ColumnReaderCacheTest, BasicCacheOperations) {
    // Test empty cache
    auto readers = _cache->get_available_readers(false);
    EXPECT_TRUE(readers.empty());

    // Test cache insertion and retrieval
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta.set_unique_id(1);
    col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(1, &reader, &_stats);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(reader, nullptr);

    // Verify cache hit
    std::shared_ptr<ColumnReader> cached_reader;
    status = _cache->get_column_reader(1, &cached_reader, &_stats);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(reader, cached_reader);

    // Verify cache contents
    readers = _cache->get_available_readers(false);
    EXPECT_EQ(readers.size(), 1);
    EXPECT_EQ(readers[1], reader);
}

// Test LRU eviction
TEST_F(ColumnReaderCacheTest, LRUEviction) {
    // Set cache size to 2
    config::max_segment_partial_column_cache_size = 2;

    // Add 3 columns to trigger eviction
    for (int i = 1; i <= 3; ++i) {
        setup_column_uid_mapping(i, 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        col_meta.set_unique_id(i);
        col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});

        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, &reader, &_stats);
        EXPECT_TRUE(status.ok());
    }

    // Verify only 2 readers remain (LRU eviction)
    auto readers = _cache->get_available_readers(false);
    EXPECT_EQ(readers.size(), 2);

    // Column 1 should be evicted (least recently used)
    EXPECT_EQ(readers.find(1), readers.end());
    EXPECT_NE(readers.find(2), readers.end());
    EXPECT_NE(readers.find(3), readers.end());
}

// Test LRU order maintenance
TEST_F(ColumnReaderCacheTest, LRUOrderMaintenance) {
    config::max_segment_partial_column_cache_size = 2;
    setup_column_uid_mapping(1, 0);
    setup_column_uid_mapping(2, 1);

    ColumnMetaPB col_meta1, col_meta2;
    col_meta1.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta1.set_unique_id(1);
    col_meta1.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta1.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    col_meta2.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta2.set_unique_id(2);
    col_meta2.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta2.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta1, col_meta2});

    // Insert column 1
    std::shared_ptr<ColumnReader> reader1;
    Status status = _cache->get_column_reader(1, &reader1, &_stats);
    EXPECT_TRUE(status.ok());

    // Insert column 2
    std::shared_ptr<ColumnReader> reader2;
    status = _cache->get_column_reader(2, &reader2, &_stats);
    EXPECT_TRUE(status.ok());

    // Access column 1 again (should move to front)
    std::shared_ptr<ColumnReader> reader1_again;
    status = _cache->get_column_reader(1, &reader1_again, &_stats);
    EXPECT_TRUE(status.ok());

    // Add column 3 to trigger eviction
    setup_column_uid_mapping(3, 2);
    ColumnMetaPB col_meta3;
    col_meta3.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta3.set_unique_id(3);
    col_meta3.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta3.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta1, col_meta2, col_meta3});

    std::shared_ptr<ColumnReader> reader3;
    status = _cache->get_column_reader(3, &reader3, &_stats);
    EXPECT_TRUE(status.ok());

    // Column 2 should be evicted (least recently used)
    auto readers = _cache->get_available_readers(false);
    EXPECT_EQ(readers.size(), 2);
    EXPECT_NE(readers.find(1), readers.end());
    EXPECT_EQ(readers.find(2), readers.end()); // Should be evicted
    EXPECT_NE(readers.find(3), readers.end());
}

// Test variant column path reading
TEST_F(ColumnReaderCacheTest, VariantColumnPathReading) {
    setup_column_uid_mapping(1, 0);

    // Create variant column meta
    ColumnMetaPB variant_meta;
    variant_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    variant_meta.set_unique_id(1);
    variant_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);

    // Create subcolumn meta
    ColumnMetaPB subcol_meta;
    subcol_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_STRING));
    subcol_meta.set_unique_id(2);
    subcol_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    subcol_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);

    setup_segment_footer({variant_meta, subcol_meta});

    // Test path column reader
    vectorized::PathInData path("field1");
    std::shared_ptr<ColumnReader> path_reader;
    Status status = _cache->get_path_column_reader(1, path, &path_reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(path_reader, nullptr);
}

// Test non-existent column
TEST_F(ColumnReaderCacheTest, NonExistentColumn) {
    // Don't set up any column mapping
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(999, &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

// Test non-existent variant path
TEST_F(ColumnReaderCacheTest, NonExistentVariantPath) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB variant_meta;
    variant_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    variant_meta.set_unique_id(1);
    variant_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({variant_meta});

    vectorized::PathInData non_existent_path("non_existent_field");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(1, non_existent_path, &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(reader, nullptr);
}

// Test concurrent access
TEST_F(ColumnReaderCacheTest, ConcurrentAccess) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta.set_unique_id(1);
    col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    const int num_threads = 10;
    const int num_operations = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count {0};

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, &success_count]() {
            for (int j = 0; j < num_operations; ++j) {
                std::shared_ptr<ColumnReader> reader;
                Status status = _cache->get_column_reader(1, &reader, &_stats);
                if (status.ok() && reader != nullptr) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count.load(), num_threads * num_operations);
}

// Test cache statistics
TEST_F(ColumnReaderCacheTest, CacheStatistics) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta.set_unique_id(1);
    col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    // First access (cache miss)
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(1, &reader, &_stats);
    EXPECT_TRUE(status.ok());

    // Second access (cache hit)
    status = _cache->get_column_reader(1, &reader, &_stats);
    EXPECT_TRUE(status.ok());

    // Verify cache contents
    auto readers = _cache->get_available_readers(false);
    EXPECT_EQ(readers.size(), 1);
}

// Test cache with different column types
TEST_F(ColumnReaderCacheTest, DifferentColumnTypes) {
    std::vector<FieldType> types = {
            FieldType::OLAP_FIELD_TYPE_INT, FieldType::OLAP_FIELD_TYPE_STRING,
            FieldType::OLAP_FIELD_TYPE_DOUBLE, FieldType::OLAP_FIELD_TYPE_BOOL};

    for (size_t i = 0; i < types.size(); ++i) {
        setup_column_uid_mapping(i + 1, 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(types[i]));
        col_meta.set_unique_id(i + 1);
        col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});

        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i + 1, &reader, &_stats);
        EXPECT_TRUE(status.ok());
        EXPECT_NE(reader, nullptr);
    }

    auto readers = _cache->get_available_readers(false);
    EXPECT_EQ(readers.size(), config::max_segment_partial_column_cache_size);
}

// Test cache with node_hint parameter
TEST_F(ColumnReaderCacheTest, NodeHintParameter) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB variant_meta, subcol_meta;
    variant_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    variant_meta.set_unique_id(1);
    variant_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    subcol_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_STRING));
    subcol_meta.set_unique_id(2);
    subcol_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    subcol_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({variant_meta, subcol_meta});

    // Create node hint
    SubcolumnColumnMetaInfo::Node node_hint(SubcolumnColumnMetaInfo::Node::SCALAR);
    node_hint.data.footer_ordinal = 1;
    node_hint.path = vectorized::PathInData("hint_field");

    vectorized::PathInData path("hint_field");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(1, path, &reader, &_stats, &node_hint);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(reader, nullptr);

    // Test cache hit
    status = _cache->get_path_column_reader(1, path, &reader, &_stats, &node_hint);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(reader, nullptr);
}

// Test cache performance under load
TEST_F(ColumnReaderCacheTest, PerformanceUnderLoad) {
    const int num_columns = 100;

    // Set up many columns
    for (int i = 1; i <= num_columns; ++i) {
        setup_column_uid_mapping(i, 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        col_meta.set_unique_id(i);
        col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    // Access all columns
    for (int i = 1; i <= num_columns; ++i) {
        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, &reader, &_stats);
        EXPECT_TRUE(status.ok());
    }

    // Access again (should be cache hits)
    for (int i = 1; i <= num_columns; ++i) {
        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, &reader, &_stats);
        EXPECT_TRUE(status.ok());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Performance should be reasonable (less than 1 second for 100 columns)
    EXPECT_LT(duration.count(), 1000);

    // Verify cache size is limited
    auto readers = _cache->get_available_readers(false);
    EXPECT_LE(readers.size(), config::max_segment_partial_column_cache_size);
}

// Test cache with empty path
TEST_F(ColumnReaderCacheTest, EmptyPath) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    col_meta.set_unique_id(1);
    col_meta.set_encoding(EncodingTypePB::DEFAULT_ENCODING);
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    vectorized::PathInData empty_path("");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(1, empty_path, &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

} // namespace doris::segment_v2