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
#include "storage/segment/column_reader_cache.h"

#include <crc32c/crc32c.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "storage/iterators.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/coding.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

namespace {

constexpr std::string_view kTestDir = "./ut_dir/column_reader_cache_test";

// Only metadata is consumed by these tests. Unique function names keep the synthetic
// compatibility boundary out of production aggregates and other tests' registrations.
class ReaderVersionTestAggregate final
        : public IAggregateFunctionDataHelper<int, ReaderVersionTestAggregate> {
public:
    ReaderVersionTestAggregate(const std::string& name, const DataTypes& args, FieldType layout)
            : IAggregateFunctionDataHelper(args), _name(name), _layout(layout) {}

    String get_name() const override { return _name; }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeString>(); }
    DataTypePtr get_serialized_type() const override {
        switch (_layout) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY:
            return std::make_shared<DataTypeArray>(argument_types[0]);
        case FieldType::OLAP_FIELD_TYPE_MAP:
            return std::make_shared<DataTypeMap>(argument_types[0], argument_types[1]);
        case FieldType::OLAP_FIELD_TYPE_STRUCT:
            return std::make_shared<DataTypeStruct>(argument_types, Strings(argument_types.size()));
        default:
            return std::make_shared<DataTypeString>();
        }
    }

    void add(AggregateDataPtr, const IColumn**, ssize_t, Arena&) const override {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "metadata-only test aggregate");
    }
    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena&) const override {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "metadata-only test aggregate");
    }
    void serialize(ConstAggregateDataPtr, BufferWritable&) const override {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "metadata-only test aggregate");
    }
    void deserialize(AggregateDataPtr, BufferReadable&, Arena&) const override {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "metadata-only test aggregate");
    }
    void insert_result_into(ConstAggregateDataPtr, IColumn&) const override {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "metadata-only test aggregate");
    }

private:
    std::string _name;
    FieldType _layout;
};

std::string reader_version_test_function_name(FieldType layout) {
    return "column_reader_cache_version_test_" + std::to_string(static_cast<int>(layout));
}

void register_reader_version_test_functions() {
    static std::once_flag once;
    std::call_once(once, [] {
        auto& factory = AggregateFunctionSimpleFactory::instance();
        for (auto layout : {FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_ARRAY,
                            FieldType::OLAP_FIELD_TYPE_MAP, FieldType::OLAP_FIELD_TYPE_STRUCT}) {
            auto name = reader_version_test_function_name(layout);
            AggregateFunctionSimpleFactory::Creator creator =
                    [layout, name](const std::string&, const DataTypes& args, const DataTypePtr&,
                                   bool, const AggregateFunctionAttr&) {
                        return std::make_shared<ReaderVersionTestAggregate>(name, args, layout);
                    };
            factory.register_function_both(name, creator);
            factory.register_function_both(name + BeExecVersionManager::get_function_suffix(16),
                                           creator);
            BeExecVersionManager::registe_old_function_compatibility(16, name);
        }
    });
}

Status append_footer_trailer(io::FileWriter* file_writer, SegmentFooterPB* footer) {
    std::string footer_buf;
    if (!footer->SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize SegmentFooterPB for test");
    }
    faststring fixed_buf;
    put_fixed32_le(&fixed_buf, static_cast<uint32_t>(footer_buf.size()));
    put_fixed32_le(&fixed_buf, crc32c::Crc32c(footer_buf.data(), footer_buf.size()));
    fixed_buf.append(k_segment_magic, k_segment_magic_length);
    std::vector<Slice> slices {Slice(footer_buf), Slice(fixed_buf)};
    return file_writer->appendv(slices.data(), slices.size());
}

} // namespace

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
    // EncodingInfo friended ColumnReaderCacheTest so the fixture can pick a valid V2
    // default encoding for the synthetic ColumnMetaPBs constructed by these tests.
    // TEST_F bodies subclass this fixture and call the helper (friendship doesn't carry
    // to subclasses).
    static segment_v2::EncodingTypePB get_v2_default_encoding(FieldType t) {
        return segment_v2::EncodingInfo::get_v2_default_encoding(t);
    }

    static ColumnMetaPB scalar_meta(FieldType type, int uid = 1) {
        ColumnMetaPB meta;
        meta.set_type(static_cast<int>(type));
        meta.set_unique_id(uid);
        meta.set_num_rows(1000);
        meta.set_encoding(get_v2_default_encoding(type));
        meta.add_indexes()->set_type(ORDINAL_INDEX);
        return meta;
    }

    static ColumnMetaPB agg_state_meta(FieldType layout, int data_version = 16) {
        auto meta = scalar_meta(FieldType::OLAP_FIELD_TYPE_AGG_STATE);
        meta.set_function_name(reader_version_test_function_name(layout));
        meta.set_result_is_nullable(false);
        meta.set_be_exec_version(data_version);
        *meta.add_children_columns() = scalar_meta(FieldType::OLAP_FIELD_TYPE_INT, 2);
        if (layout == FieldType::OLAP_FIELD_TYPE_MAP) {
            *meta.add_children_columns() = scalar_meta(FieldType::OLAP_FIELD_TYPE_INT, 3);
        }
        if (layout == FieldType::OLAP_FIELD_TYPE_ARRAY ||
            layout == FieldType::OLAP_FIELD_TYPE_MAP) {
            *meta.add_children_columns() =
                    scalar_meta(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT, 4);
        }
        return meta;
    }

    void SetUp() override {
        // Set up test configuration
        config::max_segment_partial_column_cache_size = 3;
        auto fs = io::global_local_filesystem();
        static_cast<void>(fs->delete_directory(kTestDir));
        CHECK(fs->create_directory(kTestDir).ok());

        // Create mock segment
        _mock_segment = std::make_unique<StrictMock<MockSegment>>();

        // Set up basic mock expectations before constructing the cache, because
        // ColumnReaderCache's ctor reads _mock_segment->num_rows().
        setup_basic_mocks();

        // Create cache bound to mock segment context.
        // ColumnMetaAccessor will be initialized later in setup_segment_footer()
        // once the test-specific footer has been constructed.
        io::FileReaderSPtr file_reader; // nullptr is fine for inline-only tests
        auto footer_cb = [this](std::shared_ptr<SegmentFooterPB>& footer_pb_shared,
                                OlapReaderStatistics* stats,
                                const io::IOContext* io_ctx) -> Status {
            // Delegate to MockSegment::_get_segment_footer
            return _mock_segment->_get_segment_footer(footer_pb_shared, stats, io_ctx);
        };
        _cache = std::make_unique<ColumnReaderCache>(&_accessor, _mock_segment->tablet_schema(),
                                                     file_reader, _mock_segment->num_rows(),
                                                     footer_cb);
    }

    void TearDown() override {
        _cache.reset();
        _mock_segment.reset();
        static_cast<void>(io::global_local_filesystem()->delete_directory(kTestDir));
    }

    void setup_basic_mocks() {
        // Set up file reader
        io::FileReaderSPtr file_reader;
        EXPECT_CALL(*_mock_segment, file_reader()).WillRepeatedly(Return(file_reader));

        // Set up num rows
        EXPECT_CALL(*_mock_segment, num_rows()).WillRepeatedly(Return(1000));

        // mock _get_segment_footer
        EXPECT_CALL(*_mock_segment, _get_segment_footer(_, _, _))
                .WillRepeatedly(
                        testing::Invoke([this](std::shared_ptr<SegmentFooterPB>& footer_pb_shared,
                                               OlapReaderStatistics*, const io::IOContext*) {
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
        // Initialize ColumnMetaAccessor with the current footer; external meta is not
        // used in these tests so we can pass a null file reader.
        CHECK(_accessor.init(*footer, nullptr).ok());
    }

    void setup_parsed_segment_footer(const std::vector<ColumnMetaPB>& columns) {
        SegmentFooterPB footer;
        for (const auto& col : columns) {
            *footer.add_columns() = col;
        }

        auto fs = io::global_local_filesystem();
        std::string file_path = std::string(kTestDir) + "/parsed_footer_" +
                                std::to_string(_parsed_footer_file_id++) + ".dat";
        io::FileWriterPtr file_writer;
        CHECK(fs->create_file(file_path, &file_writer).ok());
        CHECK(append_footer_trailer(file_writer.get(), &footer).ok());
        CHECK(file_writer->close().ok());

        io::FileReaderSPtr file_reader;
        io::FileReaderOptions opts;
        CHECK(fs->open_file(file_path, &file_reader, &opts).ok());
        _mock_segment->set_file_reader_for_test(file_reader);

        std::shared_ptr<SegmentFooterPB> parsed_footer;
        CHECK(_mock_segment->parse_footer_for_test(parsed_footer, &_stats).ok());
        _mock_segment->set_footer(parsed_footer);
        CHECK(_accessor.init(*parsed_footer, nullptr).ok());
    }

    std::unique_ptr<StrictMock<MockSegment>> _mock_segment;
    ColumnMetaAccessor _accessor;
    std::unique_ptr<ColumnReaderCache> _cache;
    OlapReaderStatistics _stats;
    int _parsed_footer_file_id = 0;
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
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(1, 15, &reader, &_stats);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(reader, nullptr);

    // Verify cache hit
    std::shared_ptr<ColumnReader> cached_reader;
    status = _cache->get_column_reader(1, 17, &cached_reader, &_stats);
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
        col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});

        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, BeExecVersionManager::get_newest_version(),
                                                  &reader, &_stats);
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
    col_meta1.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta1.type())));
    col_meta1.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    col_meta2.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta2.set_unique_id(2);
    col_meta2.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta2.type())));
    col_meta2.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta1, col_meta2});

    // Insert column 1
    std::shared_ptr<ColumnReader> reader1;
    Status status = _cache->get_column_reader(1, BeExecVersionManager::get_newest_version(),
                                              &reader1, &_stats);
    EXPECT_TRUE(status.ok());

    // Insert column 2
    std::shared_ptr<ColumnReader> reader2;
    status = _cache->get_column_reader(2, BeExecVersionManager::get_newest_version(), &reader2,
                                       &_stats);
    EXPECT_TRUE(status.ok());

    // Access column 1 again (should move to front)
    std::shared_ptr<ColumnReader> reader1_again;
    status = _cache->get_column_reader(1, BeExecVersionManager::get_newest_version(),
                                       &reader1_again, &_stats);
    EXPECT_TRUE(status.ok());

    // Add column 3 to trigger eviction
    setup_column_uid_mapping(3, 2);
    ColumnMetaPB col_meta3;
    col_meta3.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta3.set_unique_id(3);
    col_meta3.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta3.type())));
    col_meta3.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta1, col_meta2, col_meta3});

    std::shared_ptr<ColumnReader> reader3;
    status = _cache->get_column_reader(3, BeExecVersionManager::get_newest_version(), &reader3,
                                       &_stats);
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
    variant_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(variant_meta.type())));
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);

    // Create subcolumn meta
    ColumnMetaPB subcol_meta;
    subcol_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_STRING));
    subcol_meta.set_unique_id(2);
    subcol_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(subcol_meta.type())));
    subcol_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);

    setup_segment_footer({variant_meta, subcol_meta});

    // Test path column reader
    PathInData path("field1");
    std::shared_ptr<ColumnReader> path_reader;
    Status status = _cache->get_path_column_reader(
            1, path, BeExecVersionManager::get_newest_version(), &path_reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(path_reader, nullptr);
}

// Test non-existent column
TEST_F(ColumnReaderCacheTest, NonExistentColumn) {
    // Don't set up any column mapping
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(999, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

// Test non-existent variant path
TEST_F(ColumnReaderCacheTest, NonExistentVariantPath) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB variant_meta;
    variant_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    variant_meta.set_unique_id(1);
    variant_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(variant_meta.type())));
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({variant_meta});

    PathInData non_existent_path("non_existent_field");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(
            1, non_existent_path, BeExecVersionManager::get_newest_version(), &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(reader, nullptr);
}

// Test concurrent access
TEST_F(ColumnReaderCacheTest, ConcurrentAccess) {
    setup_column_uid_mapping(1, 0);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta.set_unique_id(1);
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
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
                Status status = _cache->get_column_reader(
                        1, BeExecVersionManager::get_newest_version(), &reader, &_stats);
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
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    // First access (cache miss)
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(1, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    EXPECT_TRUE(status.ok());

    // Second access (cache hit)
    status = _cache->get_column_reader(1, BeExecVersionManager::get_newest_version(), &reader,
                                       &_stats);
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
        setup_column_uid_mapping(static_cast<int32_t>(i + 1), 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(types[i]));
        col_meta.set_unique_id(static_cast<int32_t>(i + 1));
        col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});

        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(static_cast<int32_t>(i + 1),
                                                  BeExecVersionManager::get_newest_version(),
                                                  &reader, &_stats);
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
    variant_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(variant_meta.type())));
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    subcol_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_STRING));
    subcol_meta.set_unique_id(2);
    subcol_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(subcol_meta.type())));
    subcol_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({variant_meta, subcol_meta});

    // Create node hint
    SubcolumnColumnMetaInfo::Node node_hint(SubcolumnColumnMetaInfo::Node::SCALAR);
    node_hint.data.footer_ordinal = 1;
    node_hint.path = PathInData("hint_field");

    PathInData path("hint_field");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(
            1, path, BeExecVersionManager::get_newest_version(), &reader, &_stats, &node_hint);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(reader, nullptr);

    // Test cache hit
    status = _cache->get_path_column_reader(1, path, BeExecVersionManager::get_newest_version(),
                                            &reader, &_stats, &node_hint);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(reader, nullptr);
}

// Test cache performance under load
TEST_F(ColumnReaderCacheTest, PerformanceUnderLoad) {
    const int num_columns = 100;

    // Set up many columns in a single footer so that ColumnMetaAccessor and
    // ColumnReaderCache both see a consistent multi-column layout.
    std::vector<ColumnMetaPB> all_columns;
    for (int i = 1; i <= num_columns; ++i) {
        setup_column_uid_mapping(i, 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        col_meta.set_unique_id(i);
        col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        all_columns.push_back(col_meta);
    }
    setup_segment_footer(all_columns);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Access all columns
    for (int i = 1; i <= num_columns; ++i) {
        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, BeExecVersionManager::get_newest_version(),
                                                  &reader, &_stats);
        EXPECT_TRUE(status.ok());
    }

    // Access again (should be cache hits)
    for (int i = 1; i <= num_columns; ++i) {
        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(i, BeExecVersionManager::get_newest_version(),
                                                  &reader, &_stats);
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
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    PathInData empty_path("");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(
            1, empty_path, BeExecVersionManager::get_newest_version(), &reader, &_stats);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
}

TEST_F(ColumnReaderCacheTest, FillMissingDecimalV2PrecisionFromTabletSchema) {
    constexpr int32_t col_uid = 10;
    setup_column_uid_mapping(col_uid, 0);
    auto& tablet_column = _mock_segment->tablet_schema()->mutable_column_by_uid(col_uid);
    tablet_column.set_type(FieldType::OLAP_FIELD_TYPE_DECIMAL);
    tablet_column.set_precision(20);
    tablet_column.set_frac(6);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL));
    col_meta.set_unique_id(col_uid);
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    ASSERT_FALSE(col_meta.has_precision());
    ASSERT_FALSE(col_meta.has_frac());
    setup_parsed_segment_footer({col_meta});

    const auto& parsed_col_meta = _mock_segment->get_footer()->columns(0);
    EXPECT_EQ(parsed_col_meta.precision(), 20);
    EXPECT_EQ(parsed_col_meta.frac(), 6);

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(col_uid, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(reader, nullptr);

    const auto* data_type =
            assert_cast<const DataTypeDecimalV2*>(reader->get_vec_data_type().get());
    ASSERT_NE(data_type, nullptr);
    EXPECT_EQ(data_type->get_original_precision(), 20);
    EXPECT_EQ(data_type->get_original_scale(), 6);
    EXPECT_EQ(data_type->get_precision(), 27);
    EXPECT_EQ(data_type->get_scale(), 9);
}

TEST_F(ColumnReaderCacheTest, FillMissingDecimalV2PrecisionForComplexTypeFromTabletSchema) {
    constexpr int32_t col_uid = 11;
    setup_column_uid_mapping(col_uid, 0);
    auto& tablet_column = _mock_segment->tablet_schema()->mutable_column_by_uid(col_uid);
    tablet_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn item_column;
    item_column.set_type(FieldType::OLAP_FIELD_TYPE_DECIMAL);
    item_column.set_precision(18);
    item_column.set_frac(4);
    tablet_column.add_sub_column(item_column);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    col_meta.set_unique_id(col_uid);

    auto* item_meta = col_meta.add_children_columns();
    item_meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL));
    item_meta->set_encoding(get_v2_default_encoding(static_cast<FieldType>(item_meta->type())));
    item_meta->set_num_rows(1000);
    item_meta->mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    ASSERT_FALSE(item_meta->has_precision());
    ASSERT_FALSE(item_meta->has_frac());

    auto* offset_meta = col_meta.add_children_columns();
    offset_meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT));
    offset_meta->set_encoding(get_v2_default_encoding(static_cast<FieldType>(offset_meta->type())));
    offset_meta->set_num_rows(1000);
    offset_meta->mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_parsed_segment_footer({col_meta});

    const auto& parsed_item_meta = _mock_segment->get_footer()->columns(0).children_columns(0);
    EXPECT_EQ(parsed_item_meta.precision(), 18);
    EXPECT_EQ(parsed_item_meta.frac(), 4);

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(col_uid, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(reader, nullptr);

    auto data_type = reader->get_vec_data_type();
    ASSERT_NE(data_type, nullptr);
    EXPECT_EQ(data_type->get_primitive_type(), TYPE_ARRAY);
    const auto* array_type = assert_cast<const DataTypeArray*>(data_type.get());
    const auto* nested_type_nullable =
            assert_cast<const DataTypeNullable*>(array_type->get_nested_type().get());
    ASSERT_NE(nested_type_nullable, nullptr);
    const auto* nested_type =
            assert_cast<const DataTypeDecimalV2*>(nested_type_nullable->get_nested_type().get());
    ASSERT_NE(nested_type, nullptr);
    EXPECT_EQ(nested_type->get_original_precision(), 18);
    EXPECT_EQ(nested_type->get_original_scale(), 4);
    EXPECT_EQ(nested_type->get_precision(), 27);
    EXPECT_EQ(nested_type->get_scale(), 9);
}
TEST_F(ColumnReaderCacheTest, FillMissingDecimalV3PrecisionFromTabletSchema) {
    constexpr int32_t col_uid = 10;
    setup_column_uid_mapping(col_uid, 0);
    auto& tablet_column = _mock_segment->tablet_schema()->mutable_column_by_uid(col_uid);
    tablet_column.set_type(FieldType::OLAP_FIELD_TYPE_DECIMAL128I);
    tablet_column.set_precision(20);
    tablet_column.set_frac(6);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL128I));
    col_meta.set_unique_id(col_uid);
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    ASSERT_FALSE(col_meta.has_precision());
    ASSERT_FALSE(col_meta.has_frac());
    setup_parsed_segment_footer({col_meta});

    const auto& parsed_col_meta = _mock_segment->get_footer()->columns(0);
    EXPECT_EQ(parsed_col_meta.precision(), 20);
    EXPECT_EQ(parsed_col_meta.frac(), 6);

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(col_uid, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(reader, nullptr);

    auto data_type = reader->get_vec_data_type();
    ASSERT_NE(data_type, nullptr);
    EXPECT_EQ(data_type->get_primitive_type(), TYPE_DECIMAL128I);
    EXPECT_EQ(data_type->get_precision(), 20);
    EXPECT_EQ(data_type->get_scale(), 6);
}

TEST_F(ColumnReaderCacheTest, FillMissingDecimalV3PrecisionForComplexTypeFromTabletSchema) {
    constexpr int32_t col_uid = 11;
    setup_column_uid_mapping(col_uid, 0);
    auto& tablet_column = _mock_segment->tablet_schema()->mutable_column_by_uid(col_uid);
    tablet_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn item_column;
    item_column.set_type(FieldType::OLAP_FIELD_TYPE_DECIMAL64);
    item_column.set_precision(18);
    item_column.set_frac(4);
    tablet_column.add_sub_column(item_column);

    ColumnMetaPB col_meta;
    col_meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    col_meta.set_unique_id(col_uid);

    auto* item_meta = col_meta.add_children_columns();
    item_meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    item_meta->set_encoding(get_v2_default_encoding(static_cast<FieldType>(item_meta->type())));
    item_meta->set_num_rows(1000);
    item_meta->mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    ASSERT_FALSE(item_meta->has_precision());
    ASSERT_FALSE(item_meta->has_frac());

    auto* offset_meta = col_meta.add_children_columns();
    offset_meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT));
    offset_meta->set_encoding(get_v2_default_encoding(static_cast<FieldType>(offset_meta->type())));
    offset_meta->set_num_rows(1000);
    offset_meta->mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_parsed_segment_footer({col_meta});

    const auto& parsed_item_meta = _mock_segment->get_footer()->columns(0).children_columns(0);
    EXPECT_EQ(parsed_item_meta.precision(), 18);
    EXPECT_EQ(parsed_item_meta.frac(), 4);

    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_column_reader(col_uid, BeExecVersionManager::get_newest_version(),
                                              &reader, &_stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(reader, nullptr);

    auto data_type = reader->get_vec_data_type();
    ASSERT_NE(data_type, nullptr);
    EXPECT_EQ(data_type->get_primitive_type(), TYPE_ARRAY);
    const auto* array_type = assert_cast<const DataTypeArray*>(data_type.get());
    auto nested_type = array_type->get_nested_type();
    ASSERT_NE(nested_type, nullptr);
    EXPECT_EQ(nested_type->get_primitive_type(), TYPE_DECIMAL64);
    EXPECT_EQ(nested_type->get_precision(), 18);
    EXPECT_EQ(nested_type->get_scale(), 4);
}

TEST_F(ColumnReaderCacheTest, OperationExecutionVersion) {
    StorageReadOptions options(_stats);
    EXPECT_EQ(options.get_be_exec_version(), BeExecVersionManager::get_newest_version());
    RuntimeState state;
    state.set_be_exec_version(7);
    options.runtime_state = &state;
    EXPECT_EQ(options.get_be_exec_version(), 7);
    state.set_be_exec_version(16);
    EXPECT_EQ(options.get_be_exec_version(), 16);
}

TEST_F(ColumnReaderCacheTest, RestrictedAggregateMissAndHit) {
    // stddev_samp's real restriction is version 8; the on-disk version is valid.
    static_cast<void>(AggregateFunctionSimpleFactory::instance());
    auto meta = scalar_meta(FieldType::OLAP_FIELD_TYPE_AGG_STATE);
    meta.set_function_name("stddev_samp");
    meta.set_result_is_nullable(true);
    meta.set_be_exec_version(16);
    *meta.add_children_columns() = scalar_meta(FieldType::OLAP_FIELD_TYPE_DOUBLE, 2);
    setup_segment_footer({meta});
    std::shared_ptr<ColumnReader> reader;
    auto st = _cache->get_column_reader(1, 7, &reader, &_stats);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("input_be_exec_version=7"), std::string::npos);
    EXPECT_TRUE(_cache->get_available_readers(false).empty());
    ASSERT_TRUE(_cache->get_column_reader(1, 16, &reader, &_stats).ok());
    std::shared_ptr<ColumnReader> rejected;
    st = _cache->get_column_reader(1, 7, &rejected, &_stats);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(rejected, nullptr);
    EXPECT_NE(st.to_string().find("input_be_exec_version=7"), std::string::npos);
    std::shared_ptr<ColumnReader> cached;
    ASSERT_TRUE(_cache->get_column_reader(1, 17, &cached, &_stats).ok());
    EXPECT_EQ(cached, reader);
}

TEST_F(ColumnReaderCacheTest, DiskVersionAndComplexSerializationSurviveCacheReuse) {
    register_reader_version_test_functions();
    for (auto layout : {FieldType::OLAP_FIELD_TYPE_STRING, FieldType::OLAP_FIELD_TYPE_ARRAY,
                        FieldType::OLAP_FIELD_TYPE_MAP, FieldType::OLAP_FIELD_TYPE_STRUCT}) {
        SCOPED_TRACE(static_cast<int>(layout));
        auto meta = agg_state_meta(layout);
        const int uid = static_cast<int>(layout);
        meta.set_unique_id(uid);
        setup_segment_footer({meta});
        std::shared_ptr<ColumnReader> reader;
        auto st = _cache->get_column_reader(uid, 17, &reader, &_stats);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find("data_be_exec_version=16"), std::string::npos);
        EXPECT_EQ(_cache->get_available_readers(false).count(uid), 0);
        ASSERT_TRUE(_cache->get_column_reader(uid, 15, &reader, &_stats).ok());
        ASSERT_NE(reader, nullptr);
        if (layout != FieldType::OLAP_FIELD_TYPE_STRING) {
            EXPECT_EQ(reader->get_meta_type(), layout);
        }
        std::shared_ptr<ColumnReader> cached;
        ASSERT_TRUE(_cache->get_column_reader(uid, 16, &cached, &_stats).ok());
        EXPECT_EQ(reader, cached);
        EXPECT_FALSE(_cache->get_column_reader(uid, 17, &cached, &_stats).ok());
        EXPECT_EQ(cached, reader);
        ASSERT_TRUE(_cache->get_column_reader(uid, 15, &cached, &_stats).ok());
        EXPECT_EQ(reader, cached);
        auto logical_type = assert_cast<const DataTypeAggState*>(reader->get_vec_data_type().get());
        EXPECT_NO_THROW(logical_type->check_function_compatibility(16));
        EXPECT_THROW(logical_type->check_function_compatibility(17), Exception);
    }
}

TEST_F(ColumnReaderCacheTest, NestedAggregateConstraintOnParentCacheHit) {
    register_reader_version_test_functions();
    for (auto parent_type : {FieldType::OLAP_FIELD_TYPE_ARRAY, FieldType::OLAP_FIELD_TYPE_MAP,
                             FieldType::OLAP_FIELD_TYPE_STRUCT}) {
        SCOPED_TRACE(static_cast<int>(parent_type));
        auto parent = scalar_meta(parent_type);
        if (parent_type == FieldType::OLAP_FIELD_TYPE_MAP) {
            *parent.add_children_columns() = scalar_meta(FieldType::OLAP_FIELD_TYPE_INT, 2);
        }
        *parent.add_children_columns() = agg_state_meta(FieldType::OLAP_FIELD_TYPE_STRING);
        if (parent_type != FieldType::OLAP_FIELD_TYPE_STRUCT) {
            *parent.add_children_columns() =
                    scalar_meta(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT);
        }
        const int uid = static_cast<int>(parent_type);
        parent.set_unique_id(uid);
        setup_segment_footer({parent});
        std::shared_ptr<ColumnReader> reader;
        ASSERT_TRUE(_cache->get_column_reader(uid, 16, &reader, &_stats).ok());
        std::shared_ptr<ColumnReader> cached;
        ASSERT_TRUE(_cache->get_column_reader(uid, 15, &cached, &_stats).ok());
        EXPECT_EQ(reader, cached);
        EXPECT_FALSE(_cache->get_column_reader(uid, 17, &cached, &_stats).ok());
        EXPECT_TRUE(_cache->get_column_reader(uid, 16, &cached, &_stats).ok());
    }
}

TEST_F(ColumnReaderCacheTest, PathCacheHitsCheckCurrentVersion) {
    register_reader_version_test_functions();
    ColumnReaderOptions opts;
    opts.be_exec_version = 16;
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(ColumnReader::create(opts, agg_state_meta(FieldType::OLAP_FIELD_TYPE_STRING), 1000,
                                     nullptr, &reader)
                        .ok());
    // Isolate the path-hit contract without requiring VARIANT to support AggState leaves.
    PathInData path("state");
    _cache->_insert_direct({1, path}, reader);
    SubcolumnColumnMetaInfo::Node hint(SubcolumnColumnMetaInfo::Node::SCALAR);
    std::shared_ptr<ColumnReader> cached;
    ASSERT_TRUE(_cache->get_path_column_reader(1, path, 15, &cached, &_stats, &hint).ok());
    EXPECT_EQ(cached, reader);
    cached.reset();
    EXPECT_FALSE(_cache->get_path_column_reader(1, path, 17, &cached, &_stats, &hint).ok());
    EXPECT_EQ(cached, nullptr);
    ASSERT_TRUE(_cache->get_path_column_reader(1, path, 16, &cached, &_stats).ok());
    EXPECT_EQ(cached, reader);

    _cache->_insert_direct({1, {}}, reader);
    cached.reset();
    EXPECT_FALSE(_cache->get_path_column_reader(1, {}, 17, &cached, &_stats).ok());
    EXPECT_EQ(cached, nullptr);
    ASSERT_TRUE(_cache->get_path_column_reader(1, {}, 16, &cached, &_stats).ok());
    EXPECT_EQ(cached, reader);
}

TEST_F(ColumnReaderCacheTest, ReaderWithoutModernAggregateConstraint) {
    ColumnReader reader;
    // Legacy metadata has no function details, so its default AggState type skips
    // compatibility checks without requiring a reader-side marker.
    reader._data_type = std::make_shared<DataTypeAggState>();
    EXPECT_TRUE(reader.check_read_compatibility(7).ok());
    EXPECT_TRUE(reader.check_read_compatibility(17).ok());
}

TEST_F(ColumnReaderCacheTest, ConcurrentRequestVersionsDoNotMutateReader) {
    register_reader_version_test_functions();
    setup_segment_footer({agg_state_meta(FieldType::OLAP_FIELD_TYPE_STRING)});
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(_cache->get_column_reader(1, 16, &reader, &_stats).ok());
    std::barrier start(2);
    std::array<Status, 2> statuses;
    std::array<std::shared_ptr<ColumnReader>, 2> results;
    auto fetch = [&](size_t index, int version) {
        OlapReaderStatistics stats;
        start.arrive_and_wait();
        statuses[index] = _cache->get_column_reader(1, version, &results[index], &stats);
    };
    std::thread compatible(fetch, 0, 15);
    std::thread incompatible(fetch, 1, 17);
    compatible.join();
    incompatible.join();
    EXPECT_TRUE(statuses[0].ok());
    EXPECT_FALSE(statuses[1].ok());
    EXPECT_EQ(results[0], reader);
    EXPECT_EQ(results[1], nullptr);
    EXPECT_TRUE(reader->check_read_compatibility(16).ok());
}

} // namespace doris::segment_v2
