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

#include <barrier>
#include <chrono>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "storage/index/bloom_filter/bloom_filter_index_reader.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_reader.h"
#include "storage/segment/variant/nested_group_reader.h"
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

class HealthCheckingColumnReader : public MockColumnReader {
public:
    HealthCheckingColumnReader() : MockColumnReader(1) {}
    Status healthy_status() const override {
        return check ? check() : ColumnReader::healthy_status();
    }
    std::function<Status()> check;
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

// A first lazy-load failure must not be returned by all subsequent cache hits.
TEST_F(ColumnReaderCacheTest, RebuildAfterOrdinalLoadFailure) {
    setup_column_uid_mapping(1, 0);
    ColumnMetaPB meta;
    meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    meta.set_unique_id(1);
    meta.set_encoding(get_v2_default_encoding(FieldType::OLAP_FIELD_TYPE_INT));
    auto* index = meta.add_indexes();
    index->set_type(ORDINAL_INDEX);
    index->mutable_ordinal_index()->mutable_root_page()->set_is_root_data_page(true);
    setup_segment_footer({meta});

    std::shared_ptr<ColumnReader> failed;
    ASSERT_TRUE(_cache->get_column_reader(1, &failed, &_stats).ok());
    // Seed the retained result of a failed index read, then exercise the actual
    // ColumnReader loading entry point used by point reads and prefetch.
    ASSERT_FALSE(failed->_ordinal_index->_load_once
                         .call([] { return Status::IOError("injected ordinal read failure"); })
                         .ok());
    OrdinalIndexReader* ordinal = nullptr;
    EXPECT_FALSE(failed->get_ordinal_index_reader(ordinal, &_stats).ok());
    EXPECT_FALSE(failed->healthy_status().ok());

    std::shared_ptr<ColumnReader> replacement;
    ASSERT_TRUE(_cache->get_column_reader(1, &replacement, &_stats).ok());
    ASSERT_NE(failed, replacement);
    EXPECT_TRUE(replacement->healthy_status().ok());
    EXPECT_TRUE(replacement->get_ordinal_index_reader(ordinal, &_stats).ok());
    EXPECT_NE(ordinal, nullptr);

    // Late use of an old shared_ptr must not poison or remove its replacement.
    EXPECT_FALSE(failed->get_ordinal_index_reader(ordinal, &_stats).ok());
    std::shared_ptr<ColumnReader> cached;
    ASSERT_TRUE(_cache->get_column_reader(1, &cached, &_stats).ok());
    EXPECT_EQ(cached, replacement);
    EXPECT_EQ(_cache->_lru_list.size(), 1);
    EXPECT_EQ(_cache->_cache_map.size(), 1);
}

TEST_F(ColumnReaderCacheTest, IndexExceptionsInvalidateReader) {
    setup_column_uid_mapping(1, 0);
    ColumnMetaPB meta;
    meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    meta.set_unique_id(1);
    meta.set_encoding(get_v2_default_encoding(FieldType::OLAP_FIELD_TYPE_INT));
    meta.add_indexes()->set_type(ORDINAL_INDEX);
    setup_segment_footer({meta});

    for (bool doris_exception : {true, false}) {
        std::shared_ptr<ColumnReader> failed;
        ASSERT_TRUE(_cache->get_column_reader(1, &failed, &_stats).ok());
        auto throw_failure = [&]() -> Status {
            if (doris_exception) {
                throw doris::Exception(ErrorCode::MEM_LIMIT_EXCEEDED,
                                       "injected allocation failure");
            }
            throw std::bad_alloc();
        };
        EXPECT_ANY_THROW(failed->_ordinal_index->_load_once.call(throw_failure));
        ColumnIteratorOptions options;
        options.stats = &_stats;
        if (doris_exception) {
            EXPECT_THROW(failed->_load_ordinal_index(false, false, options), doris::Exception);
            EXPECT_TRUE(failed->healthy_status().is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        } else {
            EXPECT_THROW(failed->_load_ordinal_index(false, false, options), std::bad_alloc);
            EXPECT_TRUE(failed->healthy_status().is<ErrorCode::INTERNAL_ERROR>());
        }
        EXPECT_TRUE(_cache->get_available_readers(false).empty());
        EXPECT_TRUE(_cache->_lru_list.empty());
        EXPECT_TRUE(_cache->_cache_map.empty());
    }
}

TEST_F(ColumnReaderCacheTest, ZoneMapAndBloomFilterFailuresInvalidateReader) {
    setup_column_uid_mapping(1, 0);
    ColumnMetaPB meta;
    meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    meta.set_unique_id(1);
    meta.set_encoding(get_v2_default_encoding(FieldType::OLAP_FIELD_TYPE_INT));
    meta.add_indexes()->set_type(ORDINAL_INDEX);
    meta.add_indexes()->set_type(ZONE_MAP_INDEX);
    meta.add_indexes()->set_type(BLOOM_FILTER_INDEX);
    setup_segment_footer({meta});
    ColumnIteratorOptions options;
    options.stats = &_stats;

    std::shared_ptr<ColumnReader> zone_reader;
    ASSERT_TRUE(_cache->get_column_reader(1, &zone_reader, &_stats).ok());
    ASSERT_FALSE(zone_reader->_zone_map_index->_load_once
                         .call([] { return Status::IOError("injected zone map failure"); })
                         .ok());
    EXPECT_FALSE(zone_reader->_load_zone_map_index(false, false, options).ok());
    EXPECT_FALSE(zone_reader->healthy_status().ok());

    std::shared_ptr<ColumnReader> bloom_reader;
    ASSERT_TRUE(_cache->get_column_reader(1, &bloom_reader, &_stats).ok());
    EXPECT_NE(zone_reader, bloom_reader);
    ASSERT_FALSE(bloom_reader->_bloom_filter_index->_load_once
                         .call([] {
                             return Status::MemoryLimitExceeded("injected bloom filter failure");
                         })
                         .ok());
    EXPECT_FALSE(bloom_reader->_load_bloom_filter_index(false, false, options).ok());
    EXPECT_FALSE(bloom_reader->healthy_status().ok());
    EXPECT_TRUE(_cache->get_available_readers(false).empty());
}

TEST_F(ColumnReaderCacheTest, ChildFailureInvalidatesCachedParentAndPath) {
    ColumnMetaPB meta;
    meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    for (auto type : {FieldType::OLAP_FIELD_TYPE_INT, FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT}) {
        auto* child = meta.add_children_columns();
        child->set_type(static_cast<int32_t>(type));
        child->set_num_rows(10);
        child->set_encoding(get_v2_default_encoding(type));
        child->add_indexes()->set_type(ORDINAL_INDEX);
    }
    ColumnReaderOptions options;
    std::shared_ptr<ColumnReader> parent;
    ASSERT_TRUE(ColumnReader::create(options, meta, 10, nullptr, &parent).ok());
    auto child = parent->_sub_readers[0];
    auto ancestor = std::make_shared<MockColumnReader>(1);
    ancestor->_sub_readers.push_back(parent);
    // Reusing the same options must not couple independent reader trees.
    std::shared_ptr<ColumnReader> same_options_reader;
    ASSERT_TRUE(ColumnReader::create(options, meta, 10, nullptr, &same_options_reader).ok());
    _cache->_insert_direct({1, {}}, ancestor);
    _cache->_insert_direct({1, PathInData("element")}, child);
    auto independent = std::make_shared<MockColumnReader>(2);
    _cache->_insert_direct({2, {}}, independent);

    ASSERT_FALSE(child->_ordinal_index->_load_once
                         .call([] { return Status::IOError("injected child index failure"); })
                         .ok());
    OrdinalIndexReader* ordinal = nullptr;
    EXPECT_FALSE(child->get_ordinal_index_reader(ordinal, &_stats).ok());
    EXPECT_FALSE(parent->healthy_status().ok());
    EXPECT_FALSE(ancestor->healthy_status().ok());
    EXPECT_TRUE(parent->_healthy_status.status().ok());
    EXPECT_TRUE(ancestor->_healthy_status.status().ok());
    EXPECT_TRUE(parent->_sub_readers[1]->healthy_status().ok());
    EXPECT_TRUE(same_options_reader->healthy_status().ok());
    EXPECT_EQ(_cache->_lookup({1, PathInData("element")}), nullptr);
    EXPECT_EQ(_cache->_lookup({1, {}}), nullptr);
    EXPECT_EQ(_cache->_lookup({2, {}}), independent);
    EXPECT_TRUE(independent->healthy_status().ok());
    EXPECT_EQ(_cache->_lru_list.size(), 1);
}

TEST_F(ColumnReaderCacheTest, ParentFailureDoesNotPoisonChildren) {
    auto parent = std::make_shared<MockColumnReader>(1);
    auto child = std::make_shared<MockColumnReader>(2);
    parent->_sub_readers.push_back(child);
    parent->_healthy_status.update(Status::IOError("parent initialization failed"));
    EXPECT_FALSE(parent->healthy_status().ok());
    EXPECT_TRUE(child->healthy_status().ok());
}

TEST_F(ColumnReaderCacheTest, VariantHealthChecksAllOwnedReaderTrees) {
    auto make_variant = [] {
        auto reader = std::make_shared<VariantColumnReader>();
        reader->_root_column_reader = std::make_shared<MockColumnReader>(1);
        reader->_binary_column_reader = std::make_shared<DummyBinaryColumnReader>();
        reader->_nested_group_readers.emplace("absent", nullptr);
        reader->_nested_group_readers.emplace("invalid", std::make_unique<NestedGroupReader>());
        return reader;
    };
    auto variant = make_variant();
    EXPECT_TRUE(variant->healthy_status().ok());
    variant->_root_column_reader->_healthy_status.update(Status::IOError("root failed"));
    EXPECT_FALSE(variant->healthy_status().ok());
    EXPECT_TRUE(variant->_healthy_status.status().ok());

    for (bool bucketed : {false, true}) {
        variant = make_variant();
        auto binary = std::make_shared<MockColumnReader>(2);
        if (bucketed) {
            variant->_binary_column_reader = std::make_shared<MultipleDocColumnReader>();
        } else {
            variant->_binary_column_reader = std::make_shared<SingleSparseColumnReader>();
        }
        ASSERT_TRUE(variant->_binary_column_reader->add_binary_column_reader(binary, 0).ok());
        EXPECT_TRUE(variant->healthy_status().ok());
        binary->_healthy_status.update(Status::IOError("binary reader failed"));
        EXPECT_FALSE(variant->healthy_status().ok());
        EXPECT_TRUE(variant->_healthy_status.status().ok());
        EXPECT_TRUE(variant->_root_column_reader->healthy_status().ok());
    }

    for (bool offsets_failure : {false, true}) {
        variant = make_variant();
        auto group = std::make_unique<NestedGroupReader>();
        group->offsets_reader = std::make_shared<MockColumnReader>(3);
        auto nested = std::make_unique<NestedGroupReader>();
        auto offsets = std::make_shared<MockColumnReader>(4);
        auto child = std::make_shared<MockColumnReader>(5);
        nested->offsets_reader = offsets;
        nested->child_readers.emplace("value", child);
        nested->child_readers.emplace("absent", nullptr);
        group->nested_group_readers.emplace("nested", std::move(nested));
        variant->_nested_group_readers.emplace("array", std::move(group));
        EXPECT_TRUE(variant->healthy_status().ok());
        (offsets_failure ? offsets : child)
                ->_healthy_status.update(Status::IOError("nested-group descendant failed"));
        EXPECT_FALSE(variant->healthy_status().ok());
        EXPECT_TRUE(variant->_healthy_status.status().ok());
        EXPECT_TRUE(variant->_root_column_reader->healthy_status().ok());
    }
}

TEST_F(ColumnReaderCacheTest, RecursiveChecksReleaseCacheLockAndPreserveReplacement) {
    for (bool enumerate : {false, true}) {
        auto old = std::make_shared<HealthCheckingColumnReader>();
        _cache->_insert_direct({1, {}}, old);
        auto replacement = std::make_shared<MockColumnReader>(1);
        bool replaced = false;
        old->check = [&]() -> Status {
            bool unlocked = false;
            std::thread probe([&] {
                std::unique_lock lock(_cache->_cache_mutex, std::try_to_lock);
                unlocked = lock.owns_lock();
            });
            probe.join();
            EXPECT_TRUE(unlocked);
            if (!unlocked) {
                return Status::IOError("health checked under cache lock");
            }
            // Simulate another caller replacing the old instance while the
            // outer lookup/enumeration is checking its health without the lock.
            if (!std::exchange(replaced, true)) {
                EXPECT_EQ(_cache->_insert_direct({1, {}}, replacement), replacement);
            }
            return Status::IOError("old reader failed");
        };
        if (enumerate) {
            EXPECT_TRUE(_cache->get_available_readers(false).empty());
        } else {
            EXPECT_EQ(_cache->_lookup({1, {}}), nullptr);
        }
        EXPECT_TRUE(replaced);
        EXPECT_EQ(_cache->_lookup({1, {}}), replacement);
        EXPECT_EQ(_cache->_cache_map.size(), 1);
        EXPECT_EQ(_cache->_lru_list.size(), 1);
        // Each iteration starts from an empty cache.
        _cache->_erase_if_same({1, {}}, replacement);
    }
}

TEST_F(ColumnReaderCacheTest, ConcurrentReplacementsKeepOneCacheEntry) {
    auto failed = std::make_shared<MockColumnReader>(1);
    _cache->_insert_direct({1, {}}, failed);
    failed->_healthy_status.update(Status::IOError("injected failure"));
    constexpr int threads_count = 8;
    std::barrier ready(threads_count);
    std::vector<std::shared_ptr<ColumnReader>> readers(threads_count);
    std::vector<std::thread> threads;
    for (int i = 0; i < threads_count; ++i) {
        threads.emplace_back([&, i] {
            EXPECT_EQ(_cache->_lookup({1, {}}), nullptr);
            auto candidate = std::make_shared<MockColumnReader>(1);
            // All callers have missed before any publishes a replacement.
            ready.arrive_and_wait();
            readers[i] = _cache->_insert_direct({1, {}}, candidate);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_NE(readers[0], failed);
    for (const auto& reader : readers) {
        EXPECT_EQ(reader, readers[0]);
    }
    EXPECT_EQ(_cache->_lru_list.size(), 1);
    EXPECT_EQ(_cache->_cache_map.size(), 1);
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
    col_meta1.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta1.type())));
    col_meta1.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    col_meta2.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    col_meta2.set_unique_id(2);
    col_meta2.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta2.type())));
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
    col_meta3.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta3.type())));
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
    variant_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(variant_meta.type())));
    variant_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({variant_meta});

    PathInData non_existent_path("non_existent_field");
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
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
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
        setup_column_uid_mapping(static_cast<int32_t>(i + 1), 0);

        ColumnMetaPB col_meta;
        col_meta.set_type(static_cast<int32_t>(types[i]));
        col_meta.set_unique_id(static_cast<int32_t>(i + 1));
        col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
        col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
        setup_segment_footer({col_meta});

        std::shared_ptr<ColumnReader> reader;
        Status status = _cache->get_column_reader(static_cast<int32_t>(i + 1), &reader, &_stats);
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
    Status status = _cache->get_path_column_reader(1, path, &reader, &_stats, &node_hint);
    EXPECT_TRUE(status.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(reader, nullptr);

    // Test cache hit
    status = _cache->get_path_column_reader(1, path, &reader, &_stats, &node_hint);
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
    col_meta.set_encoding(get_v2_default_encoding(static_cast<FieldType>(col_meta.type())));
    col_meta.mutable_indexes()->Add()->set_type(ORDINAL_INDEX);
    setup_segment_footer({col_meta});

    PathInData empty_path("");
    std::shared_ptr<ColumnReader> reader;
    Status status = _cache->get_path_column_reader(1, empty_path, &reader, &_stats);
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
    Status status = _cache->get_column_reader(col_uid, &reader, &_stats);
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
    Status status = _cache->get_column_reader(col_uid, &reader, &_stats);
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
    Status status = _cache->get_column_reader(col_uid, &reader, &_stats);
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
    Status status = _cache->get_column_reader(col_uid, &reader, &_stats);
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

} // namespace doris::segment_v2
