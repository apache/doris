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

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"

namespace doris::segment_v2 {

class SegmentCorruptionTest : public testing::Test {
public:
    static constexpr const char* kTestDir = "./ut_dir/segment_corruption_test";
    static constexpr const char* kCacheDir = "./ut_dir/segment_corruption_test_cache";
    static constexpr const std::string_view tmp_dir = "./ut_dir/segment_corruption_test/tmp";

    static void SetUpTestSuite() {
        // Initialize FileCacheFactory if not already done
        if (ExecEnv::GetInstance()->file_cache_factory() == nullptr) {
            _suite_factory = std::make_unique<io::FileCacheFactory>();
            ExecEnv::GetInstance()->_file_cache_factory = _suite_factory.get();
        }
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // Initialize InvertedIndexSearcherCache
        if (ExecEnv::GetInstance()->get_inverted_index_searcher_cache() == nullptr) {
            int64_t inverted_index_cache_limit = 1024 * 1024 * 1024;
            _inverted_index_searcher_cache = std::unique_ptr<InvertedIndexSearcherCache>(
                    InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit,
                                                                       1));
            ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                    _inverted_index_searcher_cache.get());
        }
    }

    static void TearDownTestSuite() {
        // Disable sync point processing before clearing caches to prevent
        // background threads from accessing SyncPoint during/after destruction
        SyncPoint::get_instance()->disable_processing();

        auto* factory = io::FileCacheFactory::instance();
        if (factory != nullptr) {
            factory->clear_file_caches(true);

            factory->_caches.clear();
            factory->_path_to_cache.clear();
        }

        // Give background threads time to stop after cache destruction
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        _suite_factory.reset(nullptr);
        _inverted_index_searcher_cache.reset(nullptr);
    }

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        st = io::global_local_filesystem()->delete_directory(kCacheDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        st = io::global_local_filesystem()->create_directory(kCacheDir);
        ASSERT_TRUE(st.ok()) << st;

        doris::EngineOptions options;
        _engine = std::make_unique<StorageEngine>(options);
        _data_dir = std::make_unique<DataDir>(*_engine, kTestDir);
        ASSERT_TRUE(_data_dir->update_capacity().ok());

        // Setup file cache
        io::FileCacheSettings settings;
        settings.storage = "memory";
        settings.capacity = 1024 * 1024;          // 1MB
        settings.max_file_block_size = 64 * 1024; // 64KB
        auto cache = std::make_unique<io::BlockFileCache>(kCacheDir, settings);
        ASSERT_TRUE(cache->initialize());
        for (int i = 0; i < 1000; ++i) {
            if (cache->get_async_open_success()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto* raw = cache.get();
        auto* factory = io::FileCacheFactory::instance();
        factory->_caches.emplace_back(std::move(cache));
        factory->_path_to_cache[kCacheDir] = raw;
    }

    void TearDown() override {
        // Disable sync point processing before clearing caches to prevent
        // background threads from accessing SyncPoint during/after destruction
        SyncPoint::get_instance()->disable_processing();

        auto* factory = io::FileCacheFactory::instance();
        if (factory != nullptr) {
            factory->clear_file_caches(true);

            factory->_caches.clear();
            factory->_path_to_cache.clear();
        }

        // Give background threads time to stop after cache destruction
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->delete_directory(kCacheDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
    }

    // Create schema with inverted index
    TabletSchemaSPtr create_schema_with_inverted_index() {
        TabletSchemaSPtr schema = std::make_shared<TabletSchema>();

        // Add INT key column
        TabletColumn column_1;
        column_1.set_name("c1");
        column_1.set_unique_id(0);
        column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_1.set_length(4);
        column_1.set_index_length(4);
        column_1.set_is_key(true);
        column_1.set_is_nullable(false);
        schema->append_column(column_1);

        // Add VARCHAR column with inverted index
        TabletColumn column_2;
        column_2.set_name("c2");
        column_2.set_unique_id(1);
        column_2.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        column_2.set_length(65535);
        column_2.set_is_key(false);
        column_2.set_is_nullable(false);
        schema->append_column(column_2);

        // Add inverted index on c2
        TabletIndex index;
        index._index_id = 1;
        index._index_name = "idx_c2";
        index._index_type = IndexType::INVERTED;
        index._col_unique_ids.push_back(1);
        schema->append_index(std::move(index));

        schema->_keys_type = DUP_KEYS;
        schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        return schema;
    }

    std::string create_segment_with_inverted_index(TabletSchemaSPtr schema, uint32_t segment_id,
                                                   const RowsetId& rowset_id) {
        std::string filename = fmt::format("{}_{}.dat", rowset_id.to_string(), segment_id);
        std::string seg_path = fmt::format("{}/{}", kTestDir, filename);
        auto fs = io::global_local_filesystem();

        // Create segment file
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(seg_path, &file_writer);
        EXPECT_TRUE(st.ok()) << st.to_string();

        // Create inverted index file (V2 format)
        std::string index_path_prefix {
                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)};
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr idx_file_writer;
        st = fs->create_file(index_path, &idx_file_writer);
        EXPECT_TRUE(st.ok()) << st.to_string();

        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, index_path_prefix, rowset_id.to_string(), segment_id,
                InvertedIndexStorageFormatPB::V2, std::move(idx_file_writer));

        SegmentWriterOptions opts;
        SegmentWriter writer(file_writer.get(), segment_id, schema, nullptr, nullptr, opts,
                             index_file_writer.get());
        st = writer.init();
        EXPECT_TRUE(st.ok());

        // Write rows
        RowCursor row;
        auto olap_st = row._init(schema, schema->num_columns());
        EXPECT_EQ(Status::OK(), olap_st);

        // Write one row: (1, "hello")
        {
            RowCursorCell cell0 = row.cell(0);
            *(int32_t*)cell0.mutable_cell_ptr() = 1;
            cell0.set_not_null();

            RowCursorCell cell1 = row.cell(1);
            Slice value("hello");
            reinterpret_cast<Slice*>(cell1.mutable_cell_ptr())->data = value.data;
            reinterpret_cast<Slice*>(cell1.mutable_cell_ptr())->size = value.size;
            cell1.set_not_null();

            st = writer.append_row(row);
            EXPECT_TRUE(st.ok());
        }

        uint64_t file_size, index_size;
        st = writer.finalize(&file_size, &index_size);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(file_writer->close().ok());

        // Close the index file writer (it was already written by SegmentWriter during finalize)
        st = index_file_writer->begin_close();
        EXPECT_TRUE(st.ok()) << st;
        st = index_file_writer->finish_close();
        EXPECT_TRUE(st.ok()) << st;

        return seg_path;
    }

protected:
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<DataDir> _data_dir;

    inline static std::unique_ptr<io::FileCacheFactory> _suite_factory;
    inline static std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
};

// Test that _fs is correctly set when opening segment through CORRUPTION retry path.
// This test:
// 1. Creates a segment with inverted index
// 2. Injects CORRUPTION error on first open attempt
// 3. Opens segment successfully after retry
// 4. Tries to use inverted index (which requires _fs)
// If _fs is nullptr, the inverted index operation will crash.
TEST_F(SegmentCorruptionTest, TestFsSetInCorruptionRetryPath) {
    auto schema = create_schema_with_inverted_index();
    RowsetId rowset_id;
    rowset_id.init(1);

    auto path = create_segment_with_inverted_index(schema, 0, rowset_id);
    auto fs = io::global_local_filesystem();

    // Enable sync point for testing - inject CORRUPTION error on first open attempt
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();

    int corruption_count = 0;
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "Segment::open:corruption",
            [&corruption_count](auto&& args) {
                // Only trigger corruption on first attempt to simulate file cache corruption
                if (corruption_count == 0) {
                    auto* st = try_any_cast<Status*>(args[0]);
                    *st = Status::Corruption<false>("test corruption injection");
                    corruption_count++;
                }
            },
            &guard);

    std::shared_ptr<Segment> segment;
    // Use FILE_BLOCK_CACHE to trigger the corruption retry path
    io::FileReaderOptions reader_options;
    reader_options.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;

    auto st = Segment::open(fs, path, /*tablet_id=*/100, /*segment_id=*/0, rowset_id, schema,
                            reader_options, &segment);

    sp->disable_processing();

    // Verify that corruption was injected and retry happened
    ASSERT_EQ(corruption_count, 1) << "Corruption should have been injected once";

    // The segment should open successfully after retry
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(segment, nullptr);

    // Now try to use inverted index - this will trigger _open_index_file_reader()
    // which uses _fs. If _fs is nullptr, this will crash.
    auto indexes = schema->inverted_indexs(schema->column(1));
    ASSERT_FALSE(indexes.empty());
    const TabletIndex* idx_meta = indexes[0];

    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    std::unique_ptr<IndexIterator> iter;
    // This call triggers _open_index_file_reader() -> uses _fs
    // If _fs is nullptr (bug not fixed), this will crash
    st = segment->new_index_iterator(schema->column(1), idx_meta, read_options, &iter);
    st = segment->_index_file_reader->init(config::inverted_index_read_buffer_size,
                                           &read_options.io_ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // The call may fail due to missing index data in this simple test,
    // but the key point is it should NOT crash due to nullptr _fs
    // If we reach here, _fs was correctly set
}

// Test normal segment open path
TEST_F(SegmentCorruptionTest, TestFsSetInNormalPath) {
    auto schema = create_schema_with_inverted_index();
    RowsetId rowset_id;
    rowset_id.init(2);

    auto path = create_segment_with_inverted_index(schema, 0, rowset_id);
    auto fs = io::global_local_filesystem();

    std::shared_ptr<Segment> segment;
    io::FileReaderOptions reader_options;

    auto st = Segment::open(fs, path, /*tablet_id=*/100, /*segment_id=*/0, rowset_id, schema,
                            reader_options, &segment);

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(segment, nullptr);

    // Use inverted index to verify _fs is set
    auto indexes = schema->inverted_indexs(schema->column(1));
    ASSERT_FALSE(indexes.empty());
    const TabletIndex* idx_meta = indexes[0];

    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    std::unique_ptr<IndexIterator> iter;
    st = segment->new_index_iterator(schema->column(1), idx_meta, read_options, &iter);
    // If we reach here without crash, _fs was correctly set
}

} // namespace doris::segment_v2
