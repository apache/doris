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

#include <CLucene.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <string>

#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"

namespace doris::segment_v2 {

class MockTabletIndex : public TabletIndex {
public:
    MockTabletIndex(int64_t index_id, std::string index_suffix) {
        _index_id = index_id;
        _escaped_index_suffix_path = std::move(index_suffix);
        _index_type = IndexType::INVERTED;
        _index_name = "test_index";
        _col_unique_ids.push_back(index_id); // Add some dummy column uid
    }

    int64_t index_id() const { return _index_id; }
    const std::string& get_index_suffix() const { return _escaped_index_suffix_path; }
};

class InvertedIndexFileReaderTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_file_reader_test";
    constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        std::string current_dir = std::string(buffer);
        std::string absolute_dir = current_dir + "/" + kTestDir;

        auto st = io::global_local_filesystem()->delete_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;

        // Initialize tmp dir for ExecEnv
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), -1);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // Initialize InvertedIndexSearcherCache
        int64_t inverted_index_cache_limit = 1024 * 1024 * 1024;
        _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                _inverted_index_searcher_cache.get());

        // Initialize StorageEngine and DataDir
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, absolute_dir);
        ASSERT_TRUE(_data_dir->update_capacity().ok());
        std::unique_ptr<BaseStorageEngine> base_engine(engine.release());
        ExecEnv::GetInstance()->set_storage_engine(std::move(base_engine));
    }

    void TearDown() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void create_empty_index_file(const std::string& file_path) {
        std::filesystem::path parent_path = std::filesystem::path(file_path).parent_path();
        if (!std::filesystem::exists(parent_path)) {
            std::filesystem::create_directories(parent_path);
        }

        DorisFSDirectory* dir_ptr = DorisFSDirectoryFactory::getDirectory(
                io::global_local_filesystem(), parent_path.string().c_str());
        ASSERT_NE(dir_ptr, nullptr);
        std::unique_ptr<lucene::store::Directory, DirectoryDeleter> directory(dir_ptr);

        lucene::store::IndexOutput* output = directory->createOutput(
                std::filesystem::path(file_path).filename().string().c_str());
        ASSERT_NE(output, nullptr);
        std::unique_ptr<lucene::store::IndexOutput> output_guard(output);

        output_guard->close();
        directory->close();
    }

    void create_invalid_version_file(const std::string& file_path, int32_t invalid_version) {
        std::filesystem::path parent_path = std::filesystem::path(file_path).parent_path();
        if (!std::filesystem::exists(parent_path)) {
            std::filesystem::create_directories(parent_path);
        }

        DorisFSDirectory* dir_ptr = DorisFSDirectoryFactory::getDirectory(
                io::global_local_filesystem(), parent_path.string().c_str());
        ASSERT_NE(dir_ptr, nullptr);
        std::unique_ptr<lucene::store::Directory, DirectoryDeleter> directory(dir_ptr);

        lucene::store::IndexOutput* output = directory->createOutput(
                std::filesystem::path(file_path).filename().string().c_str());
        ASSERT_NE(output, nullptr);
        std::unique_ptr<lucene::store::IndexOutput> output_guard(output);

        output->writeInt(invalid_version);
        output_guard->close();
        directory->close();
    }

    void create_v2_file_with_null_bitmap(const std::string& file_path, int64_t index_id,
                                         const std::string& index_suffix, bool has_null_bitmap) {
        std::filesystem::path parent_path = std::filesystem::path(file_path).parent_path();
        if (!std::filesystem::exists(parent_path)) {
            std::filesystem::create_directories(parent_path);
        }

        // Create a mock TabletIndex
        MockTabletIndex tablet_index(index_id, index_suffix);

        // Get the index path prefix from the file path
        std::string index_path_prefix = file_path;
        size_t idx_pos = index_path_prefix.find(".idx");
        if (idx_pos != std::string::npos) {
            index_path_prefix = index_path_prefix.substr(0, idx_pos);
        }

        // Create file writer for V2 format
        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        Status st = io::global_local_filesystem()->create_file(file_path, &file_writer, &opts);
        ASSERT_TRUE(st.ok()) << st.msg();

        // Create IndexFileWriter
        auto writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), index_path_prefix, "test_rowset", 0,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Open the writer and get directory
        auto open_result = writer->open(&tablet_index);
        ASSERT_TRUE(open_result.has_value()) << open_result.error().msg();
        auto dir = open_result.value();

        // Create test index files
        auto test_output =
                std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_index_file"));
        test_output->writeString("test_data");
        test_output->close();

        // Create null bitmap file if needed
        if (has_null_bitmap) {
            std::string null_bitmap_file =
                    std::string(InvertedIndexDescriptor::get_temporary_null_bitmap_file_name());
            auto null_output = std::unique_ptr<lucene::store::IndexOutput>(
                    dir->createOutput(null_bitmap_file.c_str()));
            // Write enough data to indicate has null (> 5 bytes)
            null_output->writeBytes(reinterpret_cast<const uint8_t*>("null_bitmap_data"), 16);
            null_output->close();
        } else {
            // Create a small null bitmap file to indicate no null
            std::string null_bitmap_file =
                    std::string(InvertedIndexDescriptor::get_temporary_null_bitmap_file_name());
            auto null_output = std::unique_ptr<lucene::store::IndexOutput>(
                    dir->createOutput(null_bitmap_file.c_str()));
            // Write small data to indicate no null (<= 5 bytes)
            null_output->writeBytes(reinterpret_cast<const uint8_t*>("123"), 3);
            null_output->close();
        }

        dir->close();

        // Write and close the file - only call close(), not write()
        st = writer->close();
        ASSERT_TRUE(st.ok()) << st.msg();
    }

    void create_v2_file_with_small_null_bitmap(const std::string& file_path, int64_t index_id,
                                               const std::string& index_suffix) {
        create_v2_file_with_null_bitmap(file_path, index_id, index_suffix, false);
    }

private:
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    constexpr static uint32_t MAX_PATH_LEN = 1024;
};

// Test case for CL_ERR_EmptyIndexSegment error handling
TEST_F(InvertedIndexFileReaderTest, TestEmptyIndexSegmentError) {
    std::string index_path = kTestDir + "/empty_index_file";
    create_empty_index_file(index_path + ".idx");

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status status = reader.init(4096);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_BYPASS);
}

// Test case for unknown inverted index format error
TEST_F(InvertedIndexFileReaderTest, TestUnknownIndexFormatError) {
    std::string index_path = kTestDir + "/invalid_version_index_file";
    create_invalid_version_file(index_path + ".idx",
                                1); // Version < V2 to trigger unknown format error

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status status = reader.init(4096);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    // The error message could be either "unknown inverted index format" or CLucene error
    // since reading invalid data may trigger CLucene exceptions
    EXPECT_TRUE(status.msg().find("unknown inverted index format") != std::string::npos ||
                status.msg().find("CLuceneError") != std::string::npos);
}

// Test case for V1 format file not found error
TEST_F(InvertedIndexFileReaderTest, TestV1FileNotFoundError) {
    std::string index_path = kTestDir + "/non_existent";

    InvertedIndexFileInfo file_info;
    auto* index_info = file_info.add_index_info();
    index_info->set_index_id(1);
    index_info->set_index_suffix("test");
    index_info->set_index_file_size(100);

    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V1, file_info);

    MockTabletIndex tablet_index(1, "test");
    auto result = reader.open(&tablet_index);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

// Test case for V2 format stream is nullptr error
TEST_F(InvertedIndexFileReaderTest, TestV2StreamNullptrError) {
    std::string index_path = kTestDir + "/null_stream";

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    MockTabletIndex tablet_index(1, "test");
    auto result = reader.open(&tablet_index);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(result.error().msg().find("stream is nullptr") != std::string::npos);
}

// Test case for index not found in V2 format
TEST_F(InvertedIndexFileReaderTest, TestV2IndexNotFoundError) {
    std::string index_path = kTestDir + "/v2_no_index";
    create_v2_file_with_null_bitmap(index_path + ".idx", 100, "other", false);

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status status = reader.init(4096);
    EXPECT_TRUE(status.ok());

    MockTabletIndex tablet_index(1, "test");
    auto result = reader.open(&tablet_index);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(result.error().msg().find("No index with id") != std::string::npos);
}

// Test case for index_file_exist method error handling
TEST_F(InvertedIndexFileReaderTest, TestIndexFileExistV2StreamNullptr) {
    std::string index_path = kTestDir + "/exist_test";

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    MockTabletIndex tablet_index(1, "test");
    bool res = true;
    Status status = reader.index_file_exist(&tablet_index, &res);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(res);
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

// Test case for has_null method with V1 format
TEST_F(InvertedIndexFileReaderTest, TestHasNullV1Format) {
    std::string index_path = kTestDir + "/has_null_v1";

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V1, file_info);

    MockTabletIndex tablet_index(1, "test");
    bool res = false;
    Status status = reader.has_null(&tablet_index, &res);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(res); // V1 always returns true
}

// Test case for has_null method with V2 format and null bitmap
TEST_F(InvertedIndexFileReaderTest, TestHasNullV2WithNullBitmap) {
    std::string index_path = kTestDir + "/has_null_v2";
    create_v2_file_with_null_bitmap(index_path + ".idx", 1, "test", true);

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status init_status = reader.init(4096);
    EXPECT_TRUE(init_status.ok());

    MockTabletIndex tablet_index(1, "test");
    bool res = false;
    Status status = reader.has_null(&tablet_index, &res);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(res); // Should have null bitmap
}

// Test case for has_null method with V2 format and small null bitmap
TEST_F(InvertedIndexFileReaderTest, TestHasNullV2WithSmallNullBitmap) {
    std::string index_path = kTestDir + "/has_null_v2_small";
    create_v2_file_with_small_null_bitmap(index_path + ".idx", 1, "test");

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status init_status = reader.init(4096);
    EXPECT_TRUE(init_status.ok());

    MockTabletIndex tablet_index(1, "test");
    bool res = true;
    Status status = reader.has_null(&tablet_index, &res);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(res); // Small bitmap should return false
}

// Test case for has_null method with V2 format stream nullptr
TEST_F(InvertedIndexFileReaderTest, TestHasNullV2StreamNullptr) {
    std::string index_path = kTestDir + "/has_null_stream_null";

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    MockTabletIndex tablet_index(1, "test");
    bool res = true;
    Status status = reader.has_null(&tablet_index, &res);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

// Test case for has_null method with index not found
TEST_F(InvertedIndexFileReaderTest, TestHasNullV2IndexNotFound) {
    std::string index_path = kTestDir + "/has_null_not_found";
    create_v2_file_with_null_bitmap(index_path + ".idx", 100, "other", false);

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status init_status = reader.init(4096);
    EXPECT_TRUE(init_status.ok());

    MockTabletIndex tablet_index(1, "test");
    bool res = true;
    Status status = reader.has_null(&tablet_index, &res);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(res); // Index not found should return false
}

// Test case for debug_file_entries method
TEST_F(InvertedIndexFileReaderTest, TestDebugFileEntries) {
    std::string index_path = kTestDir + "/debug_entries";
    create_v2_file_with_null_bitmap(index_path + ".idx", 1, "test", true);

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    Status init_status = reader.init(4096);
    EXPECT_TRUE(init_status.ok());

    // This should not crash and should log file entries
    reader.debug_file_entries();
}

// Test case for get_all_directories error handling
TEST_F(InvertedIndexFileReaderTest, TestGetAllDirectoriesError) {
    std::string index_path = kTestDir + "/get_all_dirs";

    InvertedIndexFileInfo file_info;
    IndexFileReader reader(io::global_local_filesystem(), index_path,
                           InvertedIndexStorageFormatPB::V2, file_info);

    // Don't initialize to cause empty result in get_all_directories
    auto result = reader.get_all_directories();
    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().empty()); // Should be empty since no init was called
}

} // namespace doris::segment_v2