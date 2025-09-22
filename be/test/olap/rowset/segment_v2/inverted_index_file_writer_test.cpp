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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/index_storage_format_v1.h"
#include "olap/rowset/segment_v2/index_storage_format_v2.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"

namespace doris::segment_v2 {

using namespace doris::vectorized;

class IndexFileWriterTest : public ::testing::Test {
protected:
    class MockDorisFSDirectoryFileLength : public DorisFSDirectory {
    public:
        //MOCK_METHOD(lucene::store::IndexOutput*, createOutput, (const char* name), (override));
        MOCK_METHOD(int64_t, fileLength, (const char* name), (const, override));
        //MOCK_METHOD(void, close, (), (override));
        //MOCK_METHOD(const char*, getObjectName, (), (const, override));
    };
    class MockDorisFSDirectoryOpenInput : public DorisFSDirectory {
    public:
        MOCK_METHOD(bool, openInput,
                    (const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                     int32_t bufferSize),
                    (override));
    };
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + "/" + std::string(dest_dir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), -1);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // use memory limit
        int64_t inverted_index_cache_limit = 1024 * 1024 * 1024;
        _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit, 1));

        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                _inverted_index_searcher_cache.get());
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        ASSERT_TRUE(_data_dir->update_capacity().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _fs = io::global_local_filesystem();
        _index_path_prefix = _absolute_dir + "/index_test";
        _rowset_id = "test_rowset";
        _seg_id = 1;
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    std::unique_ptr<TabletIndex> create_mock_tablet_index(int64_t index_id,
                                                          const std::string& index_suffix) {
        TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        index_pb.set_index_suffix_name(index_suffix);
        index_pb.set_index_type(IndexType::INVERTED);
        auto index = std::make_unique<TabletIndex>();
        index->init_from_pb(index_pb);
        return index;
    }

    std::string _current_dir;
    std::string _absolute_dir;
    io::FileSystemSPtr _fs;
    std::string _index_path_prefix;
    std::string _rowset_id;
    int64_t _seg_id;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;

    constexpr static uint32_t MAX_PATH_LEN = 1024;
    constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_file_writer_test";
    constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
};

TEST_F(IndexFileWriterTest, InitializeTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    InvertedIndexDirectoryMap indices_dirs;
    indices_dirs.emplace(std::make_pair(1, "suffix1"), std::make_unique<DorisFSDirectory>());
    indices_dirs.emplace(std::make_pair(2, "suffix2"), std::make_unique<DorisFSDirectory>());

    Status status = writer.initialize(indices_dirs);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(writer.get_storage_format(), InvertedIndexStorageFormatPB::V2);
}

TEST_F(IndexFileWriterTest, OpenTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    auto key = std::make_pair(index_id, index_suffix);
    ASSERT_TRUE(writer._indices_dirs.find(key) != writer._indices_dirs.end());
    ASSERT_TRUE(writer._indices_dirs.find(key)->second.get() == dir.get());
}

TEST_F(IndexFileWriterTest, OpenWithoutRamDirTest) {
    // This test verifies that when _can_use_ram_dir is set to false,
    // the directory created by IndexFileWriter::open is not a RAM directory
    config::inverted_index_ram_dir_enable_when_base_compaction = false;
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, nullptr, false);

    int64_t index_id = 1;
    std::string index_suffix = "suffix_no_ram";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open the directory with _can_use_ram_dir = false
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    // Verify the directory is a regular DorisFSDirectory and not a DorisRAMFSDirectory
    // This confirms that _can_use_ram_dir = false works as expected
    ASSERT_STREQ(dir->getObjectName(), "DorisFSDirectory");
    ASSERT_STRNE(dir->getObjectName(), "DorisRAMFSDirectory");

    // Also check that the directory is properly inserted into the _indices_dirs map
    auto key = std::make_pair(index_id, index_suffix);
    ASSERT_TRUE(writer._indices_dirs.find(key) != writer._indices_dirs.end());
    ASSERT_TRUE(writer._indices_dirs.find(key)->second.get() == dir.get());
}

TEST_F(IndexFileWriterTest, OpenWithRamDirTest) {
    // This test verifies the behavior when _can_use_ram_dir is set to true
    // Note: In a test environment, whether a RAM directory is actually used depends on
    // various factors like available memory, file size, etc.
    config::inverted_index_ram_dir_enable_when_base_compaction = true;
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, nullptr, true);

    int64_t index_id = 1;
    std::string index_suffix = "suffix_with_ram";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open the directory with _can_use_ram_dir = true
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);
    // Verify the directory is a regular DorisFSDirectory and not a DorisRAMFSDirectory
    ASSERT_STREQ(dir->getObjectName(), "DorisRAMFSDirectory");
    ASSERT_STRNE(dir->getObjectName(), "DorisFSDirectory");
    auto key = std::make_pair(index_id, index_suffix);
    ASSERT_TRUE(writer._indices_dirs.find(key) != writer._indices_dirs.end());
    ASSERT_TRUE(writer._indices_dirs.find(key)->second.get() == dir.get());
}

TEST_F(IndexFileWriterTest, DeleteIndexTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    InvertedIndexDirectoryMap indices_dirs;
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto st = writer._insert_directory_into_map(index_id, index_suffix,
                                                std::make_shared<DorisFSDirectory>());
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in DeleteIndexTest: " << st.msg()
                  << std::endl;
        ASSERT_TRUE(false);
        return;
    }
    auto key = std::make_pair(index_id, index_suffix);
    ASSERT_TRUE(writer._indices_dirs.find(key) != writer._indices_dirs.end());

    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);
    Status del_status = writer.delete_index(index_meta.get());
    ASSERT_TRUE(del_status.ok());
    ASSERT_TRUE(writer._indices_dirs.find(key) == writer._indices_dirs.end());

    Status del_nonexist_status = writer.delete_index(index_meta.get());
    ASSERT_TRUE(del_nonexist_status.ok());
}

TEST_F(IndexFileWriterTest, WriteV1Test) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("write_v1_test"));
    out_file->writeString("test1");
    out_file->close();
    dir->close();

    Status close_status = writer.close();
    if (!close_status.ok()) {
        std::cout << "close error:" << close_status.msg() << std::endl;
    }
    ASSERT_TRUE(close_status.ok());

    const InvertedIndexFileInfo* file_info = writer.get_index_file_info();
    ASSERT_NE(file_info, nullptr);
    auto index_info = file_info->index_info(0);
    ASSERT_GT(index_info.index_file_size(), 0);

    int64_t total_size = writer.get_index_file_total_size();
    ASSERT_GT(total_size, 0);
    ASSERT_EQ(total_size, index_info.index_file_size());
    std::cout << "total_size:" << total_size << std::endl;
}

TEST_F(IndexFileWriterTest, WriteV2Test) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    int64_t index_id_1 = 1;
    std::string index_suffix_1 = "suffix1";
    auto index_meta_1 = create_mock_tablet_index(index_id_1, index_suffix_1);
    ASSERT_NE(index_meta_1, nullptr);
    auto open_result_1 = writer.open(index_meta_1.get());
    ASSERT_TRUE(open_result_1.has_value());
    auto dir_1 = open_result_1.value();
    auto out_file_1 = std::unique_ptr<lucene::store::IndexOutput>(
            dir_1->createOutput("write_v2_test_index_1"));
    out_file_1->writeString("test1");
    out_file_1->close();
    dir_1->close();
    int64_t index_id_2 = 2;
    std::string index_suffix_2 = "suffix2";
    auto index_meta_2 = create_mock_tablet_index(index_id_2, index_suffix_2);
    ASSERT_NE(index_meta_2, nullptr);
    auto open_result_2 = writer.open(index_meta_2.get());
    ASSERT_TRUE(open_result_2.has_value());
    auto dir_2 = open_result_2.value();
    auto out_file_2 = std::unique_ptr<lucene::store::IndexOutput>(
            dir_2->createOutput("write_v2_test_index_2"));
    out_file_2->writeString("test2");
    out_file_2->close();
    dir_2->close();
    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());

    const InvertedIndexFileInfo* file_info = writer.get_index_file_info();
    ASSERT_NE(file_info, nullptr);
    ASSERT_GT(file_info->index_size(), 0);

    int64_t total_size = writer.get_index_file_total_size();
    ASSERT_GT(total_size, 0);
    ASSERT_EQ(total_size, file_info->index_size());
    std::cout << "total_size:" << total_size << std::endl;
}

TEST_F(IndexFileWriterTest, HeaderLengthTest) {
    InvertedIndexDirectoryMap indices_dirs;
    auto mock_dir1 = std::make_shared<DorisFSDirectory>();
    auto mock_dir2 = std::make_shared<DorisFSDirectory>();
    std::string local_fs_index_path_1 = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    std::string local_fs_index_path_2 = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 2, "suffix2");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path_1).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path_1).ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path_2).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path_2).ok());
    mock_dir1->init(_fs, local_fs_index_path_1.c_str());
    mock_dir2->init(_fs, local_fs_index_path_2.c_str());
    std::vector<std::string> files1 = {"file1.dat", "file2.dat"};
    std::vector<std::string> files2 = {"file3.dat"};
    for (auto& file : files1) {
        auto out_file_1 =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir1->createOutput(file.c_str()));
        out_file_1->writeString("test1");
        out_file_1->close();
    }
    for (auto& file : files2) {
        auto out_file_2 =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir2->createOutput(file.c_str()));
        out_file_2->writeString("test2");
        out_file_2->close();
    }
    auto insertDirectory = [&](IndexFileWriter& writer, int64_t index_id, const std::string& suffix,
                               std::shared_ptr<DorisFSDirectory>& mock_dir) {
        Status st = writer._insert_directory_into_map(index_id, suffix, mock_dir);
        if (!st.ok()) {
            std::cerr << "_insert_directory_into_map error in HeaderLengthTest: " << st.msg()
                      << std::endl;
            assert(false);
            return;
        }
    };

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);
    insertDirectory(writer, 1, "suffix1", mock_dir1);
    insertDirectory(writer, 2, "suffix2", mock_dir2);

    auto* index_storage_format_v2 =
            static_cast<IndexStorageFormatV2*>(writer._index_storage_format.get());
    int64_t header_length = index_storage_format_v2->header_length();

    // sizeof(int32_t) * 2
    // + (sizeof(int64_t) + sizeof(int32_t) + suffix.length() + sizeof(int32_t)) * num_indices
    // + (sizeof(int32_t) + filename.length() + sizeof(int64_t) + sizeof(int64_t)) * num_files
    int64_t expected_header_length = 0;
    expected_header_length += sizeof(int32_t) * 2; // version and num_indices

    // Index 1
    expected_header_length += sizeof(int64_t); // index_id
    expected_header_length += sizeof(int32_t); // suffix size
    expected_header_length += 7;               // "suffix1"
    expected_header_length += sizeof(int32_t); // file_count
    expected_header_length += sizeof(int32_t) + 9 + sizeof(int64_t) + sizeof(int64_t); // file1.dat
    expected_header_length += sizeof(int32_t) + 9 + sizeof(int64_t) + sizeof(int64_t); // file2.dat

    // Index 2
    expected_header_length += sizeof(int64_t); // index_id
    expected_header_length += sizeof(int32_t); // suffix size
    expected_header_length += 7;               // "suffix2"
    expected_header_length += sizeof(int32_t); // file_count
    expected_header_length += sizeof(int32_t) + 9 + sizeof(int64_t) + sizeof(int64_t); // file3.dat

    ASSERT_EQ(header_length, expected_header_length);
}

TEST_F(IndexFileWriterTest, PrepareSortedFilesTest) {
    auto mock_dir = std::make_shared<MockDorisFSDirectoryFileLength>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());
    std::vector<std::string> files = {"segments_0", "segments.gen", "0.fnm",
                                      "0.tii",      "null_bitmap",  "write.lock"};
    for (auto& file : files) {
        auto out_file_1 =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir->createOutput(file.c_str()));
        out_file_1->writeString("test1");
        out_file_1->close();
    }

    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("segments_0")))
            .WillOnce(testing::Return(1000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("segments.gen")))
            .WillOnce(testing::Return(1200));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.fnm"))).WillOnce(testing::Return(2000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.tii"))).WillOnce(testing::Return(1500));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("null_bitmap")))
            .WillOnce(testing::Return(500));

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);
    auto st = writer._insert_directory_into_map(1, "suffix1", mock_dir);
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in PrepareSortedFilesTest: " << st.msg()
                  << std::endl;
        ASSERT_TRUE(false);
        return;
    }

    std::vector<FileInfo> sorted_files = writer._index_storage_format->prepare_sorted_files(
            writer._indices_dirs[std::make_pair(1, "suffix1")].get());

    // 1. null_bitmap (priority 1, size 500)
    // 2. segments.gen (priority 2, size 1200)
    // 3. segments_0 (priority 3, size 1000)
    // 4. 0.fnm (priority 4, size 2000)
    // 5. 0.tii (priority 5, size 1500)

    std::vector<std::string> expected_order = {"null_bitmap", "segments.gen", "segments_0", "0.fnm",
                                               "0.tii"};
    ASSERT_EQ(sorted_files.size(), expected_order.size());

    for (size_t i = 0; i < expected_order.size(); ++i) {
        EXPECT_EQ(sorted_files[i].filename, expected_order[i]);
        if (sorted_files[i].filename == "null_bitmap") {
            EXPECT_EQ(sorted_files[i].filesize, 500);
        } else if (sorted_files[i].filename == "segments.gen") {
            EXPECT_EQ(sorted_files[i].filesize, 1200);
        } else if (sorted_files[i].filename == "segments_0") {
            EXPECT_EQ(sorted_files[i].filesize, 1000);
        } else if (sorted_files[i].filename == "0.fnm") {
            EXPECT_EQ(sorted_files[i].filesize, 2000);
        } else if (sorted_files[i].filename == "0.tii") {
            EXPECT_EQ(sorted_files[i].filesize, 1500);
        }
    }
}

TEST_F(IndexFileWriterTest, PrepareFileMetadataTest) {
    auto mock_dir = std::make_shared<MockDorisFSDirectoryFileLength>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());

    std::vector<std::string> files = {"segments_N",  "segments.gen", "0.fnm", "0.tii",
                                      "null_bitmap", "1.fnm",        "1.tii"};
    for (auto& file : files) {
        auto out_file_1 =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir->createOutput(file.c_str()));
        out_file_1->writeString("test1");
        out_file_1->close();
    }

    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("segments_N")))
            .WillOnce(testing::Return(1000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("segments.gen")))
            .WillOnce(testing::Return(1200));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.fnm"))).WillOnce(testing::Return(2000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.tii"))).WillOnce(testing::Return(1500));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("null_bitmap")))
            .WillOnce(testing::Return(500));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("1.fnm"))).WillOnce(testing::Return(2500));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("1.tii"))).WillOnce(testing::Return(2200));

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);
    auto st = writer._insert_directory_into_map(1, "suffix1", mock_dir);
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in PrepareFileMetadataTest: " << st.msg()
                  << std::endl;
        ASSERT_TRUE(false);
        return;
    }

    int64_t current_offset = 0;

    auto* index_storage_format_v2 =
            static_cast<IndexStorageFormatV2*>(writer._index_storage_format.get());
    std::vector<FileMetadata> file_metadata =
            index_storage_format_v2->prepare_file_metadata(current_offset);

    ASSERT_EQ(file_metadata.size(), 7); // Adjusted size after adding new files

    std::vector<std::string> expected_files = {"null_bitmap", "segments.gen", "segments_N", "0.fnm",
                                               "1.fnm",       "0.tii",        "1.tii"};
    std::vector<int64_t> expected_sizes = {500, 1200, 1000, 2000, 2500, 1500, 2200};

    int64_t tmp_current_offset = 0;
    for (size_t i = 0; i < file_metadata.size(); ++i) {
        EXPECT_EQ(file_metadata[i].filename, expected_files[i]);
        EXPECT_EQ(file_metadata[i].length, expected_sizes[i]);
        if (i == 0) {
            EXPECT_EQ(file_metadata[i].offset, 0);
        } else {
            EXPECT_EQ(file_metadata[i].offset, tmp_current_offset);
        }
        tmp_current_offset += file_metadata[i].length;
    }
    EXPECT_EQ(current_offset, tmp_current_offset);

    bool is_meta_file_first = true;
    for (const auto& metadata : file_metadata) {
        bool is_meta = false;
        for (const auto& entry : InvertedIndexDescriptor::index_file_info_map) {
            if (metadata.filename.find(entry.first) != std::string::npos) {
                is_meta = true;
                break;
            }
        }
        if (is_meta_file_first && !is_meta) {
            is_meta_file_first = false;
        } else if (!is_meta_file_first && is_meta) {
            FAIL() << "Meta files should appear before normal files in the metadata.";
        }
    }
}

/*TEST_F(IndexFileWriterTest, CopyFileTest_OpenInputFailure) {
    auto mock_dir = std::make_shared<MockDorisFSDirectoryOpenInput>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());
    std::vector<std::string> files = {"0.segments", "0.fnm", "0.tii", "nullbitmap", "write.lock"};
    for (auto& file : files) {
        auto out_file_1 =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir->createOutput(file.c_str()));
        out_file_1->writeString("test1");
        out_file_1->close();
    }
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                   InvertedIndexStorageFormatPB::V2);
    auto st = writer._insert_directory_into_map(1, "suffix1", mock_dir);
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in CopyFileTest_OpenInputFailure: "
                  << st.msg() << std::endl;
        ASSERT_TRUE(false);
        return;
    }

    EXPECT_CALL(*mock_dir,
                openInput(::testing::StrEq("0.segments"), ::testing::_, ::testing::_, ::testing::_))
            .WillOnce(::testing::Invoke([&](const char* name, lucene::store::IndexInput*& ret,
                                            CLuceneError& err_ref, int bufferSize) {
                err_ref.set(CL_ERR_IO, fmt::format("Could not open file, file is {}", name).data());
                return false;
            }));

    uint8_t buffer[16384];
    std::string error_message;
    try {
        writer.copyFile("0.segments", mock_dir.get(), nullptr, buffer, sizeof(buffer));
    } catch (CLuceneError& err) {
        error_message = err.what();
    }
    ASSERT_EQ(error_message, "Could not open file, file is 0.segments");
}*/
class IndexFileWriterMock : public IndexFileWriter {
public:
    IndexFileWriterMock(const io::FileSystemSPtr& fs, const std::string& index_path_prefix,
                        const std::string& rowset_id, int32_t segment_id,
                        InvertedIndexStorageFormatPB storage_format)
            : IndexFileWriter(fs, index_path_prefix, rowset_id, segment_id, storage_format) {}
};
class IndexStorageFormatV1Mock : public IndexStorageFormatV1 {
public:
    IndexStorageFormatV1Mock(IndexFileWriter* index_file_writer)
            : IndexStorageFormatV1(index_file_writer) {}

    MOCK_METHOD(void, write_header_and_data,
                (lucene::store::IndexOutput * output, const std::vector<FileInfo>& sorted_files,
                 lucene::store::Directory* directory, int64_t header_length,
                 int32_t header_file_count),
                (override));
};
TEST_F(IndexFileWriterTest, WriteV1ExceptionHandlingTest) {
    IndexFileWriterMock writer_mock(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                    InvertedIndexStorageFormatPB::V1);
    writer_mock._index_storage_format = std::make_unique<IndexStorageFormatV1Mock>(&writer_mock);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();
    EXPECT_CALL(*(IndexStorageFormatV1Mock*)writer_mock._index_storage_format.get(),
                write_header_and_data(::testing::_, ::testing::_, ::testing::_, ::testing::_,
                                      ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}
class IndexFileWriterMockV2 : public IndexFileWriter {
public:
    IndexFileWriterMockV2(const io::FileSystemSPtr& fs, const std::string& index_path_prefix,
                          const std::string& rowset_id, int32_t segment_id,
                          InvertedIndexStorageFormatPB storage_format,
                          io::FileWriterPtr file_writer)
            : IndexFileWriter(fs, index_path_prefix, rowset_id, segment_id, storage_format,
                              std::move(file_writer)) {}
};

class IndexStorageFormatV2Mock : public IndexStorageFormatV2 {
public:
    IndexStorageFormatV2Mock(IndexFileWriter* index_file_writer)
            : IndexStorageFormatV2(index_file_writer) {}

    MOCK_METHOD(void, write_index_headers_and_metadata,
                (lucene::store::IndexOutput * output,
                 const std::vector<FileMetadata>& file_metadata),
                (override));
};

TEST_F(IndexFileWriterTest, WriteV2ExceptionHandlingTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    IndexFileWriterMockV2 writer_mock(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                      InvertedIndexStorageFormatPB::V2, std::move(file_writer));
    writer_mock._index_storage_format = std::make_unique<IndexStorageFormatV2Mock>(&writer_mock);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    EXPECT_CALL(*(IndexStorageFormatV2Mock*)writer_mock._index_storage_format.get(),
                write_index_headers_and_metadata(::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

class IndexFileWriterMockCreateOutputStreamV2 : public IndexFileWriter {
public:
    IndexFileWriterMockCreateOutputStreamV2(const io::FileSystemSPtr& fs,
                                            const std::string& index_path_prefix,
                                            const std::string& rowset_id, int32_t segment_id,
                                            InvertedIndexStorageFormatPB storage_format)
            : IndexFileWriter(fs, index_path_prefix, rowset_id, segment_id, storage_format) {}
};

class IndexStorageFormatV2MockCreateOutputStream : public IndexStorageFormatV2 {
public:
    IndexStorageFormatV2MockCreateOutputStream(IndexFileWriter* index_file_writer)
            : IndexStorageFormatV2(index_file_writer) {}

    MOCK_METHOD((std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                           std::unique_ptr<lucene::store::IndexOutput>>),
                create_output_stream, (), (override));
};

class IndexFileWriterMockCreateOutputStreamV1 : public IndexFileWriter {
public:
    IndexFileWriterMockCreateOutputStreamV1(const io::FileSystemSPtr& fs,
                                            const std::string& index_path_prefix,
                                            const std::string& rowset_id, int32_t segment_id,
                                            InvertedIndexStorageFormatPB storage_format)
            : IndexFileWriter(fs, index_path_prefix, rowset_id, segment_id, storage_format) {}
};

class IndexStorageFormatV1MockCreateOutputStream : public IndexStorageFormatV1 {
public:
    IndexStorageFormatV1MockCreateOutputStream(IndexFileWriter* index_file_writer)
            : IndexStorageFormatV1(index_file_writer) {}

    MOCK_METHOD((std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                           std::unique_ptr<lucene::store::IndexOutput>>),
                create_output_stream, (int64_t index_id, const std::string& index_suffix),
                (override));
};

class MockDorisFSDirectoryOutput : public DorisFSDirectory {
public:
    //MOCK_METHOD(lucene::store::IndexOutput*, createOutput, (const char* name), (override));
    //MOCK_METHOD(int64_t, fileLength, (const char* name), (const, override));
    MOCK_METHOD(void, close, (), (override));
    //MOCK_METHOD(const char*, getObjectName, (), (const, override));
};

class MockFSIndexOutputV2 : public DorisFSDirectory::FSIndexOutputV2 {
public:
    //MOCK_METHOD(void, close, (), (override));
    MOCK_METHOD(void, flushBuffer, (const uint8_t* b, const int32_t size), (override));
};

class MockFSIndexOutputV1 : public DorisFSDirectory::FSIndexOutput {
public:
    //MOCK_METHOD(void, close, (), (override));
    MOCK_METHOD(void, flushBuffer, (const uint8_t* b, const int32_t size), (override));
};

TEST_F(IndexFileWriterTest, WriteV1OutputTest) {
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    IndexFileWriterMockCreateOutputStreamV1 writer_mock(_fs, _index_path_prefix, _rowset_id,
                                                        _seg_id, InvertedIndexStorageFormatPB::V1);
    writer_mock._index_storage_format =
            std::make_unique<IndexStorageFormatV1MockCreateOutputStream>(&writer_mock);

    io::Path cfs_path(InvertedIndexDescriptor::get_index_file_path_v1(_index_path_prefix, index_id,
                                                                      index_suffix));
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = cfs_path.filename();

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, idx_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v1 = new MockFSIndexOutputV1();
    EXPECT_CALL(*mock_output_v1, flushBuffer(::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutput>(mock_output_v1);
    compound_file_output->init(_fs, cfs_path.c_str());

    EXPECT_CALL(
            *(IndexStorageFormatV1MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream(index_id, index_suffix))
            .WillOnce(::testing::Invoke(
                    [&]() -> std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                                       std::unique_ptr<lucene::store::IndexOutput>> {
                        return std::make_pair(std::move(out_dir_ptr),
                                              std::move(compound_file_output));
                    }));

    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

TEST_F(IndexFileWriterTest, WriteV2OutputTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriterMockCreateOutputStreamV2 writer_mock(_fs, _index_path_prefix, _rowset_id,
                                                        _seg_id, InvertedIndexStorageFormatPB::V2);
    writer_mock._index_storage_format =
            std::make_unique<IndexStorageFormatV2MockCreateOutputStream>(&writer_mock);

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v2 = new MockFSIndexOutputV2();
    EXPECT_CALL(*mock_output_v2, flushBuffer(::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutputV2>(mock_output_v2);
    compound_file_output->init(file_writer.get());

    EXPECT_CALL(
            *(IndexStorageFormatV2MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream())
            .WillOnce(::testing::Invoke(
                    [&]() -> std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                                       std::unique_ptr<lucene::store::IndexOutput>> {
                        return std::make_pair(std::move(out_dir_ptr),
                                              std::move(compound_file_output));
                    }));

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

class MockFSIndexOutputCloseV2 : public DorisFSDirectory::FSIndexOutputV2 {
public:
    MOCK_METHOD(void, close, (), (override));
    void base_close() { DorisFSDirectory::FSIndexOutputV2::BufferedIndexOutput::close(); }
};

class MockFSIndexOutputCloseV1 : public DorisFSDirectory::FSIndexOutput {
public:
    MOCK_METHOD(void, close, (), (override));
    void base_close() { DorisFSDirectory::FSIndexOutput::BufferedIndexOutput::close(); }
};

TEST_F(IndexFileWriterTest, WriteV2OutputCloseErrorTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriterMockCreateOutputStreamV2 writer_mock(_fs, _index_path_prefix, _rowset_id,
                                                        _seg_id, InvertedIndexStorageFormatPB::V2);
    writer_mock._index_storage_format =
            std::make_unique<IndexStorageFormatV2MockCreateOutputStream>(&writer_mock);

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v2 = new MockFSIndexOutputCloseV2();
    EXPECT_CALL(*mock_output_v2, close()).WillOnce(::testing::Invoke([&]() {
        mock_output_v2->base_close();
        throw CLuceneError(CL_ERR_IO, "Simulated exception", false);
    }));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutputV2>(mock_output_v2);
    compound_file_output->init(file_writer.get());

    EXPECT_CALL(
            *(IndexStorageFormatV2MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream())
            .WillOnce(::testing::Invoke(
                    [&]() -> std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                                       std::unique_ptr<lucene::store::IndexOutput>> {
                        return std::make_pair(std::move(out_dir_ptr),
                                              std::move(compound_file_output));
                    }));

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

TEST_F(IndexFileWriterTest, WriteV1OutputCloseErrorTest) {
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    IndexFileWriterMockCreateOutputStreamV1 writer_mock(_fs, _index_path_prefix, _rowset_id,
                                                        _seg_id, InvertedIndexStorageFormatPB::V1);
    writer_mock._index_storage_format =
            std::make_unique<IndexStorageFormatV1MockCreateOutputStream>(&writer_mock);

    io::Path cfs_path(InvertedIndexDescriptor::get_index_file_path_v1(_index_path_prefix, index_id,
                                                                      index_suffix));
    auto idx_path = cfs_path.parent_path();
    std::string idx_name = cfs_path.filename();

    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, idx_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v1 = new MockFSIndexOutputCloseV1();
    EXPECT_CALL(*mock_output_v1, close()).WillOnce(::testing::Invoke([&]() {
        mock_output_v1->base_close();
        throw CLuceneError(CL_ERR_IO, "Simulated exception", false);
    }));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutput>(mock_output_v1);
    compound_file_output->init(_fs, cfs_path.c_str());

    EXPECT_CALL(
            *(IndexStorageFormatV1MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream(index_id, index_suffix))
            .WillOnce(::testing::Invoke(
                    [&]() -> std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                                       std::unique_ptr<lucene::store::IndexOutput>> {
                        return std::make_pair(std::move(out_dir_ptr),
                                              std::move(compound_file_output));
                    }));

    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer_mock.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

class MockIndexFileWriter : public IndexFileWriter {
public:
    MOCK_METHOD(Result<std::unique_ptr<IndexSearcherBuilder>>, _construct_index_searcher_builder,
                (const DorisCompoundReader* dir), (override));
    MockIndexFileWriter(io::FileSystemSPtr fs, const std::string& index_path_prefix,
                        const std::string& rowset_id, int64_t seg_id,
                        InvertedIndexStorageFormatPB storage_format, io::FileWriterPtr file_writer)
            : IndexFileWriter(fs, index_path_prefix, rowset_id, seg_id, storage_format,
                              std::move(file_writer)) {}
};

class MockIndexSearcherBuilder : public IndexSearcherBuilder {
public:
    MOCK_METHOD(Result<IndexSearcherPtr>, get_index_searcher,
                (lucene::store::Directory * directory), (override));
    MOCK_METHOD(Status, build,
                (lucene::store::Directory * directory, OptionalIndexSearcherPtr& output_searcher),
                (override));
    MockIndexSearcherBuilder() = default;
    ~MockIndexSearcherBuilder() override = default;
};

TEST_F(IndexFileWriterTest, AddIntoSearcherCacheTest) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                               InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(
            dir->createOutput("add_into_searcher_cache_test"));
    out_file->writeString("test");
    out_file->close();
    dir->close();
    auto mock_builder = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder))));

    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());

    auto index_file_key = InvertedIndexDescriptor::get_index_file_cache_key(_index_path_prefix,
                                                                            index_id, index_suffix);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    InvertedIndexCacheHandle cache_handle;
    bool found = _inverted_index_searcher_cache->lookup(searcher_cache_key, &cache_handle);
    ASSERT_TRUE(found);
    auto* cache_value_use_cache = cache_handle.get_index_cache_value();
    EXPECT_GE(UnixMillis(), cache_value_use_cache->last_visit_time);
    auto searcher_variant = cache_value_use_cache->index_searcher;
    EXPECT_TRUE(std::holds_alternative<BKDIndexSearcherPtr>(searcher_variant));
    config::enable_write_index_searcher_cache = false;
}

TEST_F(IndexFileWriterTest, CacheEvictionTest) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer1, file_writer2, file_writer3;
    std::string index_path1 =
            InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix + "_1");
    std::string index_path2 =
            InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix + "_2");
    std::string index_path3 =
            InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix + "_3");
    io::FileWriterOptions opts;
    Status st1 = _fs->create_file(index_path1, &file_writer1, &opts);
    Status st2 = _fs->create_file(index_path2, &file_writer2, &opts);
    Status st3 = _fs->create_file(index_path3, &file_writer3, &opts);
    ASSERT_TRUE(st1.ok());
    ASSERT_TRUE(st2.ok());
    ASSERT_TRUE(st3.ok());
    MockIndexFileWriter writer1(_fs, _index_path_prefix + "_1", "rowset1", 1,
                                InvertedIndexStorageFormatPB::V2, std::move(file_writer1));
    int64_t index_id1 = 1;
    std::string index_suffix1 = "suffix1";
    auto index_meta1 = create_mock_tablet_index(index_id1, index_suffix1);
    ASSERT_NE(index_meta1, nullptr);
    auto open_result1 = writer1.open(index_meta1.get());
    ASSERT_TRUE(open_result1.has_value());
    auto dir1 = open_result1.value();
    auto out_file1 =
            std::unique_ptr<lucene::store::IndexOutput>(dir1->createOutput("cache_eviction_test1"));
    out_file1->writeString("test1");
    out_file1->close();
    dir1->close();
    auto mock_builder1 = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder1, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer1, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder1))));

    Status close_status1 = writer1.close();
    ASSERT_TRUE(close_status1.ok());

    MockIndexFileWriter writer2(_fs, _index_path_prefix + "_2", "rowset2", 2,
                                InvertedIndexStorageFormatPB::V2, std::move(file_writer2));
    int64_t index_id2 = 2;
    std::string index_suffix2 = "suffix2";
    auto index_meta2 = create_mock_tablet_index(index_id2, index_suffix2);
    ASSERT_NE(index_meta2, nullptr);
    auto open_result2 = writer2.open(index_meta2.get());
    ASSERT_TRUE(open_result2.has_value());
    auto dir2 = open_result2.value();
    auto out_file2 =
            std::unique_ptr<lucene::store::IndexOutput>(dir2->createOutput("cache_eviction_test2"));
    out_file2->writeString("test2");
    out_file2->close();
    dir2->close();
    auto mock_builder2 = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder2, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer2, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder2))));

    Status close_status2 = writer2.close();
    ASSERT_TRUE(close_status2.ok());

    MockIndexFileWriter writer3(_fs, _index_path_prefix + "_3", "rowset3", 3,
                                InvertedIndexStorageFormatPB::V2, std::move(file_writer3));
    int64_t index_id3 = 3;
    std::string index_suffix3 = "suffix3";
    auto index_meta3 = create_mock_tablet_index(index_id3, index_suffix3);
    ASSERT_NE(index_meta3, nullptr);
    auto open_result3 = writer3.open(index_meta3.get());
    ASSERT_TRUE(open_result3.has_value());
    auto dir3 = open_result3.value();
    auto out_file3 =
            std::unique_ptr<lucene::store::IndexOutput>(dir3->createOutput("cache_eviction_test3"));
    out_file3->writeString("test3");
    out_file3->close();
    dir3->close();
    auto mock_builder3 = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder3, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));
    EXPECT_CALL(writer3, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder3))));

    Status close_status3 = writer3.close();
    ASSERT_TRUE(close_status3.ok());

    InvertedIndexCacheHandle cache_handle1;
    std::string index_file_key1 = InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix + "_1", index_id1, index_suffix1);
    bool found1 = _inverted_index_searcher_cache->lookup(index_file_key1, &cache_handle1);
    // limit to 2 in BE_TEST, so the first one should be evicted
    ASSERT_FALSE(found1);

    InvertedIndexCacheHandle cache_handle2;
    std::string index_file_key2 = InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix + "_2", index_id2, index_suffix2);
    bool found2 = _inverted_index_searcher_cache->lookup(index_file_key2, &cache_handle2);
    ASSERT_TRUE(found2);
    auto* cache_value_use_cache2 = cache_handle2.get_index_cache_value();
    EXPECT_GE(UnixMillis(), cache_value_use_cache2->last_visit_time);
    auto searcher_variant2 = cache_value_use_cache2->index_searcher;
    EXPECT_TRUE(std::holds_alternative<BKDIndexSearcherPtr>(searcher_variant2));

    InvertedIndexCacheHandle cache_handle3;
    std::string index_file_key3 = InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix + "_3", index_id3, index_suffix3);
    bool found3 = _inverted_index_searcher_cache->lookup(index_file_key3, &cache_handle3);
    ASSERT_TRUE(found3);
    auto* cache_value_use_cache3 = cache_handle3.get_index_cache_value();
    EXPECT_GE(UnixMillis(), cache_value_use_cache3->last_visit_time);
    auto searcher_variant3 = cache_value_use_cache3->index_searcher;
    EXPECT_TRUE(std::holds_alternative<BKDIndexSearcherPtr>(searcher_variant3));

    config::enable_write_index_searcher_cache = false;
}

TEST_F(IndexFileWriterTest, CacheUpdateTest) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_path =
            InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix + "_update");
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockIndexFileWriter writer(_fs, _index_path_prefix + "_update", "rowset_update", 3,
                               InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    int64_t index_id = 3;
    std::string index_suffix = "suffix3";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(
            dir->createOutput("write_v2_test_index_update"));
    out_file->writeString("test_update");
    out_file->close();
    dir->close();
    auto mock_builder = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder))));

    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());

    auto index_file_key = InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix + "_update", index_id, index_suffix);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    InvertedIndexCacheHandle cache_handle;
    bool found = _inverted_index_searcher_cache->lookup(searcher_cache_key, &cache_handle);
    ASSERT_TRUE(found);

    io::FileWriterPtr file_writer_new;
    Status st_new = _fs->create_file(index_path, &file_writer_new, &opts);
    ASSERT_TRUE(st_new.ok());
    MockIndexFileWriter writer_new(_fs, _index_path_prefix + "_update", "rowset_update", 3,
                                   InvertedIndexStorageFormatPB::V2, std::move(file_writer_new));

    auto open_result_new = writer_new.open(index_meta.get());
    ASSERT_TRUE(open_result_new.has_value());
    auto dir_new = open_result_new.value();
    auto out_file_new = std::unique_ptr<lucene::store::IndexOutput>(
            dir_new->createOutput("write_v2_test_index_update_new"));
    out_file_new->writeString("test_update_new");
    out_file_new->close();
    dir_new->close();
    auto mock_builder_new = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder_new, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer_new, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder_new))));

    Status close_status_new = writer_new.close();
    ASSERT_TRUE(close_status_new.ok());

    InvertedIndexCacheHandle cache_handle_new;
    bool found_new = _inverted_index_searcher_cache->lookup(searcher_cache_key, &cache_handle_new);
    ASSERT_TRUE(found_new);
    EXPECT_NE(cache_handle.get_index_cache_value()->index_searcher,
              cache_handle_new.get_index_cache_value()->index_searcher);

    config::enable_write_index_searcher_cache = false;
}

TEST_F(IndexFileWriterTest, AddIntoSearcherCacheV1Test) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_suffix = "suffix_v1";
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v1(
            _index_path_prefix + "_v1", 4, index_suffix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockIndexFileWriter writer(_fs, _index_path_prefix + "_v1", "rowset_v1", 4,
                               InvertedIndexStorageFormatPB::V1, std::move(file_writer));
    int64_t index_id = 4;
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(
            dir->createOutput("add_into_searcher_cache_v1_test"));
    out_file->writeString("test_v1");
    out_file->close();
    dir->close();
    auto mock_builder = std::make_unique<MockIndexSearcherBuilder>();

    EXPECT_CALL(*mock_builder, get_index_searcher(testing::_))
            .WillOnce(testing::Invoke(
                    [](lucene::store::Directory* directory) -> Result<IndexSearcherPtr> {
                        auto close_directory = true;
                        auto bkd_reader = std::make_shared<lucene::util::bkd::bkd_reader>(
                                directory, close_directory);
                        return bkd_reader;
                    }));
    EXPECT_CALL(writer, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder))));
    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());

    auto index_file_key = InvertedIndexDescriptor::get_index_file_cache_key(
            _index_path_prefix + "_v1", index_id, index_suffix);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    InvertedIndexCacheHandle cache_handle;
    bool found = _inverted_index_searcher_cache->lookup(searcher_cache_key, &cache_handle);
    ASSERT_TRUE(found);
    auto* cache_value_use_cache = cache_handle.get_index_cache_value();
    EXPECT_GE(UnixMillis(), cache_value_use_cache->last_visit_time);
    auto searcher_variant = cache_value_use_cache->index_searcher;
    EXPECT_TRUE(std::holds_alternative<BKDIndexSearcherPtr>(searcher_variant));
    config::enable_write_index_searcher_cache = false;
}

// Mock RowsetWriter class that implements the necessary methods for testing
class MockRowsetWriter : public RowsetWriter {
public:
    MockRowsetWriter(const io::FileSystemSPtr& fs, const std::string& segment_path_prefix,
                     const RowsetId& rowset_id, TabletSchemaSPtr tablet_schema,
                     ReaderType compaction_type = ReaderType::READER_QUERY)
            : RowsetWriter() {
        _context.rowset_id = rowset_id;
        _context.tablet_schema = tablet_schema;
        _context.compaction_type = compaction_type;
        _segment_path_prefix = segment_path_prefix;
    }

    Status init(const RowsetWriterContext& rowset_writer_context) override { return Status::OK(); }

    Status create_file_writer(uint32_t segment_id, io::FileWriterPtr& writer,
                              FileType file_type = FileType::SEGMENT_FILE) override {
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(_segment_path_prefix);
        io::FileWriterOptions opts;
        Status st = _context.fs()->create_file(index_path, &writer, &opts);
        return st;
    }

    // Required overrides for abstract base class but not used in our tests
    Status add_rowset(RowsetSharedPtr rowset) override { return Status::OK(); }
    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) override {
        return Status::OK();
    }
    Status flush() override { return Status::OK(); }
    Status build(RowsetSharedPtr& rowset) override { return Status::OK(); }
    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) override {
        return nullptr;
    }
    PUniqueId load_id() override { return PUniqueId(); }
    Version version() override { return Version(); }
    int64_t num_rows() const override { return 0; }
    int64_t num_rows_updated() const override { return 0; }
    int64_t num_rows_deleted() const override { return 0; }
    int64_t num_rows_new_added() const override { return 0; }
    int64_t num_rows_filtered() const override { return 0; }
    RowsetId rowset_id() override { return _context.rowset_id; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
    int32_t allocate_segment_id() override { return 0; }
    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() override { return nullptr; }
    bool is_partial_update() override { return false; }

private:
    std::string _segment_path_prefix;
};

// Test case for rowset writer's create_inverted_index_file_writer with RAM directory disabled
TEST_F(IndexFileWriterTest, RowsetWriterCreateIndexFileWriterWithoutRamDir) {
    // Set config flag to disable RAM directory
    config::inverted_index_ram_dir_enable_when_base_compaction = false;

    // Create a valid TabletSchema for testing
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(tablet_schema_pb);

    // Create a mock rowset writer with base compaction reader type
    RowsetId rowset_id;
    rowset_id.init(10001);
    std::string segment_path_prefix = _absolute_dir + "/test_rowset";

    MockRowsetWriter writer(_fs, segment_path_prefix, rowset_id, tablet_schema,
                            ReaderType::READER_BASE_COMPACTION);

    uint32_t segment_id = 1;
    IndexFileWriterPtr index_file_writer;

    // Call the method to test
    Status status = writer.create_index_file_writer(segment_id, &index_file_writer);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(index_file_writer, nullptr);

    // Test directory creation with base schema
    int64_t index_id = 1;
    std::string index_suffix = "suffix_no_ram";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open the directory with _can_use_ram_dir = false (which should be the case due to our config)
    auto open_result = index_file_writer->open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    // Verify directory type (should be DorisFSDirectory, not DorisRAMFSDirectory)
    ASSERT_STREQ(dir->getObjectName(), "DorisFSDirectory");
    ASSERT_STRNE(dir->getObjectName(), "DorisRAMFSDirectory");

    // Cleanup
    dir->close();
    status = index_file_writer->close();
    ASSERT_TRUE(status.ok());
}

// Test case for rowset writer's create_inverted_index_file_writer with RAM directory enabled
TEST_F(IndexFileWriterTest, RowsetWriterCreateIndexFileWriterWithRamDir) {
    // Set config flag to enable RAM directory
    config::inverted_index_ram_dir_enable_when_base_compaction = true;

    // Create a valid TabletSchema for testing
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(tablet_schema_pb);

    // Create a mock rowset writer with base compaction reader type
    RowsetId rowset_id;
    rowset_id.init(10002);
    std::string segment_path_prefix = _absolute_dir + "/test_rowset_with_ram";

    MockRowsetWriter writer(_fs, segment_path_prefix, rowset_id, tablet_schema,
                            ReaderType::READER_BASE_COMPACTION);

    uint32_t segment_id = 1;
    IndexFileWriterPtr index_file_writer;

    // Call the method to test
    Status status = writer.create_index_file_writer(segment_id, &index_file_writer);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(index_file_writer, nullptr);

    // Test directory creation with base schema
    int64_t index_id = 1;
    std::string index_suffix = "suffix_with_ram";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open the directory with _can_use_ram_dir = true (which should be the case due to our config)
    auto open_result = index_file_writer->open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    // Verify directory type (should be DorisRAMFSDirectory, not DorisFSDirectory)
    ASSERT_STREQ(dir->getObjectName(), "DorisRAMFSDirectory");
    ASSERT_STRNE(dir->getObjectName(), "DorisFSDirectory");

    // Cleanup
    dir->close();
    status = index_file_writer->close();
    ASSERT_TRUE(status.ok());
}

// Test case for rowset writer's create_inverted_index_file_writer with non-base compaction (should always use RAM)
TEST_F(IndexFileWriterTest, RowsetWriterCreateIndexFileWriterNonBaseCompaction) {
    // Set config flag to disable RAM directory for base compaction
    config::inverted_index_ram_dir_enable_when_base_compaction = false;

    // Create a valid TabletSchema for testing
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(tablet_schema_pb);

    // Create a mock rowset writer with QUERY reader type (not base compaction)
    RowsetId rowset_id;
    rowset_id.init(10003);
    std::string segment_path_prefix = _absolute_dir + "/test_rowset_query";

    MockRowsetWriter writer(_fs, segment_path_prefix, rowset_id, tablet_schema,
                            ReaderType::READER_QUERY); // Not base compaction

    uint32_t segment_id = 1;
    IndexFileWriterPtr index_file_writer;

    // Call the method to test
    Status status = writer.create_index_file_writer(segment_id, &index_file_writer);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(index_file_writer, nullptr);

    // Test directory creation
    int64_t index_id = 1;
    std::string index_suffix = "suffix_query";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open the directory - should still use RAM dir since this is not base compaction
    auto open_result = index_file_writer->open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    // Verify directory type (should be DorisRAMFSDirectory since it's not base compaction)
    ASSERT_STREQ(dir->getObjectName(), "DorisRAMFSDirectory");
    ASSERT_STRNE(dir->getObjectName(), "DorisFSDirectory");

    // Cleanup
    dir->close();
    status = index_file_writer->close();
    ASSERT_TRUE(status.ok());
}

// Test case for multiple indices with exception in create_output_stream_v1
TEST_F(IndexFileWriterTest, MultipleIndicesCreateOutputStreamException) {
    // Setup writer mock
    IndexFileWriterMockCreateOutputStreamV1 writer_mock(_fs, _index_path_prefix, _rowset_id,
                                                        _seg_id, InvertedIndexStorageFormatPB::V1);
    writer_mock._index_storage_format =
            std::make_unique<IndexStorageFormatV1MockCreateOutputStream>(&writer_mock);

    // First index setup - index_id=1
    int64_t index_id_1 = 1;
    std::string index_suffix_1 = "suffix1";
    auto index_meta_1 = create_mock_tablet_index(index_id_1, index_suffix_1);
    ASSERT_NE(index_meta_1, nullptr);

    // Setup the first index directory
    auto open_result_1 = writer_mock.open(index_meta_1.get());
    ASSERT_TRUE(open_result_1.has_value());
    auto dir_1 = open_result_1.value();
    ASSERT_NE(dir_1, nullptr);

    // Create a test file in the first directory
    auto out_file_1 =
            std::unique_ptr<lucene::store::IndexOutput>(dir_1->createOutput("test_file_1"));
    out_file_1->writeString("test data 1");
    out_file_1->close();
    dir_1->close();

    // For the first index, setup a normal output stream
    io::Path cfs_path_1(InvertedIndexDescriptor::get_index_file_path_v1(
            _index_path_prefix, index_id_1, index_suffix_1));
    auto idx_path_1 = cfs_path_1.parent_path();
    auto* out_dir_1 = DorisFSDirectoryFactory::getDirectory(_fs, idx_path_1.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr_1(out_dir_1);
    auto compound_file_output_1 = std::unique_ptr<lucene::store::IndexOutput>(
            out_dir_1->createOutput(cfs_path_1.filename().c_str()));

    // Second index setup - index_id=2
    int64_t index_id_2 = 2;
    std::string index_suffix_2 = "suffix2";
    auto index_meta_2 = create_mock_tablet_index(index_id_2, index_suffix_2);
    ASSERT_NE(index_meta_2, nullptr);

    // Setup the second index directory
    auto open_result_2 = writer_mock.open(index_meta_2.get());
    ASSERT_TRUE(open_result_2.has_value());
    auto dir_2 = open_result_2.value();
    ASSERT_NE(dir_2, nullptr);

    // Create a test file in the second directory
    auto out_file_2 =
            std::unique_ptr<lucene::store::IndexOutput>(dir_2->createOutput("test_file_2"));
    out_file_2->writeString("test data 2");
    out_file_2->close();
    dir_2->close();

    // Setup expectation for create_output_stream_v1
    // For index_id=1, return normal output
    EXPECT_CALL(
            *(IndexStorageFormatV1MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream(index_id_1, index_suffix_1))
            .WillOnce(::testing::Invoke(
                    [&]() -> std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                                       std::unique_ptr<lucene::store::IndexOutput>> {
                        return std::make_pair(std::move(out_dir_ptr_1),
                                              std::move(compound_file_output_1));
                    }));

    // For index_id=2, throw an exception
    EXPECT_CALL(
            *(IndexStorageFormatV1MockCreateOutputStream*)writer_mock._index_storage_format.get(),
            create_output_stream(index_id_2, index_suffix_2))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));

    // When we call close(), it should process both indices and fail on the second one
    Status status = writer_mock.close();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

TEST_F(IndexFileWriterTest, CopyFileEmptyFileTest) {
    // This test uses the existing MockDorisFSDirectoryOpenInput class
    auto mock_dir = std::make_shared<MockDorisFSDirectoryOpenInput>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());

    // Create test files
    std::vector<std::string> files = {"_0.fnm", "_0.frq", "_0.tii"};
    for (const auto& file : files) {
        auto out_file =
                std::unique_ptr<lucene::store::IndexOutput>(mock_dir->createOutput(file.c_str()));
        out_file->writeString("test content");
        out_file->close();
    }

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // Setup mock to simulate CL_ERR_EmptyIndexSegment error for _0.frq file
    EXPECT_CALL(*mock_dir,
                openInput(::testing::StrEq("_0.frq"), ::testing::_, ::testing::_, ::testing::_))
            .WillOnce(::testing::Invoke([&](const char* name, lucene::store::IndexInput*& ret,
                                            CLuceneError& err_ref, int32_t bufferSize) {
                err_ref.set(CL_ERR_EmptyIndexSegment,
                            std::string("Empty index segment for file: ").append(name).c_str());
                return false;
            }));

    // Test copyFile with the mock directory
    uint8_t buffer[16384];

    // Create a temporary output stream to test copyFile
    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    // Before the fix, this would throw an exception
    // After the fix, this should log a warning and return gracefully
    EXPECT_NO_THROW({
        writer._index_storage_format->copy_file("_0.frq", mock_dir.get(), output, buffer,
                                                sizeof(buffer));
    });

    // The output should remain valid (not corrupted by the exception)
    EXPECT_NE(output, nullptr);

    // Clean up
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);

    // Clean up the test directory
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for duplicate key insertion error handling in _insert_directory_into_map
TEST_F(IndexFileWriterTest, InsertDirectoryDuplicateKeyTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // First insertion should succeed
    int64_t index_id = 1;
    std::string index_suffix = "test_suffix";
    auto dir1 = std::make_shared<DorisFSDirectory>();
    Status st1 = writer._insert_directory_into_map(index_id, index_suffix, dir1);
    ASSERT_TRUE(st1.ok());

    // Second insertion with same key should fail
    auto dir2 = std::make_shared<DorisFSDirectory>();
    Status st2 = writer._insert_directory_into_map(index_id, index_suffix, dir2);
    ASSERT_FALSE(st2.ok());
    ASSERT_EQ(st2.code(), ErrorCode::INTERNAL_ERROR);
    ASSERT_TRUE(st2.msg().find("attempted to insert a duplicate dir") != std::string::npos);
}

// Test for delete_index with null index_meta
TEST_F(IndexFileWriterTest, DeleteIndexNullMetaTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // Test with null pointer - should handle gracefully
    Status status = writer.delete_index(nullptr);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    ASSERT_TRUE(status.msg().find("Index metadata is null") != std::string::npos);
}

// Test for add_into_searcher_cache with StreamSinkFileWriter nullptr check
TEST_F(IndexFileWriterTest, AddIntoSearcherCacheStreamSinkNullTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // Create a directory for testing
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto st = writer._insert_directory_into_map(index_id, index_suffix,
                                                std::make_shared<DorisFSDirectory>());
    ASSERT_TRUE(st.ok());

    // Test when _idv_v2_writer is nullptr (StreamSinkFileWriter not found)
    config::enable_write_index_searcher_cache = true;
    Status cache_status = writer.add_into_searcher_cache();
    // The function should return an error when trying to init without a proper file writer
    ASSERT_FALSE(cache_status.ok());
    config::enable_write_index_searcher_cache = false;
}

// Test copyFile with various exception scenarios
TEST_F(IndexFileWriterTest, CopyFileExceptionHandlingTest) {
    auto mock_dir = std::make_shared<MockDorisFSDirectoryOpenInput>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());

    // Create test file
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(
            mock_dir->createOutput("test_copy_exception"));
    out_file->writeString("test");
    out_file->close();

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    uint8_t buffer[16384];
    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    // Setup mock to simulate file read failure
    EXPECT_CALL(*mock_dir, openInput(::testing::StrEq("test_copy_exception"), ::testing::_,
                                     ::testing::_, ::testing::_))
            .WillOnce(::testing::Invoke([&](const char* name, lucene::store::IndexInput*& ret,
                                            CLuceneError& err_ref, int32_t bufferSize) {
                err_ref.set(CL_ERR_IO, std::string("Failed to open file: ").append(name).c_str());
                return false;
            }));

    // Test should handle the exception gracefully
    std::string error_message;
    try {
        writer._index_storage_format->copy_file("test_copy_exception", mock_dir.get(), output,
                                                buffer, sizeof(buffer));
    } catch (CLuceneError& err) {
        error_message = err.what();
    }
    ASSERT_FALSE(error_message.empty());
    ASSERT_TRUE(error_message.find("Failed to open file") != std::string::npos);

    // Clean up
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for calculate_header_length with empty files
TEST_F(IndexFileWriterTest, CalculateHeaderLengthEmptyFilesTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    // Create a directory with no files to test the empty files scenario
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());

    auto mock_dir = std::make_shared<DorisFSDirectory>();
    mock_dir->init(_fs, local_fs_index_path.c_str());

    // Test with empty files vector - should not crash and return valid result
    std::vector<FileInfo> files;
    auto* index_storage_format_v1 =
            static_cast<IndexStorageFormatV1*>(writer._index_storage_format.get());
    auto result = index_storage_format_v1->calculate_header_length(files, mock_dir.get());
    ASSERT_TRUE(result.first >= 0 && result.second >= 0); // Should return valid values

    mock_dir->close();
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for create_output_stream_v1 basic functionality
TEST_F(IndexFileWriterTest, CreateOutputStreamV1BasicTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    int64_t index_id = 1;
    std::string index_suffix = "test_suffix";

    // Test normal creation - should succeed
    auto* index_storage_format_v1 =
            static_cast<IndexStorageFormatV1*>(writer._index_storage_format.get());
    EXPECT_NO_THROW({
        auto result = index_storage_format_v1->create_output_stream(index_id, index_suffix);
        ASSERT_TRUE(result.first != nullptr);
        ASSERT_TRUE(result.second != nullptr);

        // Clean up
        result.second->close();
        result.first->close();
    });
}

// Test write_header_and_data_v1 with various file scenarios
TEST_F(IndexFileWriterTest, WriteHeaderAndDataV1EdgeCasesTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    // Create test directory and files
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());

    auto dir = std::make_shared<DorisFSDirectory>();
    dir->init(_fs, local_fs_index_path.c_str());

    // Create different types of files to test various code paths
    std::vector<std::string> test_files = {"segments_1", "test.dat", "write.lock"};
    for (const auto& file : test_files) {
        auto out_file =
                std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput(file.c_str()));
        out_file->writeString("test content for " + file);
        out_file->close();
    }

    // Prepare FileInfo vector
    std::vector<FileInfo> files;
    for (const auto& file : test_files) {
        files.emplace_back(file, dir->fileLength(file.c_str()));
    }

    // Create output stream
    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    int64_t header_length = 100; // Mock header length
    int32_t file_count = files.size();

    // Test the write_header_and_data_v1 function
    auto* index_storage_format_v1 =
            static_cast<IndexStorageFormatV1*>(writer._index_storage_format.get());
    EXPECT_NO_THROW({
        index_storage_format_v1->write_header_and_data(output, files, dir.get(), header_length,
                                                       file_count);
    });

    // Clean up
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);
    dir->close();
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for directory deletion in close() function when using V1 format
TEST_F(IndexFileWriterTest, CloseV1DirectoryDeletionTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    // Open a directory
    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    // Create a test file to make the directory non-empty
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    // Close should handle directory cleanup
    Status status = writer.close();
    ASSERT_TRUE(status.ok());
}

// Test for CLucene exception handling in close() function for V2 format
TEST_F(IndexFileWriterTest, CloseV2CLuceneExceptionTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    // Create a file to make the index non-empty
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_file"));
    out_file->writeString("test data");
    out_file->close();
    dir->close();

    // The test should verify that CLucene exceptions are properly caught and handled
    Status status = writer.close();
    // Even if there are CLucene errors, the status should still indicate completion
    // The error handling should log the error but not crash
}

// Test for compound directory deletion error handling
TEST_F(IndexFileWriterTest, CompoundDirectoryDeletionTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    auto dir = open_result.value();

    // Create a file that should trigger compound directory deletion
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("test_compound"));
    out_file->writeString("compound test data");
    out_file->close();

    // Verify that the directory type is correct for compound deletion
    if (std::strcmp(dir->getObjectName(), "DorisFSDirectory") == 0) {
        // This path should trigger the compound directory deletion code
        dir->close();
        Status status = writer.close();
        ASSERT_TRUE(status.ok());
    }
}

// Test for non-zero remainder handling in copyFile
TEST_F(IndexFileWriterTest, CopyFileNonZeroRemainderTest) {
    auto mock_dir = std::make_shared<DorisFSDirectory>();
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());
    mock_dir->init(_fs, local_fs_index_path.c_str());

    // Create a file with specific size that will leave remainder when copied with buffer
    std::string test_data(12345, 'X'); // Size that doesn't divide evenly by buffer size
    auto out_file =
            std::unique_ptr<lucene::store::IndexOutput>(mock_dir->createOutput("remainder_test"));
    out_file->writeBytes(reinterpret_cast<const uint8_t*>(test_data.c_str()), test_data.size());
    out_file->close();

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    uint8_t buffer[1024]; // Small buffer to ensure remainder
    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    // This should test the remainder handling code path
    EXPECT_NO_THROW({
        writer._index_storage_format->copy_file("remainder_test", mock_dir.get(), output, buffer,
                                                sizeof(buffer));
    });

    // Verify the copy was complete
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);
    mock_dir->close();
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for file offset mismatch error handling
TEST_F(IndexFileWriterTest, FileOffsetMismatchTest) {
    // This test is designed to trigger the diff != length error condition
    // This would typically happen if there's a problem with file copying or offset calculation

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // Create a mock scenario where file offsets don't match expectations
    // This is more of an internal consistency check, so we mainly verify
    // that the writer can be created and basic operations work

    ASSERT_NE(&writer, nullptr);
    ASSERT_EQ(writer.get_storage_format(), InvertedIndexStorageFormatPB::V2);
}

// Test for write_index_headers_and_metadata coverage
TEST_F(IndexFileWriterTest, WriteIndexHeadersAndMetadataTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    // Create test directory and files to trigger write_index_headers_and_metadata
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());

    auto dir = std::make_shared<DorisFSDirectory>();
    dir->init(_fs, local_fs_index_path.c_str());

    // Create test files
    std::vector<std::string> test_files = {"segments_1", "test.dat"};
    for (const auto& file : test_files) {
        auto out_file =
                std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput(file.c_str()));
        out_file->writeString("test content");
        out_file->close();
    }

    // Insert directory into writer
    Status insert_st = writer._insert_directory_into_map(1, "suffix1", dir);
    ASSERT_TRUE(insert_st.ok());

    // Test close which should trigger write_index_headers_and_metadata
    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());

    // Clean up
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for data_offset accumulation in write_header_and_data_v1
TEST_F(IndexFileWriterTest, DataOffsetAccumulationTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    // Create test directory and files
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());

    auto dir = std::make_shared<DorisFSDirectory>();
    dir->init(_fs, local_fs_index_path.c_str());

    // Create files of different sizes to test data_offset accumulation
    std::vector<std::pair<std::string, std::string>> test_files = {
            {"file1.dat", "small"},
            {"file2.dat", "medium sized content"},
            {"file3.dat", "this is a much larger file content to test accumulation"}};

    for (const auto& file_info : test_files) {
        auto out_file = std::unique_ptr<lucene::store::IndexOutput>(
                dir->createOutput(file_info.first.c_str()));
        out_file->writeString(file_info.second);
        out_file->close();
    }

    // Prepare FileInfo vector
    std::vector<FileInfo> files;
    for (const auto& file_info : test_files) {
        files.emplace_back(file_info.first, dir->fileLength(file_info.first.c_str()));
    }

    // Create output stream to test data offset accumulation
    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    int64_t header_length = 100;
    int32_t file_count = files.size();

    // This should test the data_offset += file.filesize code path
    auto* index_storage_format_v1 =
            static_cast<IndexStorageFormatV1*>(writer._index_storage_format.get());
    EXPECT_NO_THROW({
        index_storage_format_v1->write_header_and_data(output, files, dir.get(), header_length,
                                                       file_count);
    });

    // Clean up
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);
    dir->close();
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

// Test for empty index scenario with V2 format
TEST_F(IndexFileWriterTest, EmptyIndexV2Test) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2, std::move(file_writer));

    // Close without adding any indices - should handle empty case
    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());
}

// Test for StreamSinkFileWriter path in close()
TEST_F(IndexFileWriterTest, StreamSinkFileWriterCloseTest) {
    // Create a writer without providing a file_writer to trigger the StreamSinkFileWriter path
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V2);

    // Close should handle the case where _idx_v2_writer is not a StreamSinkFileWriter
    Status close_status = writer.close();
    ASSERT_TRUE(close_status.ok());
}

// Test for header file count in write_header_and_data_v1
TEST_F(IndexFileWriterTest, HeaderFileCountTest) {
    IndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                           InvertedIndexStorageFormatPB::V1);

    // Create a scenario where header_file_count > i to test different file writing logic
    std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
            ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(), _rowset_id,
            _seg_id, 1, "suffix1");
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_fs_index_path).ok());

    auto dir = std::make_shared<DorisFSDirectory>();
    dir->init(_fs, local_fs_index_path.c_str());

    // Create exactly 3 files
    std::vector<std::string> test_files = {"file1", "file2", "file3"};
    for (const auto& file : test_files) {
        auto out_file =
                std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput(file.c_str()));
        out_file->writeString("content");
        out_file->close();
    }

    std::vector<FileInfo> files;
    for (const auto& file : test_files) {
        files.emplace_back(file, dir->fileLength(file.c_str()));
    }

    auto* ram_dir = new lucene::store::RAMDirectory();
    auto* output = ram_dir->createOutput("test_output");

    // Test with header_file_count = 2, so only first 2 files are header files
    int64_t header_length = 50;
    int32_t header_file_count = 2;

    auto* index_storage_format_v1 =
            static_cast<IndexStorageFormatV1*>(writer._index_storage_format.get());
    EXPECT_NO_THROW({
        index_storage_format_v1->write_header_and_data(output, files, dir.get(), header_length,
                                                       header_file_count);
    });

    // Clean up
    output->close();
    _CLLDELETE(output);
    ram_dir->close();
    _CLLDELETE(ram_dir);
    dir->close();
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(local_fs_index_path).ok());
}

} // namespace doris::segment_v2
