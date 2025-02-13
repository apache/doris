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

#include "olap/rowset/segment_v2/inverted_index_file_writer.h"

#include <gmock/gmock.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"

namespace doris::segment_v2 {

using namespace doris::vectorized;

class InvertedIndexFileWriterTest : public ::testing::Test {
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

TEST_F(InvertedIndexFileWriterTest, InitializeTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                   InvertedIndexStorageFormatPB::V2);

    InvertedIndexDirectoryMap indices_dirs;
    indices_dirs.emplace(std::make_pair(1, "suffix1"), std::make_unique<DorisFSDirectory>());
    indices_dirs.emplace(std::make_pair(2, "suffix2"), std::make_unique<DorisFSDirectory>());

    Status status = writer.initialize(indices_dirs);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(writer.get_storage_format(), InvertedIndexStorageFormatPB::V2);
}

TEST_F(InvertedIndexFileWriterTest, OpenTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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

TEST_F(InvertedIndexFileWriterTest, DeleteIndexTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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

TEST_F(InvertedIndexFileWriterTest, WriteV1Test) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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

TEST_F(InvertedIndexFileWriterTest, WriteV2Test) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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

TEST_F(InvertedIndexFileWriterTest, HeaderLengthTest) {
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
    auto insertDirectory = [&](InvertedIndexFileWriter& writer, int64_t index_id,
                               const std::string& suffix,
                               std::shared_ptr<DorisFSDirectory>& mock_dir) {
        Status st = writer._insert_directory_into_map(index_id, suffix, mock_dir);
        if (!st.ok()) {
            std::cerr << "_insert_directory_into_map error in HeaderLengthTest: " << st.msg()
                      << std::endl;
            assert(false);
            return;
        }
    };

    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                   InvertedIndexStorageFormatPB::V2);
    insertDirectory(writer, 1, "suffix1", mock_dir1);
    insertDirectory(writer, 2, "suffix2", mock_dir2);

    int64_t header_length = writer.headerLength();

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

TEST_F(InvertedIndexFileWriterTest, PrepareSortedFilesTest) {
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

    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                   InvertedIndexStorageFormatPB::V2);
    auto st = writer._insert_directory_into_map(1, "suffix1", mock_dir);
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in PrepareSortedFilesTest: " << st.msg()
                  << std::endl;
        ASSERT_TRUE(false);
        return;
    }

    std::vector<FileInfo> sorted_files =
            writer.prepare_sorted_files(writer._indices_dirs[std::make_pair(1, "suffix1")].get());

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

TEST_F(InvertedIndexFileWriterTest, PrepareFileMetadataTest) {
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

    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                   InvertedIndexStorageFormatPB::V2);
    auto st = writer._insert_directory_into_map(1, "suffix1", mock_dir);
    if (!st.ok()) {
        std::cerr << "_insert_directory_into_map error in PrepareFileMetadataTest: " << st.msg()
                  << std::endl;
        ASSERT_TRUE(false);
        return;
    }

    int64_t current_offset = 0;

    std::vector<InvertedIndexFileWriter::FileMetadata> file_metadata =
            writer.prepare_file_metadata(current_offset);

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

/*TEST_F(InvertedIndexFileWriterTest, CopyFileTest_OpenInputFailure) {
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
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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
class InvertedIndexFileWriterMock : public InvertedIndexFileWriter {
public:
    InvertedIndexFileWriterMock(const io::FileSystemSPtr& fs, const std::string& index_path_prefix,
                                const std::string& rowset_id, int32_t segment_id,
                                InvertedIndexStorageFormatPB storage_format)
            : InvertedIndexFileWriter(fs, index_path_prefix, rowset_id, segment_id,
                                      storage_format) {}

    MOCK_METHOD(void, write_header_and_data_v1,
                (lucene::store::IndexOutput * output, const std::vector<FileInfo>& files,
                 lucene::store::Directory* dir, int64_t header_length, int32_t file_count),
                (override));
};
TEST_F(InvertedIndexFileWriterTest, WriteV1ExceptionHandlingTest) {
    InvertedIndexFileWriterMock writer_mock(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                            InvertedIndexStorageFormatPB::V1);

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
    EXPECT_CALL(writer_mock, write_header_and_data_v1(::testing::_, ::testing::_, ::testing::_,
                                                      ::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));

    Status status = writer_mock.write_v1();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}
class InvertedIndexFileWriterMockV2 : public InvertedIndexFileWriter {
public:
    InvertedIndexFileWriterMockV2(const io::FileSystemSPtr& fs,
                                  const std::string& index_path_prefix,
                                  const std::string& rowset_id, int32_t segment_id,
                                  InvertedIndexStorageFormatPB storage_format,
                                  io::FileWriterPtr file_writer)
            : InvertedIndexFileWriter(fs, index_path_prefix, rowset_id, segment_id, storage_format,
                                      std::move(file_writer)) {}

    MOCK_METHOD(void, write_index_headers_and_metadata,
                (lucene::store::IndexOutput * compound_file_output,
                 const std::vector<FileMetadata>& file_metadata),
                (override));
};

TEST_F(InvertedIndexFileWriterTest, WriteV2ExceptionHandlingTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    InvertedIndexFileWriterMockV2 writer_mock(_fs, _index_path_prefix, _rowset_id, _seg_id,
                                              InvertedIndexStorageFormatPB::V2,
                                              std::move(file_writer));

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

    EXPECT_CALL(writer_mock, write_index_headers_and_metadata(::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));

    Status status = writer_mock.write();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
}

class InvertedIndexFileWriterMockCreateOutputStreamV2 : public InvertedIndexFileWriter {
public:
    InvertedIndexFileWriterMockCreateOutputStreamV2(const io::FileSystemSPtr& fs,
                                                    const std::string& index_path_prefix,
                                                    const std::string& rowset_id,
                                                    int32_t segment_id,
                                                    InvertedIndexStorageFormatPB storage_format)
            : InvertedIndexFileWriter(fs, index_path_prefix, rowset_id, segment_id,
                                      storage_format) {}

    MOCK_METHOD((std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                           std::unique_ptr<lucene::store::IndexOutput>>),
                create_output_stream, (), (override));
};

class InvertedIndexFileWriterMockCreateOutputStreamV1 : public InvertedIndexFileWriter {
public:
    InvertedIndexFileWriterMockCreateOutputStreamV1(const io::FileSystemSPtr& fs,
                                                    const std::string& index_path_prefix,
                                                    const std::string& rowset_id,
                                                    int32_t segment_id,
                                                    InvertedIndexStorageFormatPB storage_format)
            : InvertedIndexFileWriter(fs, index_path_prefix, rowset_id, segment_id,
                                      storage_format) {}

    MOCK_METHOD((std::pair<std::unique_ptr<lucene::store::Directory, DirectoryDeleter>,
                           std::unique_ptr<lucene::store::IndexOutput>>),
                create_output_stream_v1, (int64_t index_id, const std::string& index_suffix),
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

TEST_F(InvertedIndexFileWriterTest, WriteV1OutputTest) {
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    InvertedIndexFileWriterMockCreateOutputStreamV1 writer_mock(
            _fs, _index_path_prefix, _rowset_id, _seg_id, InvertedIndexStorageFormatPB::V1);
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

    EXPECT_CALL(writer_mock, create_output_stream_v1(index_id, index_suffix))
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

TEST_F(InvertedIndexFileWriterTest, WriteV2OutputTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    InvertedIndexFileWriterMockCreateOutputStreamV2 writer_mock(
            _fs, _index_path_prefix, _rowset_id, _seg_id, InvertedIndexStorageFormatPB::V2);
    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v2 = new MockFSIndexOutputV2();
    EXPECT_CALL(*mock_output_v2, flushBuffer(::testing::_, ::testing::_))
            .WillOnce(::testing::Throw(CLuceneError(CL_ERR_IO, "Simulated exception", false)));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutputV2>(mock_output_v2);
    compound_file_output->init(file_writer.get());

    EXPECT_CALL(writer_mock, create_output_stream())
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

TEST_F(InvertedIndexFileWriterTest, WriteV2OutputCloseErrorTest) {
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());

    InvertedIndexFileWriterMockCreateOutputStreamV2 writer_mock(
            _fs, _index_path_prefix, _rowset_id, _seg_id, InvertedIndexStorageFormatPB::V2);
    auto* out_dir = DorisFSDirectoryFactory::getDirectory(_fs, index_path.c_str());
    std::unique_ptr<lucene::store::Directory, DirectoryDeleter> out_dir_ptr(out_dir);
    auto* mock_output_v2 = new MockFSIndexOutputCloseV2();
    EXPECT_CALL(*mock_output_v2, close()).WillOnce(::testing::Invoke([&]() {
        mock_output_v2->base_close();
        throw CLuceneError(CL_ERR_IO, "Simulated exception", false);
    }));
    auto compound_file_output = std::unique_ptr<DorisFSDirectory::FSIndexOutputV2>(mock_output_v2);
    compound_file_output->init(file_writer.get());

    EXPECT_CALL(writer_mock, create_output_stream())
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

TEST_F(InvertedIndexFileWriterTest, WriteV1OutputCloseErrorTest) {
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    InvertedIndexFileWriterMockCreateOutputStreamV1 writer_mock(
            _fs, _index_path_prefix, _rowset_id, _seg_id, InvertedIndexStorageFormatPB::V1);
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

    EXPECT_CALL(writer_mock, create_output_stream_v1(index_id, index_suffix))
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

class MockInvertedIndexFileWriter : public InvertedIndexFileWriter {
public:
    MOCK_METHOD(Result<std::unique_ptr<IndexSearcherBuilder>>, _construct_index_searcher_builder,
                (const DorisCompoundReader* dir), (override));
    MockInvertedIndexFileWriter(io::FileSystemSPtr fs, const std::string& index_path_prefix,
                                const std::string& rowset_id, int64_t seg_id,
                                InvertedIndexStorageFormatPB storage_format,
                                io::FileWriterPtr file_writer)
            : InvertedIndexFileWriter(fs, index_path_prefix, rowset_id, seg_id, storage_format,
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

TEST_F(InvertedIndexFileWriterTest, AddIntoSearcherCacheTest) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockInvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id,
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
                        _CLDECDELETE(directory)
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

TEST_F(InvertedIndexFileWriterTest, CacheEvictionTest) {
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
    MockInvertedIndexFileWriter writer1(_fs, _index_path_prefix + "_1", "rowset1", 1,
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
                        _CLDECDELETE(directory)
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer1, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder1))));

    Status close_status1 = writer1.close();
    ASSERT_TRUE(close_status1.ok());

    MockInvertedIndexFileWriter writer2(_fs, _index_path_prefix + "_2", "rowset2", 2,
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
                        _CLDECDELETE(directory)
                        return bkd_reader;
                    }));

    EXPECT_CALL(writer2, _construct_index_searcher_builder(testing::_))
            .WillOnce(testing::Return(testing::ByMove(std::move(mock_builder2))));

    Status close_status2 = writer2.close();
    ASSERT_TRUE(close_status2.ok());

    MockInvertedIndexFileWriter writer3(_fs, _index_path_prefix + "_3", "rowset3", 3,
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
                        _CLDECDELETE(directory)
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

TEST_F(InvertedIndexFileWriterTest, CacheUpdateTest) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_path =
            InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix + "_update");
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockInvertedIndexFileWriter writer(_fs, _index_path_prefix + "_update", "rowset_update", 3,
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
                        _CLDECDELETE(directory)
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
    MockInvertedIndexFileWriter writer_new(_fs, _index_path_prefix + "_update", "rowset_update", 3,
                                           InvertedIndexStorageFormatPB::V2,
                                           std::move(file_writer_new));

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
                        _CLDECDELETE(directory)
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

TEST_F(InvertedIndexFileWriterTest, AddIntoSearcherCacheV1Test) {
    config::enable_write_index_searcher_cache = true;
    io::FileWriterPtr file_writer;
    std::string index_suffix = "suffix_v1";
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v1(
            _index_path_prefix + "_v1", 4, index_suffix);
    io::FileWriterOptions opts;
    Status st = _fs->create_file(index_path, &file_writer, &opts);
    ASSERT_TRUE(st.ok());
    MockInvertedIndexFileWriter writer(_fs, _index_path_prefix + "_v1", "rowset_v1", 4,
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
                        _CLDECDELETE(directory)
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

} // namespace doris::segment_v2
