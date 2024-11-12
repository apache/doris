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

#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"

namespace doris {
namespace segment_v2 {

using namespace doris::vectorized;

class InvertedIndexFileWriterTest : public ::testing::Test {
protected:
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
        int64_t inverted_index_cache_limit = 0;
        _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit,
                                                                   256));

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
        _storage_format = InvertedIndexStorageFormatPB::V2;

        _indices_dirs = InvertedIndexDirectoryMap();
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
    InvertedIndexStorageFormatPB _storage_format;
    InvertedIndexDirectoryMap _indices_dirs;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;

    constexpr static uint32_t MAX_PATH_LEN = 1024;
    constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_file_writer_test";
    constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
};

TEST_F(InvertedIndexFileWriterTest, InitializeTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, _storage_format);

    InvertedIndexDirectoryMap indices_dirs;
    indices_dirs.emplace(std::make_pair(1, "suffix1"), std::make_unique<DorisFSDirectory>());
    indices_dirs.emplace(std::make_pair(2, "suffix2"), std::make_unique<DorisFSDirectory>());

    Status status = writer.initialize(indices_dirs);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(writer.get_storage_format(), _storage_format);
}

TEST_F(InvertedIndexFileWriterTest, OpenTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, _storage_format);

    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    auto index_meta = create_mock_tablet_index(index_id, index_suffix);
    ASSERT_NE(index_meta, nullptr);

    auto open_result = writer.open(index_meta.get());
    ASSERT_TRUE(open_result.has_value());
    DorisFSDirectory* dir = open_result.value();
    ASSERT_NE(dir, nullptr);

    auto key = std::make_pair(index_id, index_suffix);
    ASSERT_TRUE(writer._indices_dirs.find(key) != writer._indices_dirs.end());
    ASSERT_TRUE(writer._indices_dirs.find(key)->second.get() == dir);
}

TEST_F(InvertedIndexFileWriterTest, DeleteIndexTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, _storage_format);

    InvertedIndexDirectoryMap indices_dirs;
    int64_t index_id = 1;
    std::string index_suffix = "suffix1";
    indices_dirs.emplace(std::make_pair(index_id, index_suffix),
                         std::make_unique<DorisFSDirectory>());
    Status init_status = writer.initialize(indices_dirs);
    ASSERT_TRUE(init_status.ok());
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
    DorisFSDirectory* dir = open_result.value();
    auto out_file = std::unique_ptr<lucene::store::IndexOutput>(dir->createOutput("write_v1_test"));
    out_file->writeString("test1");
    out_file->close();
    dir->close();

    Status close_status = writer.close();
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
    DorisFSDirectory* dir_1 = open_result_1.value();
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
    DorisFSDirectory* dir_2 = open_result_2.value();
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
/*TEST_F(InvertedIndexFileWriterTest, OpenTest_CreateOutputFailure) {
            InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, InvertedIndexStorageFormatPB::V1);
            std::string local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                    ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir().native(),
                    _rowset_id, _seg_id, 1, "suffix1");
            _mock_dir->init(_fs, local_fs_index_path.c_str());
            EXPECT_CALL(*_mock_dir, createOutput(::testing::_))
                    .WillOnce(::testing::Return(nullptr));

            InvertedIndexDirectoryMap indices_dirs;
            indices_dirs.emplace(std::make_pair(1, "suffix1"), std::move(_mock_dir));

            Status status = writer.initialize(indices_dirs);
            ASSERT_TRUE(status.ok());
            Status close_status = writer.close();
            ASSERT_TRUE(close_status.ok());
        }*/
TEST_F(InvertedIndexFileWriterTest, HeaderLengthTest) {
    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, _storage_format);
    InvertedIndexDirectoryMap indices_dirs;
    auto mock_dir1 = std::make_unique<DorisFSDirectory>();
    auto mock_dir2 = std::make_unique<DorisFSDirectory>();
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
    indices_dirs.emplace(std::make_pair(1, "suffix1"), std::move(mock_dir1));
    indices_dirs.emplace(std::make_pair(2, "suffix2"), std::move(mock_dir2));

    Status init_status = writer.initialize(indices_dirs);
    ASSERT_TRUE(init_status.ok());

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
class MockDorisFSDirectoryFileLength : public DorisFSDirectory {
public:
    //MOCK_METHOD(lucene::store::IndexOutput*, createOutput, (const char* name), (override));
    MOCK_METHOD(int64_t, fileLength, (const char* name), (const, override));
    //MOCK_METHOD(void, close, (), (override));
    //MOCK_METHOD(const char*, getObjectName, (), (const, override));
};
TEST_F(InvertedIndexFileWriterTest, PrepareSortedFilesTest) {
    auto mock_dir = std::make_unique<MockDorisFSDirectoryFileLength>();
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

    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.segments")))
            .WillOnce(testing::Return(1000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.fnm"))).WillOnce(testing::Return(2000));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("0.tii"))).WillOnce(testing::Return(1500));
    EXPECT_CALL(*mock_dir, fileLength(testing::StrEq("nullbitmap"))).WillOnce(testing::Return(500));

    _indices_dirs.emplace(std::make_pair(1, "suffix1"), std::move(mock_dir));

    InvertedIndexFileWriter writer(_fs, _index_path_prefix, _rowset_id, _seg_id, _storage_format);

    Status init_status = writer.initialize(_indices_dirs);
    ASSERT_TRUE(init_status.ok());

    std::vector<FileInfo> sorted_files =
            writer.prepare_sorted_files(writer._indices_dirs[std::make_pair(1, "suffix1")].get());

    // 1. segments1.dat (priority 1, size 1000)
    // 2. fnm1.dat (priority 2, size 2000)
    // 3. tii1.dat (priority 3, size 1500)
    // 4. other1.dat (priority 4, size 500)

    std::vector<std::string> expected_order = {"0.segments", "0.fnm", "0.tii", "nullbitmap"};
    ASSERT_EQ(sorted_files.size(), expected_order.size());

    for (size_t i = 0; i < expected_order.size(); ++i) {
        EXPECT_EQ(sorted_files[i].filename, expected_order[i]);
        if (sorted_files[i].filename == "0.segments") {
            EXPECT_EQ(sorted_files[i].filesize, 1000);
        } else if (sorted_files[i].filename == "0.fnm") {
            EXPECT_EQ(sorted_files[i].filesize, 2000);
        } else if (sorted_files[i].filename == "0.tii") {
            EXPECT_EQ(sorted_files[i].filesize, 1500);
        } else if (sorted_files[i].filename == "nullbitmap") {
            EXPECT_EQ(sorted_files[i].filesize, 500);
        }
    }
}
} // namespace segment_v2
} // namespace doris