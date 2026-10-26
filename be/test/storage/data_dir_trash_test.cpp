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

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"

namespace doris {

class DataDirTrashTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_path_ = "ut_dir/data_dir_trash_test";
        auto&& fs = io::global_local_filesystem();
        auto st = fs->delete_directory(dir_path_);
        ASSERT_TRUE(st.ok()) << st;
        st = fs->create_directory(dir_path_);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        auto&& fs = io::global_local_filesystem();
        auto st = fs->delete_directory(dir_path_);
        ASSERT_TRUE(st.ok()) << st;
    }

    std::string dir_path_;
};

TEST_F(DataDirTrashTest, TrashFileNumWithNoTrashDir) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    EXPECT_EQ(0, info.trash_file_num);
}

TEST_F(DataDirTrashTest, TrashFileNumWithEmptyTrashDir) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    auto trash_path = fmt::format("{}/{}", dir_path_, TRASH_PREFIX);
    auto&& fs = io::global_local_filesystem();
    st = fs->create_directory(trash_path);
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    EXPECT_EQ(0, info.trash_file_num);
}

TEST_F(DataDirTrashTest, TrashFileNumWithDirectories) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    auto trash_path = fmt::format("{}/{}", dir_path_, TRASH_PREFIX);
    auto&& fs = io::global_local_filesystem();
    st = fs->create_directory(trash_path);
    ASSERT_TRUE(st.ok()) << st;

    st = fs->create_directory(trash_path + "/20260101_0");
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(trash_path + "/20260102_0");
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(trash_path + "/20260103_0");
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    EXPECT_EQ(3, info.trash_file_num);
}

TEST_F(DataDirTrashTest, TrashFileNumIgnoresFiles) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    auto trash_path = fmt::format("{}/{}", dir_path_, TRASH_PREFIX);
    auto&& fs = io::global_local_filesystem();
    st = fs->create_directory(trash_path);
    ASSERT_TRUE(st.ok()) << st;

    st = fs->create_directory(trash_path + "/20260101_0");
    ASSERT_TRUE(st.ok()) << st;

    // Write a file to ensure trash_used_capacity > 0
    std::unique_ptr<io::FileWriter> writer;
    st = fs->create_file(trash_path + "/some_file.txt", &writer);
    ASSERT_TRUE(st.ok()) << st;
    std::string test_data = "test data for trash";
    st = writer->append(test_data);
    ASSERT_TRUE(st.ok()) << st;
    st = writer->close();
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    // File num should be 1 because only the directory is counted
    EXPECT_EQ(1, info.trash_file_num);
    // Capacity should be > 0 because we wrote data into the file
    EXPECT_GT(info.trash_used_capacity, 0);
}

TEST_F(DataDirTrashTest, DataDirInfoContainsTrashFileNum) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    auto trash_path = fmt::format("{}/{}", dir_path_, TRASH_PREFIX);
    auto&& fs = io::global_local_filesystem();
    st = fs->create_directory(trash_path);
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(trash_path + "/20260101_0");
    ASSERT_TRUE(st.ok()) << st;

    // Write a dummy file to ensure trash_used_capacity > 0
    std::unique_ptr<io::FileWriter> writer;
    st = fs->create_file(trash_path + "/20260101_0/dummy_file.txt", &writer);
    ASSERT_TRUE(st.ok()) << st;
    std::string test_data = "test trash data to ensure capacity > 0";
    st = writer->append(test_data);
    ASSERT_TRUE(st.ok()) << st;
    st = writer->close();
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    EXPECT_EQ(1, info.trash_file_num);
    EXPECT_GT(info.trash_used_capacity, 0);
}

TEST_F(DataDirTrashTest, TrashFileNumWithNestedDirectories) {
    StorageEngine engine({});
    DataDir data_dir(engine, dir_path_);
    auto st = data_dir._init_meta();
    ASSERT_TRUE(st.ok()) << st;

    auto trash_path = fmt::format("{}/{}", dir_path_, TRASH_PREFIX);
    auto&& fs = io::global_local_filesystem();
    st = fs->create_directory(trash_path);
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(trash_path + "/20260101_0");
    ASSERT_TRUE(st.ok()) << st;
    st = fs->create_directory(trash_path + "/20260101_0/sub_dir");
    ASSERT_TRUE(st.ok()) << st;

    data_dir.update_trash_capacity();
    DataDirInfo info = data_dir.get_dir_info();
    // Assuming your implementation counts only top-level directories in trash
    EXPECT_EQ(1, info.trash_file_num);
}

} // namespace doris
