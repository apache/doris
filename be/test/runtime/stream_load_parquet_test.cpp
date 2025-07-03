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

#include "gtest/gtest.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
namespace doris {

class LoadPathMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();

        _load_path_mgr = std::make_unique<LoadPathMgr>(_exec_env);
        // create tmp file
        _test_dir = "/tmp/test_clean_file";
        _test_dir1 = "/tmp/test_clean_file/mini_download";
        _test_dir2 = "/tmp/test_clean_file1/mini_download/test.parquet";

        auto result = io::global_local_filesystem()->delete_directory_or_file(_test_dir1);
        result = io::global_local_filesystem()->create_directory(_test_dir1);
        EXPECT_TRUE(result.ok());

        result = io::global_local_filesystem()->delete_directory_or_file(_test_dir2);
        result = io::global_local_filesystem()->create_directory(_test_dir2);
        EXPECT_TRUE(result.ok());

        const_cast<std::vector<StorePath>&>(_exec_env->store_paths()).emplace_back(_test_dir, 1024);
    }

    void TearDown() override {
        const_cast<std::vector<StorePath>&>(_exec_env->store_paths()).clear();
        _load_path_mgr->stop();
        _exec_env->destroy();
    }

    ExecEnv* _exec_env;
    std::unique_ptr<LoadPathMgr> _load_path_mgr;
    std::string _test_dir;
    std::string _test_dir1;
    std::string _test_dir2;
};

TEST_F(LoadPathMgrTest, CheckDiskSpaceTest) {
    // Check disk space
    bool is_available = false;
    size_t disk_capacity_bytes = 10;
    size_t available_bytes = 9;
    int64_t file_bytes = 1;
    is_available =
            _load_path_mgr->check_disk_space(disk_capacity_bytes, available_bytes, file_bytes);
    ASSERT_TRUE(is_available);

    // Check disk space
    is_available = false;
    disk_capacity_bytes = 10;
    available_bytes = 2;
    file_bytes = 1;
    is_available =
            _load_path_mgr->check_disk_space(disk_capacity_bytes, available_bytes, file_bytes);
    ASSERT_FALSE(is_available);

    std::string prefix;
    Status status = _load_path_mgr->allocate_dir("tmp", "test_label1", &prefix, 1);
    EXPECT_TRUE(status.ok());
    std::cout << "NormalAllocation: " << prefix.size() << std::endl;
    EXPECT_FALSE(prefix.empty());

    prefix.clear();
    status = _load_path_mgr->allocate_dir("tmp", "test_label2", &prefix, 999999999999999999);
    EXPECT_TRUE(!status.ok());
    std::cout << "UnNormalAllocation: " << prefix.size() << std::endl;
    EXPECT_TRUE(prefix.empty());

    std::cout << "clean_tmp_files" << std::endl;
    bool exists = false;
    status = io::global_local_filesystem()->exists(_test_dir2, &exists);
    EXPECT_TRUE(exists);
    _load_path_mgr->clean_tmp_files(_test_dir2);
    status = io::global_local_filesystem()->exists(_test_dir2, &exists);
    EXPECT_FALSE(exists);
}

} // namespace doris