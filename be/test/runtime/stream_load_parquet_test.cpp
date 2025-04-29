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

#include "runtime/load_path_mgr.h"
#include "runtime/exec_env.h"
#include "gtest/gtest.h"
#include "olap/storage_engine.h"
namespace doris {

    class LoadPathMgrTest : public testing::Test {
    protected:
        void SetUp() override {
            _exec_env = ExecEnv::GetInstance();
            _load_path_mgr = std::make_unique<LoadPathMgr>(_exec_env);

            // 创建临时测试目录
            _test_dir = "/tmp/test_clean_file";
            io::global_local_filesystem()->delete_directory_or_file(_test_dir);
            io::global_local_filesystem()->create_directory(_test_dir);
        }

        void TearDown() override {
            _load_path_mgr->stop();
            _exec_env->destroy();
        }

        ExecEnv* _exec_env;
        std::unique_ptr<LoadPathMgr> _load_path_mgr;
        std::string _test_dir;
    };

    TEST_F(LoadPathMgrTest, CheckDiskSpaceTest) {

        // Check disk space
        bool is_available = false;
        size_t disk_capacity_bytes = 10;
        size_t available_bytes = 9;
        int64_t file_bytes = 1;
        _load_path_mgr->check_disk_space(disk_capacity_bytes, available_bytes, file_bytes, &is_available);
        ASSERT_TRUE(is_available);

        // Check disk space
        is_available = false;
        disk_capacity_bytes = 10;
        available_bytes = 2;
        file_bytes = 1;
        _load_path_mgr->check_disk_space(disk_capacity_bytes, available_bytes, file_bytes, &is_available);
        ASSERT_FALSE(is_available);

        SUCCEED();
    }

    TEST_F(LoadPathMgrTest, NormalAllocation) {
        std::string prefix;
        Status status = _load_path_mgr->allocate_dir("tmp", "test_label", &prefix, 1024);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(prefix.empty());
    }
} // namespace doris