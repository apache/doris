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

#include "agent/cgroups_mgr.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

StorageEngine* k_engine = nullptr;

class CgroupsMgrTest : public testing::Test {
public:
    // create a mock cgroup folder
    static void SetUpTestCase() {
        EXPECT_TRUE(std::filesystem::remove_all(_s_cgroup_path));
        // create a mock cgroup path
        EXPECT_TRUE(std::filesystem::create_directory(_s_cgroup_path));

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        Status s = doris::StorageEngine::open(options, &k_engine);
        EXPECT_TRUE(s.ok()) << s.to_string();
    }

    // delete the mock cgroup folder
    static void TearDownTestCase() { EXPECT_TRUE(std::filesystem::remove_all(_s_cgroup_path)); }

    // test if a file contains specific number
    static bool does_contain_number(const std::string& file_path, int32_t number) {
        std::ifstream input_file(file_path.c_str());
        int32_t task_id;
        while (input_file >> task_id) {
            if (task_id == number) {
                return true;
            }
        }
        return false;
    }

    static std::string _s_cgroup_path;
    static CgroupsMgr _s_cgroups_mgr;
};

std::string CgroupsMgrTest::_s_cgroup_path = "./doris_cgroup_testxxxx123";
CgroupsMgr CgroupsMgrTest::_s_cgroups_mgr(nullptr, CgroupsMgrTest::_s_cgroup_path);

TEST_F(CgroupsMgrTest, TestIsDirectory) {
    // test folder exist
    bool exist = _s_cgroups_mgr.is_directory(CgroupsMgrTest::_s_cgroup_path.c_str());
    EXPECT_TRUE(exist);
    // test folder not exist
    bool not_exist = _s_cgroups_mgr.is_directory("./abc");
    EXPECT_FALSE(not_exist);
    // test file exist, but not folder
    bool not_folder = _s_cgroups_mgr.is_directory("/etc/profile");
    EXPECT_FALSE(not_folder);
}

TEST_F(CgroupsMgrTest, TestIsFileExist) {
    // test file exist
    bool exist = _s_cgroups_mgr.is_file_exist(CgroupsMgrTest::_s_cgroup_path.c_str());
    EXPECT_TRUE(exist);
    // test file not exist
    bool not_exist = _s_cgroups_mgr.is_file_exist("./abc");
    EXPECT_FALSE(not_exist);
}

TEST_F(CgroupsMgrTest, TestInitCgroups) {
    // test for task file not exist
    Status op_status = _s_cgroups_mgr.init_cgroups();
    EXPECT_EQ(Status::DORIS_ERROR, op_status);

    // create task file, then init should success
    std::string task_file_path = _s_cgroup_path + "/tasks";
    std::ofstream outfile(task_file_path.c_str());
    outfile << 1111111 << std::endl;
    outfile.close();

    // create a mock user under cgroup path
    EXPECT_TRUE(std::filesystem::create_directory(_s_cgroup_path + "/yiguolei"));
    std::ofstream user_out_file(_s_cgroup_path + "/yiguolei/tasks");
    user_out_file << 123 << std::endl;
    user_out_file.close();

    // create a mock user group under cgroup path
    EXPECT_TRUE(std::filesystem::create_directory(_s_cgroup_path + "/yiguolei/low"));
    std::ofstream group_out_file(CgroupsMgrTest::_s_cgroup_path + "/yiguolei/low/tasks");
    group_out_file << 456 << std::endl;
    group_out_file.close();

    op_status = _s_cgroups_mgr.init_cgroups();
    // init should be successful
    EXPECT_EQ(Status::OK(), op_status);
    // all tasks should be migrated to root cgroup path
    EXPECT_TRUE(does_contain_number(task_file_path, 1111111));
    EXPECT_TRUE(does_contain_number(task_file_path, 123));
    EXPECT_TRUE(does_contain_number(task_file_path, 456));
}

TEST_F(CgroupsMgrTest, TestAssignThreadToCgroups) {
    // default cgroup not exist, so that assign to an unknown user will fail
    Status op_status = _s_cgroups_mgr.assign_thread_to_cgroups(111, "abc", "low");
    EXPECT_EQ(Status::DORIS_ERROR, op_status);
    // user cgroup exist
    // create a mock user under cgroup path
    EXPECT_TRUE(std::filesystem::create_directory(_s_cgroup_path + "/yiguolei2"));
    std::ofstream user_out_file(_s_cgroup_path + "/yiguolei2/tasks");
    user_out_file << 123 << std::endl;
    user_out_file.close();

    op_status = _s_cgroups_mgr.assign_thread_to_cgroups(111, "yiguolei2", "aaaa");
    EXPECT_EQ(Status::OK(), op_status);
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/yiguolei2/tasks", 111));

    // user,level cgroup exist
    // create a mock user group under cgroup path
    EXPECT_TRUE(std::filesystem::create_directory(_s_cgroup_path + "/yiguolei2/low"));
    std::ofstream group_out_file(_s_cgroup_path + "/yiguolei2/low/tasks");
    group_out_file << 456 << std::endl;
    group_out_file.close();

    op_status = _s_cgroups_mgr.assign_thread_to_cgroups(111, "yiguolei2", "low");
    EXPECT_EQ(Status::OK(), op_status);
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/yiguolei2/low/tasks", 111));
}

TEST_F(CgroupsMgrTest, TestModifyUserCgroups) {
    std::map<std::string, int32_t> user_share;
    std::map<std::string, int32_t> level_share;
    user_share["cpu.shares"] = 100;
    level_share["low"] = 100;
    std::string user_name = "user_modify";
    Status op_status = _s_cgroups_mgr.modify_user_cgroups(user_name, user_share, level_share);

    EXPECT_EQ(Status::OK(), op_status);

    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/user_modify/cpu.shares", 100));
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/user_modify/low/cpu.shares", 100));
}

TEST_F(CgroupsMgrTest, TestUpdateLocalCgroups) {
    // mock TFetchResourceResult from fe
    TFetchResourceResult user_resource_result;

    TUserResource user_resource;
    user_resource.shareByGroup["low"] = 123;
    user_resource.shareByGroup["normal"] = 234;
    TResourceGroup resource_group;
    resource_group.resourceByType[TResourceType::type::TRESOURCE_CPU_SHARE] = 100;
    user_resource.resource = resource_group;

    user_resource_result.resourceVersion = 2;
    user_resource_result.resourceByUser["yiguolei3"] = user_resource;

    Status op_status = _s_cgroups_mgr.update_local_cgroups(user_resource_result);
    EXPECT_EQ(Status::OK(), op_status);
    EXPECT_EQ(2, _s_cgroups_mgr._cur_version);
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/yiguolei3/cpu.shares", 100));
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/yiguolei3/low/cpu.shares", 123));
    EXPECT_TRUE(does_contain_number(_s_cgroup_path + "/yiguolei3/normal/cpu.shares", 234));
}

TEST_F(CgroupsMgrTest, TestRelocateTasks) {
    // create a source cgroup, add some taskid into it
    Status op_status = _s_cgroups_mgr.relocate_tasks("/a/b/c/d", _s_cgroup_path);
    EXPECT_EQ(Status::DORIS_ERROR, op_status);
}

} // namespace doris
