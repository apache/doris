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

#include "util/cgroup_util.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <fstream>

#include "common/cgroup_memory_ctl.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/cgroup_util.h"

namespace doris {

class CGroupUtilTest : public ::testing::Test {
protected:
    CGroupUtilTest() {}
    ~CGroupUtilTest() override = default;
};

TEST_F(CGroupUtilTest, ReadMetrics) {
    std::string dir_path = GetCurrentRunningDir();
    std::string memory_limit_in_bytes_path(dir_path);
    memory_limit_in_bytes_path += "/util/test_data/memory.limit_in_bytes";
    int64_t value;
    auto st = CGroupUtil::read_int_line_from_cgroup_file(memory_limit_in_bytes_path, &value);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(6291456000, value);

    std::string memory_stat_path(dir_path);
    memory_stat_path += "/util/test_data/memory.stat";
    std::unordered_map<std::string, int64_t> metrics_map;
    CGroupUtil::read_int_metric_from_cgroup_file(memory_stat_path, metrics_map);
    EXPECT_EQ(5443584, metrics_map["inactive_file"]);
    EXPECT_EQ(0, metrics_map["rss"]);

    std::string error_memory_limit_in_bytes_path(dir_path);
    error_memory_limit_in_bytes_path += "/util/test_data/Zzz/memory.limit_in_bytes";
    int64_t error_value;
    auto st2 = CGroupUtil::read_int_line_from_cgroup_file(error_memory_limit_in_bytes_path,
                                                          &error_value);
    EXPECT_FALSE(st2.ok());

    std::string error_memory_stat_path(dir_path);
    error_memory_stat_path += "/util/test_data/Zzz/memory.stat";
    std::unordered_map<std::string, int64_t> error_metrics_map;
    CGroupUtil::read_int_metric_from_cgroup_file(error_memory_stat_path, error_metrics_map);
    EXPECT_TRUE(error_metrics_map.empty());
}

TEST_F(CGroupUtilTest, memlimit) {
    LOG(INFO) << CGroupMemoryCtl::debug_string();
    int64_t mem_limit;
    int64_t mem_usage;
    auto status1 = CGroupMemoryCtl::find_cgroup_mem_limit(&mem_limit);
    auto status2 = CGroupMemoryCtl::find_cgroup_mem_usage(&mem_usage);
    if (CGroupUtil::cgroupsv1_enable() || CGroupUtil::cgroupsv2_enable()) {
        std::ifstream file("/proc/self/cgroup");
        if (file.peek() == std::ifstream::traits_type::eof()) {
            EXPECT_FALSE(status1.ok());
            EXPECT_FALSE(status2.ok());
        } else {
            EXPECT_TRUE(status1.ok());
            EXPECT_TRUE(status2.ok());
        }
    } else {
        EXPECT_FALSE(status1.ok());
        EXPECT_FALSE(status2.ok());
    }
}

TEST_F(CGroupUtilTest, readcpu) {
    std::string dir_path = GetCurrentRunningDir();

    std::string cpuset_1_path(dir_path + "/util/test_data/cgroup_cpu_data/cpuset1");
    std::string cpuset_1_str = "";
    Status ret1 = CGroupUtil::read_string_line_from_cgroup_file(cpuset_1_path, &cpuset_1_str);
    EXPECT_TRUE(ret1.ok());
    int cpu_count1 = 0;
    static_cast<void>(CGroupUtil::parse_cpuset_line(cpuset_1_str, &cpu_count1));
    EXPECT_TRUE(cpu_count1 == 3);

    std::string cpuset_2_path(dir_path + "/util/test_data/cgroup_cpu_data/cpuset2");
    std::string cpuset_2_str = "";
    Status ret2 = CGroupUtil::read_string_line_from_cgroup_file(cpuset_2_path, &cpuset_2_str);
    EXPECT_TRUE(ret2.ok());
    int cpu_count2 = 0;
    static_cast<void>(CGroupUtil::parse_cpuset_line(cpuset_2_str, &cpu_count2));
    EXPECT_TRUE(cpu_count2 == 11);

    std::string cpuset_3_path(dir_path + "/util/test_data/cgroup_cpu_data/cpuset3");
    std::string cpuset_3_str = "";
    Status ret3 = CGroupUtil::read_string_line_from_cgroup_file(cpuset_3_path, &cpuset_3_str);
    EXPECT_TRUE(ret3.ok());
    int cpu_count3 = 0;
    static_cast<void>(CGroupUtil::parse_cpuset_line(cpuset_3_str, &cpu_count3));
    EXPECT_TRUE(cpu_count3 == 10);

    int ret = CGroupUtil::get_cgroup_limited_cpu_number(16);
    EXPECT_TRUE(ret > 0);

    // 1 read cgroup v2 quota
    // 1.1 read default value
    std::filesystem::path path11 = dir_path + "/util/test_data/cgroup_cpu_data/test11/child";
    std::filesystem::path default_path_11 = dir_path + "/util/test_data/cgroup_cpu_data/test11";
    int ret11 = CGroupUtil::get_cgroup_v2_cpu_quota_number(path11, default_path_11, 96);
    EXPECT_TRUE(ret11 == 96);

    // 1.2 read from child to parent
    std::filesystem::path path12 = dir_path + "/util/test_data/cgroup_cpu_data/test12/child";
    std::filesystem::path default_path_12 = dir_path + "/util/test_data/cgroup_cpu_data/test12";
    int ret12 = CGroupUtil::get_cgroup_v2_cpu_quota_number(path12, default_path_12, 96);
    EXPECT_TRUE(ret12 == 2);

    // 1.3 read parent
    std::filesystem::path path13 = dir_path + "/util/test_data/cgroup_cpu_data/test13/child";
    std::filesystem::path default_path_13 = dir_path + "/util/test_data/cgroup_cpu_data/test13";
    int ret13 = CGroupUtil::get_cgroup_v2_cpu_quota_number(path13, default_path_13, 96);
    EXPECT_TRUE(ret13 == 2);

    // 1.4 read child
    std::filesystem::path path14 = dir_path + "/util/test_data/cgroup_cpu_data/test14/child";
    std::filesystem::path default_path_14 = dir_path + "/util/test_data/cgroup_cpu_data/test14";
    int ret14 = CGroupUtil::get_cgroup_v2_cpu_quota_number(path14, default_path_14, 96);
    EXPECT_TRUE(ret14 == 3);

    // 2 read cgroup v2 cpuset
    // 2.1 read child
    std::filesystem::path path21 = dir_path + "/util/test_data/cgroup_cpu_data/test21/child";
    std::filesystem::path default_path_21 = dir_path + "/util/test_data/cgroup_cpu_data/test21";
    int ret21 = CGroupUtil::get_cgroup_v2_cpuset_number(path21, default_path_21, 96);
    EXPECT_TRUE(ret21 == 2);
    // 2.2 read parent
    std::filesystem::path path22 = dir_path + "/util/test_data/cgroup_cpu_data/test22/child";
    std::filesystem::path default_path_22 = dir_path + "/util/test_data/cgroup_cpu_data/test22";
    int ret22 = CGroupUtil::get_cgroup_v2_cpuset_number(path22, default_path_22, 96);
    EXPECT_TRUE(ret22 == 7);

    // 3 read cgroup v1 quota
    // 3.1 read child
    std::filesystem::path path31 = dir_path + "/util/test_data/cgroup_cpu_data/test31/child";
    std::filesystem::path default_path_31 = dir_path + "/util/test_data/cgroup_cpu_data/test31";
    int ret31 = CGroupUtil::get_cgroup_v1_cpu_quota_number(path31, default_path_31, 96);
    EXPECT_TRUE(ret31 == 5);
    // 3.2 read parent
    std::filesystem::path path32 = dir_path + "/util/test_data/cgroup_cpu_data/test32/child";
    std::filesystem::path default_path_32 = dir_path + "/util/test_data/cgroup_cpu_data/test32";
    int ret32 = CGroupUtil::get_cgroup_v1_cpu_quota_number(path32, default_path_32, 96);
    EXPECT_TRUE(ret32 == 6);
    // 3.3 read default
    std::filesystem::path path33 = dir_path + "/util/test_data/cgroup_cpu_data/test33/child";
    std::filesystem::path default_path_33 = dir_path + "/util/test_data/cgroup_cpu_data/test33";
    int ret33 = CGroupUtil::get_cgroup_v1_cpu_quota_number(path33, default_path_33, 96);
    EXPECT_TRUE(ret33 == 96);

    // 4 read cgroup v1 cpuset
    std::filesystem::path path41 = dir_path + "/util/test_data/cgroup_cpu_data/test41";
    int ret41 = CGroupUtil::get_cgroup_v1_cpuset_number(path41, 96);
    EXPECT_TRUE(ret41 == 3);
}

} // namespace doris
