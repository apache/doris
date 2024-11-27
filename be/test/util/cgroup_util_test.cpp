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

} // namespace doris
