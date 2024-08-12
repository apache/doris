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

namespace doris {

class CGroupUtilTest : public ::testing::Test {
protected:
    CGroupUtilTest() {}
    ~CGroupUtilTest() override = default;
};

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
