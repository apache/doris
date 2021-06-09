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

#include <gtest/gtest.h>

#include <fstream>

namespace doris {

class CGroupUtilTest : public ::testing::Test {
protected:
    CGroupUtilTest() {}
    virtual ~CGroupUtilTest() {}
};
TEST_F(CGroupUtilTest, memlimit) {
    int64_t bytes;
    float cpu_counts;
    CGroupUtil cgroup_util;
    LOG(INFO) << cgroup_util.debug_string();
    Status status1 = cgroup_util.find_cgroup_mem_limit(&bytes);
    Status status2 = cgroup_util.find_cgroup_cpu_limit(&cpu_counts);
    if (cgroup_util.enable()) {
        std::ifstream file("/proc/self/cgroup");
        if (file.peek() == std::ifstream::traits_type::eof()) {
            ASSERT_FALSE(status1.ok());
            ASSERT_FALSE(status2.ok());
        } else {
            ASSERT_TRUE(status1.ok());
            ASSERT_TRUE(status2.ok());
        }
    } else {
        ASSERT_FALSE(status1.ok());
        ASSERT_FALSE(status2.ok());
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
