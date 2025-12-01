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

#include <algorithm>

#include "common/config.h"
#include "common/status.h"
#include "util/cpu_info.h"

namespace doris {
using namespace config;

DEFINE_mInt16(cfg_validator_1, "15");
DEFINE_Validator(cfg_validator_1,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

DEFINE_mInt16(cfg_validator_2, "5");
DEFINE_Validator(cfg_validator_2,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

TEST(ConfigValidatorTest, Validator) {
    EXPECT_FALSE(config::init(nullptr, true));
    config::Register::_s_field_map->erase("cfg_validator_1");

    EXPECT_TRUE(config::init(nullptr, true));

    Status s = config::set_config("cfg_validator_2", "15");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.to_string(), "Invalid argument: validate cfg_validator_2=15 failed");
    EXPECT_EQ(cfg_validator_2, 5);

    s = config::set_config("cfg_validator_2", "8");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_validator_2, 8);
}

// When a positive value is specified, validator should use it directly.
TEST(ConfigValidatorTest, OmpThreadsLimitExplicitValue) {
    int32_t original_limit = config::omp_threads_limit;

    Status st = config::set_config("omp_threads_limit", "7", false, true);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(7, config::omp_threads_limit);

    config::omp_threads_limit = original_limit;
}

// When value is -1 and config::num_cores is set,
// validator should pick 80% of that core count.
TEST(ConfigValidatorTest, OmpThreadsLimitAutoUsesConfiguredNumCores) {
    int32_t original_limit = config::omp_threads_limit;
    int32_t original_num_cores = config::num_cores;

    config::num_cores = 20;
    Status st = config::set_config("omp_threads_limit", "-1", false, true);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(16, config::omp_threads_limit);

    config::num_cores = original_num_cores;
    config::omp_threads_limit = original_limit;
}

// When value is -1 and config::num_cores is 0,
// validator should fall back to CpuInfo::num_cores().
TEST(ConfigValidatorTest, OmpThreadsLimitAutoFallsBackToCpuInfo) {
    int32_t original_limit = config::omp_threads_limit;
    int32_t original_num_cores = config::num_cores;

    config::num_cores = 0;
    Status st = config::set_config("omp_threads_limit", "-1", false, true);
    ASSERT_TRUE(st.ok());
    CpuInfo::init();
    int expected = std::max(1, CpuInfo::num_cores() * 4 / 5);
    EXPECT_EQ(expected, config::omp_threads_limit);

    config::num_cores = original_num_cores;
    config::omp_threads_limit = original_limit;
}
} // namespace doris
