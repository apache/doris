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
#include <fstream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/cpu_info.h"

namespace doris {
using namespace config;

// These configs are checked when the test binary loads be.conf at startup,
// so their default values must pass the validators.
DEFINE_mInt16(cfg_validator_1, "5");
DEFINE_Validator(cfg_validator_1,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

DEFINE_mInt16(cfg_validator_2, "5");
DEFINE_Validator(cfg_validator_2,
                 [](int16_t config) -> bool { return 0 <= config && config <= 10; });

static const std::string TEST_DIR = "./ut_dir/config_validator_test";

class ConfigValidatorTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

    // Load a conf file with only `content` in it. set_to_default=false makes init()
    // touch only the configs in the file, so the other configs keep their values.
    static bool init_from_conf(const std::string& content) {
        const std::string conf_file = TEST_DIR + "/be.conf";
        std::ofstream(conf_file) << content;
        return config::init(conf_file.c_str(), false, true, false);
    }
};

TEST_F(ConfigValidatorTest, Validator) {
    int16_t old_value = cfg_validator_1;
    EXPECT_FALSE(init_from_conf("cfg_validator_1 = 15\n"));
    EXPECT_EQ(cfg_validator_1, old_value);

    EXPECT_TRUE(init_from_conf("cfg_validator_1 = 8\n"));
    EXPECT_EQ(cfg_validator_1, 8);

    old_value = cfg_validator_2;
    Status s = config::set_config("cfg_validator_2", "15");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("validate cfg_validator_2=15 failed") != std::string::npos)
            << s.to_string();
    EXPECT_EQ(cfg_validator_2, old_value);

    s = config::set_config("cfg_validator_2", "8");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_validator_2, 8);
}

} // namespace doris
