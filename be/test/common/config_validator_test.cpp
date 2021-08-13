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

#define __IN_CONFIGBASE_CPP__
#include "common/configbase.h"
#undef __IN_CONFIGBASE_CPP__

#include <gtest/gtest.h>

#include "common/status.h"

namespace doris {
using namespace config;

CONF_mInt16(cfg_validator_1, "15");
CONF_Validator(cfg_validator_1, [](int16_t config) -> bool {
    return 0 <= config && config <= 10;
});

CONF_mInt16(cfg_validator_2, "5");
CONF_Validator(cfg_validator_2, [](int16_t config) -> bool {
    return 0 <= config && config <= 10;
});

TEST(ConfigValidatorTest, Validator) {
    ASSERT_FALSE(config::init(nullptr, true));
    config::Register::_s_field_map->erase("cfg_validator_1");

    ASSERT_TRUE(config::init(nullptr, true));

    Status s = config::set_config("cfg_validator_2", "15");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: validate cfg_validator_2=15 failed");
    ASSERT_EQ(cfg_validator_2, 5);

    s = config::set_config("cfg_validator_2", "8");
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(cfg_validator_2, 8);
}
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
