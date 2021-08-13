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

class ConfigTest : public testing::Test {
    void SetUp() override { config::Register::_s_field_map->clear(); }
};

TEST_F(ConfigTest, DumpAllConfigs) {
    CONF_Bool(cfg_bool_false, "false");
    CONF_Bool(cfg_bool_true, "true");
    CONF_Double(cfg_double, "123.456");
    CONF_Int16(cfg_int16_t, "2561");
    CONF_Int32(cfg_int32_t, "65536123");
    CONF_Int64(cfg_int64_t, "4294967296123");
    CONF_String(cfg_std_string, "doris_config_test_string");
    CONF_Bools(cfg_std_vector_bool, "true,false,true");
    CONF_Doubles(cfg_std_vector_double, "123.456,123.4567,123.45678");
    CONF_Int16s(cfg_std_vector_int16_t, "2561,2562,2563");
    CONF_Int32s(cfg_std_vector_int32_t, "65536123,65536234,65536345");
    CONF_Int64s(cfg_std_vector_int64_t, "4294967296123,4294967296234,4294967296345");
    CONF_Strings(cfg_std_vector_std_string, "doris,config,test,string");

    ASSERT_TRUE(config::init(nullptr, true));
    std::stringstream ss;
    for (const auto& it : *(config::full_conf_map)) {
        ss << it.first << "=" << it.second << std::endl;
    }
    ASSERT_EQ(
            "cfg_bool_false=0\ncfg_bool_true=1\ncfg_double=123.456\ncfg_int16_t=2561\ncfg_int32_t="
            "65536123\ncfg_int64_t=4294967296123\ncfg_std_string=doris_config_test_string\ncfg_std_"
            "vector_bool=1, 0, 1\ncfg_std_vector_double=123.456, 123.457, "
            "123.457\ncfg_std_vector_int16_t=2561, 2562, 2563\ncfg_std_vector_int32_t=65536123, "
            "65536234, 65536345\ncfg_std_vector_int64_t=4294967296123, 4294967296234, "
            "4294967296345\ncfg_std_vector_std_string=doris, config, test, string\n",
            ss.str());
}

TEST_F(ConfigTest, UpdateConfigs) {
    CONF_Bool(cfg_bool_immutable, "true");
    CONF_mBool(cfg_bool, "false");
    CONF_mDouble(cfg_double, "123.456");
    CONF_mInt16(cfg_int16_t, "2561");
    CONF_mInt32(cfg_int32_t, "65536123");
    CONF_mInt64(cfg_int64_t, "4294967296123");
    CONF_String(cfg_std_string, "doris_config_test_string");

    ASSERT_TRUE(config::init(nullptr, true));

    // bool
    ASSERT_FALSE(cfg_bool);
    ASSERT_TRUE(config::set_config("cfg_bool", "true").ok());
    ASSERT_TRUE(cfg_bool);

    // double
    ASSERT_EQ(cfg_double, 123.456);
    ASSERT_TRUE(config::set_config("cfg_double", "654.321").ok());
    ASSERT_EQ(cfg_double, 654.321);

    // int16
    ASSERT_EQ(cfg_int16_t, 2561);
    ASSERT_TRUE(config::set_config("cfg_int16_t", "2562").ok());
    ASSERT_EQ(cfg_int16_t, 2562);

    // int32
    ASSERT_EQ(cfg_int32_t, 65536123);
    ASSERT_TRUE(config::set_config("cfg_int32_t", "65536124").ok());
    ASSERT_EQ(cfg_int32_t, 65536124);

    // int64
    ASSERT_EQ(cfg_int64_t, 4294967296123);
    ASSERT_TRUE(config::set_config("cfg_int64_t", "4294967296124").ok());
    ASSERT_EQ(cfg_int64_t, 4294967296124);

    // not exist
    Status s = config::set_config("cfg_not_exist", "123");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not found: 'cfg_not_exist' is not found");

    // immutable
    ASSERT_TRUE(cfg_bool_immutable);
    s = config::set_config("cfg_bool_immutable", "false");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not supported: 'cfg_bool_immutable' is not support to modify");
    ASSERT_TRUE(cfg_bool_immutable);

    // convert error
    s = config::set_config("cfg_bool", "falseeee");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert 'falseeee' as bool failed");
    ASSERT_TRUE(cfg_bool);

    s = config::set_config("cfg_double", "");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert '' as double failed");
    ASSERT_EQ(cfg_double, 654.321);

    // convert error
    s = config::set_config("cfg_int32_t", "4294967296124");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Invalid argument: convert '4294967296124' as int32_t failed");
    ASSERT_EQ(cfg_int32_t, 65536124);

    // not support
    s = config::set_config("cfg_std_string", "test");
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.to_string(), "Not supported: 'cfg_std_string' is not support to modify");
    ASSERT_EQ(cfg_std_string, "doris_config_test_string");
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
