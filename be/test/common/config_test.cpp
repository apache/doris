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

#include "common/config.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {
using namespace config;

class ConfigTest : public testing::Test {
    void SetUp() override { config::Register::_s_field_map->clear(); }
    void TearDown() override { config::Register::_s_field_map->clear(); }
};

TEST_F(ConfigTest, DumpAllConfigs) {
    DEFINE_Bool(cfg_bool_false, "false");
    DEFINE_Bool(cfg_bool_true, "true");
    DEFINE_Double(cfg_double, "123.456");
    DEFINE_Int16(cfg_int16_t, "2561");
    DEFINE_Int32(cfg_int32_t, "65536123");
    DEFINE_Int64(cfg_int64_t, "4294967296123");
    DEFINE_String(cfg_std_string, "doris_config_test_string");
    DEFINE_Bools(cfg_std_vector_bool, "true,false,true");
    DEFINE_Doubles(cfg_std_vector_double, "123.456,123.4567,123.45678");
    DEFINE_Int16s(cfg_std_vector_int16_t, "2561,2562,2563");
    DEFINE_Int32s(cfg_std_vector_int32_t, "65536123,65536234,65536345");
    DEFINE_Int64s(cfg_std_vector_int64_t, "4294967296123,4294967296234,4294967296345");
    DEFINE_Strings(cfg_std_vector_std_string, "doris,config,test,string");

    EXPECT_TRUE(config::init(nullptr, true));
    std::stringstream ss;
    for (const auto& it : *(config::full_conf_map)) {
        ss << it.first << "=" << it.second << std::endl;
    }
    EXPECT_EQ(
            "cfg_bool_false=0\ncfg_bool_true=1\ncfg_double=123.456\ncfg_int16_t=2561\ncfg_int32_t="
            "65536123\ncfg_int64_t=4294967296123\ncfg_std_string=doris_config_test_string\ncfg_std_"
            "vector_bool=1, 0, 1\ncfg_std_vector_double=123.456, 123.457, "
            "123.457\ncfg_std_vector_int16_t=2561, 2562, 2563\ncfg_std_vector_int32_t=65536123, "
            "65536234, 65536345\ncfg_std_vector_int64_t=4294967296123, 4294967296234, "
            "4294967296345\ncfg_std_vector_std_string=doris, config, test, string\n",
            ss.str());
}

TEST_F(ConfigTest, UpdateConfigs) {
    DEFINE_Bool(cfg_bool_immutable, "true");
    DEFINE_mBool(cfg_bool, "false");
    DEFINE_mDouble(cfg_double, "123.456");
    DEFINE_mInt16(cfg_int16_t, "2561");
    DEFINE_mInt32(cfg_int32_t, "65536123");
    DEFINE_mInt64(cfg_int64_t, "4294967296123");
    DEFINE_String(cfg_std_string, "doris_config_test_string");

    EXPECT_TRUE(config::init(nullptr, true));

    // bool
    EXPECT_FALSE(cfg_bool);
    EXPECT_TRUE(config::set_config("cfg_bool", "true").ok());
    EXPECT_TRUE(cfg_bool);

    // double
    EXPECT_EQ(cfg_double, 123.456);
    EXPECT_TRUE(config::set_config("cfg_double", "654.321").ok());
    EXPECT_EQ(cfg_double, 654.321);

    // int16
    EXPECT_EQ(cfg_int16_t, 2561);
    EXPECT_TRUE(config::set_config("cfg_int16_t", "2562").ok());
    EXPECT_EQ(cfg_int16_t, 2562);

    // int32
    EXPECT_EQ(cfg_int32_t, 65536123);
    EXPECT_TRUE(config::set_config("cfg_int32_t", "65536124").ok());
    EXPECT_EQ(cfg_int32_t, 65536124);

    // int64
    EXPECT_EQ(cfg_int64_t, 4294967296123);
    EXPECT_TRUE(config::set_config("cfg_int64_t", "4294967296124").ok());
    EXPECT_EQ(cfg_int64_t, 4294967296124);

    // not exist
    Status s = config::set_config("cfg_not_exist", "123");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("[NOT_FOUND]") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("'cfg_not_exist' is not found") != std::string::npos);

    // immutable
    EXPECT_TRUE(cfg_bool_immutable);
    s = config::set_config("cfg_bool_immutable", "false");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("NOT_IMPLEMENTED_ERROR") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("'cfg_bool_immutable' is not support to modify") !=
                std::string::npos);
    EXPECT_TRUE(cfg_bool_immutable);

    // convert error
    s = config::set_config("cfg_bool", "falseeee");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("INVALID_ARGUMENT") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("convert 'falseeee' as bool failed") != std::string::npos);
    EXPECT_TRUE(cfg_bool);

    s = config::set_config("cfg_double", "");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("INVALID_ARGUMENT") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("convert '' as double failed") != std::string::npos);
    EXPECT_EQ(cfg_double, 654.321);

    // convert error
    s = config::set_config("cfg_int32_t", "4294967296124");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("INVALID_ARGUMENT") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("convert '4294967296124' as int32_t failed") !=
                std::string::npos);
    EXPECT_EQ(cfg_int32_t, 65536124);

    // not support
    s = config::set_config("cfg_std_string", "test");
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.to_string().find("NOT_IMPLEMENTED_ERROR") != std::string::npos);
    EXPECT_TRUE(s.to_string().find("'cfg_std_string' is not support to modify") !=
                std::string::npos);
    EXPECT_EQ(cfg_std_string, "doris_config_test_string");
}

} // namespace doris
