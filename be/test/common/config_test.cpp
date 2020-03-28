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

namespace doris {
using namespace config;

class ConfigTest : public testing::Test {
    void SetUp() override { config::Register::_s_fieldlist->clear(); }
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

    config::init(nullptr, true);
    std::stringstream ss;
    for (const auto& it : *(config::confmap)) {
        ss << it.first << "=" << it.second << std::endl;
    }
    ASSERT_EQ(
            R"*(cfg_bool_false=0
cfg_bool_true=1
cfg_double=123.456
cfg_int16_t=2561
cfg_int32_t=65536123
cfg_int64_t=4294967296123
cfg_std_string=doris_config_test_string
cfg_std_vector_bool=1, 0, 1
cfg_std_vector_double=123.456, 123.457, 123.457
cfg_std_vector_int16_t=2561, 2562, 2563
cfg_std_vector_int32_t=65536123, 65536234, 65536345
cfg_std_vector_int64_t=4294967296123, 4294967296234, 4294967296345
cfg_std_vector_std_string=doris, config, test, string
)*",
            ss.str());
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
