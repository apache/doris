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

#include <string>

#include "function_test_util.h"
#include "util/cpu_info.h"
#include "vec/core/types.h"

namespace doris::vectorized {

TEST(FunctionLikeTest, like) {
    std::string func_name = "like";

    DataSet data_set = {// sub_string
                        {{std::string("abc"), std::string("%b%")}, uint8_t(1)},
                        {{std::string("abc"), std::string("%ad%")}, uint8_t(0)},
                        // end with
                        {{std::string("abc"), std::string("%c")}, uint8_t(1)},
                        {{std::string("ab"), std::string("%c")}, uint8_t(0)},
                        // start with
                        {{std::string("abc"), std::string("a%")}, uint8_t(1)},
                        {{std::string("bc"), std::string("a%")}, uint8_t(0)},
                        // equals
                        {{std::string("abc"), std::string("abc")}, uint8_t(1)},
                        {{std::string("abc"), std::string("ab")}, uint8_t(0)},
                        // full regexp match
                        {{std::string("abcd"), std::string("a_c%")}, uint8_t(1)},
                        {{std::string("abcd"), std::string("a_d%")}, uint8_t(0)},
                        {{std::string("abc"), std::string("__c")}, uint8_t(1)},
                        {{std::string("abc"), std::string("_c")}, uint8_t(0)},
                        {{std::string("abc"), std::string("_b_")}, uint8_t(1)},
                        {{std::string("abc"), std::string("_a_")}, uint8_t(0)},
                        {{std::string("abc"), std::string("a__")}, uint8_t(1)},
                        {{std::string("abc"), std::string("a_")}, uint8_t(0)},
                        // null
                        {{std::string("abc"), Null()}, Null()},
                        {{Null(), std::string("_x__ab%")}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        check_function<DataTypeUInt8, true>(func_name, const_pattern_input_types,
                                            const_pattern_dataset);
    }

    // pattern is not constant value
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(FunctionLikeTest, regexp) {
    std::string func_name = "regexp";

    DataSet data_set = {// sub_string
                        {{std::string("abc"), std::string(".*b.*")}, uint8_t(1)},
                        {{std::string("abc"), std::string(".*ad.*")}, uint8_t(0)},
                        {{std::string("abc"), std::string(".*c")}, uint8_t(1)},
                        {{std::string("abc"), std::string("a.*")}, uint8_t(1)},
                        // end with
                        {{std::string("abc"), std::string(".*c$")}, uint8_t(1)},
                        {{std::string("ab"), std::string(".*c$")}, uint8_t(0)},
                        // start with
                        {{std::string("abc"), std::string("^a.*")}, uint8_t(1)},
                        {{std::string("bc"), std::string("^a.*")}, uint8_t(0)},
                        // equals
                        {{std::string("abc"), std::string("^abc$")}, uint8_t(1)},
                        {{std::string("abc"), std::string("^ab$")}, uint8_t(0)},
                        // partial regexp match
                        {{std::string("abcde"), std::string("a.*d")}, uint8_t(1)},
                        {{std::string("abcd"), std::string("a.d")}, uint8_t(0)},
                        {{std::string("abc"), std::string(".c")}, uint8_t(1)},
                        {{std::string("abc"), std::string(".b.")}, uint8_t(1)},
                        {{std::string("abc"), std::string(".a.")}, uint8_t(0)},
                        // null
                        {{std::string("abc"), Null()}, Null()},
                        {{Null(), std::string("xxx.*")}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        check_function<DataTypeUInt8, true>(func_name, const_pattern_input_types,
                                            const_pattern_dataset);
    }

    // pattern is not constant value
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
