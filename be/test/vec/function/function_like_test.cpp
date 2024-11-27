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

#include <cstdint>
#include <string>

#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(FunctionLikeTest, like) {
    std::string func_name = "like";

    DataSet data_set = {
            // sub_string
            {{std::string("abc"), std::string("%b%")}, uint8_t(1)},
            {{std::string("abc"), std::string("%ad%")}, uint8_t(0)},
            // end with
            {{std::string("abc"), std::string("%c")}, uint8_t(1)},
            {{std::string("ab"), std::string("%c")}, uint8_t(0)},
            // start with
            {{std::string("abc"), std::string("a%")}, uint8_t(1)},
            {{std::string("bc"), std::string("a%")}, uint8_t(0)},
            // equals
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string(""), std::string(" ")}, uint8_t(0)},
            {{std::string(" "), std::string(" ")}, uint8_t(1)},
            {{std::string(" "), std::string("")}, uint8_t(0)},
            {{std::string("abc"), std::string("")}, uint8_t(0)},
            {{std::string("abc"), std::string(" ")}, uint8_t(0)},
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
            {{Null(), std::string("_x__ab%")}, Null()},
            // escape chars
            {{std::string("facebook_10008_T1+T2-ALL_AAA-VO_LowestCost_20230830_HSJ"),
              std::string("%facebook_10008_T1+T2%")},
             uint8_t(1)},
            {{std::string("!z23]"), std::string("_[z]%")}, uint8_t(0)},
            {{std::string("[123]"), std::string("%[1.*]%")}, uint8_t(0)},
            {{std::string("1\\b\\b"), std::string("%_\\b\\b%")}, uint8_t(1)},
            {{std::string("1\\d\\d"), std::string("%_\\d\\d%")}, uint8_t(1)},
            {{std::string("1dd"), std::string("%_dd%")},
             uint8_t(1)} // this case can't be same as regression-test because of FE's string escape operation
    };

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, TypeIndex::String};
    static_cast<void>(
            check_function<DataTypeUInt8, true>(func_name, const_pattern_input_types, data_set));
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
        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, const_pattern_input_types,
                                                              const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_extract) {
    std::string func_name = "regexp_extract";

    DataSet data_set = {{{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("x=([0-9]+)([a-z]+)"), (int64_t)0},
                         std::string("x=18abc")},
                        {{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("^x=([a-z]+)([0-9]+)"), (int64_t)0},
                         std::string("x=a3")},
                        {{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("^x=([a-z]+)([0-9]+)"), (int64_t)1},
                         std::string("a")},
                        {{std::string("http://a.m.baidu.com/i41915173660.htm"),
                          std::string("i([0-9]+)"), (int64_t)0},
                         std::string("i41915173660")},
                        {{std::string("http://a.m.baidu.com/i41915173660.htm"),
                          std::string("i([0-9]+)"), (int64_t)1},
                         std::string("41915173660")},

                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)0},
                         std::string("itde")},
                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)1},
                         std::string("i")},
                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)2},
                         std::string("td")},
                        // null
                        {{std::string("abc"), Null(), (int64_t)0}, Null()},
                        {{Null(), std::string("i([0-9]+)"), (int64_t)0}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String},
                                              TypeIndex::Int64};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_extract_or_null) {
    std::string func_name = "regexp_extract_or_null";

    DataSet data_set = {{{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("x=([0-9]+)([a-z]+)"), (int64_t)0},
                         std::string("x=18abc")},
                        {{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("^x=([a-z]+)([0-9]+)"), (int64_t)0},
                         std::string("x=a3")},
                        {{std::string("x=a3&x=18abc&x=2&y=3&x=4"),
                          std::string("^x=([a-z]+)([0-9]+)"), (int64_t)1},
                         std::string("a")},
                        {{std::string("http://a.m.baidu.com/i41915173660.htm"),
                          std::string("i([0-9]+)"), (int64_t)0},
                         std::string("i41915173660")},
                        {{std::string("http://a.m.baidu.com/i41915173660.htm"),
                          std::string("i([0-9]+)"), (int64_t)1},
                         std::string("41915173660")},

                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)0},
                         std::string("itde")},
                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)1},
                         std::string("i")},
                        {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)"), (int64_t)2},
                         std::string("td")},
                        // null
                        {{std::string("abc"), Null(), (int64_t)0}, Null()},
                        {{Null(), std::string("i([0-9]+)"), (int64_t)0}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String},
                                              TypeIndex::Int64};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_extract_all) {
    std::string func_name = "regexp_extract_all";

    DataSet data_set = {
            {{std::string("x=a3&x=18abc&x=2&y=3&x=4&x=17bcd"), std::string("x=([0-9]+)([a-z]+)")},
             std::string("['18','17']")},
            {{std::string("x=a3&x=18abc&x=2&y=3&x=4"), std::string("^x=([a-z]+)([0-9]+)")},
             std::string("['a']")},
            {{std::string("http://a.m.baidu.com/i41915173660.htm"), std::string("i([0-9]+)")},
             std::string("['41915173660']")},
            {{std::string("http://a.m.baidu.com/i41915i73660.htm"), std::string("i([0-9]+)")},
             std::string("['41915','73660']")},

            {{std::string("hitdecisiondlist"), std::string("(i)(.*?)(e)")}, std::string("['i']")},
            {{std::string("hitdecisioendlist"), std::string("(i)(.*?)(e)")},
             std::string("['i','i']")},
            {{std::string("hitdecisioendliset"), std::string("(i)(.*?)(e)")},
             std::string("['i','i','i']")},
            // null
            {{std::string("abc"), Null()}, Null()},
            {{Null(), std::string("i([0-9]+)")}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_replace) {
    std::string func_name = "regexp_replace";

    DataSet data_set = {
            {{std::string("2022-03-02"), std::string("-"), std::string("")},
             std::string("20220302")},
            {{std::string("2022-03-02"), std::string(""), std::string("s")},
             std::string("s2s0s2s2s-s0s3s-s0s2s")},
            {{std::string("100-200"), std::string("(\\d+)"), std::string("doris")},
             std::string("doris-doris")},

            {{std::string("a b c"), std::string(" "), std::string("-")}, std::string("a-b-c")},
            {{std::string("a b c"), std::string("(b)"), std::string("<\\1>")},
             std::string("a <b> c")},
            {{std::string("qwewe"), std::string(""), std::string("true")},
             std::string("trueqtruewtrueetruewtrueetrue")},
            // null
            {{std::string("abc"), std::string("x=18abc"), Null()}, Null()},
            {{Null(), std::string("i([0-9]+)"), std::string("x=18abc")}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String},
                                              TypeIndex::String};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_replace_one) {
    std::string func_name = "regexp_replace_one";

    DataSet data_set = {
            {{std::string("2022-03-02"), std::string("-"), std::string("")},
             std::string("202203-02")},
            {{std::string("2022-03-02"), std::string(""), std::string("s")},
             std::string("s2022-03-02")},
            {{std::string("100-200"), std::string("(\\d+)"), std::string("doris")},
             std::string("doris-200")},

            {{std::string("a b c"), std::string(" "), std::string("-")}, std::string("a-b c")},
            {{std::string("a b c"), std::string("(b)"), std::string("<\\1>")},
             std::string("a <b> c")},
            {{std::string("qwewe"), std::string(""), std::string("true")},
             std::string("trueqwewe")},
            // null
            {{std::string("abc"), std::string("x=18abc"), Null()}, Null()},
            {{Null(), std::string("i([0-9]+)"), std::string("x=18abc")}, Null()}};

    // pattern is constant value
    InputTypeSet const_pattern_input_types = {TypeIndex::String, Consted {TypeIndex::String},
                                              TypeIndex::String};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

} // namespace doris::vectorized
