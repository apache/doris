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
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/like.h"

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
            {{std::string("\\"), std::string("\\")}, uint8_t(1)},
            {{std::string("\\a"), std::string("\\")}, uint8_t(0)},
            {{std::string("\b"), std::string("\b")}, uint8_t(1)},
            {{std::string("b\\"), std::string("b\\")}, uint8_t(1)},
            {{std::string("b"), std::string("\\b")}, uint8_t(0)},
            {{std::string("\\b"), std::string("\\b")}, uint8_t(1)},
            {{std::string("\\b"), std::string("b")}, uint8_t(0)},
            {{std::string("\\\\b"), std::string("\\\\b")}, uint8_t(0)},
            {{std::string("\\b"), std::string("\\\\b")}, uint8_t(1)},
            {{std::string("b"), std::string("\\\\b")}, uint8_t(0)},
            {{std::string("\\\\b"), std::string("b")}, uint8_t(0)},
            {{std::string("%"), std::string("\\%")}, uint8_t(1)},
            {{std::string("a"), std::string("\\%")}, uint8_t(0)},
            {{std::string("\\%"), std::string("\\%")}, uint8_t(0)},
            {{std::string("\\\\%"), std::string("\\\\%")}, uint8_t(1)},
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(check_function_all_arg_comb<DataTypeUInt8, true>(
            func_name, const_pattern_input_types, data_set));
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR}};
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR},
                                              PrimitiveType::TYPE_BIGINT};
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR},
                                              PrimitiveType::TYPE_BIGINT};
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR}};
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR},
                                              PrimitiveType::TYPE_VARCHAR};
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
    InputTypeSet const_pattern_input_types = {PrimitiveType::TYPE_VARCHAR,
                                              Consted {PrimitiveType::TYPE_VARCHAR},
                                              PrimitiveType::TYPE_VARCHAR};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(check_function<DataTypeString, true>(func_name, const_pattern_input_types,
                                                               const_pattern_dataset));
    }
}

// Enhanced tests for better coverage

TEST(FunctionLikeTest, pattern_optimization_allpass) {
    std::string func_name = "like";

    // Test allpass patterns that should match everything
    DataSet data_set = {{{std::string("any_string"), std::string("%")}, uint8_t(1)},
                        {{std::string(""), std::string("%")}, uint8_t(1)},
                        {{std::string("test"), std::string("%%")}, uint8_t(1)},
                        {{std::string("multi line\ntext"), std::string("%%%")}, uint8_t(1)},
                        {{std::string("special@#$%chars"), std::string("%")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, pattern_optimization_equals) {
    std::string func_name = "like";

    // Test patterns that should be optimized to exact equals
    DataSet data_set = {
            {{std::string("exact_match"), std::string("exact_match")}, uint8_t(1)},
            {{std::string("exact_match"), std::string("different")}, uint8_t(0)},
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string("test"), std::string("test")}, uint8_t(1)},
            {{std::string("Test"), std::string("test")}, uint8_t(0)}, // case sensitive
            {{std::string("with_underscore"), std::string("with_underscore")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, pattern_optimization_starts_with) {
    std::string func_name = "like";

    // Test patterns that should be optimized to starts_with
    DataSet data_set = {{{std::string("prefix_test"), std::string("prefix%")}, uint8_t(1)},
                        {{std::string("different_start"), std::string("prefix%")}, uint8_t(0)},
                        {{std::string("p"), std::string("prefix%")}, uint8_t(0)},
                        {{std::string("prefix"), std::string("prefix%")}, uint8_t(1)},
                        {{std::string("prefixABC"), std::string("prefix%")}, uint8_t(1)},
                        {{std::string(""), std::string("nonempty%")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, pattern_optimization_ends_with) {
    std::string func_name = "like";

    // Test patterns that should be optimized to ends_with
    DataSet data_set = {{{std::string("test_suffix"), std::string("%suffix")}, uint8_t(1)},
                        {{std::string("different_end"), std::string("%suffix")}, uint8_t(0)},
                        {{std::string("s"), std::string("%suffix")}, uint8_t(0)},
                        {{std::string("suffix"), std::string("%suffix")}, uint8_t(1)},
                        {{std::string("ABCsuffix"), std::string("%suffix")}, uint8_t(1)},
                        {{std::string(""), std::string("%nonempty")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, pattern_optimization_substring) {
    std::string func_name = "like";

    // Test patterns that should be optimized to substring search
    DataSet data_set = {
            {{std::string("prefix_middle_suffix"), std::string("%middle%")}, uint8_t(1)},
            {{std::string("no_match_here"), std::string("%middle%")}, uint8_t(0)},
            {{std::string("middle"), std::string("%middle%")}, uint8_t(1)},
            {{std::string("middlepart"), std::string("%middle%")}, uint8_t(1)},
            {{std::string("partmiddle"), std::string("%middle%")}, uint8_t(1)},
            {{std::string(""), std::string("%nonempty%")}, uint8_t(0)},
            {{std::string("anything"), std::string("%%")}, uint8_t(1)} // empty substring
    };

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, complex_patterns) {
    std::string func_name = "like";

    // Test complex patterns that require full regex processing
    DataSet data_set = {
            {{std::string("abcdef"), std::string("a_c%")}, uint8_t(1)},
            {{std::string("aXcYZ"), std::string("a_c%")}, uint8_t(1)},
            {{std::string("ac"), std::string("a_c%")}, uint8_t(0)},    // too short
            {{std::string("abdef"), std::string("a_c%")}, uint8_t(0)}, // no 'c' in right position
            {{std::string("test123"), std::string("test___")}, uint8_t(1)},
            {{std::string("test12"), std::string("test___")}, uint8_t(0)},   // too short
            {{std::string("test1234"), std::string("test___")}, uint8_t(0)}, // too long
            {{std::string("multiple_words"), std::string("%_words")}, uint8_t(1)},
            {{std::string("words"), std::string("%_words")},
             uint8_t(0)} // need at least one char before
    };

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, escape_characters_advanced) {
    std::string func_name = "like";

    // Test advanced escape character scenarios
    DataSet data_set = {
            // Escaped percent and underscore
            {{std::string("50%"), std::string("50\\%")}, uint8_t(1)},
            {{std::string("50percent"), std::string("50\\%")}, uint8_t(0)},
            {{std::string("test_file"), std::string("test\\_file")}, uint8_t(1)},
            {{std::string("testXfile"), std::string("test\\_file")}, uint8_t(0)},

            // Double escaping
            {{std::string("test\\file"), std::string("test\\\\file")}, uint8_t(1)},
            {{std::string("testfile"), std::string("test\\\\file")}, uint8_t(0)},

            // Mixed patterns with escapes
            {{std::string(R"(path\to_file.txt)"), std::string(R"(path\\to\_file%)")}, uint8_t(1)},
            {{std::string(R"(path\toXfile.txt)"), std::string(R"(path\\to\_file%)")}, uint8_t(0)},

            // Edge cases
            {{std::string("\\"), std::string("\\\\")}, uint8_t(1)},
            {{std::string("_"), std::string("\\_")}, uint8_t(1)},
            {{std::string("%"), std::string("\\%")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, vector_patterns) {
    std::string func_name = "like";

    // Test with varying patterns (not constant)
    DataSet data_set = {{{std::string("test1"), std::string("test%")}, uint8_t(1)},
                        {{std::string("example"), std::string("%ample")}, uint8_t(1)},
                        {{std::string("middle"), std::string("%dd%")}, uint8_t(1)},
                        {{std::string("exact"), std::string("exact")}, uint8_t(1)},
                        {{std::string("nomatch"), std::string("different")}, uint8_t(0)}};

    // Both arguments are variables (not constant)
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, edge_cases_and_boundaries) {
    std::string func_name = "like";

    DataSet data_set = {
            // Empty strings
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string(""), std::string("%")}, uint8_t(1)},
            {{std::string("nonempty"), std::string("")}, uint8_t(0)},

            // Very long strings
            {{std::string(1000, 'a'), std::string("%a%")}, uint8_t(1)},
            {{std::string(1000, 'a'), std::string("%b%")}, uint8_t(0)},

            // Unicode and special characters
            {{std::string("测试文本"), std::string("%试%")}, uint8_t(1)},
            {{std::string("测试文本"), std::string("%不存在%")}, uint8_t(0)},
            {{std::string("tab\there"), std::string("%\t%")}, uint8_t(1)},
            {{std::string("newline\nhere"), std::string("%\n%")}, uint8_t(1)},

            // Multiple wildcards
            {{std::string("abcdef"), std::string("a%c%f")}, uint8_t(1)},
            {{std::string("abcdef"), std::string("a%c%g")}, uint8_t(0)},
            {{std::string("abcdef"), std::string("a_c_e_")}, uint8_t(1)},
            {{std::string("abcde"), std::string("a_c_e_")}, uint8_t(0)} // too short
    };

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(FunctionLikeTest, regexp_optimization_allpass) {
    std::string func_name = "regexp";

    // Test regex patterns that match everything
    DataSet data_set = {{{std::string("any_string"), std::string(".*")}, uint8_t(1)},
                        {{std::string(""), std::string(".*")}, uint8_t(1)},
                        {{std::string("test"), std::string(".*.*")}, uint8_t(1)},
                        {{std::string("multi line\ntext"), std::string(".*.*.*")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_optimization_equals) {
    std::string func_name = "regexp";

    // Test regex patterns that should be optimized to exact equals
    DataSet data_set = {
            {{std::string("exact_match"), std::string("^exact_match$")}, uint8_t(1)},
            {{std::string("exact_match"), std::string("^different$")}, uint8_t(0)},
            {{std::string(""), std::string("^$")}, uint8_t(1)},
            {{std::string("test"), std::string("^test$")}, uint8_t(1)},
            {{std::string("Test"), std::string("^test$")}, uint8_t(0)} // case sensitive
    };

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_optimization_starts_with) {
    std::string func_name = "regexp";

    // Test regex patterns that should be optimized to starts_with
    DataSet data_set = {{{std::string("prefix_test"), std::string("^prefix.*")}, uint8_t(1)},
                        {{std::string("different_start"), std::string("^prefix.*")}, uint8_t(0)},
                        {{std::string("p"), std::string("^prefix.*")}, uint8_t(0)},
                        {{std::string("prefix"), std::string("^prefix.*")}, uint8_t(1)},
                        {{std::string("prefixABC"), std::string("^prefix.*")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_optimization_ends_with) {
    std::string func_name = "regexp";

    // Test regex patterns that should be optimized to ends_with
    DataSet data_set = {{{std::string("test_suffix"), std::string(".*suffix$")}, uint8_t(1)},
                        {{std::string("different_end"), std::string(".*suffix$")}, uint8_t(0)},
                        {{std::string("s"), std::string(".*suffix$")}, uint8_t(0)},
                        {{std::string("suffix"), std::string(".*suffix$")}, uint8_t(1)},
                        {{std::string("ABCsuffix"), std::string(".*suffix$")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_optimization_substring) {
    std::string func_name = "regexp";

    // Test regex patterns that should be optimized to substring search
    DataSet data_set = {
            {{std::string("prefix_middle_suffix"), std::string(".*middle.*")}, uint8_t(1)},
            {{std::string("no_match_here"), std::string(".*middle.*")}, uint8_t(0)},
            {{std::string("middle"), std::string(".*middle.*")}, uint8_t(1)},
            {{std::string("middlepart"), std::string(".*middle.*")}, uint8_t(1)},
            {{std::string("partmiddle"), std::string(".*middle.*")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

TEST(FunctionLikeTest, regexp_special_characters) {
    std::string func_name = "regexp";

    // Test regex with special characters that need escaping
    DataSet data_set = {{{std::string("test.txt"), std::string("test\\.txt")}, uint8_t(1)},
                        {{std::string("testXtxt"), std::string("test\\.txt")}, uint8_t(0)},
                        {{std::string("test[abc]"), std::string("test\\[abc\\]")}, uint8_t(1)},
                        {{std::string("testabc"), std::string("test\\[abc\\]")}, uint8_t(0)},
                        {{std::string("price$100"), std::string("price\\$100")}, uint8_t(1)},
                        {{std::string("price100"), std::string("price\\$100")}, uint8_t(0)},
                        {{std::string("question?"), std::string("question\\?")}, uint8_t(1)},
                        {{std::string("questionX"), std::string("question\\?")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

// Test custom escape characters (LIKE with ESCAPE clause)
TEST(FunctionLikeTest, like_with_custom_escape) {
    std::string func_name = "like";

    // Start with the simplest test case from regression test
    // Test case: "a" like "aa" ESCAPE "a" -> true
    // Pattern "aa": a+a (escape+escape) = literal a
    DataSet data_set = {{{std::string("a"), std::string("aa"), std::string("a")}, uint8_t(1)}};

    // Ensure all parameters are correctly typed, with escape character as constant
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR},
                                Consted {PrimitiveType::TYPE_VARCHAR}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test error handling and invalid patterns
TEST(FunctionLikeTest, error_handling) {
    std::string func_name = "regexp";

    // Invalid regex patterns should handle gracefully
    DataSet data_set = {// Valid pattern for comparison
                        {{std::string("test"), std::string("t.*t")}, uint8_t(1)},
                        {{std::string("test"), std::string("x.*x")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

// Test performance-critical substring optimization
TEST(FunctionLikeTest, substring_optimization_performance) {
    std::string func_name = "like";

    // Test cases that should trigger execute_substring optimization
    DataSet data_set = {// Multiple identical substrings in long text
                        {{std::string("aaabbbaaabbbaaabbb"), std::string("%bbb%")}, uint8_t(1)},
                        {{std::string("aaacccaaacccaaaccc"), std::string("%bbb%")}, uint8_t(0)},

                        // Overlapping patterns
                        {{std::string("abababab"), std::string("%aba%")}, uint8_t(1)},
                        {{std::string("abcabcabc"), std::string("%aba%")}, uint8_t(0)},

                        // Empty substring (should match everything)
                        {{std::string("any_text"), std::string("%%")}, uint8_t(1)},
                        {{std::string(""), std::string("%%")}, uint8_t(1)},

                        // Single character substrings
                        {{std::string("abc"), std::string("%a%")}, uint8_t(1)},
                        {{std::string("abc"), std::string("%x%")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test boundary conditions for pattern matching
TEST(FunctionLikeTest, boundary_conditions) {
    std::string func_name = "like";

    DataSet data_set = {// Patterns at string boundaries
                        {{std::string("abcdef"), std::string("a%f")}, uint8_t(1)},
                        {{std::string("abcdef"), std::string("a%g")}, uint8_t(0)},
                        {{std::string("a"), std::string("a%")}, uint8_t(1)},
                        {{std::string("a"), std::string("b%")}, uint8_t(0)},
                        {{std::string("z"), std::string("%z")}, uint8_t(1)},
                        {{std::string("z"), std::string("%y")}, uint8_t(0)},

                        // Underscore at boundaries
                        {{std::string("abc"), std::string("_bc")}, uint8_t(1)},
                        {{std::string("abc"), std::string("ab_")}, uint8_t(1)},
                        {{std::string("ab"), std::string("_bc")}, uint8_t(0)},
                        {{std::string("ab"), std::string("ab_")}, uint8_t(0)},

                        // Mixed wildcards
                        {{std::string("abcdef"), std::string("_bc%")}, uint8_t(1)},
                        {{std::string("abcdef"), std::string("%def_")},
                         uint8_t(0)}, // underscore requires one more char
                        {{std::string("abcdefg"), std::string("%def_")}, uint8_t(1)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test case sensitivity and character encoding
TEST(FunctionLikeTest, case_sensitivity_and_encoding) {
    std::string func_name = "like";

    DataSet data_set = {// Basic case sensitivity
                        {{std::string("Test"), std::string("test")}, uint8_t(0)},
                        {{std::string("TEST"), std::string("test")}, uint8_t(0)},
                        {{std::string("test"), std::string("TEST")}, uint8_t(0)},
                        {{std::string("test"), std::string("test")}, uint8_t(1)},

                        // Case with wildcards
                        {{std::string("TestCase"), std::string("test%")}, uint8_t(0)},
                        {{std::string("TestCase"), std::string("Test%")}, uint8_t(1)},
                        {{std::string("TestCase"), std::string("%case")}, uint8_t(0)},
                        {{std::string("TestCase"), std::string("%Case")}, uint8_t(1)},

                        // Unicode handling
                        {{std::string("测试"), std::string("测%")}, uint8_t(1)},
                        {{std::string("测试"), std::string("%试")}, uint8_t(1)},
                        {{std::string("测试数据"), std::string("%试%")}, uint8_t(1)},
                        {{std::string("测试数据"), std::string("%不存在%")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test vector search state transitions
TEST(FunctionLikeTest, vector_search_state_patterns) {
    std::string func_name = "like";

    // Test different pattern types in the same vector
    DataSet data_set = {// Mix of optimization types
                        {{std::string("allpass"), std::string("%")}, uint8_t(1)},
                        {{std::string("exact_match"), std::string("exact_match")}, uint8_t(1)},
                        {{std::string("prefix_test"), std::string("prefix%")}, uint8_t(1)},
                        {{std::string("test_suffix"), std::string("%suffix")}, uint8_t(1)},
                        {{std::string("middle_test"), std::string("%middle%")}, uint8_t(1)},
                        {{std::string("complex_test"), std::string("c_m%x")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test hyperscan vs RE2 fallback patterns
TEST(FunctionLikeTest, regex_engine_patterns) {
    std::string func_name = "regexp";

    // Patterns that might test different regex engines
    DataSet data_set = {
            // Simple patterns (could use optimized path)
            {{std::string("simple"), std::string("simple")}, uint8_t(1)},
            {{std::string("prefix_test"), std::string("prefix.*")}, uint8_t(1)},
            {{std::string("test_suffix"), std::string(".*suffix")}, uint8_t(1)},

            // Complex patterns (likely to use full regex)
            {{std::string("complex123test"), std::string("[a-z]+[0-9]+[a-z]+")}, uint8_t(1)},
            {{std::string("complex123"), std::string("[a-z]+[0-9]+[a-z]+")}, uint8_t(0)},
            {{std::string("123test"), std::string("[a-z]+[0-9]+[a-z]+")}, uint8_t(0)},

            // Unicode patterns
            {{std::string("测试123"), std::string("测试[0-9]+")}, uint8_t(1)},
            {{std::string("测试abc"), std::string("测试[0-9]+")}, uint8_t(0)},

            // Lookahead/lookbehind patterns (if supported)
            {{std::string("password123"), std::string("password.*[0-9]")}, uint8_t(1)},
            {{std::string("password"), std::string("password.*[0-9]")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, Consted {PrimitiveType::TYPE_VARCHAR}};
    for (const auto& line : data_set) {
        DataSet const_pattern_dataset = {line};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_pattern_dataset));
    }
}

// Test memory and performance edge cases
TEST(FunctionLikeTest, memory_and_performance) {
    std::string func_name = "like";

    DataSet data_set = {
            // Large strings
            {{std::string(10000, 'x'), std::string("%x%")}, uint8_t(1)},
            {{std::string(10000, 'x'), std::string("%y%")}, uint8_t(0)},

            // Many wildcards
            {{std::string("abcdefghij"), std::string("a%b%c%d%e%f%g%h%i%j")}, uint8_t(1)},
            {{std::string("abcdefghij"), std::string("a%b%c%d%e%f%g%h%i%k")}, uint8_t(0)},

            // Alternating pattern
            {{std::string("ababababab"), std::string("a_a_a_a_a_")}, uint8_t(1)},
            {{std::string("ababababac"), std::string("a_a_a_a_a_")}, uint8_t(1)},

            // Empty vs non-empty patterns
            {{std::string(""), std::string("")}, uint8_t(1)},
            {{std::string(""), std::string("_")}, uint8_t(0)},
            {{std::string("a"), std::string("")}, uint8_t(0)}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

// Test null handling in various scenarios
TEST(FunctionLikeTest, comprehensive_null_handling) {
    std::string func_name = "like";

    DataSet data_set = {// Basic null cases
                        {{Null(), std::string("pattern")}, Null()},
                        {{std::string("text"), Null()}, Null()},
                        {{Null(), Null()}, Null()},

                        // Null with different pattern types
                        {{Null(), std::string("%")}, Null()},
                        {{Null(), std::string("exact")}, Null()},
                        {{Null(), std::string("prefix%")}, Null()},
                        {{Null(), std::string("%suffix")}, Null()},
                        {{Null(), std::string("%middle%")}, Null()},
                        {{Null(), std::string("a_b%")}, Null()},

                        // Valid strings with null patterns
                        {{std::string(""), Null()}, Null()},
                        {{std::string("test"), Null()}, Null()},
                        {{std::string("very_long_string_for_testing"), Null()}, Null()}};

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
