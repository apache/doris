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
#include <time.h>

#include <string>

#include "function_test_util.h"
#include "runtime/tuple_row.h"
#include "util/encryption_util.h"
#include "util/url_coding.h"
#include "vec/core/field.h"

namespace doris {

using vectorized::Null;
using vectorized::DataSet;
using vectorized::TypeIndex;

TEST(function_string_test, function_string_substr_test) {
    std::string func_name = "substr";
    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("asd你好"), 4, 10}, std::string("\xE4\xBD\xA0\xE5\xA5\xBD")}, //你好
            {{std::string("hello word"), -5, 5}, std::string(" word")},
            {{std::string("hello word"), 1, 12}, std::string("hello word")},
            {{std::string("HELLO,!^%"), 4, 2}, std::string("LO")},
            {{std::string(""), 5, 4}, Null()},
            {{Null(), 5, 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_strright_test) {
    std::string func_name = "strright";
    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {{{std::string("asd"), 1}, std::string("d")},
                        {{std::string("hello word"), -2}, std::string("ello word")},
                        {{std::string("hello word"), 20}, std::string("hello word")},
                        {{std::string("HELLO,!^%"), 2}, std::string("^%")},
                        {{std::string(""), 3}, std::string("")},
                        {{Null(), 3}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_strleft_test) {
    std::string func_name = "strleft";
    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {{{std::string("asd"), 1}, std::string("a")},
                        {{std::string("hel  lo  "), 5}, std::string("hel  ")},
                        {{std::string("hello word"), 20}, std::string("hello word")},
                        {{std::string("HELLO,!^%"), 7}, std::string("HELLO,!")},
                        {{std::string(""), 2}, Null()},
                        {{Null(), 3}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_lower_test) {
    std::string func_name = "lower";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("ASD")}, std::string("asd")},
                        {{std::string("HELLO123")}, std::string("hello123")},
                        {{std::string("MYtestSTR")}, std::string("myteststr")},
                        {{std::string("HELLO,!^%")}, std::string("hello,!^%")},
                        {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_upper_test) {
    std::string func_name = "upper";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd")}, std::string("ASD")},
                        {{std::string("hello123")}, std::string("HELLO123")},
                        {{std::string("HELLO,!^%")}, std::string("HELLO,!^%")},
                        {{std::string("MYtestStr")}, std::string("MYTESTSTR")},
                        {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_trim_test) {
    std::string func_name = "trim";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("a sd")}, std::string("a sd")},
                        {{std::string("  hello 123  ")}, std::string("hello 123")},
                        {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
                        {{std::string("MY test Str你好  ")}, std::string("MY test Str你好")},
                        {{Null()}, Null()},
                        {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_ltrim_test) {
    std::string func_name = "ltrim";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("a sd")}, std::string("a sd")},
            {{std::string("  hello 123  ")}, std::string("hello 123  ")},
            {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
            {{std::string("  你好MY test Str你好  ")}, std::string("你好MY test Str你好  ")},
            {{std::string("")}, std::string("")}};
    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_rtrim_test) {
    std::string func_name = "rtrim";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("a sd ")}, std::string("a sd")},
                        {{std::string("hello 123  ")}, std::string("hello 123")},
                        {{std::string("  HELLO,!^%")}, std::string("  HELLO,!^%")},
                        {{std::string("  MY test Str你好  ")}, std::string("  MY test Str你好")},
                        {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}
TEST(function_string_test, function_string_repeat_test) {
    std::string func_name = "repeat";
    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {{{std::string("a"), 3}, std::string("aaa")},
                        {{std::string("hel lo"), 2}, std::string("hel lohel lo")},
                        {{std::string("hello word"), -1}, std::string("")},
                        {{std::string(""), 1}, std::string("")},
                        {{std::string("HELLO,!^%"), 2}, std::string("HELLO,!^%HELLO,!^%")},
                        {{std::string("你"), 2}, std::string("你你")}};
    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_reverse_test) {
    std::string func_name = "reverse";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd ")}, std::string(" dsa")},
                        {{std::string("  hello 123  ")}, std::string("  321 olleh  ")},
                        {{std::string("  HELLO,!^%")}, std::string("%^!,OLLEH  ")},
                        {{std::string("你好啊")}, std::string("啊好你")},
                        {{std::string("")}, std::string("")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_length_test) {
    std::string func_name = "length";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd ")}, int32_t(4)},
                        {{std::string("  hello 123  ")}, int32_t(13)},
                        {{std::string("  HELLO,!^%")}, int32_t(11)},
                        {{std::string("你好啊")}, int32_t(9)},
                        {{std::string("")}, int32_t(0)}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_append_trailing_char_if_absent_test) {
    std::string func_name = "append_trailing_char_if_absent";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("ASD"), std::string("D")}, std::string("ASD")},
                        {{std::string("AS"), std::string("D")}, std::string("ASD")},
                        {{std::string(""), std::string("")}, Null()},
                        {{std::string(""), std::string("A")}, std::string("A")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_starts_with_test) {
    std::string func_name = "starts_with";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(1)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(0)},
                        {{std::string("你好"), std::string("你")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    vectorized::check_function<vectorized::DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ends_with_test) {
    std::string func_name = "ends_with";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(0)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(1)},
                        {{std::string("你好"), std::string("好")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    vectorized::check_function<vectorized::DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_lpad_test) {
    std::string func_name = "lpad";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

    DataSet data_set = {{{std::string("hi"), 5, std::string("?")}, std::string("???hi")},
                        {{std::string("g8%7IgY%AHx7luNtf8Kh"), 20, std::string("")},
                         std::string("g8%7IgY%AHx7luNtf8Kh")},
                        {{std::string("hi"), 1, std::string("?")}, std::string("h")},
                        {{std::string("你好"), 1, std::string("?")}, std::string("你")},
                        {{std::string("hi"), 0, std::string("?")}, std::string("")},
                        {{std::string("hi"), -1, std::string("?")}, Null()},
                        {{std::string("h"), 1, std::string("")}, std::string("h")},
                        {{std::string("hi"), 5, std::string("")}, Null()},
                        {{std::string("hi"), 5, std::string("ab")}, std::string("abahi")},
                        {{std::string("hi"), 5, std::string("呵呵")}, std::string("呵呵呵hi")},
                        {{std::string("呵呵"), 5, std::string("hi")}, std::string("hih呵呵")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_rpad_test) {
    std::string func_name = "rpad";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

    DataSet data_set = {{{std::string("hi"), 5, std::string("?")}, std::string("hi???")},
                        {{std::string("g8%7IgY%AHx7luNtf8Kh"), 20, std::string("")},
                         std::string("g8%7IgY%AHx7luNtf8Kh")},
                        {{std::string("hi"), 1, std::string("?")}, std::string("h")},
                        {{std::string("你好"), 1, std::string("?")}, std::string("你")},
                        {{std::string("hi"), 0, std::string("?")}, std::string("")},
                        {{std::string("hi"), -1, std::string("?")}, Null()},
                        {{std::string("h"), 1, std::string("")}, std::string("h")},
                        {{std::string("hi"), 5, std::string("")}, Null()},
                        {{std::string("hi"), 5, std::string("ab")}, std::string("hiaba")},
                        {{std::string("hi"), 5, std::string("呵呵")}, std::string("hi呵呵呵")},
                        {{std::string("呵呵"), 5, std::string("hi")}, std::string("呵呵hih")}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ascii_test) {
    std::string func_name = "ascii";

    std::vector<std::any> input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, 0},
                        {{std::string("aa")}, 97},
                        {{std::string("我")}, 230},
                        {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_char_length_test) {
    std::string func_name = "char_length";

    std::vector<std::any> input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, 0},    {{std::string("aa")}, 2},
                        {{std::string("我")}, 1},  {{std::string("我a")}, 2},
                        {{std::string("a我")}, 2}, {{std::string("123")}, 3},
                        {{Null()}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_concat_test) {
    std::string func_name = "concat";
    {
        std::vector<std::any> input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("")}, std::string("")},
                            {{std::string("123")}, std::string("123")},
                            {{Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string(""), std::string("")}, std::string("")},
                            {{std::string("123"), std::string("45")}, std::string("12345")},
                            {{std::string("123"), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String,
                                             TypeIndex::String};

        DataSet data_set = {
                {{std::string(""), std::string("1"), std::string("")}, std::string("1")},
                {{std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("123"), Null(), std::string("789")}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };
}

TEST(function_string_test, function_concat_ws_test) {
    std::string func_name = "concat_ws";
    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("-"), std::string("")}, std::string("")},
                            {{std::string(""), std::string("123")}, std::string("123")},
                            {{std::string(""), std::string("")}, std::string("")},
                            {{Null(), std::string("")}, Null()},
                            {{Null(), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String,
                                             TypeIndex::String};

        DataSet data_set = {
                {{std::string("-"), std::string(""), std::string("")}, std::string("-")},
                {{std::string(""), std::string("123"), std::string("456")}, std::string("123456")},
                {{std::string(""), std::string(""), std::string("")}, std::string("")},
                {{Null(), std::string(""), std::string("")}, Null()},
                {{Null(), std::string(""), Null()}, Null()}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String,
                                             TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("-"), std::string(""), std::string(""), std::string("")},
                 std::string("--")},
                {{std::string(""), std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("-"), std::string(""), std::string("?"), std::string("")},
                 std::string("-?-")},
                {{Null(), std::string(""), std::string("?"), std::string("")}, Null()},
                {{std::string("-"), std::string("123"), Null(), std::string("456")},
                 std::string("123-456")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    };
}

TEST(function_string_test, function_null_or_empty_test) {
    std::string func_name = "null_or_empty";

    std::vector<std::any> input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, uint8(true)},
                        {{std::string("aa")}, uint8(false)},
                        {{std::string("我")}, uint8(false)},
                        {{Null()}, uint8(true)}};

    vectorized::check_function<vectorized::DataTypeUInt8, false>(func_name, input_types, data_set);
}

TEST(function_string_test, function_to_base64_test) {
    std::string func_name = "to_base64";
    std::vector<std::any> input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("asd你好")}, {std::string("YXNk5L2g5aW9")}},
                        {{std::string("hello world")}, {std::string("aGVsbG8gd29ybGQ=")}},
                        {{std::string("HELLO,!^%")}, {std::string("SEVMTE8sIV4l")}},
                        {{std::string("")}, {Null()}},
                        {{std::string("MYtestSTR")}, {std::string("TVl0ZXN0U1RS")}},
                        {{std::string("ò&ø")}, {std::string("w7Imw7g=")}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_from_base64_test) {
    std::string func_name = "from_base64";
    std::vector<std::any> input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("YXNk5L2g5aW9")}, {std::string("asd你好")}},
                        {{std::string("aGVsbG8gd29ybGQ=")}, {std::string("hello world")}},
                        {{std::string("SEVMTE8sIV4l")}, {std::string("HELLO,!^%")}},
                        {{std::string("")}, {Null()}},
                        {{std::string("TVl0ZXN0U1RS")}, {std::string("MYtestSTR")}},
                        {{std::string("w7Imw7g=")}, {std::string("ò&ø")}},
                        {{std::string("ò&ø")}, {Null()}},
                        {{std::string("你好哈喽")}, {Null()}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_reverse_test) {
    std::string func_name = "reverse";
    std::vector<std::any> input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("")}, {std::string("")}},
            {{std::string("a")}, {std::string("a")}},
            {{std::string("美团和和阿斯顿百度ab")}, {std::string("ba度百顿斯阿和和团美")}},
            {{std::string("!^%")}, {std::string("%^!")}},
            {{std::string("ò&ø")}, {std::string("ø&ò")}},
            {{std::string("A攀c")}, {std::string("c攀A")}},
            {{std::string("NULL")}, {std::string("LLUN")}}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_instr_test) {
    std::string func_name = "instr";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("abcdefg"), std::string("efg")}, 5},
                        {{std::string("aa"), std::string("a")}, 1},
                        {{std::string("我是"), std::string("是")}, 2},
                        {{std::string("abcd"), std::string("e")}, 0},
                        {{std::string("abcdef"), std::string("")}, 1},
                        {{std::string(""), std::string("")}, 1},
                        {{std::string("aaaab"), std::string("bb")}, 0}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_find_in_set_test) {
    std::string func_name = "find_in_set";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("abcdefg"), std::string("a,b,c")}, 0},
                        {{std::string("aa"), std::string("a,aa,aaa")}, 2},
                        {{std::string("aa"), std::string("aa,aa,aa")}, 1},
                        {{std::string("a"), Null()}, Null()},
                        {{Null(), std::string("aa")}, Null()},
                        {{std::string("a"), std::string("")}, 0},
                        {{std::string(""), std::string(",,")}, 1}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_splitpart_test) {
    std::string func_name = "split_part";
    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("prefix_string1"), std::string("_"), 2}, std::string("string1")},
            {{std::string("prefix__string2"), std::string("__"), 2}, std::string("string2")},
            {{std::string("prefix__string2"), std::string("_"), 2}, std::string("")},
            {{std::string("prefix_string2"), std::string("__"), 1}, Null()},
            {{Null(), std::string("__"), 1}, Null()},
            {{std::string("prefix_string"), Null(), 1}, Null()},
            {{std::string("prefix_string"), std::string("__"), Null()}, Null()},
            {{std::string("prefix_string"), std::string("__"), -1}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_md5sum_test) {
    std::string func_name = "md5sum";

    {
        std::vector<std::any> input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd你好")}, {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                {{std::string("hello world")}, {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                {{std::string("HELLO,!^%")}, {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                {{std::string("")}, {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                {{std::string(" ")}, {std::string("7215ee9c7d9dc229d2921a40e899ec5f")}},
                {{Null()}, {Null()}},
                {{std::string("MYtestSTR")}, {std::string("cd24c90b3fc1192eb1879093029e87d4")}},
                {{std::string("ò&ø")}, {std::string("fd157b4cb921fa91acc667380184d59c")}}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {{{std::string("asd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string("hello "), std::string("world")},
                             {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                            {{std::string("HELLO"), std::string(",!^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO")}, {Null()}}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String,
                                             TypeIndex::String};
        DataSet data_set = {{{std::string("a"), std::string("sd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string(""), std::string(""), std::string("")},
                             {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                            {{std::string("HEL"), std::string("LO,!"), std::string("^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO"), Null()}, {Null()}}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
}

TEST(function_string_test, function_aes_encrypt_test) {
    std::string func_name = "aes_encrypt";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    const char* key = "doris";
    const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
    std::string r[5];

    for (int i = 0; i < 5; i++) {
        int cipher_len = strlen(src[i]) + 16;
        char p[cipher_len];

        int outlen =
                EncryptionUtil::encrypt(AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                                 (unsigned char*)key, strlen(key), NULL, true, (unsigned char*)p);
        r[i] = std::string(p, outlen);
    }

    DataSet data_set = {{{std::string(src[0]), std::string(key)}, r[0]},
                        {{std::string(src[1]), std::string(key)}, r[1]},
                        {{std::string(src[2]), std::string(key)}, r[2]},
                        {{std::string(src[3]), std::string(key)}, r[3]},
                        {{std::string(src[4]), std::string(key)}, r[4]},
                        {{std::string(src[5]), std::string(key)}, Null()},
                        {{Null(), std::string(key)}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_aes_decrypt_test) {
    std::string func_name = "aes_decrypt";

    std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};

    const char* key = "doris";
    const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
    std::string r[5];

    for (int i = 0; i < 5; i++) {
        int cipher_len = strlen(src[i]) + 16;
        char p[cipher_len];

        int outlen =
                EncryptionUtil::encrypt(AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                                 (unsigned char*)key, strlen(key), NULL, true, (unsigned char*)p);
        r[i] = std::string(p, outlen);
    }

    DataSet data_set = {{{r[0], std::string(key)}, std::string(src[0])},
                        {{r[1], std::string(key)}, std::string(src[1])},
                        {{r[2], std::string(key)}, std::string(src[2])},
                        {{r[3], std::string(key)}, std::string(src[3])},
                        {{r[4], std::string(key)}, std::string(src[4])},
                        {{Null(), std::string(key)}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_parse_url_test) {
    std::string func_name = "parse_url";

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("zhangsan"), std::string("HOST")}, {Null()}},
                {{std::string("facebook.com/path/p1"), std::string("HOST")}, {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("HOST")},
                 {std::string("fb.com")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("AUTHORITY")},
                 {std::string("facebook.com")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("authority")},
                 {std::string("facebook.com")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("FILE")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("file")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("PATH")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("path")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("PROTOCOL")},
                 {std::string("http")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("protocol")},
                 {std::string("http")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("QUERY")},
                 {std::string("a=b")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("query")},
                 {std::string("a=b")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("REF")}, {Null()}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("ref")}, {Null()}},
                {{std::string("http://www.baidu.com:9090/a/b/c?a=b"), std::string("PORT")},
                 {std::string("9090")}},
                {{std::string("http://www.baidu.com/a/b/c?a=b"), std::string("PORT")}, {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("QUERY")},
                 {std::string("q=1")}}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }

    {
        std::vector<std::any> input_types = {TypeIndex::String, TypeIndex::String,
                                             TypeIndex::String};
        DataSet data_set = {
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("QUERY"),
                  std::string("q")},
                 {std::string("1")}},
                {{std::string("fb.com/path/p1.p?q=1#f"), std::string("QUERY"), std::string("q")},
                 {std::string("1")}},
                {{std::string("http://facebook.com/path/p1"), std::string("QUERY"),
                  std::string("q")},
                 {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("HOST"),
                  std::string("q")},
                 {Null()}}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
}

TEST(function_string_test, function_hex_test) {
    std::string func_name = "hex";
    std::vector<std::any> input_types = {vectorized::TypeIndex::String};
    DataSet data_set = {{{Null()}, Null()},
                        {{std::string("0")}, std::string("30")},
                        {{std::string("1")}, std::string("31")},
                        {{std::string("")}, std::string("")},
                        {{std::string("123")}, std::string("313233")},
                        {{std::string("A")}, std::string("41")},
                        {{std::string("a")}, std::string("61")},
                        {{std::string("我")}, std::string("E68891")},
                        {{std::string("?")}, std::string("3F")},
                        {{std::string("？")}, std::string("EFBC9F")}};
    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_unhex_test) {
    std::string func_name = "unhex";
    std::vector<std::any> input_types = {vectorized::TypeIndex::String};
    DataSet data_set = {{{Null()}, {Null()}},
                        {{std::string("@!#")}, std::string("")},
                        {{std::string("")}, std::string("")},
                        {{std::string("ò&ø")}, std::string("")},
                        {{std::string("@@")}, std::string("")},
                        {{std::string("61")}, std::string("a")},
                        {{std::string("41")}, std::string("A")},
                        {{std::string("313233")}, std::string("123")},
                        {{std::string("EFBC9F")}, std::string("？")}};
    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_coalesce_test) {
    std::string func_name = "coalesce";
    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32,
                                             vectorized::TypeIndex::Int32,
                                             vectorized::TypeIndex::Int32};
        DataSet data_set = {{{Null(), Null(), (int32_t)1}, {(int32_t)1}},
                            {{Null(), Null(), (int32_t)2}, {(int32_t)2}},
                            {{Null(), Null(), (int32_t)3}, {(int32_t)3}},
                            {{Null(), Null(), (int32_t)4}, {(int32_t)4}}};
        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types,
                                                                    data_set);
    }

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::String,
                                             vectorized::TypeIndex::String,
                                             vectorized::TypeIndex::Int32};
        DataSet data_set = {
                {{std::string("qwer"), Null(), (int32_t)1}, {std::string("qwer")}},
                {{std::string("asdf"), Null(), (int32_t)2}, {std::string("asdf")}},
                {{std::string("zxcv"), Null(), (int32_t)3}, {std::string("zxcv")}},
                {{std::string("vbnm"), Null(), (int32_t)4}, {std::string("vbnm")}},
        };
        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::String,
                                             vectorized::TypeIndex::String,
                                             vectorized::TypeIndex::String};
        DataSet data_set = {
                {{Null(), std::string("abc"), std::string("hij")}, {std::string("abc")}},
                {{Null(), std::string("def"), std::string("klm")}, {std::string("def")}},
                {{Null(), std::string(""), std::string("xyz")}, {std::string("")}},
                {{Null(), Null(), std::string("uvw")}, {std::string("uvw")}}};
        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
