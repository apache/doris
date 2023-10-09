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

#include <stdint.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/integral_types.h"
#include "testutil/any_type.h"
#include "util/encryption_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(function_string_test, function_string_substr_test) {
    std::string func_name = "substr";

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("asd你好"), 4, 10}, std::string("\xE4\xBD\xA0\xE5\xA5\xBD")}, //你好
                {{std::string("hello word"), -5, 5}, std::string(" word")},
                {{std::string("hello word"), 1, 12}, std::string("hello word")},
                {{std::string("HELLO,!^%"), 4, 2}, std::string("LO")},
                {{std::string(""), 5, 4}, std::string("")},
                {{std::string(""), -1, 4}, std::string("")},
                {{std::string("12"), 3, 4}, std::string("")},
                {{std::string(""), 0, 4}, std::string("")},
                {{std::string("123"), 0, 4}, std::string("")},
                {{std::string("123"), 1, 0}, std::string("")},
                {{Null(), 5, 4}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("asd你好"), 4}, std::string("\xE4\xBD\xA0\xE5\xA5\xBD")}, //你好
                {{std::string("hello word"), -5}, std::string(" word")},
                {{std::string("hello word"), 1}, std::string("hello word")},
                {{std::string("HELLO,!^%"), 4}, std::string("LO,!^%")},
                {{std::string(""), 5}, std::string("")},
                {{std::string(""), -1}, std::string("")},
                {{std::string("12"), 3}, std::string("")},
                {{std::string(""), 0}, std::string("")},
                {{std::string("123"), 0}, std::string("")},
                {{Null(), 5, 4}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_string_strright_test) {
    std::string func_name = "strright";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {{{std::string("asd"), 1}, std::string("d")},
                        {{std::string("hello word"), -2}, std::string("ello word")},
                        {{std::string("hello word"), 20}, std::string("hello word")},
                        {{std::string("HELLO,!^%"), 2}, std::string("^%")},
                        {{std::string(""), 3}, std::string("")},
                        {{Null(), 3}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_strleft_test) {
    std::string func_name = "strleft";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {{{std::string("asd"), 1}, std::string("a")},
                        {{std::string("hel  lo  "), 5}, std::string("hel  ")},
                        {{std::string("hello word"), 20}, std::string("hello word")},
                        {{std::string("HELLO,!^%"), 7}, std::string("HELLO,!")},
                        {{std::string(""), 2}, std::string("")},
                        {{std::string(""), -2}, std::string("")},
                        {{std::string(""), 0}, std::string("")},
                        {{std::string("123"), 0}, std::string("")},
                        {{Null(), 3}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_lower_test) {
    std::string func_name = "lower";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("ASD")}, std::string("asd")},
                        {{std::string("HELLO123")}, std::string("hello123")},
                        {{std::string("MYtestSTR")}, std::string("myteststr")},
                        {{std::string("HELLO,!^%")}, std::string("hello,!^%")},
                        {{std::string("")}, std::string("")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_upper_test) {
    std::string func_name = "upper";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd")}, std::string("ASD")},
                        {{std::string("hello123")}, std::string("HELLO123")},
                        {{std::string("HELLO,!^%")}, std::string("HELLO,!^%")},
                        {{std::string("MYtestStr")}, std::string("MYTESTSTR")},
                        {{std::string("")}, std::string("")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_trim_test) {
    std::string func_name = "trim";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("a sd")}, std::string("a sd")},
                        {{std::string("  hello 123  ")}, std::string("hello 123")},
                        {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
                        {{std::string("MY test Str你好  ")}, std::string("MY test Str你好")},
                        {{Null()}, Null()},
                        {{std::string("")}, std::string("")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_ltrim_test) {
    std::string func_name = "ltrim";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("a sd")}, std::string("a sd")},
            {{std::string("  hello 123  ")}, std::string("hello 123  ")},
            {{std::string("  HELLO,!^%")}, std::string("HELLO,!^%")},
            {{std::string("  你好MY test Str你好  ")}, std::string("你好MY test Str你好  ")},
            {{std::string("")}, std::string("")}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_rtrim_test) {
    std::string func_name = "rtrim";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("a sd ")}, std::string("a sd")},
                        {{std::string("hello 123  ")}, std::string("hello 123")},
                        {{std::string("  HELLO,!^%")}, std::string("  HELLO,!^%")},
                        {{std::string("  MY test Str你好  ")}, std::string("  MY test Str你好")},
                        {{std::string("")}, std::string("")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}
TEST(function_string_test, function_string_repeat_test) {
    std::string func_name = "repeat";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("a"), 3}, std::string("aaa")},
            {{std::string("hel lo"), 2}, std::string("hel lohel lo")},
            {{std::string("hello word"), -1}, std::string("")},
            {{std::string(""), 1}, std::string("")},
            {{std::string("a"), 1073741825}, std::string("aaaaaaaaaa")}, // ut repeat max num 10
            {{std::string("HELLO,!^%"), 2}, std::string("HELLO,!^%HELLO,!^%")},
            {{std::string("你"), 2}, std::string("你你")}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_reverse_test) {
    std::string func_name = "reverse";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd ")}, std::string(" dsa")},
                        {{std::string("  hello 123  ")}, std::string("  321 olleh  ")},
                        {{std::string("  HELLO,!^%")}, std::string("%^!,OLLEH  ")},
                        {{std::string("你好啊")}, std::string("啊好你")},
                        {{std::string("")}, std::string("")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_string_length_test) {
    std::string func_name = "length";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{std::string("asd ")}, int32_t(4)},
                        {{std::string("  hello 123  ")}, int32_t(13)},
                        {{std::string("  HELLO,!^%")}, int32_t(11)},
                        {{std::string("你好啊")}, int32_t(9)},
                        {{std::string("")}, int32_t(0)}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_append_trailing_char_if_absent_test) {
    std::string func_name = "append_trailing_char_if_absent";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("ASD"), std::string("D")}, std::string("ASD")},
                        {{std::string("AS"), std::string("D")}, std::string("ASD")},
                        {{std::string(""), std::string("")}, Null()},
                        {{std::string(""), std::string("A")}, std::string("A")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_starts_with_test) {
    std::string func_name = "starts_with";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(1)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(0)},
                        {{std::string("你好"), std::string("你")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_ends_with_test) {
    std::string func_name = "ends_with";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(0)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(1)},
                        {{std::string("你好"), std::string("好")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_lpad_test) {
    std::string func_name = "lpad";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

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

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_rpad_test) {
    std::string func_name = "rpad";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

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

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_ascii_test) {
    std::string func_name = "ascii";

    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, 0},
                        {{std::string("aa")}, 97},
                        {{std::string("我")}, 230},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_char_length_test) {
    std::string func_name = "char_length";

    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, 0},    {{std::string("aa")}, 2},
                        {{std::string("我")}, 1},  {{std::string("我a")}, 2},
                        {{std::string("a我")}, 2}, {{std::string("123")}, 3},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_concat_test) {
    std::string func_name = "concat";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("")}, std::string("")},
                            {{std::string("123")}, std::string("123")},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string(""), std::string("")}, std::string("")},
                            {{std::string("123"), std::string("45")}, std::string("12345")},
                            {{std::string("123"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string(""), std::string("1"), std::string("")}, std::string("1")},
                {{std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("123"), Null(), std::string("789")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };
}

TEST(function_string_test, function_elt_test) {
    std::string func_name = "elt";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{1, std::string("hello"), std::string("world")}, std::string("hello")},
                            {{1, std::string("你好"), std::string("百度")}, std::string("你好")},
                            {{1, std::string("hello"), std::string("")}, std::string("hello")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{2, std::string("hello"), std::string("world")}, std::string("world")},
                            {{2, std::string("你好"), std::string("百度")}, std::string("百度")},
                            {{2, std::string("hello"), std::string("")}, std::string("")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{0, std::string("hello"), std::string("world")}, Null()},
                            {{0, std::string("你好"), std::string("百度")}, Null()},
                            {{0, std::string("hello"), std::string("")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{3, std::string("hello"), std::string("world")}, Null()},
                            {{3, std::string("你好"), std::string("百度")}, Null()},
                            {{3, std::string("hello"), std::string("")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };
}

TEST(function_string_test, function_concat_ws_test) {
    std::string func_name = "concat_ws";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("-"), std::string("")}, std::string("")},
                            {{std::string(""), std::string("123")}, std::string("123")},
                            {{std::string(""), std::string("")}, std::string("")},
                            {{Null(), std::string("")}, Null()},
                            {{Null(), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("-"), std::string(""), std::string("")}, std::string("-")},
                {{std::string(""), std::string("123"), std::string("456")}, std::string("123456")},
                {{std::string(""), std::string(""), std::string("")}, std::string("")},
                {{Null(), std::string(""), std::string("")}, Null()},
                {{Null(), std::string(""), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};

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

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field("", 0), Field("", 0), Field("", 0)};
        Array vec2 = {Field("123", 3), Field("456", 3), Field("789", 3)};
        Array vec3 = {Field("", 0), Field("?", 1), Field("", 0)};
        Array vec4 = {Field("abc", 3), Field("", 0), Field("def", 3)};
        Array vec5 = {Field("abc", 3), Field("def", 3), Field("ghi", 3)};
        DataSet data_set = {{{std::string("-"), vec1}, std::string("--")},
                            {{std::string(""), vec2}, std::string("123456789")},
                            {{std::string("-"), vec3}, std::string("-?-")},
                            {{Null(), vec4}, Null()},
                            {{std::string("-"), vec5}, std::string("abc-def-ghi")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    };
}

TEST(function_string_test, function_null_or_empty_test) {
    std::string func_name = "null_or_empty";

    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, uint8(true)},
                        {{std::string("aa")}, uint8(false)},
                        {{std::string("我")}, uint8(false)},
                        {{Null()}, uint8(true)}};

    static_cast<void>(check_function<DataTypeUInt8, false>(func_name, input_types, data_set));
}

TEST(function_string_test, function_to_base64_test) {
    std::string func_name = "to_base64";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("asd你好")}, {std::string("YXNk5L2g5aW9")}},
                        {{std::string("hello world")}, {std::string("aGVsbG8gd29ybGQ=")}},
                        {{std::string("HELLO,!^%")}, {std::string("SEVMTE8sIV4l")}},
                        {{std::string("")}, {Null()}},
                        {{std::string("MYtestSTR")}, {std::string("TVl0ZXN0U1RS")}},
                        {{std::string("ò&ø")}, {std::string("w7Imw7g=")}}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_from_base64_test) {
    std::string func_name = "from_base64";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("YXNk5L2g5aW9")}, {std::string("asd你好")}},
                        {{std::string("aGVsbG8gd29ybGQ=")}, {std::string("hello world")}},
                        {{std::string("SEVMTE8sIV4l")}, {std::string("HELLO,!^%")}},
                        {{std::string("")}, {Null()}},
                        {{std::string("TVl0ZXN0U1RS")}, {std::string("MYtestSTR")}},
                        {{std::string("w7Imw7g=")}, {std::string("ò&ø")}},
                        {{std::string("ò&ø")}, {Null()}},
                        {{std::string("你好哈喽")}, {Null()}}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_reverse_test) {
    std::string func_name = "reverse";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("")}, {std::string("")}},
            {{std::string("a")}, {std::string("a")}},
            {{std::string("美团和和阿斯顿百度ab")}, {std::string("ba度百顿斯阿和和团美")}},
            {{std::string("!^%")}, {std::string("%^!")}},
            {{std::string("ò&ø")}, {std::string("ø&ò")}},
            {{std::string("A攀c")}, {std::string("c攀A")}},
            {{std::string("NULL")}, {std::string("LLUN")}}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_instr_test) {
    std::string func_name = "instr";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
            {{STRING("abcdefg"), STRING("efg")}, INT(5)}, {{STRING("aa"), STRING("a")}, INT(1)},
            {{STRING("我是"), STRING("是")}, INT(2)},     {{STRING("abcd"), STRING("e")}, INT(0)},
            {{STRING("abcdef"), STRING("")}, INT(1)},     {{STRING(""), STRING("")}, INT(1)},
            {{STRING("aaaab"), STRING("bb")}, INT(0)}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_locate_test) {
    std::string func_name = "locate";

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{STRING("efg"), STRING("abcdefg")}, INT(5)},
                            {{STRING("a"), STRING("aa")}, INT(1)},
                            {{STRING("是"), STRING("我是")}, INT(2)},
                            {{STRING("e"), STRING("abcd")}, INT(0)},
                            {{STRING(""), STRING("abcdef")}, INT(1)},
                            {{STRING(""), STRING("")}, INT(1)},
                            {{STRING("bb"), STRING("aaaab")}, INT(0)}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {{{STRING("bar"), STRING("foobarbar"), INT(5)}, INT(7)},
                            {{STRING("xbar"), STRING("foobar"), INT(1)}, INT(0)},
                            {{STRING(""), STRING("foobar"), INT(2)}, INT(2)},
                            {{STRING("A"), STRING("大A写的A"), INT(0)}, INT(0)},
                            {{STRING("A"), STRING("大A写的A"), INT(1)}, INT(2)},
                            {{STRING("A"), STRING("大A写的A"), INT(2)}, INT(2)},
                            {{STRING("A"), STRING("大A写的A"), INT(3)}, INT(5)}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_find_in_set_test) {
    std::string func_name = "find_in_set";

    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("abcdefg"), std::string("a,b,c")}, 0},
                        {{std::string("aa"), std::string("a,aa,aaa")}, 2},
                        {{std::string("aa"), std::string("aa,aa,aa")}, 1},
                        {{std::string("a"), Null()}, Null()},
                        {{Null(), std::string("aa")}, Null()},
                        {{std::string("a"), std::string("")}, 0},
                        {{std::string(""), std::string(",,")}, 1}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_md5sum_test) {
    std::string func_name = "md5sum";

    {
        InputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd你好")}, {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                {{std::string("hello world")}, {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                {{std::string("HELLO,!^%")}, {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                {{std::string("")}, {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                {{std::string(" ")}, {std::string("7215ee9c7d9dc229d2921a40e899ec5f")}},
                {{Null()}, {Null()}},
                {{std::string("MYtestSTR")}, {std::string("cd24c90b3fc1192eb1879093029e87d4")}},
                {{std::string("ò&ø")}, {std::string("fd157b4cb921fa91acc667380184d59c")}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {{{std::string("asd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string("hello "), std::string("world")},
                             {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                            {{std::string("HELLO"), std::string(",!^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO")}, {Null()}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {{{std::string("a"), std::string("sd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string(""), std::string(""), std::string("")},
                             {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                            {{std::string("HEL"), std::string("LO,!"), std::string("^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO"), Null()}, {Null()}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_sm3sum_test) {
    std::string func_name = "sm3sum";

    {
        InputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string("hello world")},
                 {std::string("44f0061e69fa6fdfc290c494654a05dc0c053da7e5c52b84ef93a9d67d3fff88")}},
                {{std::string("HELLO,!^%")},
                 {std::string("5fc6e38f40b31a659a59e1daba9b68263615f20c02037b419d9deb3509e6b5c6")}},
                {{std::string("")},
                 {std::string("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b")}},
                {{std::string(" ")},
                 {std::string("2ae1d69bb8483e5944310c877573b21d0a420c3bf4a2a91b1a8370d760ba67c5")}},
                {{Null()}, {Null()}},
                {{std::string("MYtestSTR")},
                 {std::string("3155ae9f834cae035385fc15b69b6f2c051b91de943ea9a03ab8bfd497aef4c6")}},
                {{std::string("ò&ø")},
                 {std::string(
                         "aa47ac31c85aa819d4cc80c932e7900fa26a3073a67aa7eb011bc2ba4924a066")}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd"), std::string("你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string("hello "), std::string("world")},
                 {std::string("44f0061e69fa6fdfc290c494654a05dc0c053da7e5c52b84ef93a9d67d3fff88")}},
                {{std::string("HELLO "), std::string(",!^%")},
                 {std::string("1f5866e786ebac9ffed0dbd8f2586e3e99d1d05f7efe7c5915478b57b7423570")}},
                {{Null(), std::string("HELLO")}, {Null()}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("a"), std::string("sd"), std::string("你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string(""), std::string(""), std::string("")},
                 {std::string("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b")}},
                {{std::string("HEL"), std::string("LO,!"), std::string("^%")},
                 {std::string("5fc6e38f40b31a659a59e1daba9b68263615f20c02037b419d9deb3509e6b5c6")}},
                {{Null(), std::string("HELLO"), Null()}, {Null()}}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_aes_encrypt_test) {
    std::string func_name = "aes_encrypt";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        const char* mode = "AES_128_ECB";
        const char* key = "doris";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];

            int outlen = EncryptionUtil::encrypt(
                    EncryptionMode::AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                    (unsigned char*)key, strlen(key), nullptr, 0, true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {{{std::string(src[0]), std::string(key), std::string(mode)}, r[0]},
                            {{std::string(src[1]), std::string(key), std::string(mode)}, r[1]},
                            {{std::string(src[2]), std::string(key), std::string(mode)}, r[2]},
                            {{std::string(src[3]), std::string(key), std::string(mode)}, r[3]},
                            {{std::string(src[4]), std::string(key), std::string(mode)}, r[4]},
                            {{std::string(src[5]), std::string(key), std::string(mode)}, Null()},
                            {{Null(), std::string(key), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};
        const char* iv = "0123456789abcdef";
        const char* mode = "AES_256_ECB";
        const char* key = "vectorized";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::AES_256_ECB, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_aes_decrypt_test) {
    std::string func_name = "aes_decrypt";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        const char* mode = "AES_128_ECB";
        const char* key = "doris";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];

            int outlen = EncryptionUtil::encrypt(
                    EncryptionMode::AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                    (unsigned char*)key, strlen(key), nullptr, 0, true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {{{r[0], std::string(key), std::string(mode)}, std::string(src[0])},
                            {{r[1], std::string(key), std::string(mode)}, std::string(src[1])},
                            {{r[2], std::string(key), std::string(mode)}, std::string(src[2])},
                            {{r[3], std::string(key), std::string(mode)}, std::string(src[3])},
                            {{r[4], std::string(key), std::string(mode)}, std::string(src[4])},
                            {{Null(), std::string(key), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};
        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "AES_128_OFB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};

        std::string r[5];
        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::AES_128_OFB, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }
        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_sm4_encrypt_test) {
    std::string func_name = "sm4_encrypt";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};

        const char* key = "doris";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_ECB";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::SM4_128_ECB, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};

        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_CTR";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::SM4_128_CTR, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_sm4_decrypt_test) {
    std::string func_name = "sm4_decrypt";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};

        const char* key = "doris";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_ECB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::SM4_128_ECB, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                    TypeIndex::String};

        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_OFB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            char p[cipher_len];
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen =
                    EncryptionUtil::encrypt(EncryptionMode::SM4_128_OFB, (unsigned char*)src[i],
                                            strlen(src[i]), (unsigned char*)key, strlen(key),
                                            init_vec.get(), strlen(iv), true, (unsigned char*)p);
            r[i] = std::string(p, outlen);
        }

        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), Null(), std::string(iv), std::string(mode)}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_extract_url_parameter_test) {
    std::string func_name = "extract_url_parameter";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR(""), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa"), VARCHAR("")}, {VARCHAR("")}},
            {{VARCHAR("https://doris.apache.org/"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa"), VARCHAR("k1")}, {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org:8080?k1&k2=bb#99"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa#999"), VARCHAR("k1")}, {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k1")},
             {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k2")},
             {VARCHAR("bb")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("999")},
             {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k3")},
             {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("test")},
             {VARCHAR("dd")}}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_parse_url_test) {
    std::string func_name = "parse_url";

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("zhangsan"), std::string("HOST")}, {Null()}},
                {{std::string("facebook.com/path/p1"), std::string("HOST")}, {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("HOST")},
                 {std::string("fb.com")}},
                {{std::string("https://www.facebook.com/aa/bb?returnpage=https://www.facebook.com/"
                              "aa/bb/cc"),
                  std::string("HOST")},
                 {std::string("www.facebook.com")}},
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

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
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

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_hex_test) {
    std::string func_name = "hex";
    InputTypeSet input_types = {TypeIndex::String};
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
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_unhex_test) {
    std::string func_name = "unhex";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{Null()}, {Null()}},
                        {{std::string("@!#")}, std::string("")},
                        {{std::string("")}, std::string("")},
                        {{std::string("ò&ø")}, std::string("")},
                        {{std::string("@@")}, std::string("")},
                        {{std::string("61")}, std::string("a")},
                        {{std::string("41")}, std::string("A")},
                        {{std::string("313233")}, std::string("123")},
                        {{std::string("EFBC9F")}, std::string("？")}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_coalesce_test) {
    std::string func_name = "coalesce";
    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32, TypeIndex::Int32};
        DataSet data_set = {{{Null(), Null(), (int32_t)1}, {(int32_t)1}},
                            {{Null(), Null(), (int32_t)2}, {(int32_t)2}},
                            {{Null(), Null(), (int32_t)3}, {(int32_t)3}},
                            {{Null(), Null(), (int32_t)4}, {(int32_t)4}}};
        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};
        DataSet data_set = {
                {{std::string("qwer"), Null(), (int32_t)1}, {std::string("qwer")}},
                {{std::string("asdf"), Null(), (int32_t)2}, {std::string("asdf")}},
                {{std::string("zxcv"), Null(), (int32_t)3}, {std::string("zxcv")}},
                {{std::string("vbnm"), Null(), (int32_t)4}, {std::string("vbnm")}},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{Null(), std::string("abc"), std::string("hij")}, {std::string("abc")}},
                {{Null(), std::string("def"), std::string("klm")}, {std::string("def")}},
                {{Null(), std::string(""), std::string("xyz")}, {std::string("")}},
                {{Null(), Null(), std::string("uvw")}, {std::string("uvw")}}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_str_to_date_test) {
    std::string func_name = "str_to_date";
    InputTypeSet input_types = {
            TypeIndex::String,
            TypeIndex::String,
    };
    DataSet data_set = {
            {{Null(), std::string("%Y-%m-%d %H:%i:%s")}, {Null()}},
            {{std::string("2014-12-21 12:34:56"), std::string("%Y-%m-%d %H:%i:%s")},
             str_to_date_time("2014-12-21 12:34:56", false)},
            {{std::string("2014-12-21 12:34%3A56"), std::string("%Y-%m-%d %H:%i%%3A%s")},
             str_to_date_time("2014-12-21 12:34:56", false)},
            {{std::string("11/09/2011"), std::string("%m/%d/%Y")},
             str_to_date_time("2011-11-09", false)},
            {{std::string("2020-09-01"), std::string("%Y-%m-%d %H:%i:%s")},
             str_to_date_time("2020-09-01 00:00:00", false)}};
    static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_replace) {
    std::string func_name = "replace";
    InputTypeSet input_types = {
            TypeIndex::String,
            TypeIndex::String,
            TypeIndex::String,
    };
    DataSet data_set = {{{Null(), VARCHAR("9090"), VARCHAR("")}, {Null()}},
                        {{VARCHAR("http://www.baidu.com:9090"), VARCHAR("9090"), VARCHAR("")},
                         {VARCHAR("http://www.baidu.com:")}},
                        {{VARCHAR("aaaaa"), VARCHAR("a"), VARCHAR("")}, {VARCHAR("")}},
                        {{VARCHAR("aaaaa"), VARCHAR("aa"), VARCHAR("")}, {VARCHAR("a")}},
                        {{VARCHAR("aaaaa"), VARCHAR("aa"), VARCHAR("a")}, {VARCHAR("aaa")}}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_bit_length_test) {
    std::string func_name = "bit_length";
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{Null()}, {Null()}},
                        {{std::string("@!#")}, 24},
                        {{std::string("")}, 0},
                        {{std::string("ò&ø")}, 40},
                        {{std::string("@@")}, 16},
                        {{std::string("你好")}, 48},
                        {{std::string("hello你好")}, 88},
                        {{std::string("313233")}, 48},
                        {{std::string("EFBC9F")}, 48}};
    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
