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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "function_test_util.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"

namespace doris::vectorized {

using namespace ut_type;

TEST(function_lpad_spark_test, string_utf8) {
    std::string func_name = "lpad_spark";

    // 3-arg
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                {{std::string("hello"), std::int32_t(3), std::string("????")}, std::string("hel")},
                {{std::string("hello"), std::int32_t(5), std::string("????")},
                 std::string("hello")},
                {{std::string("hello"), std::int32_t(6), std::string("????")},
                 std::string("?hello")},
                {{std::string("hello"), std::int32_t(12), std::string("????")},
                 std::string("???????hello")},
                {{std::string("hello"), std::int32_t(10), std::string("?????")},
                 std::string("?????hello")},
                {{std::string(""), std::int32_t(7), std::string("?????")}, std::string("???????")},
                {{std::string("数据砖头"), std::int32_t(3), std::string("????")},
                 std::string("数据砖")},
                {{std::string("数据砖头"), std::int32_t(5), std::string("????")},
                 std::string("?数据砖头")},
                {{std::string("数据砖头"), std::int32_t(6), std::string("????")},
                 std::string("??数据砖头")},
                {{std::string("数据砖头"), std::int32_t(6), std::string("孙行者")},
                 std::string("孙行数据砖头")},
                {{std::string("数据砖头"), std::int32_t(7), std::string("孙行者")},
                 std::string("孙行者数据砖头")},
                {{std::string("数据砖头"), std::int32_t(12), std::string("孙行者")},
                 std::string("孙行者孙行者孙行数据砖头")},
                // len <= 0
                {{std::string("数据砖头"), std::int32_t(-10), std::string("孙行者")},
                 std::string("")},
                {{std::string("数据砖头"), std::int32_t(-10), std::string("")}, std::string("")},
                {{std::string("hello"), std::int32_t(0), std::string("????")}, std::string("")},
                // empty pad
                {{std::string("数据砖头"), std::int32_t(5), std::string("")},
                 std::string("数据砖头")},
                {{std::string("数据砖头"), std::int32_t(3), std::string("")},
                 std::string("数据砖")},
                {{std::string(""), std::int32_t(3), std::string("")}, std::string("")},
                // Null
                {{Null(), std::int32_t(5), std::string("????")}, Null()},
                {{std::string("hi"), Null(), std::string("????")}, Null()},
                {{std::string("hi"), std::int32_t(5), Null()}, Null()},
                {{Null(), Null(), Null()}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    // 2-arg
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{std::string("hi"), std::int32_t(5)}, std::string("   hi")},
                {{std::string("hello"), std::int32_t(3)}, std::string("hel")},
                {{std::string("hello"), std::int32_t(-2)}, std::string("")},
                {{std::string("hello"), std::int32_t(0)}, std::string("")},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_lpad_spark_test, varbinary_bytes) {
    static const auto b_aabb = std::string_view("\xAA\xBB", 2);
    static const auto b_5_zero_aabb = std::string_view("\x00\x00\x00\xAA\xBB", 5);
    static const auto b_abcd = std::string_view("abcd", 4);
    static const auto b_ab = std::string_view("ab", 2);
    static const auto b_abax = std::string_view("\x61\x62\x61\x78", 4);

    {
        std::string func_name = "lpad_spark";
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{VARBINARY(b_aabb), std::int32_t(5)}, VARBINARY(b_5_zero_aabb)},
                {{VARBINARY(b_aabb), std::int32_t(0)}, VARBINARY("")},
                {{VARBINARY(b_aabb), std::int32_t(-1)}, VARBINARY("")},
                {{VARBINARY(b_aabb), std::int32_t(2)}, VARBINARY(b_aabb)},
                {{VARBINARY(""), std::int32_t(3)}, VARBINARY(std::string_view("\x00\x00\x00", 3))},
        };
        static_cast<void>(
                check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
    }
    {
        std::string func_name = "lpad_spark";
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_VARBINARY};
        DataSet data_set = {
                {{VARBINARY(std::string_view("x", 1)), std::int32_t(4), VARBINARY(b_ab)},
                 VARBINARY(b_abax)},
                {{VARBINARY(b_abcd), std::int32_t(10), VARBINARY("")}, VARBINARY(b_abcd)},
                {{VARBINARY(b_abcd), std::int32_t(2), VARBINARY("")}, VARBINARY("ab")},
                {{VARBINARY(b_abcd), std::int32_t(2), VARBINARY(b_ab)}, VARBINARY("ab")},
        };
        static_cast<void>(
                check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
