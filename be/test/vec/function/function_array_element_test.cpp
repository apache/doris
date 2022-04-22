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
#include "util/url_coding.h"
#include "vec/core/field.h"

namespace doris::vectorized {

TEST(function_array_element_test, element_at) {
    std::string func_name = "element_at";
    Array empty_arr;

    // element_at(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int32(1)},     {{vec, 4}, Null()},
                {{vec, -1}, Int32(3)}, {{vec, -3}, Int32(1)},    {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    }

    // element_at(Array<Int8>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int8, TypeIndex::Int32};

        Array vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int8(1)},      {{vec, 4}, Null()},
                {{vec, -1}, Int8(3)},  {{vec, -3}, Int8(1)},     {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        check_function<DataTypeInt8, true>(func_name, input_types, data_set);
    }

    // element_at(Array<String>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::Int32};

        Array vec = {Field("abc", 3), Field("", 0), Field("def", 3)};
        DataSet data_set = {{{vec, 1}, std::string("abc")},
                            {{vec, 2}, std::string("")},
                            {{vec, 10}, Null()},
                            {{vec, -2}, std::string("")},
                            {{vec, 0}, Null()},
                            {{vec, -10}, Null()},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 0}, Null()},
                            {{empty_arr, 1}, Null()}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
}

} // namespace doris::vectorized
