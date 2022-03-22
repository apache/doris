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

TEST(function_array_index_test, array_contains) {
    std::string func_name = "array_contains";
    Array empty_arr;

    // array_contains(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, UInt8(1)}, {{vec, 4}, UInt8(0)}, {{Null(), 1}, Null()}, {{empty_arr, 1}, UInt8(0)}};

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }

    // array_contains(Array<Int32>, Int8)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int8};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, Int8(2)}, UInt8(1)}, {{vec, Int8(4)}, UInt8(0)}, {{Null(), Int8(1)}, Null()}, {{empty_arr, Int8(1)}, UInt8(0)}};

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }

    // array_contains(Array<Int8>, Int64)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int8, TypeIndex::Int64};

        Array vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int64(2)}, UInt8(1)}, {{vec, Int64(4)}, UInt8(0)}, {{Null(), Int64(1)}, Null()}, {{empty_arr, Int64(1)}, UInt8(0)}};

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }

    // array_contains(Array<String>, String)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::String};

        Array vec = {Field("abc", 3), Field("", 0), Field("def",3)};
        DataSet data_set = {{{vec, std::string("abc")}, UInt8(1)}, {{vec, std::string("aaa")}, UInt8(0)},
                            {{vec, std::string("")}, UInt8(1)}, {{Null(), std::string("abc")}, Null()}, {{empty_arr, std::string("")}, UInt8(0)}};

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
}

TEST(function_array_index_test, array_position) {
    std::string func_name = "array_position";
    Array empty_arr;

    // array_position(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, Int64(2)}, {{vec, 4}, Int64(0)}, {{Null(), 1}, Null()}, {{empty_arr, 1}, Int64(0)}};

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }

    // array_position(Array<Int32>, Int8)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int8};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, Int8(2)}, Int64(2)}, {{vec, Int8(4)}, Int64(0)}, {{Null(), Int8(1)}, Null()}, {{empty_arr, Int8(1)}, Int64(0)}};

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }

    // array_position(Array<Int8>, Int64)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int8, TypeIndex::Int64};

        Array vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int64(2)}, Int64(2)}, {{vec, Int64(4)}, Int64(0)}, {{Null(), Int64(1)}, Null()}, {{empty_arr, Int64(1)}, Int64(0)}};

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }

    // array_position(Array<String>, String)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::String};

        Array vec = {Field("abc", 3), Field("", 0), Field("def",3)};
        DataSet data_set = {{{vec, std::string("abc")}, Int64(1)}, {{vec, std::string("aaa")}, Int64(0)},
                            {{vec, std::string("")}, Int64(2)}, {{Null(), std::string("abc")}, Null()}, {{empty_arr, std::string("")}, Int64(0)}};

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
