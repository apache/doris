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

#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_array_size_test, size) {
    std::string func_name = "size";
    Array empty_arr;

    // size(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec}, Int64(3)}, {{Null()}, Null()}, {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // size(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field("abc", 3), Field("", 0), Field("def", 3)};
        Array vec2 = {Field("abc", 3), Field("123", 0), Field("def", 3)};
        DataSet data_set = {{{vec1}, Int64(3)},
                            {{vec2}, Int64(3)},
                            {{Null()}, Null()},
                            {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_size_test, cardinality) {
    std::string func_name = "cardinality";
    Array empty_arr;

    // cardinality(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec}, Int64(3)}, {{Null()}, Null()}, {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // cardinality(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field("abc", 3), Field("", 0), Field("def", 3)};
        Array vec2 = {Field("abc", 3), Field("123", 0), Field("def", 3)};
        DataSet data_set = {{{vec1}, Int64(3)},
                            {{vec2}, Int64(3)},
                            {{Null()}, Null()},
                            {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_size_test, array_size) {
    std::string func_name = "array_size";
    Array empty_arr;

    // array_size(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec}, Int64(3)}, {{Null()}, Null()}, {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_size(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field("abc", 3), Field("", 0), Field("def", 3)};
        Array vec2 = {Field("abc", 3), Field("123", 0), Field("def", 3)};
        DataSet data_set = {{{vec1}, Int64(3)},
                            {{vec2}, Int64(3)},
                            {{Null()}, Null()},
                            {{empty_arr}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
