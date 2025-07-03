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

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_array_size_test, size) {
    std::string func_name = "size";
    TestArray empty_arr;

    // size(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{AnyType(vec)}, Int64(3)}, {{Null()}, Null()}, {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // size(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};

        TestArray vec1 = {std::string("abc"), std::string(""), std::string("def")};
        TestArray vec2 = {std::string("abc"), std::string("123"), std::string("def")};
        DataSet data_set = {{{AnyType(vec1)}, Int64(3)},
                            {{AnyType(vec2)}, Int64(3)},
                            {{Null()}, Null()},
                            {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_size_test, cardinality) {
    std::string func_name = "cardinality";
    TestArray empty_arr;

    // cardinality(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{AnyType(vec)}, Int64(3)}, {{Null()}, Null()}, {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // cardinality(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};

        TestArray vec1 = {std::string("abc"), std::string(""), std::string("def")};
        TestArray vec2 = {std::string("abc"), std::string("123"), std::string("def")};
        DataSet data_set = {{{AnyType(vec1)}, Int64(3)},
                            {{AnyType(vec2)}, Int64(3)},
                            {{Null()}, Null()},
                            {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_size_test, array_size) {
    std::string func_name = "array_size";
    TestArray empty_arr;

    // array_size(Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{AnyType(vec)}, Int64(3)}, {{Null()}, Null()}, {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_size(Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};

        TestArray vec1 = {std::string("abc"), std::string(""), std::string("def")};
        TestArray vec2 = {std::string("abc"), std::string("123"), std::string("def")};
        DataSet data_set = {{{AnyType(vec1)}, Int64(3)},
                            {{AnyType(vec2)}, Int64(3)},
                            {{Null()}, Null()},
                            {{AnyType(empty_arr)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
