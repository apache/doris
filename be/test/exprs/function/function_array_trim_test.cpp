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

#include <memory>
#include <string>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"

namespace doris {

TEST(FunctionArrayTrimTest, all_argument_combinations) {
    const std::string function_name = "trim_array";
    const InputArgTypeSet input_types = {{PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT},
                                         {PrimitiveType::TYPE_BIGINT}};

    TestArray empty;
    TestArray values = {Int32(1), Int32(2), Int32(3), Int32(4)};
    TestArray values_with_null = {Int32(1), Null(), Int32(3)};
    DataSet data_set = {
            {{AnyType(values), Int64(0)}, AnyType(values)},
            {{AnyType(values), Int64(2)}, AnyType(TestArray {Int32(1), Int32(2)})},
            {{AnyType(values), Int64(4)}, AnyType(empty)},
            {{AnyType(empty), Int64(0)}, AnyType(empty)},
            {{AnyType(values_with_null), Int64(1)}, AnyType(TestArray {Int32(1), Null()})},
            {{Null(), Int64(0)}, Null()},
            {{AnyType(values), Null()}, Null()}};

    auto result_nested_type = make_nullable(std::make_shared<DataTypeInt32>());
    check_function_all_arg_comb<DataTypeArray, true>(function_name, input_types, data_set,
                                                     result_nested_type);
}

} // namespace doris
