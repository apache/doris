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

#include <string>
#include <vector>

#include "function_test_util.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

using namespace doris;
using namespace doris::vectorized;

TEST(function_string_test, function_levenshtein_comprehensive_test) {
    std::string func_name = "levenshtein";

    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{std::string("kitten"), std::string("sitting")}, (int32_t)3},
            {{std::string("saturday"), std::string("sunday")}, (int32_t)3},
            {{std::string("rosettacode"), std::string("raisethysword")}, (int32_t)8},

            {{std::string("test"), std::string("test")}, (int32_t)0},

            {{std::string("cat"), std::string("bat")}, (int32_t)1},
            {{std::string("abc"), std::string("xyz")}, (int32_t)3},

            {{std::string("book"), std::string("books")}, (int32_t)1},
            {{std::string("test"), std::string("mytest")}, (int32_t)2},

            {{std::string("apple"), std::string("app")}, (int32_t)2},


            {{std::string(""), std::string("")}, (int32_t)0},
            {{std::string("a"), std::string("")}, (int32_t)1},
            {{std::string(""), std::string("abc")}, (int32_t)3},

            {{std::string("A"), std::string("a")}, (int32_t)1}, 
            {{std::string("Doris"), std::string("doris")}, (int32_t)1},

            {{std::string("a-b-c"), std::string("a_b_c")}, (int32_t)2}, 
            {{std::string("1234567890"), std::string("1234567890")}, (int32_t)0},

            {{std::string("中"), std::string("中")}, (int32_t)0},

            {{std::string("中国"), std::string("中")}, (int32_t)3},

            {{std::string("你好"), std::string("您好")}, (int32_t)3},

            {{Null(), std::string("abc")}, Null()},

            {{std::string("abc"), Null()}, Null()},

            {{Null(), Null()}, Null()}};

    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}
