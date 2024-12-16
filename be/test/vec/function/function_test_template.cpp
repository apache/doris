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
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(FunctionTestTemplate, two_args_template) {
    std::string func_name = "atan2";

    BaseInputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};

    DataSet data_set = {
            {{-1.0, -2.0}, -2.677945044588987},    {{0.0, 0.0}, 0.0},
            {{0.5, 0.5}, 0.7853981633974483},      {{M_PI, M_PI / 2}, 1.1071487177940904},
            {{1e100, 1e-100}, 1.5707963267948966}, {{Null(), Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(FunctionTestTemplate, three_args_template) {
    std::string func_name = "concat";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string(""), std::string(""), std::string("")}, std::string("")},
                        {{std::string("123"), std::string("456"), std::string("789")},
                         std::string("123456789")},
                        {{std::string("123"), Null(), std::string("789")}, Null()},
                        {{std::string("中文"), std::string("中文"), std::string("中文")},
                         std::string("中文中文中文")},
                        {{std::string("   "), std::string("   "), std::string("   ")},
                         std::string("         ")}};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}
} // namespace doris::vectorized