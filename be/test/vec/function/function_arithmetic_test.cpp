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

#include <iomanip>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_arithmetic_test, function_arithmetic_mod_test) {
    std::string func_name = "mod";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {{{10, 1}, 0}, {{10, -2}, 0}, {{1234, 33}, 13}, {{1234, 0}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_arithmetic_test, function_arithmetic_divide_test) {
    std::string func_name = "divide";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};
        DataSet data_set = {{{1234, 34}, 36.294117647058826}, {{1234, 0}, Null()}};
        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};
        DataSet data_set = {{{1234.1, 34.6}, 35.667630057803464}, {{1234.34, 0.0}, Null()}};
        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(function_arithmetic_test, bitnot_test) {
    std::string func_name = "bitnot";

    {
        InputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30}, ~(int32_t)30},
                            {{(int32_t)0}, ~(int32_t)0},
                            {{(int32_t)-10}, ~(int32_t)-10},
                            {{(int32_t)-10.44}, ~(int32_t)-10},
                            {{(int32_t)-999.888}, ~(int32_t)-999}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_arithmetic_test, bitand_test) {
    std::string func_name = "bitand";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30, (int32_t)12}, 30 & 12},
                            {{(int32_t)0, (int32_t)12}, 0 & 12},
                            {{(int32_t)-10, (int32_t)111}, -10 & 111},
                            {{(int32_t)-999, (int32_t)888}, -999 & 888}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_arithmetic_test, bitor_test) {
    std::string func_name = "bitor";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30, (int32_t)12}, 30 | 12},
                            {{(int32_t)0, (int32_t)12}, 0 | 12},
                            {{(int32_t)-10, (int32_t)111}, -10 | 111},
                            {{(int32_t)-999, (int32_t)888}, -999 | 888}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_arithmetic_test, bitxor_test) {
    std::string func_name = "bitxor";

    {
        InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {{{(int32_t)30, (int32_t)12}, 30 ^ 12},
                            {{(int32_t)0, (int32_t)12}, 0 ^ 12},
                            {{(int32_t)-10, (int32_t)111}, -10 ^ 111},
                            {{(int32_t)-999, (int32_t)888}, -999 ^ 888}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
