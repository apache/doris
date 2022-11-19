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

#include <any>
#include <cmath>
#include <iostream>
#include <string>

#include "function_test_util.h"
namespace doris::vectorized {
using namespace ut_type;
TEST(FunctionRunningDifferenceTest, function_running_difference_test) {
    std::string func_name = "running_difference";
    {
        InputTypeSet input_types = {TypeIndex::Int32};

        DataSet data_set = {{{Null()}, Null()},
                            {{(int32_t)1}, Null()},
                            {{(int32_t)2}, (int64_t)1},
                            {{(int32_t)3}, (int64_t)1},
                            {{(int32_t)5}, (int64_t)2}};

        check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Float64};
        DataSet data_set = {{{(double)0.0}, (double)0.0},
                            {{Null()}, Null()},
                            {{(double)2.33}, Null()},
                            {{(double)8.45}, (double)6.12},
                            {{(double)4.22}, (double)-4.23}};
        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime};
        DataSet data_set = {{{std::string("2019-07-18 12:00:00")}, (double)0.0},
                            {{std::string("2019-07-18 12:00:05")}, (double)5.0},
                            {{std::string("2019-07-18 12:00:06")}, (double)1.0},
                            {{std::string("2019-07-18 12:00:08")}, (double)2.0},
                            {{std::string("2019-07-18 12:00:10")}, (double)2.0}};
        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Date};
        DataSet data_set = {{{std::string("2019-07-18")}, (int32_t)0},
                            {{std::string("2019-08-19")}, (int32_t)32},
                            {{std::string("2019-07-20")}, (int32_t)-30},
                            {{std::string("2019-07-22")}, (int32_t)2},
                            {{std::string("2019-08-01")}, (int32_t)10}};
        check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Date};
        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("2019-08-19")}, Null()},
                            {{std::string("2019-07-20")}, (int32_t)-30},
                            {{std::string("2019-07-22")}, (int32_t)2},
                            {{std::string("2019-08-01")}, (int32_t)10}};
        check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    }
}
} // namespace doris::vectorized