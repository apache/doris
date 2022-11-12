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

TEST(FunctionHasAllTest, function_has_all_test) {
    std::string func_name = "has_all";
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Array, TypeIndex::Int32};

        Array vec1 = {Int32(1),Int32(2), Int32(3),Null()};
        Array vec2 = {Int32(2), Int32(3)};
        Array vec3 = {Int32(2), Int32(4)};
        Array vec4 = {};
        Array vec5 = {Null()};
        Array vec6 = {Int32(1),Int32(2), Null()};

        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(0)},
                            {{vec1, vec4}, UInt8(1)},
                            {{vec1, vec5}, UInt8(1)},
                            {{vec6, vec5}, UInt8(1)},
                            };

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Float64, TypeIndex::Array, TypeIndex::Float64};

        Array vec1 = {Float64(1.34),Float64(2.54), Float64(3.34)};
        Array vec2 = {Float64(3.34), Float64(1.34)};
        Array vec3 = {Float64(2.54), Float64(4.456)};
        Array vec4 = {};
        Array vec5 = {Float64(1.34),Null()};
        Array vec6 = {Float64(1.34),Float64(2.54), Null()};

        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(0)},
                            {{vec1, vec4}, UInt8(1)},
                            {{vec1, vec5}, UInt8(0)},
                            {{vec6, vec5}, UInt8(1)},
                            {{Null(), vec1}, Null()},
                            };

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
     {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field("abc", 3),Field("def", 3),Field("ghi", 3)};
        Array vec2 = {Field("def", 3),Field("ghi", 3)};
        Array vec3 = {Field("rgf", 3),Field("ghi", 3)};
        Array vec4 = {};
        Array vec5 = {Field("abc", 3),Field("", 0)};
        Array vec6 = {Field("abc", 3),Field("def", 3), Field("", 0)};

        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(0)},
                            {{vec1, vec4}, UInt8(1)},
                            {{vec1, vec5}, UInt8(0)},
                            {{vec6, vec5}, UInt8(1)},
                            };

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date, TypeIndex::Array, TypeIndex::Date};

        Array vec1 = {str_to_date_time("2022-11-12", false),str_to_date_time("2022-11-11", false), 
                      str_to_date_time("", false)};
        Array vec2 = {str_to_date_time("2022-11-12", false)};
        Array vec3 = {};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, Null()}, Null()},
                            {{vec1, vec3}, UInt8(1)},
                           };

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::DateTime, TypeIndex::Array, TypeIndex::DateTime};

        Array vec1 = {str_to_date_time("2022-11-02 00:00:00"),str_to_date_time("2022-11-12 00:00:00"), 
                      str_to_date_time("")};
        Array vec2 = {str_to_date_time("2022-11-02 00:00:00")};
        Array vec3 = {str_to_date_time("")};

        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, Null()}, Null()},
                            {{vec1, vec3}, UInt8(1)},    
                           };

        check_function<DataTypeUInt8, true>(func_name, input_types, data_set);
    }
    
}
}