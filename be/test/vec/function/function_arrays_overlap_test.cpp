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

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_arrays_overlap_test, arrays_overlap) {
    std::string func_name = "arrays_overlap";
    Array empty_arr;

    // arrays_overlap(Array<Int32>, Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Array,
                                    TypeIndex::Int32};

        Array vec1 = {Int32(1), Int32(2), Int32(3)};
        Array vec2 = {Int32(3)};
        Array vec3 = {Int32(4), Int32(5)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(0)},
                            {{Null(), vec1}, Null()},
                            {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Int128>, Array<Int128>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int128, TypeIndex::Array,
                                    TypeIndex::Int128};

        Array vec1 = {Int128(11111111111LL), Int128(22222LL), Int128(333LL)};
        Array vec2 = {Int128(11111111111LL)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Float64>, Array<Float64>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Float64, TypeIndex::Array,
                                    TypeIndex::Float64};

        Array vec1 = {double(1.2345), double(2.222), double(3.0)};
        Array vec2 = {double(1.2345)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Date>, Array<Date>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date, TypeIndex::Array,
                                    TypeIndex::Date};

        Array vec1 = {str_to_date_time("2022-01-02", false), str_to_date_time("", false),
                      str_to_date_time("2022-07-08", false)};
        Array vec2 = {str_to_date_time("2022-01-02", false)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<DateTime>, Array<DateTime>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::DateTime, TypeIndex::Array,
                                    TypeIndex::DateTime};

        Array vec1 = {str_to_date_time("2022-01-02 00:00:00"), str_to_date_time(""),
                      str_to_date_time("2022-07-08 00:00:00")};
        Array vec2 = {str_to_date_time("2022-01-02 00:00:00")};
        Array vec3 = {str_to_date_time("")};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(1)},
                            {{Null(), vec1}, Null()},
                            {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Decimal128V2>, Array<Decimal128V2>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Decimal128V2, TypeIndex::Array,
                                    TypeIndex::Decimal128V2};

        Array vec1 = {ut_type::DECIMALFIELD(17014116.67), ut_type::DECIMALFIELD(-17014116.67),
                      ut_type::DECIMALFIELD(0.0)};
        Array vec2 = {ut_type::DECIMALFIELD(17014116.67)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<String>, Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::Array,
                                    TypeIndex::String};

        Array vec1 = {Field("abc", 3), Field("", 0), Field("def", 3)};
        Array vec2 = {Field("abc", 3)};
        Array vec3 = {Field("", 0)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(1)},
                            {{Null(), vec1}, Null()},
                            {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
