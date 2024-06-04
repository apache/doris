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
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_array_index_test, array_contains) {
    std::string func_name = "array_contains";
    Array empty_arr;

    // array_contains(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, UInt8(1)},
                            {{vec, 4}, UInt8(0)},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int8>, Int8)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int8, TypeIndex::Int8};

        Array vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int8(2)}, UInt8(1)},
                            {{vec, Int8(4)}, UInt8(0)},
                            {{Null(), Int8(1)}, Null()},
                            {{empty_arr, Int8(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int64>, Int64)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int64, TypeIndex::Int64};

        Array vec = {Int64(1), Int64(2), Int64(3)};
        DataSet data_set = {{{vec, Int64(2)}, UInt8(1)},
                            {{vec, Int64(4)}, UInt8(0)},
                            {{Null(), Int64(1)}, Null()},
                            {{empty_arr, Int64(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int128>, Int128)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int128, TypeIndex::Int128};

        Array vec = {Int128(11111111111LL), Int128(22222LL), Int128(333LL)};
        DataSet data_set = {{{vec, Int128(11111111111LL)}, UInt8(1)},
                            {{vec, Int128(4)}, UInt8(0)},
                            {{Null(), Int128(1)}, Null()},
                            {{empty_arr, Int128(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Float32>, Float32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Float32, TypeIndex::Float32};

        Array vec = {float(1.2345), float(2.222), float(3.0)};
        DataSet data_set = {{{vec, float(2.222)}, UInt8(1)},
                            {{vec, float(4)}, UInt8(0)},
                            {{Null(), float(1)}, Null()},
                            {{empty_arr, float(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Float64>, Float64)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Float64, TypeIndex::Float64};

        Array vec = {double(1.2345), double(2.222), double(3.0)};
        DataSet data_set = {{{vec, double(2.222)}, UInt8(1)},
                            {{vec, double(4)}, UInt8(0)},
                            {{Null(), double(1)}, Null()},
                            {{empty_arr, double(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Date>, Date)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date, TypeIndex::Date};

        Array vec = {str_to_date_time("2022-01-02", false), str_to_date_time("2022-07-08", false)};
        DataSet data_set = {{{vec, std::string("2022-01-02")}, UInt8(1)},
                            {{vec, std::string("2022-01-03")}, UInt8(0)},
                            {{Null(), std::string("2022-01-04")}, Null()},
                            {{empty_arr, std::string("2022-01-02")}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<DateTime>, DateTime)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::DateTime, TypeIndex::DateTime};

        Array vec = {str_to_date_time("2022-01-02 00:00:00"),
                     str_to_date_time("2022-07-08 00:00:00")};
        DataSet data_set = {{{vec, std::string("2022-01-02 00:00:00")}, UInt8(1)},
                            {{vec, std::string("2022-01-03 00:00:00")}, UInt8(0)},
                            {{Null(), std::string("2022-01-04 00:00:00")}, Null()},
                            {{empty_arr, std::string("2022-01-02 00:00:00")}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Decimal128V2>, Decimal128V2)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Decimal128V2,
                                    TypeIndex::Decimal128V2};

        Array vec = {ut_type::DECIMALFIELD(17014116.67), ut_type::DECIMALFIELD(-17014116.67),
                     ut_type::DECIMALFIELD(0.0)};
        DataSet data_set = {{{vec, ut_type::DECIMAL(-17014116.67)}, UInt8(1)},
                            {{vec, ut_type::DECIMAL(0)}, UInt8(1)},
                            {{Null(), ut_type::DECIMAL(0)}, Null()},
                            {{empty_arr, ut_type::DECIMAL(0)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<String>, String)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::String};

        Array vec = {Field("abc", 3), Field("", 0), Field("def", 3)};
        DataSet data_set = {{{vec, std::string("abc")}, UInt8(1)},
                            {{vec, std::string("aaa")}, UInt8(0)},
                            {{vec, std::string("")}, UInt8(1)},
                            {{Null(), std::string("abc")}, Null()},
                            {{empty_arr, std::string("")}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_index_test, array_position) {
    std::string func_name = "array_position";
    Array empty_arr;

    // array_position(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, Int64(2)},
                            {{vec, 4}, Int64(0)},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 1}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32, TypeIndex::Int32};

        Array vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, Int32(2)}, Int64(2)},
                            {{vec, Int32(4)}, Int64(0)},
                            {{Null(), Int32(1)}, Null()},
                            {{empty_arr, Int32(1)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Int8>, Int8)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int8, TypeIndex::Int8};

        Array vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int8(2)}, Int64(2)},
                            {{vec, Int8(4)}, Int64(0)},
                            {{Null(), Int8(1)}, Null()},
                            {{empty_arr, Int8(1)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Date>, Date)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date, TypeIndex::Date};

        Array vec = {str_to_date_time("2022-01-02", false), str_to_date_time("2022-07-08", false)};
        DataSet data_set = {{{vec, std::string("2022-01-02")}, Int64(1)},
                            {{vec, std::string("2022-01-03")}, Int64(0)},
                            {{Null(), std::string("2022-01-04")}, Null()},
                            {{empty_arr, std::string("2022-01-02")}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<DateTime>, DateTime)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::DateTime, TypeIndex::DateTime};

        Array vec = {str_to_date_time("2022-01-02 00:00:00"),
                     str_to_date_time("2022-07-08 00:00:00")};
        DataSet data_set = {{{vec, std::string("2022-01-02 00:00:00")}, Int64(1)},
                            {{vec, std::string("2022-01-03 00:00:00")}, Int64(0)},
                            {{Null(), std::string("2022-01-04 00:00:00")}, Null()},
                            {{empty_arr, std::string("2022-01-02 00:00:00")}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Decimal128V2>, Decimal128V2)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Decimal128V2,
                                    TypeIndex::Decimal128V2};

        Array vec = {ut_type::DECIMALFIELD(17014116.67), ut_type::DECIMALFIELD(-17014116.67),
                     ut_type::DECIMALFIELD(0)};
        DataSet data_set = {{{vec, ut_type::DECIMAL(-17014116.67)}, Int64(2)},
                            {{vec, ut_type::DECIMAL(0)}, Int64(3)},
                            {{Null(), ut_type::DECIMAL(0)}, Null()},
                            {{empty_arr, ut_type::DECIMAL(0)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<String>, String)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String, TypeIndex::String};

        Array vec = {Field("abc", 3), Field("", 0), Field("def", 3)};
        DataSet data_set = {{{vec, std::string("abc")}, Int64(1)},
                            {{vec, std::string("aaa")}, Int64(0)},
                            {{vec, std::string("")}, Int64(2)},
                            {{Null(), std::string("abc")}, Null()},
                            {{empty_arr, std::string("")}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
