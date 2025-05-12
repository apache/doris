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

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_arrays_overlap_test, arrays_overlap) {
    std::string func_name = "arrays_overlap";
    TestArray empty_arr;

    // arrays_overlap(Array<Int32>, Array<Int32>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};

        TestArray vec1 = {Int32(1), Int32(2), Int32(3)};
        TestArray vec2 = {Int32(3)};
        TestArray vec3 = {Int32(4), Int32(5)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, UInt8(0)},
                            {{Null(), vec1}, Null()},
                            {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Int128>, Array<Int128>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT};

        TestArray vec1 = {Int128(11111111111LL), Int128(22222LL), Int128(333LL)};
        TestArray vec2 = {Int128(11111111111LL)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Float64>, Array<Float64>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE};

        TestArray vec1 = {double(1.2345), double(2.222), double(3.0)};
        TestArray vec2 = {double(1.2345)};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Date>, Array<Date>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATE,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATE};

        TestArray vec1 = {std::string("2022-01-02"), std::string("2022-01-02"),
                          std::string("2022-07-08")};
        TestArray vec2 = {std::string("2022-01-02")};
        DataSet data_set = {
                {{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<DateTime>, Array<DateTime>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIME,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIME};

        TestArray vec1 = {std::string("2022-01-02 00:00:00"), std::string("2022-01-02 00:00:00"),
                          std::string("2022-07-08 00:00:00")};
        TestArray vec2 = {std::string("2022-01-02 00:00:00")};
        TestArray vec3 = {std::string("")};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},
                            {{vec1, vec3}, Null()},
                            {{Null(), vec1}, Null()},
                            {{empty_arr, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Decimal128V2>, Array<Decimal128V2>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};

        TestArray vec1 = {ut_type::DECIMALV2(17014116.67), ut_type::DECIMALV2(-17014116.67),
                          ut_type::DECIMALV2(0.0)};
        TestArray vec2 = {ut_type::DECIMALV2(17014116.67)};

        TestArray vec3 = {ut_type::DECIMALV2(17014116.67), ut_type::DECIMALV2(-17014116.67),
                          Null()};
        TestArray vec4 = {ut_type::DECIMALV2(-17014116.67)};
        TestArray vec5 = {ut_type::DECIMALV2(-17014116.68)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()},
                            {{vec1, Null()}, Null()}, {{empty_arr, vec1}, UInt8(0)},
                            {{vec3, vec4}, UInt8(1)}, {{vec3, vec5}, Null()},
                            {{vec4, vec3}, UInt8(1)}, {{vec5, vec3}, Null()}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<String>, Array<String>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};

        TestArray vec1 = {std::string("abc"), std::string(""), std::string("def")};
        TestArray vec2 = {std::string("abc")};
        TestArray vec3 = {std::string("")};
        TestArray vec4 = {std::string("abc"), Null()};
        TestArray vec5 = {std::string("abcd"), Null()};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)}, {{vec1, vec3}, UInt8(1)},
                            {{Null(), vec1}, Null()}, {{empty_arr, vec1}, UInt8(0)},
                            {{vec4, vec1}, UInt8(1)}, {{vec1, vec5}, Null()},
                            {{vec1, vec4}, UInt8(1)}, {{vec5, vec1}, Null()}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Decimal128V2>, Array<Decimal128V2>), Non-nullable
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};

        TestArray vec1 = {ut_type::DECIMALV2(17014116.67), ut_type::DECIMALV2(-17014116.67),
                          ut_type::DECIMALV2(0.0)};
        TestArray vec2 = {ut_type::DECIMALV2(17014116.67)};

        TestArray vec3 = {ut_type::DECIMALV2(17014116.67), ut_type::DECIMALV2(-17014116.67)};
        TestArray vec4 = {ut_type::DECIMALV2(-17014116.67)};
        TestArray vec5 = {ut_type::DECIMALV2(-17014116.68)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)}, {{empty_arr, vec1}, UInt8(0)},
                            {{vec3, vec4}, UInt8(1)}, {{vec3, vec5}, UInt8(0)},
                            {{vec4, vec3}, UInt8(1)}, {{vec5, vec3}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<String>, Array<String>), Non-nullable
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR};

        TestArray vec1 = {std::string("abc"), std::string(""), std::string("def")};
        TestArray vec2 = {std::string("abc")};
        TestArray vec3 = {std::string("")};
        TestArray vec4 = {std::string("abc")};
        TestArray vec5 = {std::string("abcd")};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},      {{vec1, vec3}, UInt8(1)},
                            {{empty_arr, vec1}, UInt8(0)}, {{vec4, vec1}, UInt8(1)},
                            {{vec1, vec5}, UInt8(0)},      {{vec1, vec4}, UInt8(1)},
                            {{vec5, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
