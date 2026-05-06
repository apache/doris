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

#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/datetime_ut_util.h"

namespace doris {

namespace {

ColumnPtr create_nullable_timestamptz_column(std::initializer_list<TimestampTzValue> values) {
    auto nested = ColumnTimeStampTz::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value);
        null_map->insert_value(0);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

} // namespace

TEST(function_array_index_test, array_contains) {
    std::string func_name = "array_contains";
    TestArray empty_arr;

    // array_contains(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, UInt8(1)},
                            {{vec, 4}, UInt8(0)},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int8>, Int8)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_TINYINT};

        TestArray vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int8(2)}, UInt8(1)},
                            {{vec, Int8(4)}, UInt8(0)},
                            {{Null(), Int8(1)}, Null()},
                            {{empty_arr, Int8(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int64>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_BIGINT,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {Int64(1), Int64(2), Int64(3)};
        DataSet data_set = {{{vec, Int64(2)}, UInt8(1)},
                            {{vec, Int64(4)}, UInt8(0)},
                            {{Null(), Int64(1)}, Null()},
                            {{empty_arr, Int64(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Int128>, Int128)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT,
                                    PrimitiveType::TYPE_LARGEINT};

        TestArray vec = {Int128(11111111111LL), Int128(22222LL), Int128(333LL)};
        DataSet data_set = {{{vec, Int128(11111111111LL)}, UInt8(1)},
                            {{vec, Int128(4)}, UInt8(0)},
                            {{Null(), Int128(1)}, Null()},
                            {{empty_arr, Int128(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Float32>, Float32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_FLOAT};

        TestArray vec = {float(1.2345), float(2.222), float(3.0)};
        DataSet data_set = {{{vec, float(2.222)}, UInt8(1)},
                            {{vec, float(4)}, UInt8(0)},
                            {{Null(), float(1)}, Null()},
                            {{empty_arr, float(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<Float64>, Float64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_DOUBLE};

        TestArray vec = {double(1.2345), double(2.222), double(3.0)};
        DataSet data_set = {{{vec, double(2.222)}, UInt8(1)},
                            {{vec, double(4)}, UInt8(0)},
                            {{Null(), double(1)}, Null()},
                            {{empty_arr, double(1)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_DECIMALV2};

        TestArray vec = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(0.0)};
        DataSet data_set = {{{vec, ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)}, UInt8(1)},
                            {{vec, ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, UInt8(1)},
                            {{Null(), ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, Null()},
                            {{empty_arr, ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // array_contains(Array<String>, String)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        DataSet data_set = {{{vec, std::string("abc")}, UInt8(1)},
                            {{vec, std::string("aaa")}, UInt8(0)},
                            {{vec, std::string("")}, UInt8(1)},
                            {{Null(), std::string("abc")}, Null()},
                            {{empty_arr, std::string("")}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(function_array_index_test, array_contains_timestamptz) {
    const std::string func_name = "array_contains";
    auto element_type = std::make_shared<DataTypeTimeStampTz>(6);
    auto array_type = std::make_shared<DataTypeArray>(make_nullable(element_type));
    auto return_type = std::make_shared<DataTypeBool>();
    auto arguments_template = ColumnsWithTypeAndName {{nullptr, array_type, "array"},
                                                      {nullptr, element_type, "item"}};

    auto function = SimpleFunctionFactory::instance().get_function(
            func_name, arguments_template, return_type, {true},
            BeExecVersionManager::get_newest_version());
    ASSERT_TRUE(function != nullptr);

    auto pre = make_timestamptz(2024, 11, 3, 1, 5, 0, 0);
    auto post = make_timestamptz(2024, 11, 3, 1, 5, 0, 0);

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);

    auto probe_column = ColumnTimeStampTz::create();
    probe_column->insert_value(post);

    Block block;
    block.insert({ColumnArray::create(create_nullable_timestamptz_column({pre, post}),
                                      std::move(offsets)),
                  array_type, "array"});
    block.insert({std::move(probe_column), element_type, "item"});
    block.insert({nullptr, return_type, "result"});

    auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_column = assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
    EXPECT_EQ(result_column.get_element(0), 1);
}

TEST(function_array_index_test, array_position) {
    std::string func_name = "array_position";
    TestArray empty_arr;

    // array_position(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, 2}, Int64(2)},
                            {{vec, 4}, Int64(0)},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 1}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {{{vec, Int32(2)}, Int64(2)},
                            {{vec, Int32(4)}, Int64(0)},
                            {{Null(), Int32(1)}, Null()},
                            {{empty_arr, Int32(1)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Int8>, Int8)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_TINYINT};

        TestArray vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {{{vec, Int8(2)}, Int64(2)},
                            {{vec, Int8(4)}, Int64(0)},
                            {{Null(), Int8(1)}, Null()},
                            {{empty_arr, Int8(1)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<Decimal128V2>, Decimal128V2)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_DECIMALV2};

        TestArray vec = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(0)};
        DataSet data_set = {{{vec, ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)}, Int64(2)},
                            {{vec, ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, Int64(3)},
                            {{Null(), ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, Null()},
                            {{empty_arr, ut_type::DECIMALV2VALUEFROMDOUBLE(0)}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    // array_position(Array<String>, String)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        DataSet data_set = {{{vec, std::string("abc")}, Int64(1)},
                            {{vec, std::string("aaa")}, Int64(0)},
                            {{vec, std::string("")}, Int64(2)},
                            {{Null(), std::string("abc")}, Null()},
                            {{empty_arr, std::string("")}, Int64(0)}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris
