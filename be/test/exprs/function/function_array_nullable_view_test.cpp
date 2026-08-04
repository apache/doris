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

#include <memory>
#include <string>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

static void check_array_function(const std::string& function_name,
                                 const DataTypePtr& input_nested_type,
                                 const DataTypePtr& result_nested_type, const TestArray& input,
                                 const std::string& expected) {
    auto input_type = std::make_shared<DataTypeArray>(input_nested_type);
    auto result_type = std::make_shared<DataTypeArray>(result_nested_type);
    auto input_column = input_type->create_column();
    ASSERT_TRUE(insert_cell(input_column, input_type, AnyType {input}));
    ColumnPtr input_column_ptr = std::move(input_column);

    ColumnsWithTypeAndName arguments = {{input_column_ptr, input_type, "array"}};
    auto function =
            SimpleFunctionFactory::instance().get_function(function_name, arguments, result_type);
    ASSERT_NE(function, nullptr);

    Block block;
    block.insert({std::move(input_column_ptr), input_type, "array"});
    block.insert({nullptr, result_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());
    EXPECT_EQ(result_type->to_string(*block.get_by_position(1).column, 0), expected);
}

TEST(FunctionArrayNullableViewTest, CumSumCoversNumericTypes) {
    check_array_function("array_cum_sum", std::make_shared<DataTypeUInt8>(),
                         std::make_shared<DataTypeInt64>(), {UInt8(1), UInt8(0), UInt8(1)},
                         "[1, 1, 2]");
    check_array_function("array_cum_sum", std::make_shared<DataTypeInt64>(),
                         std::make_shared<DataTypeInt64>(), {Int64(1), Int64(2), Int64(3)},
                         "[1, 3, 6]");
    check_array_function("array_cum_sum", std::make_shared<DataTypeInt128>(),
                         std::make_shared<DataTypeInt128>(), {Int128(1), Int128(2), Int128(3)},
                         "[1, 3, 6]");
}

TEST(FunctionArrayNullableViewTest, CumSumCoversDecimalTypes) {
    check_array_function("array_cum_sum", std::make_shared<DataTypeDecimal128>(38, 2),
                         std::make_shared<DataTypeDecimal128>(38, 2),
                         {ut_type::DECIMAL128V3(1, 0, 2), ut_type::DECIMAL128V3(2, 0, 2),
                          ut_type::DECIMAL128V3(3, 0, 2)},
                         "[1.00, 3.00, 6.00]");
    check_array_function("array_cum_sum", std::make_shared<DataTypeDecimal256>(76, 2),
                         std::make_shared<DataTypeDecimal256>(76, 2),
                         {ut_type::DECIMAL256(1, 0, 2), ut_type::DECIMAL256(2, 0, 2),
                          ut_type::DECIMAL256(3, 0, 2)},
                         "[1.00, 3.00, 6.00]");
}

TEST(FunctionArrayNullableViewTest, DifferenceCoversNumericTypes) {
    check_array_function("array_difference", std::make_shared<DataTypeUInt8>(),
                         std::make_shared<DataTypeInt16>(), {UInt8(1), UInt8(0), UInt8(1)},
                         "[0, -1, 1]");
}

TEST(FunctionArrayNullableViewTest, DifferenceCoversDecimalTypes) {
    check_array_function(
            "array_difference", std::make_shared<DataTypeDecimal32>(9, 2),
            std::make_shared<DataTypeDecimal32>(9, 2),
            {ut_type::DECIMAL32(1, 0, 2), ut_type::DECIMAL32(3, 0, 2), ut_type::DECIMAL32(6, 0, 2)},
            "[0.00, 2.00, 3.00]");
    check_array_function(
            "array_difference", std::make_shared<DataTypeDecimal64>(18, 2),
            std::make_shared<DataTypeDecimal64>(18, 2),
            {ut_type::DECIMAL64(1, 0, 2), ut_type::DECIMAL64(3, 0, 2), ut_type::DECIMAL64(6, 0, 2)},
            "[0.00, 2.00, 3.00]");
    check_array_function(
            "array_difference", std::make_shared<DataTypeDecimalV2>(27, 9),
            std::make_shared<DataTypeDecimalV2>(27, 9),
            {ut_type::DECIMALV2VALUEFROMDOUBLE(1), ut_type::DECIMALV2VALUEFROMDOUBLE(3),
             ut_type::DECIMALV2VALUEFROMDOUBLE(6)},
            "[0.000000000, 2.000000000, 3.000000000]");
    check_array_function("array_difference", std::make_shared<DataTypeDecimal256>(76, 2),
                         std::make_shared<DataTypeDecimal256>(76, 2),
                         {ut_type::DECIMAL256(1, 0, 2), ut_type::DECIMAL256(3, 0, 2),
                          ut_type::DECIMAL256(6, 0, 2)},
                         "[0.00, 2.00, 3.00]");
}

TEST(FunctionArrayNullableViewTest, DifferencePropagatesNestedNulls) {
    check_array_function("array_difference", std::make_shared<DataTypeInt32>(),
                         std::make_shared<DataTypeInt64>(), {Int32(1), Null(), Int32(3)},
                         "[0, null, null]");
}

} // namespace doris
