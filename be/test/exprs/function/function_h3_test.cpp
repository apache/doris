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

#include <limits>

#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(FunctionH3Test, LatLngToCell) {
    const InputTypeSet types = {TYPE_DOUBLE, TYPE_DOUBLE, TYPE_INT};
    const DataSet cases = {{{55.71290588, 37.79506683, int32_t(15)}, int64_t(644325524701193974)},
                           {{91.0, 0.0, int32_t(9)}, Null()},
                           {{-91.0, 0.0, int32_t(9)}, Null()},
                           {{0.0, 181.0, int32_t(9)}, Null()},
                           {{0.0, -181.0, int32_t(9)}, Null()},
                           {{0.0, 0.0, int32_t(-1)}, Null()},
                           {{0.0, 0.0, int32_t(16)}, Null()},
                           {{std::numeric_limits<double>::quiet_NaN(), 0.0, int32_t(9)}, Null()},
                           {{0.0, std::numeric_limits<double>::quiet_NaN(), int32_t(9)}, Null()},
                           {{0.0, std::numeric_limits<double>::infinity(), int32_t(9)}, Null()},
                           {{-std::numeric_limits<double>::infinity(), 0.0, int32_t(9)}, Null()},
                           {{Null(), 0.0, int32_t(9)}, Null()},
                           {{0.0, Null(), int32_t(9)}, Null()},
                           {{0.0, 0.0, Null()}, Null()}};
    check_function_all_arg_comb<DataTypeInt64, true>("h3_lat_lng_to_cell", types, cases);
}

// GTest assertion macros dominate the reported complexity of these checks.
// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST(FunctionH3Test, OnlyNullLatLngToCell) {
    const size_t rows = 7;
    for (size_t null_argument = 0; null_argument < 3; ++null_argument) {
        DataTypes types = {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(),
                           std::make_shared<DataTypeInt32>()};
        types[null_argument] = make_nullable(std::make_shared<DataTypeNothing>());
        Block block;
        for (const auto& type : types) {
            block.insert({type->create_column_const_with_default_value(rows), type, "argument"});
        }
        auto result_type = make_nullable(std::make_shared<DataTypeInt64>());
        auto function = SimpleFunctionFactory::instance().get_function(
                "h3_lat_lng_to_cell", block.get_columns_with_type_and_name(), result_type);
        ASSERT_NE(function, nullptr);
        FunctionUtils utils(result_type, types, false);
        auto* context = utils.get_fn_ctx();
        ASSERT_TRUE(
                function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        ASSERT_TRUE(
                function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(context, block, {0, 1, 2}, 3, rows).ok());
        EXPECT_EQ(block.get_by_position(3).column->size(), rows);
        EXPECT_TRUE(block.get_by_position(3).column->only_null());
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    }
}

static void check_coordinates(bool boundary, bool constant, bool only_null, size_t rows) {
    DataTypePtr input_type =
            make_nullable(only_null ? DataTypePtr(std::make_shared<DataTypeNothing>())
                                    : DataTypePtr(std::make_shared<DataTypeInt64>()));
    DataTypePtr output_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat64>()));
    if (boundary) {
        output_type = std::make_shared<DataTypeArray>(make_nullable(output_type));
    }
    output_type = make_nullable(output_type);
    auto input = input_type->create_column();
    const Int64 cells[] = {644325524701193974, 0, 581109487465660415, -1, 576636674163867647};
    for (size_t i = 0; i < (constant ? 1 : rows); ++i) {
        if (only_null || i % 6 == 5) {
            input->insert(Field());
        } else {
            input->insert(Field::create_field<TYPE_BIGINT>(cells[i % 6]));
        }
    }
    if (constant) {
        input = ColumnConst::create(std::move(input), rows);
    }
    Block block;
    block.insert({std::move(input), input_type, "cell"});
    auto function = SimpleFunctionFactory::instance().get_function(
            boundary ? "h3_cell_to_boundary" : "h3_cell_to_lat_lng",
            block.get_columns_with_type_and_name(), output_type);
    ASSERT_NE(function, nullptr);
    FunctionUtils utils(output_type, {input_type}, false);
    auto* context = utils.get_fn_ctx();
    std::vector<std::shared_ptr<ConstantExprResult>> constants(1);
    if (constant) {
        constants[0] = std::make_shared<ConstantExprResult>(block.get_by_position(0).column);
    }
    context->set_constant_cols(constants);
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    block.insert({nullptr, output_type, "result"});
    ASSERT_TRUE(function->execute(context, block, {0}, 1, rows).ok());
    auto result = block.get_by_position(1).column->convert_to_full_column_if_const();
    ASSERT_EQ(result->size(), rows);
    for (size_t i = 0; i < rows; ++i) {
        Field value;
        result->get(i, value);
        const auto index = constant ? 0 : i % 6;
        if (only_null || index == 1 || index == 3 || index == 5) {
            EXPECT_TRUE(value.is_null());
            continue;
        }
        ASSERT_FALSE(value.is_null());
        const auto& array = value.get<TYPE_ARRAY>();
        ASSERT_EQ(array.size(), boundary ? (index == 2 ? 10 : index == 4 ? 5 : 6) : 2);
        if (index == 0) {
            if (boundary) {
                const double expected[][2] = {{55.712900225355526, 37.79505811173474},
                                              {55.71289713485416, 37.79506506997184},
                                              {55.712899340954856, 37.79507312653982},
                                              {55.71290463755745, 37.79507422487167},
                                              {55.71290772805917, 37.79506726663346},
                                              {55.712905521957914, 37.795059210064515}};
                for (size_t vertex = 0; vertex < array.size(); ++vertex) {
                    const auto& point = array[vertex].get<TYPE_ARRAY>();
                    ASSERT_EQ(point.size(), 2);
                    EXPECT_NEAR(point[0].get<TYPE_DOUBLE>(), expected[vertex][0], 1e-10);
                    EXPECT_NEAR(point[1].get<TYPE_DOUBLE>(), expected[vertex][1], 1e-10);
                }
            } else {
                EXPECT_NEAR(array[0].get<TYPE_DOUBLE>(), 55.71290243145668, 1e-10);
                EXPECT_NEAR(array[1].get<TYPE_DOUBLE>(), 37.795066168302505, 1e-10);
            }
        }
    }
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    EXPECT_TRUE(function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}
// NOLINTEND(readability-function-cognitive-complexity)

TEST(FunctionH3Test, ReverseCoordinates) {
    for (bool boundary : {false, true}) {
        check_coordinates(boundary, false, false, 8193);
        check_coordinates(boundary, true, false, 7);
        check_coordinates(boundary, true, true, 7);
        check_coordinates(boundary, false, true, 7);
        check_coordinates(boundary, false, false, 0);
    }
}

} // namespace doris
