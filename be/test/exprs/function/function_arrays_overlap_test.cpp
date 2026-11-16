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
#include <vector>

#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {

// Unlike check_function, store only one physical row for constant arguments while
// executing multiple rows and blocks, as VectorizedFnCall does.
static void check_const_arrays_overlap(const InputTypeSet& input_types, const DataSet& data_set) {
    ut_type::UTDataTypeDescs descs;
    ASSERT_TRUE(parse_ut_data_type(input_types, descs));
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    const auto rows = data_set.size();
    DataTypes arg_types;
    Block block;
    std::vector<std::shared_ptr<ConstantExprResult>> constants(2);
    for (size_t arg = 0; arg < 2; ++arg) {
        const auto& desc = descs[arg];
        auto column = desc.data_type->create_column();
        for (size_t row = 0; row < (desc.is_const ? 1 : rows); ++row) {
            ASSERT_TRUE(insert_cell(column, desc.data_type, data_set[row].first[arg]));
        }
        ColumnPtr input_column = std::move(column);
        if (desc.is_const) {
            input_column = ColumnConst::create(std::move(input_column), rows);
            constants[arg] = std::make_shared<ConstantExprResult>(input_column);
        }
        block.insert({std::move(input_column), desc.data_type, desc.col_name});
        arg_types.push_back(desc.data_type);
    }

    auto function = SimpleFunctionFactory::instance().get_function(
            "arrays_overlap", block.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    auto context = FunctionContext::create_context(nullptr, result_type, arg_types);
    context->set_constant_cols(constants);
    constants.clear();
    ASSERT_TRUE(function->open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    if (descs[0].is_const && descs[1].is_const) {
        EXPECT_EQ(context->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL),
                  nullptr)
                << "All-constant expressions must not build a lookup set";
    }
    ASSERT_TRUE(
            function->open(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    auto expected = result_type->create_column();
    for (const auto& row : data_set) {
        ASSERT_TRUE(insert_cell(expected, result_type, row.second));
    }
    block.insert({nullptr, result_type, "result"});
    for (int batch = 0; batch < 2; ++batch) {
        ASSERT_TRUE(function->execute(context.get(), block, {0, 1}, 2, rows).ok());
        const auto actual = block.get_by_position(2).column->convert_to_full_column_if_const();
        ASSERT_EQ(actual->size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_EQ(0, actual->compare_at(row, row, *expected, 1)) << "row " << row;
        }
    }
    ASSERT_TRUE(
            function->close(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(function->close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

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

    // arrays_overlap(Array<Decimal128V2>, Array<Decimal128V2>)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};

        TestArray vec1 = {ut_type::DECIMALV2VALUE(17014116, 670000000, 9),
                          ut_type::DECIMALV2VALUE(-17014116, 670000000, 9),
                          ut_type::DECIMALV2VALUE(0, 0, 9)};
        TestArray vec2 = {ut_type::DECIMALV2VALUE(17014116, 670000000, 9)};

        TestArray vec3 = {ut_type::DECIMALV2VALUE(17014116, 670000000, 9),
                          ut_type::DECIMALV2VALUE(-17014116, 670000000, 9), Null()};
        TestArray vec4 = {ut_type::DECIMALV2VALUE(-17014116, 670000000, 9)};
        TestArray vec5 = {ut_type::DECIMALV2VALUE(-17014116, 680000000, 9)};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)}, {{Null(), vec1}, Null()},
                            {{vec1, Null()}, Null()}, {{empty_arr, vec1}, UInt8(0)},
                            {{vec3, vec4}, UInt8(1)}, {{vec3, vec5}, UInt8(0)},
                            {{vec4, vec3}, UInt8(1)}, {{vec5, vec3}, UInt8(0)}};

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
                            {{vec4, vec1}, UInt8(1)}, {{vec1, vec5}, UInt8(0)},
                            {{vec1, vec4}, UInt8(1)}, {{vec5, vec1}, UInt8(0)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    // arrays_overlap(Array<Decimal128V2>, Array<Decimal128V2>), Non-nullable
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2};

        TestArray vec1 = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                          ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67),
                          ut_type::DECIMALV2VALUEFROMDOUBLE(0.0)};
        TestArray vec2 = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67)};

        TestArray vec3 = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                          ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)};
        TestArray vec4 = {ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)};
        TestArray vec5 = {ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.68)};
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
        TestArray vec6 = {std::string("abc"), Null()};
        DataSet data_set = {{{vec1, vec2}, UInt8(1)},      {{vec1, vec3}, UInt8(1)},
                            {{empty_arr, vec1}, UInt8(0)}, {{vec4, vec1}, UInt8(1)},
                            {{vec1, vec5}, UInt8(0)},      {{vec1, vec4}, UInt8(1)},
                            {{vec5, vec1}, UInt8(0)},      {{vec1, vec6}, UInt8(1)}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(function_arrays_overlap_test, const_arguments) {
    {
        InputTypeSet input_types = {Consted {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};
        TestArray left = {Int32(1), Null(), Int32(3)};
        TestArray right = {Int32(4), Null()};
        DataSet data_set = {{{left, right}, UInt8(1)}};
        static_cast<void>(
                check_function<DataTypeUInt8, true>("arrays_overlap", input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    Consted {PrimitiveType::TYPE_ARRAY},
                                    PrimitiveType::TYPE_VARCHAR};
        TestArray left = {std::string("abc"), std::string("def")};
        TestArray right = {std::string("xyz"), std::string("abc")};
        DataSet data_set = {{{left, right}, UInt8(1)}};
        static_cast<void>(
                check_function<DataTypeUInt8, true>("arrays_overlap", input_types, data_set));
    }
}

TEST(function_arrays_overlap_test, prepared_set_nulls_and_empty_arrays) {
    const TestArray constant = {Int32(1), Null(), Int32(3), Int32(1)};
    const TestArray empty;
    const DataSet data = {
            {{constant, TestArray {Int32(3)}}, UInt8(1)},
            {{constant, TestArray {Int32(4)}}, UInt8(0)},
            {{constant, TestArray {Null()}}, UInt8(1)},
            {{constant, empty}, UInt8(0)},
            {{constant, Null()}, Null()},
            {{constant, TestArray {Int32(4), Int32(5), Int32(6), Int32(7), Int32(1)}}, UInt8(1)}};
    for (bool const_left : {true, false}) {
        InputTypeSet types = {Consted {TYPE_ARRAY}, TYPE_INT, TYPE_ARRAY, TYPE_INT};
        if (!const_left) {
            types = {TYPE_ARRAY, TYPE_INT, Consted {TYPE_ARRAY}, TYPE_INT};
        }
        auto check_cases = [&](DataSet cases) {
            if (!const_left) {
                for (auto& row : cases) {
                    std::swap(row.first[0], row.first[1]);
                }
            }
            check_const_arrays_overlap(types, cases);
        };
        check_cases(data);
        check_cases({{{empty, empty}, UInt8(0)},
                     {{empty, TestArray {Null()}}, UInt8(0)},
                     {{empty, Null()}, Null()}});
        check_cases({{{Null(), Null()}, Null()},
                     {{Null(), empty}, Null()},
                     {{Null(), TestArray {Int32(1)}}, Null()}});
        check_cases({{{TestArray {Int32(1)}, TestArray {Null()}}, UInt8(0)}});
    }
}

TEST(function_arrays_overlap_test, both_const_arguments) {
    const InputTypeSet types = {Consted {TYPE_ARRAY}, TYPE_INT, Consted {TYPE_ARRAY}, TYPE_INT};
    check_const_arrays_overlap(types, {{{TestArray {Null()}, TestArray {Null()}}, UInt8(1)},
                                       {{TestArray {Null()}, TestArray {Null()}}, UInt8(1)}});
    check_const_arrays_overlap(types, {{{TestArray {}, TestArray {Null()}}, UInt8(0)}});
    check_const_arrays_overlap(types, {{{Null(), TestArray {}}, Null()}});

    TestArray large;
    for (Int32 value = 0; value < 10000; ++value) {
        large.emplace_back(value);
    }
    check_const_arrays_overlap(types, {{{large, TestArray {}}, UInt8(0)}});
    check_const_arrays_overlap(types, {{{large, Null()}, Null()}});
}

TEST(function_arrays_overlap_test, prepared_decimal_set) {
    const auto value = ut_type::DECIMALV2VALUE(123, 450000000, 9);
    const auto other = ut_type::DECIMALV2VALUE(123, 460000000, 9);
    check_const_arrays_overlap({TYPE_ARRAY, TYPE_DECIMALV2, Consted {TYPE_ARRAY}, TYPE_DECIMALV2},
                               {{{TestArray {value}, TestArray {value, Null()}}, UInt8(1)},
                                {{TestArray {other}, TestArray {value, Null()}}, UInt8(0)},
                                {{TestArray {Null()}, TestArray {value, Null()}}, UInt8(1)}});
}

TEST(function_arrays_overlap_test, prepared_strings_survive_context_clone) {
    const auto type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>()));
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    auto context = FunctionContext::create_context(nullptr, result_type, {type, type});
    auto constant = type->create_column();
    ASSERT_TRUE(insert_cell(constant, type, TestArray {std::string(128, 'a'), std::string("")}));
    context->set_constant_cols(
            {nullptr, std::make_shared<ConstantExprResult>(std::move(constant))});
    const auto function = SimpleFunctionFactory::instance().get_function(
            "arrays_overlap", {{nullptr, type, "left"}, {nullptr, type, "right"}}, result_type);
    ASSERT_NE(function, nullptr);
    ASSERT_TRUE(function->open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    ASSERT_NE(context->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL),
              nullptr);
    auto clone = context->clone();
    context.reset();
    ASSERT_TRUE(
            function->open(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto left = type->create_column();
    ASSERT_TRUE(insert_cell(left, type, TestArray {std::string(128, 'a')}));
    ASSERT_TRUE(insert_cell(left, type, TestArray {std::string("different")}));
    ASSERT_TRUE(insert_cell(left, type, TestArray {std::string("")}));
    Block block {{std::move(left), type, "left"},
                 {ColumnConst::create(clone->get_constant_col(1)->column_ptr(), 3), type, "right"},
                 {nullptr, result_type, "result"}};
    ASSERT_TRUE(function->execute(clone.get(), block, {0, 1}, 2, 3).ok());
    auto expected = result_type->create_column();
    for (auto value : {UInt8(1), UInt8(0), UInt8(1)}) {
        ASSERT_TRUE(insert_cell(expected, result_type, value));
    }
    for (size_t row = 0; row < 3; ++row) {
        EXPECT_EQ(0, block.get_by_position(2).column->compare_at(row, row, *expected, 1));
    }
    ASSERT_TRUE(
            function->close(clone.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
}

} // namespace doris
