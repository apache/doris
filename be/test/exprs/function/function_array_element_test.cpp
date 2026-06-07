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
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

TEST(function_array_element_test, element_at) {
    std::string func_name = "element_at";
    TestArray empty_arr;

    // element_at(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int32(1)},     {{vec, 4}, Null()},
                {{vec, -1}, Int32(3)}, {{vec, -3}, Int32(1)},    {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Int8>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int8(1)},      {{vec, 4}, Null()},
                {{vec, -1}, Int8(3)},  {{vec, -3}, Int8(1)},     {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Int128>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {Int128(1), Int128(2), Int128(3)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},      {{vec, Int64(1)}, Int128(1)},
                            {{vec, Int64(4)}, Null()},      {{vec, Int64(-1)}, Int128(3)},
                            {{vec, Int64(-3)}, Int128(1)},  {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},   {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeInt128, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Float64>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {double(1.11), double(2.22), double(3.33)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},        {{vec, Int64(1)}, double(1.11)},
                            {{vec, Int64(4)}, Null()},        {{vec, Int64(-1)}, double(3.33)},
                            {{vec, Int64(-3)}, double(1.11)}, {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},     {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Decimal128V2>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67),
                         ut_type::DECIMALV2VALUEFROMDOUBLE(0.0)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},
                            {{vec, Int64(1)}, ut_type::DECIMALV2VALUEFROMDOUBLE(17014116.67)},
                            {{vec, Int64(4)}, Null()},
                            {{vec, Int64(-1)}, ut_type::DECIMALV2VALUEFROMDOUBLE(0.0)},
                            {{vec, Int64(-2)}, ut_type::DECIMALV2VALUEFROMDOUBLE(-17014116.67)},
                            {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},
                            {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDecimalV2, true>(func_name, input_types, data_set, 9, 27));
    }

    // element_at(Array<String>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        DataSet data_set = {{{vec, 1}, std::string("abc")},
                            {{vec, 2}, std::string("")},
                            {{vec, 10}, Null()},
                            {{vec, -2}, std::string("")},
                            {{vec, 0}, Null()},
                            {{vec, -10}, Null()},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 0}, Null()},
                            {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

// Helper: build a ColumnConst wrapping a single Array(Int32) value [values...].
// The apparent size of the returned ColumnConst is `apparent_size`.
static ColumnPtr make_const_int32_array(std::vector<Int32> values, size_t apparent_size) {
    auto data_col = ColumnInt32::create();
    for (auto v : values) {
        data_col->insert_value(v);
    }
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(static_cast<Int64>(values.size()));
    auto arr = ColumnArray::create(std::move(data_col), std::move(offsets));
    // element_at always produces Nullable, so use a non-null wrapper
    auto null_map = ColumnUInt8::create(1, 0 /*not null*/);
    auto nullable_arr = ColumnNullable::create(std::move(arr), std::move(null_map));
    return ColumnConst::create(std::move(nullable_arr), apparent_size);
}

// Helper: build a ColumnConst wrapping a single Array(String) value [values...].
static ColumnPtr make_const_string_array(std::vector<std::string> values, size_t apparent_size) {
    auto data_col = ColumnString::create();
    for (const auto& v : values) {
        data_col->insert_data(v.data(), v.size());
    }
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(static_cast<Int64>(values.size()));
    auto arr = ColumnArray::create(std::move(data_col), std::move(offsets));
    auto null_map = ColumnUInt8::create(1, 0);
    auto nullable_arr = ColumnNullable::create(std::move(arr), std::move(null_map));
    return ColumnConst::create(std::move(nullable_arr), apparent_size);
}

// Invoke element_at(arr_col, idx_col) and return the result ColumnNullable.
// arr_type must match arr_col; idx_type must match idx_col.
static ColumnPtr run_element_at(ColumnPtr arr_col, DataTypePtr arr_type, ColumnPtr idx_col,
                                DataTypePtr idx_type, DataTypePtr result_type,
                                size_t input_rows_count) {
    ColumnsWithTypeAndName args = {{arr_col, arr_type, "arr"}, {idx_col, idx_type, "idx"}};
    auto func = SimpleFunctionFactory::instance().get_function("element_at", args, result_type);
    EXPECT_NE(func, nullptr);

    Block block;
    block.insert({arr_col, arr_type, "arr"});
    block.insert({idx_col, idx_type, "idx"});
    block.insert({nullptr, result_type, "result"});

    EXPECT_TRUE(func->execute(nullptr, block, {0, 1}, 2, input_rows_count).ok());
    return block.get_by_position(2).column;
}

// Tests for element_at with a constant (ColumnConst) array argument and a varying index.
// The key invariant: no row-count copies of the const array should be made; the function
// must produce correct results using the single stored value.
TEST(function_array_element_test, element_at_const_int32_array_varying_index) {
    // Const array: [10, 20, 30]  (same for every row)
    // Indices:     [ 1,  2,  3,  4, -1, -3, -4,  0]
    // Expected:    [10, 20, 30, NG, 30, 10, NG, NG]   (NG = NULL)
    constexpr size_t N = 8;

    auto arr_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto const_arr = make_const_int32_array({10, 20, 30}, N);

    auto idx_data = ColumnInt32::create();
    for (Int32 v : {1, 2, 3, 4, -1, -3, -4, 0}) {
        idx_data->insert_value(v);
    }
    auto idx_null_map = ColumnUInt8::create(N, 0);
    auto idx_col = ColumnNullable::create(std::move(idx_data), std::move(idx_null_map));
    auto idx_type = make_nullable(std::make_shared<DataTypeInt32>());

    auto result_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto result = run_element_at(const_arr, arr_type, std::move(idx_col), idx_type, result_type, N);

    ASSERT_EQ(result->size(), N);
    const auto& nr = assert_cast<const ColumnNullable&>(*result);
    const auto& data = assert_cast<const ColumnInt32&>(nr.get_nested_column());

    struct Expected {
        bool is_null;
        Int32 val;
    };
    const Expected expected[N] = {{false, 10}, {false, 20}, {false, 30}, {true, 0},
                                  {false, 30}, {false, 10}, {true, 0},   {true, 0}};
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(nr.is_null_at(i), expected[i].is_null) << "row " << i;
        if (!expected[i].is_null) {
            EXPECT_EQ(data.get_element(i), expected[i].val) << "row " << i;
        }
    }
}

// Const array where the array itself is NULL → every row must return NULL.
TEST(function_array_element_test, element_at_const_null_array) {
    constexpr size_t N = 4;

    auto arr_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));

    // Build a ColumnConst wrapping a size-1 Nullable(Array) that IS null.
    auto inner_data = ColumnInt32::create();
    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    inner_offsets->insert_value(0);
    auto inner_arr = ColumnArray::create(std::move(inner_data), std::move(inner_offsets));
    auto null_map = ColumnUInt8::create(1, 1 /*null*/);
    auto nullable_arr = ColumnNullable::create(std::move(inner_arr), std::move(null_map));
    ColumnPtr const_arr = ColumnConst::create(std::move(nullable_arr), N);

    auto idx_data = ColumnInt32::create();
    for (Int32 v : {1, 1, 1, 1}) {
        idx_data->insert_value(v);
    }
    auto idx_null_map = ColumnUInt8::create(N, 0);
    auto idx_col = ColumnNullable::create(std::move(idx_data), std::move(idx_null_map));
    auto idx_type = make_nullable(std::make_shared<DataTypeInt32>());

    auto result_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto result = run_element_at(const_arr, arr_type, std::move(idx_col), idx_type, result_type, N);

    ASSERT_EQ(result->size(), N);
    const auto& nr = assert_cast<const ColumnNullable&>(*result);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_TRUE(nr.is_null_at(i)) << "row " << i;
    }
}

// Const array with nullable index: NULL index → NULL result.
TEST(function_array_element_test, element_at_const_array_null_index) {
    constexpr size_t N = 4;

    auto arr_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    auto const_arr = make_const_int32_array({10, 20, 30}, N);

    auto idx_data = ColumnInt32::create();
    for (Int32 v : {1, 1, 1, 1}) {
        idx_data->insert_value(v);
    }
    // All indices are NULL
    auto idx_null_map = ColumnUInt8::create(N, 1 /*null*/);
    auto idx_col = ColumnNullable::create(std::move(idx_data), std::move(idx_null_map));
    auto idx_type = make_nullable(std::make_shared<DataTypeInt32>());

    auto result_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto result = run_element_at(const_arr, arr_type, std::move(idx_col), idx_type, result_type, N);

    ASSERT_EQ(result->size(), N);
    const auto& nr = assert_cast<const ColumnNullable&>(*result);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_TRUE(nr.is_null_at(i)) << "row " << i;
    }
}

// Const Array(String) – exercises _execute_string code path.
TEST(function_array_element_test, element_at_const_string_array_varying_index) {
    constexpr size_t N = 5;

    auto arr_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
    auto const_arr = make_const_string_array({"hello", "world", ""}, N);

    auto idx_data = ColumnInt32::create();
    for (Int32 v : {1, 2, 3, 4, -1}) {
        idx_data->insert_value(v);
    }
    auto idx_null_map = ColumnUInt8::create(N, 0);
    auto idx_col = ColumnNullable::create(std::move(idx_data), std::move(idx_null_map));
    auto idx_type = make_nullable(std::make_shared<DataTypeInt32>());

    auto result_type = make_nullable(std::make_shared<DataTypeString>());
    auto result = run_element_at(const_arr, arr_type, std::move(idx_col), idx_type, result_type, N);

    ASSERT_EQ(result->size(), N);
    const auto& nr = assert_cast<const ColumnNullable&>(*result);
    const auto& str_col = assert_cast<const ColumnString&>(nr.get_nested_column());

    EXPECT_FALSE(nr.is_null_at(0));
    EXPECT_EQ(str_col.get_data_at(0), std::string_view("hello"));
    EXPECT_FALSE(nr.is_null_at(1));
    EXPECT_EQ(str_col.get_data_at(1), std::string_view("world"));
    EXPECT_FALSE(nr.is_null_at(2));
    EXPECT_EQ(str_col.get_data_at(2), std::string_view("", 0));
    EXPECT_TRUE(nr.is_null_at(3)); // index 4 is out of bounds
    EXPECT_FALSE(nr.is_null_at(4));
    EXPECT_EQ(str_col.get_data_at(4), std::string_view("", 0)); // -1 → last = ""
}

// Large batch: verifies const-array optimization is correct for batch_size > 1.
TEST(function_array_element_test, element_at_const_array_large_batch) {
    constexpr size_t N = 4096;

    auto arr_type =
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()));
    // Const array: [100, 200, 300]
    auto const_arr = make_const_int32_array({100, 200, 300}, N);

    // Indices cycle through: 1→100, 2→200, 3→300, 4→NULL
    auto idx_data = ColumnInt32::create();
    idx_data->reserve(N);
    for (size_t i = 0; i < N; ++i) {
        idx_data->insert_value(static_cast<Int32>(i % 4 + 1));
    }
    auto idx_null_map = ColumnUInt8::create(N, 0);
    auto idx_col = ColumnNullable::create(std::move(idx_data), std::move(idx_null_map));
    auto idx_type = make_nullable(std::make_shared<DataTypeInt32>());

    auto result_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto result = run_element_at(const_arr, arr_type, std::move(idx_col), idx_type, result_type, N);

    ASSERT_EQ(result->size(), N);
    const auto& nr = assert_cast<const ColumnNullable&>(*result);
    const auto& data = assert_cast<const ColumnInt32&>(nr.get_nested_column());

    const Int32 expected_vals[4] = {100, 200, 300, 0 /*null*/};
    const bool expected_null[4] = {false, false, false, true};
    for (size_t i = 0; i < N; ++i) {
        size_t slot = i % 4;
        ASSERT_EQ(nr.is_null_at(i), expected_null[slot]) << "row " << i;
        if (!expected_null[slot]) {
            ASSERT_EQ(data.get_element(i), expected_vals[slot]) << "row " << i;
        }
    }
}

} // namespace doris
