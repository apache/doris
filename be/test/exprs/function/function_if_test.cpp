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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "gtest/gtest_pred_impl.h"

// FunctionIf (be/src/exprs/function/if.cpp) is a class local to its translation unit. In
// production the SQL `if()` is rewritten to VectorizedIfExpr, so the only caller that reaches
// FunctionIf is `nullif`. These tests drive FunctionIf directly through the factory
// (SimpleFunctionFactory::instance().get_function("if", ...)) plus the free function
// count_true_with_notnull, exercising the branches that nullif alone cannot.
//
// A handful of branches are dead given how execute_impl prepares its inputs and therefore are
// intentionally not covered here:
//   * if.cpp:~90    - count_true_with_notnull all-null nullable returns early via only_null().
//   * if.cpp:525-528 - const-cond tail; the condition is full by the time it is reached.

namespace doris {

// Defined with external linkage in if.cpp, declared here because it has no public header.
size_t count_true_with_notnull(const ColumnPtr& col);
#ifdef BE_TEST
Status execute_function_if_for_null_then_else_test(FunctionContext* context, Block& block,
                                                   const ColumnWithTypeAndName& arg_cond,
                                                   const ColumnWithTypeAndName& arg_then,
                                                   const ColumnWithTypeAndName& arg_else,
                                                   uint32_t result, size_t input_rows_count,
                                                   bool& handled);
Status execute_function_if_for_null_condition_test(FunctionContext* context, Block& block,
                                                   const ColumnWithTypeAndName& arg_cond,
                                                   const ColumnWithTypeAndName& arg_then,
                                                   const ColumnWithTypeAndName& arg_else,
                                                   uint32_t result, bool& handled);
Status execute_function_if_basic_type_test(PrimitiveType primitive_type, Block& block,
                                           const ColumnUInt8* cond_col,
                                           const ColumnWithTypeAndName& arg_then,
                                           const ColumnWithTypeAndName& arg_else, uint32_t result,
                                           Status& status);
#endif

namespace {

constexpr size_t kRows = 16;

uint8_t mixed_cond_value(size_t i) {
    return static_cast<uint8_t>(i % 2);
}

ColumnPtr make_mixed_cond(size_t rows) {
    auto col = ColumnUInt8::create();
    auto& data = col->get_data();
    for (size_t i = 0; i < rows; ++i) {
        data.push_back(mixed_cond_value(i));
    }
    return col;
}

DataTypePtr bool_type() {
    return std::make_shared<DataTypeUInt8>();
}

DataTypePtr int32_type() {
    return std::make_shared<DataTypeInt32>();
}

DataTypePtr nullable_int32_type() {
    return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
}

ColumnPtr full_int32(const std::vector<int32_t>& vals) {
    auto col = ColumnInt32::create();
    for (int32_t v : vals) {
        col->get_data().push_back(v);
    }
    return col;
}

ColumnPtr const_int32(int32_t v, size_t rows) {
    return ColumnConst::create(ColumnInt32::create(1, v), rows);
}

ColumnPtr nullable_int32(const std::vector<int32_t>& vals, const std::vector<uint8_t>& nulls) {
    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < vals.size(); ++i) {
        nested->get_data().push_back(vals[i]);
        null_map->get_data().push_back(nulls[i]);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

// ColumnConst wrapping a single all-NULL nullable row.
ColumnPtr const_null_int32(size_t rows) {
    return ColumnConst::create(
            ColumnNullable::create(ColumnInt32::create(1, 0), ColumnUInt8::create(1, 1)), rows);
}

// ColumnConst wrapping a single non-NULL nullable row.
ColumnPtr const_nonnull_nullable_int32(int32_t v, size_t rows) {
    return ColumnConst::create(
            ColumnNullable::create(ColumnInt32::create(1, v), ColumnUInt8::create(1, 0)), rows);
}

ColumnPtr full_string(const std::vector<std::string>& vals) {
    auto col = ColumnString::create();
    for (const auto& s : vals) {
        col->insert_data(s.data(), s.size());
    }
    return col;
}

ColumnPtr const_string(const std::string& s, size_t rows) {
    auto inner = ColumnString::create();
    inner->insert_data(s.data(), s.size());
    return ColumnConst::create(std::move(inner), rows);
}

ColumnPtr nullable_string(const std::vector<std::string>& vals, const std::vector<uint8_t>& nulls) {
    auto nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < vals.size(); ++i) {
        nested->insert_data(vals[i].data(), vals[i].size());
        null_map->get_data().push_back(nulls[i]);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

// Builds the if() function via the factory and executes it on a fresh block.
std::pair<Status, ColumnPtr> execute_if(const ColumnWithTypeAndName& cond,
                                        const ColumnWithTypeAndName& then_arg,
                                        const ColumnWithTypeAndName& else_arg,
                                        const DataTypePtr& return_type, size_t rows) {
    auto func = SimpleFunctionFactory::instance().get_function(
            "if", ColumnsWithTypeAndName {cond, then_arg, else_arg}, return_type);
    Block block(ColumnsWithTypeAndName {cond, then_arg, else_arg});
    block.insert({nullptr, return_type, "result"});
    FunctionContext* context = nullptr;
    Status st = func->execute(context, block, {0, 1, 2}, 3, rows);
    return {st, st.ok() ? block.get_by_position(3).column : nullptr};
}

// Element-wise comparison that works for scalar, string and nullable result columns.
void expect_equal(const ColumnPtr& result, const IColumn& expected, size_t rows) {
    ASSERT_TRUE(result);
    auto full = result->convert_to_full_column_if_const();
    ASSERT_EQ(full->size(), rows);
    for (size_t i = 0; i < rows; ++i) {
        ASSERT_EQ(full->compare_at(i, i, expected, 1), 0) << "mismatch at row " << i;
    }
}

// Fills then/else columns of a scalar primitive type with distinct values where the C++ type can
// be built from an integer, and with defaults for the date/time/decimalv2 types that have no such
// constructor (identical values are fine - the goal is to drive execute_basic_type<PType>).
template <PrimitiveType PT>
void fill_then_else(IColumn& then_col, IColumn& else_col, size_t rows) {
    using CppType = typename PrimitiveTypeTraits<PT>::CppType;
    using ColumnType = typename PrimitiveTypeTraits<PT>::ColumnType;
    if constexpr (is_date_type(PT) || PT == TYPE_TIMESTAMPTZ || PT == TYPE_DECIMALV2) {
        then_col.insert_many_defaults(rows);
        else_col.insert_many_defaults(rows);
    } else {
        auto& tcol = assert_cast<ColumnType&>(then_col);
        auto& ecol = assert_cast<ColumnType&>(else_col);
        for (size_t i = 0; i < rows; ++i) {
            if constexpr (PT == TYPE_DECIMAL256) {
                tcol.insert_value(CppType(int64_t(i)));
                ecol.insert_value(CppType(int64_t(i + 100)));
            } else if constexpr (is_decimal(PT)) {
                using Native = typename CppType::NativeType;
                tcol.insert_value(CppType(Native(i)));
                ecol.insert_value(CppType(Native(i + 100)));
            } else {
                tcol.insert_value(CppType(i));
                ecol.insert_value(CppType(i + 100));
            }
        }
    }
}

// Runs if(mixed_cond, then, else) for one scalar type with full, distinct then/else columns and
// verifies the result selects then/else per the condition.
template <PrimitiveType PT>
void run_scalar_full_full(size_t rows) {
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>();
    auto then_mut = type->create_column();
    auto else_mut = type->create_column();
    fill_then_else<PT>(*then_mut, *else_mut, rows);
    ColumnPtr then_col = std::move(then_mut);
    ColumnPtr else_col = std::move(else_mut);
    ColumnPtr cond = make_mixed_cond(rows);

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, rows);
    ASSERT_TRUE(st.ok()) << st.to_string() << " for type " << type->get_name();

    auto expected = type->create_column();
    for (size_t i = 0; i < rows; ++i) {
        expected->insert_from(mixed_cond_value(i) ? *then_col : *else_col, i);
    }
    expect_equal(result, *expected, rows);
}

template <PrimitiveType PT>
DataTypePtr mismatch_type_for_basic_type_error() {
    if constexpr (PT == TYPE_INT) {
        return std::make_shared<DataTypeInt64>();
    } else {
        return int32_type();
    }
}

template <PrimitiveType PT>
ColumnPtr make_scalar_column(size_t rows) {
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>();
    return type->create_column()->clone_resized(rows);
}

template <PrimitiveType PT>
void run_basic_type_error_branches() {
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>();
    ColumnPtr cond = make_mixed_cond(kRows);
    const auto* cond_col = assert_cast<const ColumnUInt8*>(cond.get());

    // Type mismatch before NumIfImpl dispatch (if.cpp:204-208).
    {
        auto mismatch_type = mismatch_type_for_basic_type_error<PT>();
        ColumnPtr then_col = make_scalar_column<PT>(kRows);
        ColumnPtr else_col = mismatch_type->create_column()->clone_resized(kRows);
        ColumnWithTypeAndName then_arg {then_col, type, "then"};
        ColumnWithTypeAndName else_arg {else_col, mismatch_type, "else"};
        Block block(ColumnsWithTypeAndName {{cond, bool_type(), "cond"}, then_arg, else_arg});
        block.insert({nullptr, type, "result"});
        Status status;

        auto st = execute_function_if_basic_type_test(PT, block, cond_col, then_arg, else_arg, 3,
                                                      status);
        ASSERT_FALSE(st.ok()) << " for type " << type->get_name();
    }

    // Declared scalar type but actual column shape does not match, so NumIfImpl returns nullptr
    // (if.cpp:213-217).
    {
        std::vector<std::string> wrong_values(kRows, "wrong");
        ColumnPtr then_col = full_string(wrong_values);
        ColumnPtr else_col = make_scalar_column<PT>(kRows);
        ColumnWithTypeAndName then_arg {then_col, type, "then"};
        ColumnWithTypeAndName else_arg {else_col, type, "else"};
        Block block(ColumnsWithTypeAndName {{cond, bool_type(), "cond"}, then_arg, else_arg});
        block.insert({nullptr, type, "result"});
        Status status;

        auto st = execute_function_if_basic_type_test(PT, block, cond_col, then_arg, else_arg, 3,
                                                      status);
        ASSERT_FALSE(st.ok()) << " for type " << type->get_name();
    }
}

void run_int_combo(bool then_const, bool else_const, size_t rows) {
    auto type = int32_type();
    std::vector<int32_t> then_vals(rows);
    std::vector<int32_t> else_vals(rows);
    for (size_t i = 0; i < rows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
    }
    ColumnPtr then_col = then_const ? const_int32(7, rows) : full_int32(then_vals);
    ColumnPtr else_col = else_const ? const_int32(99, rows) : full_int32(else_vals);
    ColumnPtr cond = make_mixed_cond(rows);

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, rows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto then_full = then_col->convert_to_full_column_if_const();
    auto else_full = else_col->convert_to_full_column_if_const();
    auto expected = type->create_column();
    for (size_t i = 0; i < rows; ++i) {
        expected->insert_from(mixed_cond_value(i) ? *then_full : *else_full, i);
    }
    expect_equal(result, *expected, rows);
}

void run_string_combo(bool then_const, bool else_const, size_t rows) {
    auto type = std::make_shared<DataTypeString>();
    std::vector<std::string> then_vals(rows);
    std::vector<std::string> else_vals(rows);
    for (size_t i = 0; i < rows; ++i) {
        then_vals[i] = "then_" + std::to_string(i);
        else_vals[i] = "else_" + std::to_string(i);
    }
    ColumnPtr then_col = then_const ? const_string("THEN", rows) : full_string(then_vals);
    ColumnPtr else_col = else_const ? const_string("ELSE", rows) : full_string(else_vals);
    ColumnPtr cond = make_mixed_cond(rows);

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, rows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto then_full = then_col->convert_to_full_column_if_const();
    auto else_full = else_col->convert_to_full_column_if_const();
    auto expected = type->create_column();
    for (size_t i = 0; i < rows; ++i) {
        expected->insert_from(mixed_cond_value(i) ? *then_full : *else_full, i);
    }
    expect_equal(result, *expected, rows);
}

} // namespace

TEST(FunctionIfTest, CountTrueWithNotNull) {
    // const(UInt8 = 1) -> rows ; const(UInt8 = 0) -> 0  (if.cpp:75-78)
    EXPECT_EQ(count_true_with_notnull(ColumnConst::create(ColumnUInt8::create(1, 1), kRows)),
              kRows);
    EXPECT_EQ(count_true_with_notnull(ColumnConst::create(ColumnUInt8::create(1, 0), kRows)), 0);

    // const-null -> 0 via only_null early return (if.cpp:71)
    EXPECT_EQ(count_true_with_notnull(const_null_int32(kRows)), 0);

    // plain ColumnUInt8 mixed -> number of ones (if.cpp:98-101)
    {
        size_t expected_true = 0;
        auto col = ColumnUInt8::create();
        for (size_t i = 0; i < kRows; ++i) {
            uint8_t v = mixed_cond_value(i);
            col->get_data().push_back(v);
            expected_true += v;
        }
        ColumnPtr plain = std::move(col);
        EXPECT_EQ(count_true_with_notnull(plain), expected_true);
    }

    // nullable(UInt8) without nulls, mixed -> number of ones (if.cpp:91-93)
    {
        size_t expected_true = 0;
        auto nested = ColumnUInt8::create();
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < kRows; ++i) {
            uint8_t v = mixed_cond_value(i);
            nested->get_data().push_back(v);
            null_map->get_data().push_back(0);
            expected_true += v;
        }
        ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
        EXPECT_EQ(count_true_with_notnull(nullable), expected_true);
    }

    // nullable(UInt8) with some nulls -> returns null_count (if.cpp:94-96)
    {
        size_t null_count = 0;
        auto nested = ColumnUInt8::create();
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < kRows; ++i) {
            uint8_t is_null = (i % 4 == 0) ? 1 : 0;
            nested->get_data().push_back(mixed_cond_value(i));
            null_map->get_data().push_back(is_null);
            null_count += is_null;
        }
        ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
        EXPECT_EQ(count_true_with_notnull(nullable), null_count);
    }

    // nullable(UInt8) all-null -> 0 via only_null (if.cpp:71)
    {
        auto nested = ColumnUInt8::create(kRows, 1);
        auto null_map = ColumnUInt8::create(kRows, 1);
        ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
        EXPECT_EQ(count_true_with_notnull(nullable), 0);
    }
}

TEST(FunctionIfTest, SameColumnPointer) {
    // then and else are the same ColumnPtr -> result points at it (if.cpp:458-463).
    std::vector<int32_t> vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        vals[i] = static_cast<int32_t>(i * 3);
    }
    ColumnPtr shared = full_int32(vals);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {shared, type, "then"};
    ColumnWithTypeAndName else_arg {shared, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(result.get(), shared.get());
}

TEST(FunctionIfTest, AllTrueAllFalse_NonNullable) {
    std::vector<int32_t> then_vals(kRows);
    std::vector<int32_t> else_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
    }
    auto type = int32_type();

    // all-true -> result equals then (if.cpp:473-478)
    {
        ColumnPtr cond = ColumnUInt8::create(kRows, 1);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, type, "then"};
        ColumnWithTypeAndName else_arg {else_col, type, "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();
        expect_equal(result, *then_col, kRows);
    }

    // all-false -> result equals else (if.cpp:479-485)
    {
        ColumnPtr cond = ColumnUInt8::create(kRows, 0);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, type, "then"};
        ColumnWithTypeAndName else_arg {else_col, type, "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();
        expect_equal(result, *else_col, kRows);
    }
}

TEST(FunctionIfTest, AllTrueAllFalse_NullableResult) {
    std::vector<int32_t> then_vals(kRows);
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
        else_nulls[i] = (i % 3 == 0) ? 1 : 0;
    }
    auto then_type = int32_type();
    auto else_type = nullable_int32_type();
    auto ret_type = nullable_int32_type();

    // all-true -> then wrapped nullable, no nulls (if.cpp:473-478 nullable branch)
    {
        ColumnPtr cond = ColumnUInt8::create(kRows, 1);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::vector<uint8_t> no_nulls(kRows, 0);
        ColumnPtr expected = nullable_int32(then_vals, no_nulls);
        expect_equal(result, *expected, kRows);
    }

    // all-false -> else returned as-is, keeps its nulls (if.cpp:479-485 nullable branch)
    {
        ColumnPtr cond = ColumnUInt8::create(kRows, 0);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();

        ColumnPtr expected = nullable_int32(else_vals, else_nulls);
        expect_equal(result, *expected, kRows);
    }
}

TEST(FunctionIfTest, BasicTypeIntAllCombos) {
    // (full, full), (full, const), (const, full), (const, const); cond stays full (if.cpp:533-542).
    run_int_combo(false, false, kRows);
    run_int_combo(false, true, kRows);
    run_int_combo(true, false, kRows);
    run_int_combo(true, true, kRows);
}

TEST(FunctionIfTest, BasicTypesAllScalar) {
    // All 21 DispatchDataTypeMask::SCALAR types except INT (covered above) (if.cpp:533-542).
    run_scalar_full_full<TYPE_BOOLEAN>(kRows);
    run_scalar_full_full<TYPE_TINYINT>(kRows);
    run_scalar_full_full<TYPE_SMALLINT>(kRows);
    run_scalar_full_full<TYPE_BIGINT>(kRows);
    run_scalar_full_full<TYPE_LARGEINT>(kRows);
    run_scalar_full_full<TYPE_FLOAT>(kRows);
    run_scalar_full_full<TYPE_DOUBLE>(kRows);
    run_scalar_full_full<TYPE_DECIMAL32>(kRows);
    run_scalar_full_full<TYPE_DECIMAL64>(kRows);
    run_scalar_full_full<TYPE_DECIMALV2>(kRows);
    run_scalar_full_full<TYPE_DECIMAL128I>(kRows);
    run_scalar_full_full<TYPE_DECIMAL256>(kRows);
    run_scalar_full_full<TYPE_DATE>(kRows);
    run_scalar_full_full<TYPE_DATEV2>(kRows);
    run_scalar_full_full<TYPE_DATETIMEV2>(kRows);
    run_scalar_full_full<TYPE_DATETIME>(kRows);
    run_scalar_full_full<TYPE_TIMEV2>(kRows);
    run_scalar_full_full<TYPE_TIMESTAMPTZ>(kRows);
    run_scalar_full_full<TYPE_IPV4>(kRows);
    run_scalar_full_full<TYPE_IPV6>(kRows);
}

TEST(FunctionIfTest, ThenElseTypeMismatch) {
    // then INT, else BIGINT -> execute_basic_type reports a type mismatch (if.cpp:204-208).
    std::vector<int32_t> then_vals(kRows, 1);
    auto then_col = full_int32(then_vals);
    auto else_col = ColumnInt64::create();
    for (size_t i = 0; i < kRows; ++i) {
        else_col->get_data().push_back(static_cast<int64_t>(i));
    }
    ColumnPtr else_ptr = std::move(else_col);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_ptr, std::make_shared<DataTypeInt64>(), "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_FALSE(st.ok());
}

TEST(FunctionIfTest, UnexpectedColumn) {
    // then declared INT but column is ColumnInt64 -> NumIfImpl returns nullptr (if.cpp:213-217).
    auto wrong = ColumnInt64::create();
    for (size_t i = 0; i < kRows; ++i) {
        wrong->get_data().push_back(static_cast<int64_t>(i));
    }
    ColumnPtr then_col = std::move(wrong);
    std::vector<int32_t> else_vals(kRows, 5);
    ColumnPtr else_col = full_int32(else_vals);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_FALSE(st.ok());
}

TEST(FunctionIfTest, ExecuteGenericString) {
    // STRING is not scalar -> execute_generic; exercise its 4 const-combos (if.cpp:159-194).
    run_string_combo(false, false, kRows);
    run_string_combo(false, true, kRows);
    run_string_combo(true, false, kRows);
    run_string_combo(true, true, kRows);
}

TEST(FunctionIfTest, BothOnlyNull) {
    // then and else both const-NULL (distinct pointers) -> const all-NULL result (if.cpp:235-241).
    ColumnPtr then_col = const_null_int32(kRows);
    ColumnPtr else_col = const_null_int32(kRows);
    ASSERT_NE(then_col.get(), else_col.get());
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(result->only_null());

    std::vector<int32_t> vals(kRows, 0);
    std::vector<uint8_t> nulls(kRows, 1);
    ColumnPtr expected = nullable_int32(vals, nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, ThenNullElseNullable) {
    // if(cond, NULL, nullable) -> else null mask OR-ed with cond (if.cpp:248-255).
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        else_vals[i] = static_cast<int32_t>(i + 50);
        else_nulls[i] = (i % 4 == 1) ? 1 : 0;
    }
    ColumnPtr then_col = const_null_int32(kRows);
    ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        exp_vals[i] = else_vals[i];
        exp_nulls[i] = mixed_cond_value(i) ? 1 : else_nulls[i];
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, ThenNullElseNotNullable) {
    // if(cond, NULL, not_nullable) -> Nullable(else, null mask = cond) (if.cpp:256-261).
    std::vector<int32_t> else_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        else_vals[i] = static_cast<int32_t>(i + 7);
    }
    ColumnPtr then_col = const_null_int32(kRows);
    ColumnPtr else_col = full_int32(else_vals);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto then_type = nullable_int32_type();
    auto else_type = int32_type();
    auto ret_type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
    ColumnWithTypeAndName else_arg {else_col, else_type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        exp_vals[i] = else_vals[i];
        exp_nulls[i] = mixed_cond_value(i) ? 1 : 0;
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, ThenNullConstCondition) {
    std::vector<int32_t> else_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        else_vals[i] = static_cast<int32_t>(i + 30);
    }
    auto then_type = nullable_int32_type();
    auto else_type = int32_type();
    auto ret_type = nullable_int32_type();

    // if(true, NULL, else) -> empty nullable result column resized to rows (if.cpp:262-266).
    {
        ColumnPtr cond = ColumnConst::create(ColumnUInt8::create(1, 1), kRows);
        ColumnPtr then_col = const_null_int32(kRows);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = false;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_TRUE(handled);

        std::vector<int32_t> exp_vals(kRows, 0);
        std::vector<uint8_t> exp_nulls(kRows, 1);
        ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
        expect_equal(block.get_by_position(3).column, *expected, kRows);
    }

    // if(false, NULL, else) -> else wrapped as nullable with no nulls (if.cpp:267-270).
    {
        ColumnPtr cond = ColumnConst::create(ColumnUInt8::create(1, 0), kRows);
        ColumnPtr then_col = const_null_int32(kRows);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = false;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_TRUE(handled);

        std::vector<uint8_t> exp_nulls(kRows, 0);
        ColumnPtr expected = nullable_int32(else_vals, exp_nulls);
        expect_equal(block.get_by_position(3).column, *expected, kRows);
    }

    // Invalid condition column reports an internal error (if.cpp:271-276).
    {
        ColumnPtr cond = full_int32(else_vals);
        ColumnPtr then_col = const_null_int32(kRows);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnWithTypeAndName cond_arg {cond, int32_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = true;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_FALSE(st.ok());
        ASSERT_FALSE(handled);
    }
}

TEST(FunctionIfTest, ElseNullConstCondition) {
    std::vector<int32_t> then_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i + 40);
    }
    auto then_type = int32_type();
    auto else_type = nullable_int32_type();
    auto ret_type = nullable_int32_type();

    // if(true, then, NULL) -> then wrapped as nullable with no nulls (if.cpp:304-306).
    {
        ColumnPtr cond = ColumnConst::create(ColumnUInt8::create(1, 1), kRows);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = const_null_int32(kRows);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = false;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_TRUE(handled);

        std::vector<uint8_t> exp_nulls(kRows, 0);
        ColumnPtr expected = nullable_int32(then_vals, exp_nulls);
        expect_equal(block.get_by_position(3).column, *expected, kRows);
    }

    // if(false, then, NULL) -> empty nullable result column resized to rows (if.cpp:307-311).
    {
        ColumnPtr cond = ColumnConst::create(ColumnUInt8::create(1, 0), kRows);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = const_null_int32(kRows);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = false;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_TRUE(handled);

        std::vector<int32_t> exp_vals(kRows, 0);
        std::vector<uint8_t> exp_nulls(kRows, 1);
        ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
        expect_equal(block.get_by_position(3).column, *expected, kRows);
    }

    // Invalid condition column reports an internal error (if.cpp:312-317).
    {
        ColumnPtr cond = full_int32(then_vals);
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = const_null_int32(kRows);
        ColumnWithTypeAndName cond_arg {cond, int32_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
        ColumnWithTypeAndName else_arg {else_col, else_type, "else"};
        Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
        block.insert({nullptr, ret_type, "result"});
        bool handled = true;

        auto st = execute_function_if_for_null_then_else_test(nullptr, block, cond_arg, then_arg,
                                                              else_arg, 3, kRows, handled);
        ASSERT_FALSE(st.ok());
        ASSERT_FALSE(handled);
    }
}

TEST(FunctionIfTest, NullConditionOnlyNull) {
    std::vector<int32_t> then_vals(kRows);
    std::vector<int32_t> else_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i + 10);
        else_vals[i] = static_cast<int32_t>(i + 100);
    }
    ColumnPtr cond = ColumnConst::create(
            ColumnNullable::create(ColumnUInt8::create(1, 0), ColumnUInt8::create(1, 1)), kRows);
    ColumnPtr then_col = full_int32(then_vals);
    ColumnPtr else_col = full_int32(else_vals);
    auto type = int32_type();

    ColumnWithTypeAndName cond_arg {cond, std::make_shared<DataTypeNullable>(bool_type()), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};
    Block block(ColumnsWithTypeAndName {cond_arg, then_arg, else_arg});
    block.insert({nullptr, type, "result"});
    bool handled = false;

    auto st = execute_function_if_for_null_condition_test(nullptr, block, cond_arg, then_arg,
                                                          else_arg, 3, handled);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(handled);
    expect_equal(block.get_by_position(3).column, *else_col, kRows);
}

TEST(FunctionIfTest, BasicTypeAllScalarErrors) {
    run_basic_type_error_branches<TYPE_BOOLEAN>();
    run_basic_type_error_branches<TYPE_TINYINT>();
    run_basic_type_error_branches<TYPE_SMALLINT>();
    run_basic_type_error_branches<TYPE_INT>();
    run_basic_type_error_branches<TYPE_BIGINT>();
    run_basic_type_error_branches<TYPE_LARGEINT>();
    run_basic_type_error_branches<TYPE_FLOAT>();
    run_basic_type_error_branches<TYPE_DOUBLE>();
    run_basic_type_error_branches<TYPE_DECIMAL32>();
    run_basic_type_error_branches<TYPE_DECIMAL64>();
    run_basic_type_error_branches<TYPE_DECIMALV2>();
    run_basic_type_error_branches<TYPE_DECIMAL128I>();
    run_basic_type_error_branches<TYPE_DECIMAL256>();
    run_basic_type_error_branches<TYPE_DATE>();
    run_basic_type_error_branches<TYPE_DATEV2>();
    run_basic_type_error_branches<TYPE_DATETIMEV2>();
    run_basic_type_error_branches<TYPE_DATETIME>();
    run_basic_type_error_branches<TYPE_TIMEV2>();
    run_basic_type_error_branches<TYPE_TIMESTAMPTZ>();
    run_basic_type_error_branches<TYPE_IPV4>();
    run_basic_type_error_branches<TYPE_IPV6>();
}

TEST(FunctionIfTest, ElseNullThenNullable) {
    // if(cond, nullable, NULL) -> then null mask OR-ed with negated cond (if.cpp:281-287).
    std::vector<int32_t> then_vals(kRows);
    std::vector<uint8_t> then_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i + 20);
        then_nulls[i] = (i % 3 == 0) ? 1 : 0;
    }
    ColumnPtr then_col = nullable_int32(then_vals, then_nulls);
    ColumnPtr else_col = const_null_int32(kRows);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        exp_vals[i] = then_vals[i];
        exp_nulls[i] = mixed_cond_value(i) ? then_nulls[i] : 1;
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, ElseNullThenNotNullable) {
    // if(cond, not_nullable, NULL) -> Nullable(then, null mask = !cond) (if.cpp:288-302).
    std::vector<int32_t> then_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i + 3);
    }
    ColumnPtr then_col = full_int32(then_vals);
    ColumnPtr else_col = const_null_int32(kRows);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto then_type = int32_type();
    auto else_type = nullable_int32_type();
    auto ret_type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
    ColumnWithTypeAndName else_arg {else_col, else_type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        exp_vals[i] = then_vals[i];
        exp_nulls[i] = mixed_cond_value(i) ? 0 : 1;
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, ElseNullThenConst) {
    // if(cond, const(not_nullable), NULL) -> materialize the const then column (if.cpp:300).
    ColumnPtr then_col = const_int32(42, kRows);
    ColumnPtr else_col = const_null_int32(kRows);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto then_type = int32_type();
    auto else_type = nullable_int32_type();
    auto ret_type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, then_type, "then"};
    ColumnWithTypeAndName else_arg {else_col, else_type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows, 42);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        exp_nulls[i] = mixed_cond_value(i) ? 0 : 1;
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, NullableThenElse_BothFull) {
    // Both nullable, mixed nulls (if.cpp:323-412 plus recursion).
    std::vector<int32_t> then_vals(kRows);
    std::vector<uint8_t> then_nulls(kRows);
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
        then_nulls[i] = (i % 3 == 0) ? 1 : 0;
        else_nulls[i] = (i % 4 == 1) ? 1 : 0;
    }
    ColumnPtr then_col = nullable_int32(then_vals, then_nulls);
    ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        bool t = mixed_cond_value(i);
        exp_vals[i] = t ? then_vals[i] : else_vals[i];
        exp_nulls[i] = t ? then_nulls[i] : else_nulls[i];
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, NullableThenElse_ConstNullableThen) {
    // then is const(nullable, non-null) (if.cpp:339-345, 368-369, 138-140).
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        else_vals[i] = static_cast<int32_t>(i + 100);
        else_nulls[i] = (i % 4 == 1) ? 1 : 0;
    }
    constexpr int32_t kThen = 9;
    ColumnPtr then_col = const_nonnull_nullable_int32(kThen, kRows);
    ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        bool t = mixed_cond_value(i);
        exp_vals[i] = t ? kThen : else_vals[i];
        exp_nulls[i] = t ? 0 : else_nulls[i];
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, NullableThenElse_ConstNullableElse) {
    // else is const(nullable, non-null) (if.cpp:347-353, 378-379).
    std::vector<int32_t> then_vals(kRows);
    std::vector<uint8_t> then_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        then_nulls[i] = (i % 3 == 0) ? 1 : 0;
    }
    constexpr int32_t kElse = 77;
    ColumnPtr then_col = nullable_int32(then_vals, then_nulls);
    ColumnPtr else_col = const_nonnull_nullable_int32(kElse, kRows);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = nullable_int32_type();

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<int32_t> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        bool t = mixed_cond_value(i);
        exp_vals[i] = t ? then_vals[i] : kElse;
        exp_nulls[i] = t ? then_nulls[i] : 0;
    }
    ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, NullableThenElse_MixNullability) {
    std::vector<int32_t> then_vals(kRows);
    std::vector<uint8_t> then_nulls(kRows);
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
        then_nulls[i] = (i % 3 == 0) ? 1 : 0;
        else_nulls[i] = (i % 4 == 1) ? 1 : 0;
    }
    auto ret_type = nullable_int32_type();

    // then nullable, else plain (if.cpp:375-380 else-branch, 142).
    {
        ColumnPtr then_col = nullable_int32(then_vals, then_nulls);
        ColumnPtr else_col = full_int32(else_vals);
        ColumnPtr cond = make_mixed_cond(kRows);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, nullable_int32_type(), "then"};
        ColumnWithTypeAndName else_arg {else_col, int32_type(), "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::vector<int32_t> exp_vals(kRows);
        std::vector<uint8_t> exp_nulls(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            bool t = mixed_cond_value(i);
            exp_vals[i] = t ? then_vals[i] : else_vals[i];
            exp_nulls[i] = t ? then_nulls[i] : 0;
        }
        ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
        expect_equal(result, *expected, kRows);
    }

    // then plain, else nullable (if.cpp:368-369 then-branch, 142).
    {
        ColumnPtr then_col = full_int32(then_vals);
        ColumnPtr else_col = nullable_int32(else_vals, else_nulls);
        ColumnPtr cond = make_mixed_cond(kRows);
        ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
        ColumnWithTypeAndName then_arg {then_col, int32_type(), "then"};
        ColumnWithTypeAndName else_arg {else_col, nullable_int32_type(), "else"};
        auto [st, result] = execute_if(cond_arg, then_arg, else_arg, ret_type, kRows);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::vector<int32_t> exp_vals(kRows);
        std::vector<uint8_t> exp_nulls(kRows);
        for (size_t i = 0; i < kRows; ++i) {
            bool t = mixed_cond_value(i);
            exp_vals[i] = t ? then_vals[i] : else_vals[i];
            exp_nulls[i] = t ? 0 : else_nulls[i];
        }
        ColumnPtr expected = nullable_int32(exp_vals, exp_nulls);
        expect_equal(result, *expected, kRows);
    }
}

TEST(FunctionIfTest, NullableThenElse_String) {
    // Nullable strings -> nested recursion goes through execute_generic (if.cpp:323-412).
    std::vector<std::string> then_vals(kRows);
    std::vector<uint8_t> then_nulls(kRows);
    std::vector<std::string> else_vals(kRows);
    std::vector<uint8_t> else_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = "t" + std::to_string(i);
        else_vals[i] = "e" + std::to_string(i);
        then_nulls[i] = (i % 3 == 0) ? 1 : 0;
        else_nulls[i] = (i % 4 == 1) ? 1 : 0;
    }
    ColumnPtr then_col = nullable_string(then_vals, then_nulls);
    ColumnPtr else_col = nullable_string(else_vals, else_nulls);
    ColumnPtr cond = make_mixed_cond(kRows);
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    ColumnWithTypeAndName cond_arg {cond, bool_type(), "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::vector<std::string> exp_vals(kRows);
    std::vector<uint8_t> exp_nulls(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        bool t = mixed_cond_value(i);
        exp_vals[i] = t ? then_vals[i] : else_vals[i];
        exp_nulls[i] = t ? then_nulls[i] : else_nulls[i];
    }
    ColumnPtr expected = nullable_string(exp_vals, exp_nulls);
    expect_equal(result, *expected, kRows);
}

TEST(FunctionIfTest, NullableCondition) {
    // Nullable condition: null rows behave as false (if.cpp:414-449). execute mutates the cond
    // nested data in place, so the expected result is computed up front.
    std::vector<int32_t> then_vals(kRows);
    std::vector<int32_t> else_vals(kRows);
    std::vector<uint8_t> cond_bool(kRows);
    std::vector<uint8_t> cond_null(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        then_vals[i] = static_cast<int32_t>(i);
        else_vals[i] = static_cast<int32_t>(i + 100);
        cond_bool[i] = static_cast<uint8_t>(i % 2);
        cond_null[i] = (i % 4 == 3) ? 1 : 0;
    }

    std::vector<int32_t> exp_vals(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        bool take_then = cond_bool[i] != 0 && cond_null[i] == 0;
        exp_vals[i] = take_then ? then_vals[i] : else_vals[i];
    }

    auto cond_nested = ColumnUInt8::create();
    auto cond_nullmap = ColumnUInt8::create();
    for (size_t i = 0; i < kRows; ++i) {
        cond_nested->get_data().push_back(cond_bool[i]);
        cond_nullmap->get_data().push_back(cond_null[i]);
    }
    ColumnPtr cond = ColumnNullable::create(std::move(cond_nested), std::move(cond_nullmap));
    ColumnPtr then_col = full_int32(then_vals);
    ColumnPtr else_col = full_int32(else_vals);
    auto cond_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    auto type = int32_type();

    ColumnWithTypeAndName cond_arg {cond, cond_type, "cond"};
    ColumnWithTypeAndName then_arg {then_col, type, "then"};
    ColumnWithTypeAndName else_arg {else_col, type, "else"};

    auto [st, result] = execute_if(cond_arg, then_arg, else_arg, type, kRows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ColumnPtr expected = full_int32(exp_vals);
    expect_equal(result, *expected, kRows);
}

} // namespace doris
