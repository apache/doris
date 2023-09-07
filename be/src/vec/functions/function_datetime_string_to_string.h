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

#pragma once

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

#define GET_COLUMN_CONST_RETURN_IF_ERROR(Index, ColumnType)                                                         \
    const IColumn& source_col##Index = *block.get_by_position(arguments[Index]).column;                 \
    const auto* delta_const_column##Index = typeid_cast<const ColumnType*>(&source_col##Index);        \
    if (!delta_const_column##Index) {                                                                   \
        return Status::InternalError("Illegal column " +                                                \
                block.get_by_position(arguments[Index]).column->get_name() + "(index=" +                \
                std::to_string(Index) + ", type=" + typeid(source_col##Index).name() +                  \
                                     ") is not const " + name);                                         \
    }                                                                                                   \

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename Transform>
class FunctionDateTimeStringToStringOneArg : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;
    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToStringOneArg>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
        return {};
    }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        const auto* sources = check_and_get_column<ColumnVector<typename Transform::FromType>>(
                nullable_column ? nullable_column->get_nested_column_ptr().get()
                                : source_col.get());
        if (sources) {
            auto col_res = ColumnString::create();
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create();
            auto& vec_null_map_to = col_null_map_to->get_data();
            TransformerToStringOneArgument<Transform>::vector(
                    sources->get_data(), col_res->get_chars(), col_res->get_offsets(), vec_null_map_to);
            if (nullable_column) {
                const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
                for (int i = 0; i < origin_null_map.size(); ++i) {
                    vec_null_map_to[i] |= origin_null_map[i];
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        } else {
            return Status::InternalError("Illegal column " +
                                         block.get_by_position(arguments[0]).column->get_name() +
                                         " of first argument of function " + name);
        }
        return Status::OK();
    }
};

template <typename Transform>
class FunctionDateTimeStringToStringTwoArgs : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToStringTwoArgs>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
        return {};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;

        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        const auto* sources = check_and_get_column<ColumnVector<typename Transform::FromType>>(
                nullable_column ? nullable_column->get_nested_column_ptr().get()
                                : source_col.get());

        if (sources) {
            auto col_res = ColumnString::create();
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create();
            auto& vec_null_map_to = col_null_map_to->get_data();

            if (arguments.size() == 2) {
                const IColumn& source_col1 = *block.get_by_position(arguments[1]).column;
                StringRef formatter =
                        source_col1.get_data_at(0); // for both ColumnString or ColumnConst.
                TransformerToStringTwoArgument<Transform>::vector_constant(
                        context, sources->get_data(), formatter, col_res->get_chars(),
                        col_res->get_offsets(), vec_null_map_to);
            } else { //default argument
                TransformerToStringTwoArgument<Transform>::vector_constant(
                        context, sources->get_data(), StringRef("%Y-%m-%d %H:%i:%s"),
                        col_res->get_chars(), col_res->get_offsets(), vec_null_map_to);
            }

            if (nullable_column) {
                const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
                for (int i = 0; i < origin_null_map.size(); ++i) {
                    vec_null_map_to[i] |= origin_null_map[i];
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        } else {
            return Status::InternalError("Illegal column {} of first argument of function {}",
                                         block.get_by_position(arguments[0]).column->get_name(),
                                         name);
        }
        return Status::OK();
    }
};

template <typename Transform>
class FunctionDateTimeStringToStringFourArgs : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToStringFourArgs>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 4; }
    bool is_variadic() const override { return true; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
        return {};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1, 2, 3}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;

        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        const auto* sources = check_and_get_column<ColumnVector<typename Transform::FromType>>(
                nullable_column ? nullable_column->get_nested_column_ptr().get()
                                : source_col.get());

        if (sources) {
            auto col_res = ColumnString::create();
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create();
            auto& vec_null_map_to = col_null_map_to->get_data();

            if(arguments.size() == 4) {
                GET_COLUMN_CONST_RETURN_IF_ERROR(1, ColumnConst);
                GET_COLUMN_CONST_RETURN_IF_ERROR(2, ColumnConst);
                GET_COLUMN_CONST_RETURN_IF_ERROR(3, ColumnConst);

                Int32 first_day_of_week = delta_const_column1->get_field().get<Int32>();
                Int32 policy_of_first_week = delta_const_column2->get_field().get<Int32>();
                if (first_day_of_week < 0 || first_day_of_week > 6) {
                    return Status::InternalError("Illegal second arg, first_day_of_week "
                            "from 0(Mon) to 6(Sun). value: " + std::to_string(first_day_of_week));
                }
                if (policy_of_first_week < 0 || policy_of_first_week > 2) {
                    return Status::InternalError("Illegal third arg, policy_of_first_week "
                            "from 0 to 3. value: " + std::to_string(policy_of_first_week));
                }
                TransformerToStringFourArgument<Transform>::vector_constant(
                        sources->get_data(), first_day_of_week, policy_of_first_week,
                        delta_const_column3->get_field().get<String>(),
                        col_res->get_chars(), col_res->get_offsets(), vec_null_map_to);
            } else {
                return Status::InternalError("Illegal args num : " + std::to_string(arguments.size()));
            }

            if (nullable_column) {
                const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
                for (int i = 0; i < origin_null_map.size(); ++i) {
                    vec_null_map_to[i] |= origin_null_map[i];
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        } else {
            return Status::InternalError("Illegal column " +
                                         block.get_by_position(arguments[0]).column->get_name() +
                                         " of first argument of function " + name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
