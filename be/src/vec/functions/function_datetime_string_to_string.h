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

#include <fmt/compile.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/cast_set.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/date_format_type.h"
#include "vec/functions/datetime_errors.h"
#include "vec/functions/function.h"

namespace doris {
#include "common/compile_check_begin.h"
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename Transform>
class FunctionDateTimeStringToString : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;
    using ColumnType = typename PrimitiveTypeTraits<Transform::FromPType>::ColumnType;
    constexpr static bool IsDecimal = is_decimal(Transform::FromPType);

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; } // because format string is optional
    size_t get_number_of_arguments() const override { return 0; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }

    struct FormatState {
        std::string format_str;
        // Check if the format string is null or exceeds the length limit.
        bool is_valid = true;
        time_format_type::FormatImplVariant format_type;
    };

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<FormatState> state = std::make_shared<FormatState>();
        DCHECK((context->get_num_args() == 1) || (context->get_num_args() == 2));
        context->set_function_state(scope, state);
        if (context->get_num_args() == 1) {
            // default argument
            if constexpr (IsDecimal) {
                state->format_str = time_format_type::DEFAULT_FORMAT_DECIMAL;
            } else {
                state->format_str = time_format_type::DEFAULT_FORMAT;
                state->format_type = time_format_type::DEFAULT_IMPL;
            }
            return IFunction::open(context, scope);
        }

        const auto* column_string = context->get_constant_col(1);

        if (column_string == nullptr) {
            return Status::InvalidArgument(
                    "The second parameter of the function {} must be a constant.", get_name());
        }

        auto string_vale = column_string->column_ptr->get_data_at(0);
        if (string_vale.data == nullptr) {
            // func(col , null);
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        string_vale = string_vale.trim();
        // no need to rewrite, we choose implement by format_type. format_type's check has compatible logic about
        // special format string.
        auto format_str = StringRef(string_vale.data, string_vale.size);
        if (format_str.size > 128) {
            // exceeds the length limit.
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        // Preprocess special format strings.
        state->format_str = format_str;
        state->format_type = time_format_type::string_to_impl(state->format_str);

        return IFunction::open(context, scope);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    // avoid random value of nested for null row which would cause false positive of out of range error.
    bool need_replace_null_data_to_default() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& sources =
                assert_cast<const ColumnType&>(*block.get_by_position(arguments[0]).column);

        auto col_res = ColumnString::create();

        // in open we have checked the second argument is constant,
        RETURN_IF_ERROR(
                vector_constant(context, sources, col_res->get_chars(), col_res->get_offsets()));

        block.get_by_position(result).column = std::move(col_res);

        return Status::OK();
    }

    Status vector_constant(FunctionContext* context, const ColumnType& col,
                           ColumnString::Chars& res_data,
                           ColumnString::Offsets& res_offsets) const {
        auto* format_state = reinterpret_cast<FormatState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!format_state) {
            return Status::RuntimeError("funciton context for function '{}' must have FormatState;",
                                        get_name());
        }

        StringRef format(format_state->format_str);
        auto result_row_length = std::visit([](auto type) { return decltype(type)::row_size; },
                                            format_state->format_type);

        const auto& pod_array = col.get_data();
        const auto len = pod_array.size();

        if (!format_state->is_valid) {
            // invalid parameter -> throw
            throw_invalid_string(name, "invalid or oversized format");
        }
        res_offsets.resize(len);
        res_data.reserve(len * result_row_length);

        if constexpr (IsDecimal) {
            // FromUnixTimeDecimalImpl. may use UserDefinedImpl or yyyy_MM_dd_HH_mm_ss_SSSSSSImpl.
            size_t offset = 0;
            if (format_state->format_str == time_format_type::DEFAULT_FORMAT_DECIMAL) {
                for (int i = 0; i < len; ++i) {
                    bool invalid = Transform::template execute_decimal<
                            time_format_type::yyyy_MM_dd_HH_mm_ss_SSSSSSImpl>(
                            col.get_intergral_part(i), col.get_fractional_part(i), format, res_data,
                            offset, context->state()->timezone_obj());
                    if (invalid) [[unlikely]] {
                        throw_invalid_strings(
                                name,
                                fmt::format(FMT_COMPILE("{}.{}"), col.get_intergral_part(i),
                                            col.get_fractional_part(i)),
                                format_state->format_str);
                    }
                    res_offsets[i] = cast_set<uint32_t>(offset);
                }
            } else {
                for (int i = 0; i < len; ++i) {
                    bool invalid =
                            Transform::template execute_decimal<time_format_type::UserDefinedImpl>(
                                    col.get_intergral_part(i), col.get_fractional_part(i), format,
                                    res_data, offset, context->state()->timezone_obj());
                    if (invalid) [[unlikely]] {
                        throw_invalid_strings(
                                name,
                                fmt::format(FMT_COMPILE("{}.{}"), col.get_intergral_part(i),
                                            col.get_fractional_part(i)),
                                format_state->format_str);
                    }
                    res_offsets[i] = cast_set<uint32_t>(offset);
                }
            }
            res_data.resize(offset);
        } else {
            std::visit(
                    [&](auto type) {
                        using Impl = decltype(type);
                        size_t offset = 0;
                        for (int i = 0; i < len; ++i) {
                            bool invalid = Transform::template execute<Impl>(
                                    pod_array[i], format, res_data, offset,
                                    context->state()->timezone_obj());
                            if (invalid) [[unlikely]] {
                                throw_invalid_strings(name, std::to_string(pod_array[i]),
                                                      format_state->format_str);
                            }
                            res_offsets[i] = cast_set<uint32_t>(offset);
                        }
                        res_data.resize(offset);
                    },
                    format_state->format_type);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
