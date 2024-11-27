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
#include <variant>

#include "common/cast_set.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
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
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"
#include "vec/runtime/vdatetime_value.h"

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

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
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
            state->format_str = time_format_type::default_format;
            state->format_type = time_format_type::default_impl;
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
        auto format_str =
                time_format_type::rewrite_specific_format(string_vale.data, string_vale.size);
        if (format_str.size > 128) {
            //  exceeds the length limit.
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        // Preprocess special format strings.
        state->format_str = format_str;
        state->format_type = time_format_type::string_to_impl(state->format_str);

        return IFunction::open(context, scope);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;

        const auto* nullable_column = check_and_get_column<ColumnNullable>(source_col.get());
        const auto* sources = assert_cast<const ColumnVector<typename Transform::FromType>*>(
                nullable_column ? nullable_column->get_nested_column_ptr().get()
                                : source_col.get());

        auto col_res = ColumnString::create();
        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create();
        auto& vec_null_map_to = col_null_map_to->get_data();

        RETURN_IF_ERROR(vector_constant(context, sources->get_data(), col_res->get_chars(),
                                        col_res->get_offsets(), vec_null_map_to));

        if (nullable_column) {
            // input column is nullable
            const auto& origin_null_map = nullable_column->get_null_map_column().get_data();
            for (int i = 0; i < origin_null_map.size(); ++i) {
                vec_null_map_to[i] |= origin_null_map[i];
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));

        return Status::OK();
    }

    Status vector_constant(FunctionContext* context,
                           const PaddedPODArray<typename Transform::FromType>& ts,
                           ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                           PaddedPODArray<UInt8>& null_map) const {
        auto* format_state = reinterpret_cast<FormatState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!format_state) {
            return Status::RuntimeError("funciton context for function '{}' must have FormatState;",
                                        get_name());
        }

        StringRef format(format_state->format_str);

        const auto len = ts.size();

        if (!format_state->is_valid) {
            res_offsets.resize_fill(len, 0);
            null_map.resize_fill(len, true);
            return Status::OK();
        }
        res_offsets.resize(len);
        res_data.reserve(len * format.size + len);
        null_map.resize_fill(len, false);

        std::visit(
                [&](auto type) {
                    using Impl = decltype(type);
                    size_t offset = 0;
                    for (int i = 0; i < len; ++i) {
                        null_map[i] = Transform::template execute<Impl>(
                                ts[i], format, res_data, offset, context->state()->timezone_obj());
                        res_offsets[i] = cast_set<uint32_t>(offset);
                    }
                    res_data.resize(offset);
                },
                format_state->format_type);
        return Status::OK();
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
