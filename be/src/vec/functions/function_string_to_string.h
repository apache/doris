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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionStringToString.h
// and modified by Doris

#pragma once

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Impl, typename Name>
class FunctionStringToString : public IFunction {
public:
    static constexpr auto name = Name::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Impl>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionStringToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (!is_string_or_fixed_string(arguments[0])) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Illegal type {} of argument of function {}",
                                   arguments[0]->get_name(), get_name());
        }

        return arguments[0];
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Impl::get_variadic_argument_types();
        }
        return {};
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if (const auto* col = check_and_get_column<ColumnString>(column.get())) {
            auto col_res = ColumnString::create();
            static_cast<void>(Impl::vector(col->get_chars(), col->get_offsets(),
                                           col_res->get_chars(), col_res->get_offsets()));
            block.replace_by_position(result, std::move(col_res));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
