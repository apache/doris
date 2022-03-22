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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayIndex.h
// and modified by Doris
#pragma once

#include <string_view>

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

struct ArrayContainsAction
{
    using ResultType = UInt8;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t) noexcept { current = 1; }
};

struct ArrayPositionAction
{
    using ResultType = Int64;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t j) noexcept { current = j + 1; }
};

template <typename ConcreteAction, typename Name>
class FunctionArrayIndex : public IFunction
{
public:
    using ResultType = typename ConcreteAction::ResultType;

    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(WhichDataType(arguments[0]).is_array());
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return execute_non_nullable(block, arguments, result, input_rows_count);
    }

private:
    static bool execute_string(Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) {
        // check array nested column type and get data
        auto array_column = check_and_get_column<ColumnArray>(*block.get_by_position(arguments[0]).column);
        DCHECK(array_column != nullptr);
        auto nested_column = check_and_get_column<ColumnString>(array_column->get_data());
        if (!nested_column) {
            return false;
        }
        const auto& arr_offs = array_column->get_offsets();
        const auto& str_offs = nested_column->get_offsets();
        const auto& str_chars = nested_column->get_chars();

        // check right column type
        auto ptr = block.get_by_position(arguments[1]).column;
        if (is_column_const(*ptr)) {
            ptr = check_and_get_column<ColumnConst>(ptr)->get_data_column_ptr();
        }
        if (!check_and_get_column<ColumnString>(*ptr)) {
            return false;
        }

        // expand const column and get data
        auto right_column = check_and_get_column<ColumnString>(*block.get_by_position(arguments[1]).column->convert_to_full_column_if_const());
        const auto& right_offs = right_column->get_offsets();
        const auto& right_chars = right_column->get_chars();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(input_rows_count);

        // process
        for (size_t row = 0; row < input_rows_count; ++row) {
            ResultType res = 0;
            size_t off = arr_offs[row - 1];
            size_t len = arr_offs[row] - off;

            size_t right_off = right_offs[row - 1];
            size_t right_len = right_offs[row] - right_off;
            for (size_t pos = 0; pos < len; ++pos) {
                size_t str_pos = str_offs[pos + off - 1];
                size_t str_len = str_offs[pos + off] - str_pos;

                const char* left_raw_v = reinterpret_cast<const char*>(&str_chars[str_pos]);
                const char* right_raw_v = reinterpret_cast<const char*>(&right_chars[right_off]);
                if (std::string_view(left_raw_v, str_len) == std::string_view(right_raw_v, right_len)) {
                    ConcreteAction::apply(res, pos);
                    break;
                }
            }
            dst_data[row] = res;
        }
        block.replace_by_position(result, std::move(dst));
        return true;
    }

#define INTEGRAL_TPL_PACK UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64
    template <typename... Integral>
    static bool execute_integral(Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) {
        return (execute_integral_expanded<Integral, Integral...>(block, arguments, result, input_rows_count) || ...);
    }
    template <typename A, typename... Other>
    static bool execute_integral_expanded(Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) {
        return (execute_integral_impl<A, Other>(block, arguments, result, input_rows_count) || ...);
    }
    template <typename Initial, typename Resulting>
    static bool execute_integral_impl(Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) {
        // check array nested column type and get data
        auto array_column = check_and_get_column<ColumnArray>(*block.get_by_position(arguments[0]).column);
        DCHECK(array_column != nullptr);
        auto nested_column = check_and_get_column<ColumnVector<Initial>>(array_column->get_data());
        if (!nested_column) {
            return false;
        }
        const auto& offsets = array_column->get_offsets();
        const auto& nested_data = nested_column->get_data();

        // check right column type
        auto ptr = block.get_by_position(arguments[1]).column;
        if (is_column_const(*ptr)) {
            ptr = check_and_get_column<ColumnConst>(ptr)->get_data_column_ptr();
        }
        if (!check_and_get_column<ColumnVector<Resulting>>(*ptr)) {
            return false;
        }

        // expand const column and get data
        auto right_column = block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto& right_data = check_and_get_column<ColumnVector<Resulting>>(*right_column)->get_data();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(input_rows_count);

        // process
        for (size_t row = 0; row < input_rows_count; ++row) {
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            for (size_t pos = 0; pos < len; ++pos) {
                if (nested_data[pos + off] == right_data[row]) {
                    ConcreteAction::apply(res, pos);
                    break;
                }
            }
            dst_data[row] = res;
        }
        block.replace_by_position(result, std::move(dst));
        return true;
    }

    Status execute_non_nullable(Block& block, const ColumnNumbers& arguments, size_t result, size_t input_rows_count) {
        WhichDataType right_type(block.get_by_position(arguments[1]).type);
        if ((right_type.is_string() && execute_string(block, arguments, result, input_rows_count)) ||
                execute_integral<INTEGRAL_TPL_PACK>(block, arguments, result, input_rows_count)) {
            return Status::OK();
        }
        return Status::OK();
    }
#undef INTEGRAL_TPL_PACK
};

} // namespace doris::vectorized
