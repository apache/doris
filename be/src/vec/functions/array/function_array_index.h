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
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

struct ArrayContainsAction {
    using ResultType = UInt8;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t) noexcept { current = 1; }
};

struct ArrayPositionAction {
    using ResultType = Int64;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t j) noexcept { current = j + 1; }
};

struct ArrayCountEqual {
    using ResultType = Int64;
    static constexpr const bool resume_execution = true;
    static constexpr void apply(ResultType& current, size_t j) noexcept { ++current; }
};

template <typename ConcreteAction, typename Name>
class FunctionArrayIndex : public IFunction {
public:
    using ResultType = typename ConcreteAction::ResultType;

    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]));
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return _execute_non_nullable(block, arguments, result, input_rows_count);
    }

private:
    ColumnPtr _execute_string(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column) {
        // check array nested column type and get data
        const auto& str_offs = reinterpret_cast<const ColumnString&>(nested_column).get_offsets();
        const auto& str_chars = reinterpret_cast<const ColumnString&>(nested_column).get_chars();

        // check right column type and get data
        const auto& right_offs = reinterpret_cast<const ColumnString&>(right_column).get_offsets();
        const auto& right_chars = reinterpret_cast<const ColumnString&>(right_column).get_chars();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;

            size_t right_off = right_offs[row - 1];
            size_t right_len = right_offs[row] - right_off;
            for (size_t pos = 0; pos < len; ++pos) {
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }

                size_t str_pos = str_offs[pos + off - 1];
                size_t str_len = str_offs[pos + off] - str_pos;
                const char* left_raw_v = reinterpret_cast<const char*>(&str_chars[str_pos]);
                const char* right_raw_v = reinterpret_cast<const char*>(&right_chars[right_off]);
                if (std::string_view(left_raw_v, str_len) ==
                    std::string_view(right_raw_v, right_len)) {
                    ConcreteAction::apply(res, pos);
                    break;
                }
            }
            dst_data[row] = res;
        }
        return dst;
    }

    template <typename NestedColumnType, typename RightColumnType>
    ColumnPtr _execute_number(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column) {
        // check array nested column type and get data
        const auto& nested_data =
                reinterpret_cast<const NestedColumnType&>(nested_column).get_data();

        // check right column type and get data
        const auto& right_data = reinterpret_cast<const RightColumnType&>(right_column).get_data();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            for (size_t pos = 0; pos < len; ++pos) {
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }

                if (nested_data[pos + off] == right_data[row]) {
                    ConcreteAction::apply(res, pos);
                    break;
                }
            }
            dst_data[row] = res;
        }
        return dst;
    }

    template <typename NestedColumnType>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets64& offsets,
                                       const UInt8* nested_null_map, const IColumn& nested_column,
                                       const IColumn& right_column) {
        if (check_column<ColumnUInt8>(right_column)) {
            return _execute_number<NestedColumnType, ColumnUInt8>(offsets, nested_null_map,
                                                                  nested_column, right_column);
        } else if (check_column<ColumnInt8>(right_column)) {
            return _execute_number<NestedColumnType, ColumnInt8>(offsets, nested_null_map,
                                                                 nested_column, right_column);
        } else if (check_column<ColumnInt16>(right_column)) {
            return _execute_number<NestedColumnType, ColumnInt16>(offsets, nested_null_map,
                                                                  nested_column, right_column);
        } else if (check_column<ColumnInt32>(right_column)) {
            return _execute_number<NestedColumnType, ColumnInt32>(offsets, nested_null_map,
                                                                  nested_column, right_column);
        } else if (check_column<ColumnInt64>(right_column)) {
            return _execute_number<NestedColumnType, ColumnInt64>(offsets, nested_null_map,
                                                                  nested_column, right_column);
        } else if (check_column<ColumnInt128>(right_column)) {
            return _execute_number<NestedColumnType, ColumnInt128>(offsets, nested_null_map,
                                                                   nested_column, right_column);
        } else if (check_column<ColumnFloat32>(right_column)) {
            return _execute_number<NestedColumnType, ColumnFloat32>(offsets, nested_null_map,
                                                                    nested_column, right_column);
        } else if (check_column<ColumnFloat64>(right_column)) {
            return _execute_number<NestedColumnType, ColumnFloat64>(offsets, nested_null_map,
                                                                    nested_column, right_column);
        } else if (right_column.is_date_type()) {
            return _execute_number<NestedColumnType, ColumnDate>(offsets, nested_null_map,
                                                                 nested_column, right_column);
        } else if (right_column.is_datetime_type()) {
            return _execute_number<NestedColumnType, ColumnDateTime>(offsets, nested_null_map,
                                                                     nested_column, right_column);
        } else if (check_column<ColumnDecimal128>(right_column)) {
            return _execute_number<NestedColumnType, ColumnDecimal128>(offsets, nested_null_map,
                                                                       nested_column, right_column);
        }
        return nullptr;
    }

    Status _execute_non_nullable(Block& block, const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) {
        // extract array offsets and nested data
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& array_column = reinterpret_cast<const ColumnArray&>(*left_column);
        const auto& offsets = array_column.get_offsets();
        const UInt8* nested_null_map = nullptr;
        ColumnPtr nested_column = nullptr;
        if (array_column.get_data().is_nullable()) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            nested_column = array_column.get_data_ptr();
        }

        // get right column
        auto right_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        // execute
        auto left_element_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*block.get_by_position(arguments[0]).type)
                        .get_nested_type());
        auto right_type = remove_nullable(block.get_by_position(arguments[1]).type);

        ColumnPtr return_column = nullptr;
        if (is_string(right_type) && is_string(left_element_type)) {
            return_column =
                    _execute_string(offsets, nested_null_map, *nested_column, *right_column);
        } else if (is_number(right_type) && is_number(left_element_type)) {
            if (check_column<ColumnUInt8>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnUInt8>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnInt8>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnInt8>(offsets, nested_null_map,
                                                                     *nested_column, *right_column);
            } else if (check_column<ColumnInt16>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnInt16>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnInt32>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnInt32>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnInt64>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnInt64>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnInt128>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnInt128>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnFloat32>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnFloat32>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnFloat64>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnFloat64>(
                        offsets, nested_null_map, *nested_column, *right_column);
            } else if (check_column<ColumnDecimal128>(*nested_column)) {
                return_column = _execute_number_expanded<ColumnDecimal128>(
                        offsets, nested_null_map, *nested_column, *right_column);
            }
        } else if ((is_date_or_datetime(right_type) || is_date_v2_or_datetime_v2(right_type)) &&
                   (is_date_or_datetime(left_element_type) ||
                    is_date_v2_or_datetime_v2(left_element_type))) {
            if (nested_column->is_date_type()) {
                return_column = _execute_number_expanded<ColumnDate>(offsets, nested_null_map,
                                                                     *nested_column, *right_column);
            } else if (nested_column->is_datetime_type()) {
                return_column = _execute_number_expanded<ColumnDateTime>(
                        offsets, nested_null_map, *nested_column, *right_column);
            }
        }

        if (return_column) {
            block.replace_by_position(result, std::move(return_column));
            return Status::OK();
        }
        return Status::RuntimeError("execute failed or unsupported types for function {}({}, {})",
                                    get_name(),
                                    block.get_by_position(arguments[0]).type->get_name(),
                                    block.get_by_position(arguments[1]).type->get_name());
    }
};

} // namespace doris::vectorized
