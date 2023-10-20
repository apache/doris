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

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

struct ArrayContainsAction {
    using ResultType = UInt8;
    static constexpr auto name = "array_contains";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t) noexcept { current = 1; }
};

struct ArrayPositionAction {
    using ResultType = Int64;
    static constexpr auto name = "array_position";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t j) noexcept { current = j + 1; }
};

struct ArrayCountEqual {
    using ResultType = Int64;
    static constexpr auto name = "countequal";
    static constexpr const bool resume_execution = true;
    static constexpr void apply(ResultType& current, size_t j) noexcept { ++current; }
};

template <typename ConcreteAction, bool OldVersion = false>
class FunctionArrayIndex : public IFunction {
public:
    using ResultType = typename ConcreteAction::ResultType;

    static constexpr auto name = ConcreteAction::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if constexpr (OldVersion) {
            return make_nullable(std::make_shared<DataTypeNumber<ResultType>>());
        } else {
            if (arguments[0]->is_nullable()) {
                return make_nullable(std::make_shared<DataTypeNumber<ResultType>>());
            } else {
                return std::make_shared<DataTypeNumber<ResultType>>();
            }
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return _execute_dispatch(block, arguments, result, input_rows_count);
    }

private:
    ColumnPtr _execute_string(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column,
                              const UInt8* right_nested_null_map,
                              const UInt8* outer_null_map) const {
        // check array nested column type and get data
        const auto& str_offs = reinterpret_cast<const ColumnString&>(nested_column).get_offsets();
        const auto& str_chars = reinterpret_cast<const ColumnString&>(nested_column).get_chars();

        // check right column type and get data
        const auto& right_offs = reinterpret_cast<const ColumnString&>(right_column).get_offsets();
        const auto& right_chars = reinterpret_cast<const ColumnString&>(right_column).get_chars();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(offsets.size());
        auto& dst_null_data = dst_null_column->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            if (outer_null_map && outer_null_map[row]) {
                dst_null_data[row] = true;
                continue;
            }
            dst_null_data[row] = false;
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;

            size_t right_off = right_offs[row - 1];
            size_t right_len = right_offs[row] - right_off;
            for (size_t pos = 0; pos < len; ++pos) {
                // match null value
                if (right_nested_null_map && right_nested_null_map[row] && nested_null_map &&
                    nested_null_map[pos + off]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
                // some is null while another is not
                if (right_nested_null_map && nested_null_map &&
                    right_nested_null_map[row] != nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }
                size_t str_pos = str_offs[pos + off - 1];
                size_t str_len = str_offs[pos + off] - str_pos;
                const char* left_raw_v = reinterpret_cast<const char*>(&str_chars[str_pos]);
                const char* right_raw_v = reinterpret_cast<const char*>(&right_chars[right_off]);
                // StringRef operator == using vec impl
                if (StringRef(left_raw_v, str_len) == StringRef(right_raw_v, right_len)) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
            }
            dst_data[row] = res;
        }
        if constexpr (OldVersion) {
            return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
        } else {
            if (outer_null_map == nullptr) {
                return dst;
            }
            return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
        }
    }

    template <typename NestedColumnType, typename RightColumnType>
    ColumnPtr _execute_number(const ColumnArray::Offsets64& offsets, const UInt8* nested_null_map,
                              const IColumn& nested_column, const IColumn& right_column,
                              const UInt8* right_nested_null_map,
                              const UInt8* outer_null_map) const {
        // check array nested column type and get data
        const auto& nested_data =
                reinterpret_cast<const NestedColumnType&>(nested_column).get_data();

        // check right column type and get data
        const auto& right_data = reinterpret_cast<const RightColumnType&>(right_column).get_data();

        // prepare return data
        auto dst = ColumnVector<ResultType>::create(offsets.size());
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(offsets.size());
        auto& dst_null_data = dst_null_column->get_data();

        // process
        for (size_t row = 0; row < offsets.size(); ++row) {
            if (outer_null_map && outer_null_map[row]) {
                dst_null_data[row] = true;
                continue;
            }
            dst_null_data[row] = false;
            ResultType res = 0;
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            for (size_t pos = 0; pos < len; ++pos) {
                // match null value
                if (right_nested_null_map && right_nested_null_map[row] && nested_null_map &&
                    nested_null_map[pos + off]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
                // some is null while another is not
                if (right_nested_null_map && nested_null_map &&
                    right_nested_null_map[row] != nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_null_map && nested_null_map[pos + off]) {
                    continue;
                }
                if (nested_data[pos + off] == right_data[row]) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
            }
            dst_data[row] = res;
        }
        if constexpr (OldVersion) {
            return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
        } else {
            if (outer_null_map == nullptr) {
                return dst;
            }
            return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
        }
    }

    template <typename NestedColumnType>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets64& offsets,
                                       const UInt8* nested_null_map, const IColumn& nested_column,
                                       const IColumn& right_column,
                                       const UInt8* right_nested_null_map,
                                       const UInt8* outer_null_map) const {
        if (check_column<NestedColumnType>(right_column)) {
            return _execute_number<NestedColumnType, NestedColumnType>(
                    offsets, nested_null_map, nested_column, right_column, right_nested_null_map,
                    outer_null_map);
        }
        return nullptr;
    }

    Status _execute_dispatch(Block& block, const ColumnNumbers& arguments, size_t result,
                             size_t input_rows_count) const {
        // extract array offsets and nested data
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (!is_array(remove_nullable(block.get_by_position(arguments[0]).type))) {
            return Status::InvalidArgument(get_name() + " first argument must be array, but got " +
                                           block.get_by_position(arguments[0]).type->get_name());
        }
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (left_column->is_nullable()) {
            auto nullable_array = reinterpret_cast<const ColumnNullable*>(left_column.get());
            array_column =
                    reinterpret_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = reinterpret_cast<const ColumnArray*>(left_column.get());
        }
        const auto& offsets = array_column->get_offsets();
        const UInt8* nested_null_map = nullptr;
        ColumnPtr nested_column = nullptr;
        if (array_column->get_data().is_nullable()) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column->get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            nested_column = array_column->get_data_ptr();
        }

        // get right column
        ColumnPtr right_full_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr right_column = right_full_column;
        const UInt8* right_nested_null_map = nullptr;
        if (right_column->is_nullable()) {
            const auto& nested_null_column = assert_cast<const ColumnNullable&>(*right_full_column);
            right_column = nested_null_column.get_nested_column_ptr();
            right_nested_null_map = nested_null_column.get_null_map_column().get_data().data();
        }
        // execute
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        auto right_type = remove_nullable(block.get_by_position(arguments[1]).type);

        ColumnPtr return_column = nullptr;
        WhichDataType left_which_type(left_element_type);

        if (is_string(right_type) && is_string(left_element_type)) {
            return_column = _execute_string(offsets, nested_null_map, *nested_column, *right_column,
                                            right_nested_null_map, array_null_map);
        } else if (is_number(right_type) && is_number(left_element_type)) {
            if (left_which_type.is_uint8()) {
                return_column = _execute_number_expanded<ColumnUInt8>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int8()) {
                return_column = _execute_number_expanded<ColumnInt8>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int16()) {
                return_column = _execute_number_expanded<ColumnInt16>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int32()) {
                return_column = _execute_number_expanded<ColumnInt32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int64()) {
                return_column = _execute_number_expanded<ColumnInt64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_int128()) {
                return_column = _execute_number_expanded<ColumnInt128>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_float32()) {
                return_column = _execute_number_expanded<ColumnFloat32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_float64()) {
                return_column = _execute_number_expanded<ColumnFloat64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal32()) {
                return_column = _execute_number_expanded<ColumnDecimal32>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal64()) {
                return_column = _execute_number_expanded<ColumnDecimal64>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal128i()) {
                return_column = _execute_number_expanded<ColumnDecimal128I>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_decimal128()) {
                return_column = _execute_number_expanded<ColumnDecimal128>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            }
        } else if ((is_date_or_datetime(right_type) || is_date_v2_or_datetime_v2(right_type)) &&
                   (is_date_or_datetime(left_element_type) ||
                    is_date_v2_or_datetime_v2(left_element_type))) {
            if (left_which_type.is_date()) {
                return_column = _execute_number_expanded<ColumnDate>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_time()) {
                return_column = _execute_number_expanded<ColumnDateTime>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_v2()) {
                return_column = _execute_number_expanded<ColumnDateV2>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
            } else if (left_which_type.is_date_time_v2()) {
                return_column = _execute_number_expanded<ColumnDateTimeV2>(
                        offsets, nested_null_map, *nested_column, *right_column,
                        right_nested_null_map, array_null_map);
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
