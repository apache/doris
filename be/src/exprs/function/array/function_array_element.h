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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayElement.cpp
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

class FunctionArrayElement : public IFunction {
public:
    using MapIndiceDataType = DataTypeInt64;

    static constexpr auto name = "element_at";
    static FunctionPtr create() { return std::make_shared<FunctionArrayElement>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr arg_0 = remove_nullable(arguments[0]);
        DCHECK(arg_0->get_primitive_type() == TYPE_ARRAY || arg_0->get_primitive_type() == TYPE_MAP)
                << "first argument for function: " << name
                << " should be DataTypeArray or DataTypeMap, but it is " << arg_0->get_name();
        if (arg_0->get_primitive_type() == TYPE_ARRAY) {
            DCHECK(is_int_or_bool(arguments[1]->get_primitive_type()))
                    << "second argument for function: " << name
                    << " should be Integer for array element";
            return make_nullable(
                    check_and_get_data_type<DataTypeArray>(arg_0.get())->get_nested_type());
        } else if (arg_0->get_primitive_type() == TYPE_MAP) {
            return make_nullable(
                    check_and_get_data_type<DataTypeMap>(arg_0.get())->get_value_type());
        } else {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    fmt::format("element_at only support array and map so far, but got {}",
                                arg_0->get_name()));
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto dst_null_column = ColumnUInt8::create(input_rows_count, 0);
        UInt8* dst_null_map = dst_null_column->get_data().data();
        const UInt8* src_null_map = nullptr;
        ColumnsWithTypeAndName args;
        block.replace_by_position(
                arguments[0],
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const());
        auto col_left = block.get_by_position(arguments[0]);
        if (col_left.column->is_nullable()) {
            auto null_col = check_and_get_column<ColumnNullable>(*col_left.column);
            src_null_map = null_col->get_null_map_column().get_data().data();
            args = {{null_col->get_nested_column_ptr(), remove_nullable(col_left.type),
                     col_left.name},
                    block.get_by_position(arguments[1])};
        } else {
            args = {col_left, block.get_by_position(arguments[1])};
        }
        ColumnPtr res_column = nullptr;
        if (is_column<ColumnMap>(args[0].column.get()) ||
            check_column_const<ColumnMap>(args[0].column.get())) {
            res_column = _execute_map(args, input_rows_count, src_null_map, dst_null_map);
        } else {
            res_column = _execute_nullable(args, input_rows_count, src_null_map, dst_null_map);
        }
        if (!res_column) {
            return Status::RuntimeError("unsupported types for function {}({}, {})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name(),
                                        block.get_by_position(arguments[1]).type->get_name());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(res_column, std::move(dst_null_column)));
        return Status::OK();
    }

private:
    //=========================== map element===========================//
    ColumnPtr _get_mapped_idx(const ColumnArray& column,
                              const ColumnWithTypeAndName& argument) const {
        auto right_column = make_nullable(argument.column->convert_to_full_column_if_const());
        const ColumnArray::Offsets64& offsets = column.get_offsets();
        ColumnPtr nested_ptr = make_nullable(column.get_data_ptr());
        size_t rows = offsets.size();
        // prepare return data
        auto matched_indices = ColumnVector<MapIndiceDataType::PType>::create();
        matched_indices->reserve(rows);

        for (size_t i = 0; i < rows; i++) {
            bool matched = false;
            size_t begin = offsets[i - 1];
            size_t end = offsets[i];
            for (size_t j = begin; j < end; j++) {
                if (nested_ptr->compare_at(j, i, *right_column, -1) == 0) {
                    matched_indices->insert_value(
                            cast_set<MapIndiceDataType::FieldType, size_t, false>(j - begin + 1));
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                matched_indices->insert_value(cast_set<MapIndiceDataType::FieldType, size_t, false>(
                        end - begin + 1)); // make indices for null
            }
        }

        return matched_indices;
    }

    template <typename ColumnType, typename IndexColumnType>
    ColumnPtr _execute_number(const ColumnArray::Offsets64& offsets, const IColumn& nested_column,
                              const UInt8* arr_null_map, const IColumn& indices,
                              const UInt8* nested_null_map, UInt8* dst_null_map,
                              const UInt8* idx_null_map, bool is_const_index) const {
        const auto& nested_data = reinterpret_cast<const ColumnType&>(nested_column).get_data();
        const auto& index_data = assert_cast<const IndexColumnType&>(indices).get_data();

        auto dst_column = nested_column.clone_empty();
        auto& dst_data = reinterpret_cast<ColumnType&>(*dst_column).get_data();
        dst_data.resize(offsets.size());

        for (size_t row = 0; row < offsets.size(); ++row) {
            size_t off = row == 0 ? 0 : offsets[row - 1];
            size_t len = offsets[row] - off;
            size_t idx = index_check_const(row, is_const_index);
            auto index =
                    (idx_null_map && idx_null_map[idx]) ? 0 : static_cast<Int64>(index_data[idx]);
            bool null_flag = bool(arr_null_map && arr_null_map[row]);
            if (!null_flag && index > 0 && index <= len) {
                index += off - 1;
            } else if (!null_flag && index < 0 && -index <= len) {
                index += off + len;
            } else {
                null_flag = true;
            }
            if (!null_flag && nested_null_map && nested_null_map[index]) {
                null_flag = true;
            }
            dst_null_map[row] = null_flag;
            dst_data[row] = !null_flag ? nested_data[index] : typename ColumnType::value_type();
        }
        return dst_column;
    }

    template <typename IndexColumnType>
    ColumnPtr _execute_string(const ColumnArray::Offsets64& offsets, const IColumn& nested_column,
                              const UInt8* arr_null_map, const IColumn& indices,
                              const UInt8* nested_null_map, UInt8* dst_null_map,
                              const UInt8* idx_null_map, bool is_const_index) const {
        const auto& src_str_offs =
                reinterpret_cast<const ColumnString&>(nested_column).get_offsets();
        const auto& src_str_chars =
                reinterpret_cast<const ColumnString&>(nested_column).get_chars();
        const auto& index_data = assert_cast<const IndexColumnType&>(indices).get_data();

        // prepare return data
        auto dst_column = ColumnString::create();
        auto& dst_str_offs = dst_column->get_offsets();
        dst_str_offs.resize(offsets.size());
        auto& dst_str_chars = dst_column->get_chars();
        dst_str_chars.reserve(src_str_chars.size());

        for (size_t row = 0; row < offsets.size(); ++row) {
            size_t off = row == 0 ? 0 : offsets[row - 1];
            size_t len = offsets[row] - off;
            size_t idx = index_check_const(row, is_const_index);
            auto index =
                    (idx_null_map && idx_null_map[idx]) ? 0 : static_cast<Int64>(index_data[idx]);
            bool null_flag = bool(arr_null_map && arr_null_map[row]);
            if (!null_flag && index > 0 && index <= len) {
                index += off - 1;
            } else if (!null_flag && index < 0 && -index <= len) {
                index += off + len;
            } else {
                null_flag = true;
            }
            if (!null_flag && nested_null_map && nested_null_map[index]) {
                null_flag = true;
            }
            if (!null_flag) {
                dst_null_map[row] = false;
                auto element_size = src_str_offs[index] - src_str_offs[index - 1];
                dst_str_offs[row] = (row == 0 ? 0 : dst_str_offs[row - 1]) + element_size;
                auto src_string_pos = src_str_offs[index - 1];
                auto dst_string_pos = row == 0 ? 0 : dst_str_offs[row - 1];
                dst_str_chars.resize(dst_string_pos + element_size);
                memcpy(&dst_str_chars[dst_string_pos], &src_str_chars[src_string_pos],
                       element_size);
            } else {
                dst_null_map[row] = true;
                dst_str_offs[row] = row == 0 ? 0 : dst_str_offs[row - 1];
            }
        }
        return dst_column;
    }

    ColumnPtr _execute_map(const ColumnsWithTypeAndName& arguments, size_t input_rows_count,
                           const UInt8* src_null_map, UInt8* dst_null_map) const {
        auto left_column = arguments[0].column->convert_to_full_column_if_const();
        DataTypePtr val_type =
                reinterpret_cast<const DataTypeMap&>(*arguments[0].type).get_value_type();
        const auto& map_column = reinterpret_cast<const ColumnMap&>(*left_column);

        // create column array to find keys
        auto key_arr = ColumnArray::create(map_column.get_keys_ptr(), map_column.get_offsets_ptr());
        auto val_arr =
                ColumnArray::create(map_column.get_values_ptr(), map_column.get_offsets_ptr());

        ColumnPtr matched_indices = _get_mapped_idx(*key_arr, arguments[1]);
        if (!matched_indices) {
            return nullptr;
        }
        DataTypePtr indices_type(std::make_shared<MapIndiceDataType>());
        ColumnWithTypeAndName indices(matched_indices, indices_type, "indices");
        ColumnWithTypeAndName data(std::move(val_arr), std::make_shared<DataTypeArray>(val_type),
                                   "value");
        ColumnsWithTypeAndName args = {data, indices};
        return _execute_nullable(args, input_rows_count, src_null_map, dst_null_map);
    }

    template <typename IndexColumnType>
    ColumnPtr _execute_common(const ColumnArray::Offsets64& offsets, const IColumn& nested_column,
                              const UInt8* arr_null_map, const IColumn& indices,
                              const UInt8* nested_null_map, UInt8* dst_null_map,
                              const UInt8* idx_null_map, bool is_const_index) const {
        const auto& index_data = assert_cast<const IndexColumnType&>(indices).get_data();

        auto dst_column = nested_column.clone_empty();
        dst_column->reserve(offsets.size());

        for (size_t row = 0; row < offsets.size(); ++row) {
            size_t off = row == 0 ? 0 : offsets[row - 1];
            size_t len = offsets[row] - off;
            size_t idx = index_check_const(row, is_const_index);
            auto index =
                    (idx_null_map && idx_null_map[idx]) ? 0 : static_cast<Int64>(index_data[idx]);
            bool null_flag = bool(arr_null_map && arr_null_map[row]);
            if (!null_flag && index > 0 && index <= len) {
                index += off - 1;
            } else if (!null_flag && index < 0 && -index <= len) {
                index += off + len;
            } else {
                null_flag = true;
            }
            if (!null_flag && nested_null_map && nested_null_map[index]) {
                null_flag = true;
            }
            if (!null_flag) {
                dst_null_map[row] = false;
                dst_column->insert_from(nested_column, index);
            } else {
                dst_null_map[row] = true;
                dst_column->insert_default();
            }
        }
        return dst_column;
    }

    ColumnPtr _execute_nullable(const ColumnsWithTypeAndName& arguments, size_t input_rows_count,
                                const UInt8* src_null_map, UInt8* dst_null_map) const {
        // check array nested column type and get data
        auto left_column = arguments[0].column->convert_to_full_column_if_const();
        const auto& array_column = assert_cast<const ColumnArray&>(*left_column);
        const auto& offsets = array_column.get_offsets();
        DCHECK(offsets.size() == input_rows_count);
        const UInt8* nested_null_map = nullptr;
        ColumnPtr nested_column = nullptr;
        if (is_column_nullable(array_column.get_data())) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            nested_null_map = nested_null_column.get_null_map_column().get_data().data();
            nested_column = nested_null_column.get_nested_column_ptr();
        } else {
            nested_column = array_column.get_data_ptr();
        }

        ColumnPtr res = nullptr;
        auto left_element_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*remove_nullable(arguments[0].type))
                        .get_nested_type());
        const UInt8* idx_null_map = nullptr;
        auto idx_col_with_const = unpack_if_const(arguments[1].column);
        if (idx_col_with_const.first->is_nullable()) {
            const auto& idx_null_column =
                    reinterpret_cast<const ColumnNullable&>(*idx_col_with_const.first);
            idx_null_map = idx_null_column.get_null_map_column().get_data().data();
        }
        auto idx_col_raw = remove_nullable(idx_col_with_const.first);
        bool is_const_index = idx_col_with_const.second;

        PrimitiveType idx_ptype = remove_nullable(arguments[1].type)->get_primitive_type();

        // Outer dispatch on index column type (Int8/Int16/Int32/Int64),
        // inner dispatch on nested data column type.
        auto idx_dispatch = [&](const auto& idx_type) -> bool {
            using IdxDispatchType = std::decay_t<decltype(idx_type)>;
            using IndexColumnType = typename IdxDispatchType::ColumnType;

            auto data_call = [&](const auto& data_type) -> bool {
                using DataDispatchType = std::decay_t<decltype(data_type)>;
                res = _execute_number<typename DataDispatchType::ColumnType, IndexColumnType>(
                        offsets, *nested_column, src_null_map, *idx_col_raw, nested_null_map,
                        dst_null_map, idx_null_map, is_const_index);
                return true;
            };

            if (is_string_type(left_element_type->get_primitive_type())) {
                res = _execute_string<IndexColumnType>(offsets, *nested_column, src_null_map,
                                                       *idx_col_raw, nested_null_map, dst_null_map,
                                                       idx_null_map, is_const_index);
            } else if (!dispatch_switch_scalar(left_element_type->get_primitive_type(),
                                               data_call)) {
                res = _execute_common<IndexColumnType>(offsets, *nested_column, src_null_map,
                                                       *idx_col_raw, nested_null_map, dst_null_map,
                                                       idx_null_map, is_const_index);
            }
            return true;
        };

        bool dispatched = dispatch_switch_int(idx_ptype, idx_dispatch);
        DCHECK(dispatched) << "Unsupported index column type for element_at: " << idx_ptype;
        return res;
    }
};

} // namespace doris
