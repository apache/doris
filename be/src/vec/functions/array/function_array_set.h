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

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

enum class SetOperation { UNION, EXCEPT, INTERSECT };

template <typename ColumnType>
struct UnionAction;

template <typename ColumnType>
struct ExceptAction;

template <typename ColumnType>
struct IntersectAction;

template <typename ColumnType, SetOperation operation>
struct ActionImpl;

template <typename ColumnType>
struct ActionImpl<ColumnType, SetOperation::UNION> {
    using Action = UnionAction<ColumnType>;
    static constexpr auto apply_left_first = true;
};

template <typename ColumnType>
struct ActionImpl<ColumnType, SetOperation::EXCEPT> {
    using Action = ExceptAction<ColumnType>;
    static constexpr auto apply_left_first = false;
};

template <typename ColumnType>
struct ActionImpl<ColumnType, SetOperation::INTERSECT> {
    using Action = IntersectAction<ColumnType>;
    static constexpr auto apply_left_first = false;
};

template <SetOperation operation, typename ColumnType>
struct OpenSetImpl {
    using Action = typename ActionImpl<ColumnType, operation>::Action;
    Action action;
    void apply_left(const ColumnArrayExecutionData& src, size_t off, size_t len,
                    ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_data = assert_cast<const ColumnType&>(*src.nested_col).get_data();
        auto& dst_data = assert_cast<ColumnType&>(*dst.nested_col).get_data();
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null_left()) {
                    dst_data.push_back(typename ColumnType::value_type());
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply_left(&src_data[i])) {
                    dst_data.push_back(src_data[i]);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }

    void apply_right(const ColumnArrayExecutionData& src, size_t off, size_t len,
                    ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_data = assert_cast<const ColumnType&>(*src.nested_col).get_data();
        auto& dst_data = assert_cast<ColumnType&>(*dst.nested_col).get_data();
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null_right()) {
                    dst_data.push_back(typename ColumnType::value_type());
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply_right(&src_data[i])) {
                    dst_data.push_back(src_data[i]);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }
};

template <SetOperation operation>
struct OpenSetImpl<operation, ColumnString> {
    using Action = typename ActionImpl<ColumnString, operation>::Action;
    Action action;
    void apply_left(const ColumnArrayExecutionData& src, size_t off, size_t len,
                    ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_column = assert_cast<const ColumnString&>(*src.nested_col);
        auto& dst_column = assert_cast<ColumnString&>(*dst.nested_col);
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null_left()) {
                    dst_column.insert_default();
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply_left(src_column.get_data_at(i))) {
                    dst_column.insert_from(src_column, i);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }

    void apply_right(const ColumnArrayExecutionData& src, size_t off, size_t len,
                    ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_column = assert_cast<const ColumnString&>(*src.nested_col);
        auto& dst_column = assert_cast<ColumnString&>(*dst.nested_col);
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null_right()) {
                    dst_column.insert_default();
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply_right(src_column.get_data_at(i))) {
                    dst_column.insert_from(src_column, i);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }
};

template <SetOperation operation>
struct ArraySetImpl {
public:
    static DataTypePtr get_return_type(const DataTypes& arguments) {
        const DataTypeArray* array_left =
                check_and_get_data_type<DataTypeArray>(arguments[0].get());
        // if any nested type of array arguments is nullable then return array with
        // nullable nested type.
        if (array_left->get_nested_type()->is_nullable()) {
            return arguments[0];
        }
        return arguments[1];
    }

    static Status execute(ColumnPtr& res_ptr, const ColumnArrayExecutionData& left_data,
                          const ColumnArrayExecutionData& right_data) {
        MutableColumnPtr array_nested_column = nullptr;
        ColumnArrayMutableData dst;
        if (left_data.nested_nullmap_data || right_data.nested_nullmap_data) {
            array_nested_column =
                    ColumnNullable::create(left_data.nested_col->clone_empty(), ColumnUInt8::create());
            dst.nullable_col = reinterpret_cast<ColumnNullable*>(array_nested_column.get());
            dst.nested_nullmap_data = &dst.nullable_col->get_null_map_data();
            dst.nested_col = dst.nullable_col->get_nested_column_ptr().get();
        } else {
            array_nested_column = left_data.nested_col->clone_empty();
            dst.nested_col = array_nested_column.get();
        }
        auto dst_offsets_column = ColumnArray::ColumnOffsets::create();
        dst.offsets_col = dst_offsets_column.get();
        dst.offsets_ptr = &dst_offsets_column->get_data();

        ColumnPtr res_column;
        if (_execute_expand<ColumnString>(dst, left_data, right_data) ||
            _execute_expand<ColumnDate>(dst, left_data, right_data) ||
            _execute_expand<ColumnDateTime>(dst, left_data, right_data) ||
            _execute_expand<ColumnUInt8>(dst, left_data, right_data) ||
            _execute_expand<ColumnInt8>(dst, left_data, right_data) ||
            _execute_expand<ColumnInt16>(dst, left_data, right_data) ||
            _execute_expand<ColumnInt32>(dst, left_data, right_data) ||
            _execute_expand<ColumnInt64>(dst, left_data, right_data) ||
            _execute_expand<ColumnInt128>(dst, left_data, right_data) ||
            _execute_expand<ColumnFloat32>(dst, left_data, right_data) ||
            _execute_expand<ColumnFloat64>(dst, left_data, right_data) ||
            _execute_expand<ColumnDecimal128>(dst, left_data, right_data)) {
            res_column = assemble_column_array(dst);
            if (res_column) {
                res_ptr = std::move(res_column);
                return Status::OK();
            }
        }
        return Status::RuntimeError("Unexpected columns: {}, {}", left_data.nested_col->get_name(),
                                    right_data.nested_col->get_name());
    }

private:
    template <typename ColumnType>
    static Status _execute_internal(ColumnArrayMutableData& dst, const ColumnArrayExecutionData& left_data,
                                    const ColumnArrayExecutionData& right_data) {
        using Impl = OpenSetImpl<operation, ColumnType>;
        static constexpr auto apply_left_first = Impl::Action::apply_left_first;
        size_t current = 0;
        for (size_t row = 0; row < left_data.offsets_ptr->size(); ++row) {
            size_t count = 0;
            size_t left_off = (*left_data.offsets_ptr)[row - 1];
            size_t left_len = (*left_data.offsets_ptr)[row] - left_off;
            size_t right_off = (*right_data.offsets_ptr)[row - 1];
            size_t right_len = (*right_data.offsets_ptr)[row] - right_off;
            Impl impl;
            if (apply_left_first) {
                impl.apply_left(left_data, left_off, left_len, dst, &count);
                impl.apply_right(right_data, right_off, right_len, dst, &count);
            } else {
                impl.apply_right(right_data, right_off, right_len, dst, &count);
                impl.apply_left(left_data, left_off, left_len, dst, &count);
            }
            current += count;
            dst.offsets_ptr->push_back(current);
        }
        return Status::OK();
    }

    template <typename ColumnType>
    static bool _execute_expand(ColumnArrayMutableData& dst, const ColumnArrayExecutionData& left_data,
                                const ColumnArrayExecutionData& right_data) {
        if (!check_column<ColumnType>(*left_data.nested_col)) {
            return false;
        }
        _execute_internal<ColumnType>(dst, left_data, right_data);
        return true;
    }
};

} // namespace doris::vectorized