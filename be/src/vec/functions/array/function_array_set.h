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

#include <type_traits>

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

enum class SetOperation { UNION, EXCEPT };

template <typename Set, typename Element>
struct UnionAction;

template <typename Set, typename Element>
struct ExceptAction;

template <typename Set, typename Element, SetOperation operation>
struct ActionImpl;

template <typename Set, typename Element>
struct ActionImpl<Set, Element, SetOperation::EXCEPT> {
    using Action = ExceptAction<Set, Element>;
};

template <SetOperation operation, typename ColumnType>
struct OpenSetImpl {
    using Element = typename ColumnType::value_type;
    using ElementNativeType = typename NativeType<Element>::Type;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    using Action = typename ActionImpl<Set, Element, operation>::Action;
    Action action;
    Set set;
    Set result_set;
    void reset() {
        set.clear();
        result_set.clear();
        action.reset();
    }
    template <bool is_left>
    void apply(const ColumnArrayExecutionData& src, size_t off, size_t len,
               ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_data = assert_cast<const ColumnType&>(*src.nested_col).get_data();
        auto& dst_data = assert_cast<ColumnType&>(*dst.nested_col).get_data();
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.template apply_null<is_left>()) {
                    dst_data.push_back(Element());
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else {
                if (action.template apply<is_left>(set, result_set, src_data[i])) {
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
    using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, 4>;
    using Action = typename ActionImpl<Set, StringRef, operation>::Action;
    Action action;
    Set set;
    Set result_set;
    void reset() {
        set.clear();
        result_set.clear();
        action.reset();
    }
    template <bool is_left>
    void apply(const ColumnArrayExecutionData& src, size_t off, size_t len,
               ColumnArrayMutableData& dst, size_t* count) {
        const auto& src_column = assert_cast<const ColumnString&>(*src.nested_col);
        auto& dst_column = assert_cast<ColumnString&>(*dst.nested_col);
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.template apply_null<is_left>()) {
                    dst_column.insert_default();
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else {
                if (action.template apply<is_left>(set, result_set, src_column.get_data_at(i))) {
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
        DataTypePtr res;
        // if any nested type of array arguments is nullable then return array with
        // nullable nested type.
        for (const auto& arg : arguments) {
            const DataTypeArray* array_type = check_and_get_data_type<DataTypeArray>(arg.get());
            if (array_type->get_nested_type()->is_nullable()) {
                res = arg;
                break;
            }
        }
        res = res ? res : arguments[0];
        return res;
    }

    static Status execute(ColumnPtr& res_ptr, const ColumnArrayExecutionData& left_data,
                          const ColumnArrayExecutionData& right_data, bool left_const,
                          bool right_const) {
        ColumnArrayMutableData dst;
        if (left_data.nested_nullmap_data || right_data.nested_nullmap_data) {
            dst = create_mutable_data(left_data.nested_col, true);
        } else {
            dst = create_mutable_data(left_data.nested_col, false);
        }
        ColumnPtr res_column;
        if (left_const) {
            if (_execute_internal<true, false, ALL_COLUMNS_SIMPLE>(dst, left_data, right_data)) {
                res_column = assemble_column_array(dst);
            }
        } else if (right_const) {
            if (_execute_internal<false, true, ALL_COLUMNS_SIMPLE>(dst, left_data, right_data)) {
                res_column = assemble_column_array(dst);
            }
        } else {
            if (_execute_internal<false, false, ALL_COLUMNS_SIMPLE>(dst, left_data, right_data)) {
                res_column = assemble_column_array(dst);
            }
        }
        if (res_column) {
            res_ptr = std::move(res_column);
            return Status::OK();
        }
        return Status::RuntimeError("Unexpected columns: {}, {}", left_data.nested_col->get_name(),
                                    right_data.nested_col->get_name());
    }

private:
    template <bool LCONST, bool RCONST, typename ColumnType>
    static bool _execute_internal(ColumnArrayMutableData& dst,
                                  const ColumnArrayExecutionData& left_data,
                                  const ColumnArrayExecutionData& right_data) {
        using Impl = OpenSetImpl<operation, ColumnType>;
        if (!check_column<ColumnType>(*left_data.nested_col)) {
            return false;
        }
        constexpr auto execute_left_column_first = Impl::Action::execute_left_column_first;
        size_t current = 0;
        Impl impl;
        size_t row_size = left_data.offsets_ptr->size();
        if constexpr (LCONST) {
            row_size = right_data.offsets_ptr->size();
        }
        for (size_t row = 0; row < row_size; ++row) {
            size_t count = 0;
            size_t left_off = (*left_data.offsets_ptr)[index_check_const(row, LCONST) - 1];
            size_t left_len = (*left_data.offsets_ptr)[index_check_const(row, LCONST)] - left_off;
            size_t right_off = (*right_data.offsets_ptr)[index_check_const(row, RCONST) - 1];
            size_t right_len =
                    (*right_data.offsets_ptr)[index_check_const(row, RCONST)] - right_off;
            if constexpr (execute_left_column_first) {
                impl.template apply<true>(left_data, left_off, left_len, dst, &count);
                impl.template apply<false>(right_data, right_off, right_len, dst, &count);
            } else {
                impl.template apply<false>(right_data, right_off, right_len, dst, &count);
                impl.template apply<true>(left_data, left_off, left_len, dst, &count);
            }
            current += count;
            dst.offsets_ptr->push_back(current);
            impl.reset();
        }
        return true;
    }
    template <bool LCONST, bool RCONST, typename T, typename... Ts>
        requires(sizeof...(Ts) > 0)
    static bool _execute_internal(ColumnArrayMutableData& dst,
                                  const ColumnArrayExecutionData& left_data,
                                  const ColumnArrayExecutionData& right_data) {
        return _execute_internal<LCONST, RCONST, T>(dst, left_data, right_data) ||
               _execute_internal<LCONST, RCONST, Ts...>(dst, left_data, right_data);
    }
};

} // namespace doris::vectorized
