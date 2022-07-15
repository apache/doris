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

template <typename Set, typename Element>
struct UnionAction;

template <typename Set, typename Element>
struct ExceptAction;

template <typename Set, typename Element>
struct IntersectAction;

template <typename Set, typename Element, SetOperation operation>
struct ActionImpl;

template <typename Set, typename Element>
struct ActionImpl<Set, Element, SetOperation::UNION> {
    using Action = UnionAction<Set, Element>;
};

template <typename Set, typename Element>
struct ActionImpl<Set, Element, SetOperation::EXCEPT> {
    using Action = ExceptAction<Set, Element>;
};

template <typename Set, typename Element>
struct ActionImpl<Set, Element, SetOperation::INTERSECT> {
    using Action = IntersectAction<Set, Element>;
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

    void apply(const ColumnArrayExecutionData& src, size_t off, size_t len,
               ColumnArrayMutableData& dst, size_t* count,  bool left_or_right) {
        const auto& src_data = assert_cast<const ColumnType&>(*src.nested_col).get_data();
        auto& dst_data = assert_cast<ColumnType&>(*dst.nested_col).get_data();
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null(left_or_right)) {
                    dst_data.push_back(Element());
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply(set, result_set, src_data[i], left_or_right)) {
                    dst_data.push_back(src_data[i]);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }

    void reset() {
        set.clear();
        result_set.clear();
    }
};

template <SetOperation operation>
struct OpenSetImpl<operation, ColumnString> {
    using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, 4>;
    using Action = typename ActionImpl<Set, StringRef, operation>::Action;
    Action action;
    Set set;
    Set result_set;

    void apply(const ColumnArrayExecutionData& src, size_t off, size_t len,
               ColumnArrayMutableData& dst, size_t* count, bool left_or_right) {
        const auto& src_column = assert_cast<const ColumnString&>(*src.nested_col);
        auto& dst_column = assert_cast<ColumnString&>(*dst.nested_col);
        for (size_t i = off; i < off + len; ++i) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[i]) {
                if (action.apply_null(left_or_right)) {
                    dst_column.insert_default();
                    dst.nested_nullmap_data->push_back(1);
                    ++(*count);
                }
            } else  {
                if (action.apply(set, result_set, src_column.get_data_at(i), left_or_right)) {
                    dst_column.insert_from(src_column, i);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                    ++(*count);
                }
            }
        }
    }

    void reset() {
        set.clear();
        result_set.clear();
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
        if (_execute_internal<ColumnString>(dst, left_data, right_data) ||
            _execute_internal<ColumnDate>(dst, left_data, right_data) ||
            _execute_internal<ColumnDateTime>(dst, left_data, right_data) ||
            _execute_internal<ColumnUInt8>(dst, left_data, right_data) ||
            _execute_internal<ColumnInt8>(dst, left_data, right_data) ||
            _execute_internal<ColumnInt16>(dst, left_data, right_data) ||
            _execute_internal<ColumnInt32>(dst, left_data, right_data) ||
            _execute_internal<ColumnInt64>(dst, left_data, right_data) ||
            _execute_internal<ColumnInt128>(dst, left_data, right_data) ||
            _execute_internal<ColumnFloat32>(dst, left_data, right_data) ||
            _execute_internal<ColumnFloat64>(dst, left_data, right_data) ||
            _execute_internal<ColumnDecimal128>(dst, left_data, right_data)) {
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
    static bool _execute_internal(ColumnArrayMutableData& dst, const ColumnArrayExecutionData& left_data,
                                    const ColumnArrayExecutionData& right_data) {
        using Impl = OpenSetImpl<operation, ColumnType>;
        if (!check_column<ColumnType>(*left_data.nested_col)) {
            return false;
        }
        constexpr auto apply_left_first = Impl::Action::apply_left_first;
        size_t current = 0;
        Impl impl;
        for (size_t row = 0; row < left_data.offsets_ptr->size(); ++row) {
            size_t count = 0;
            size_t left_off = (*left_data.offsets_ptr)[row - 1];
            size_t left_len = (*left_data.offsets_ptr)[row] - left_off;
            size_t right_off = (*right_data.offsets_ptr)[row - 1];
            size_t right_len = (*right_data.offsets_ptr)[row] - right_off;
            if (apply_left_first) {
                impl.apply(left_data, left_off, left_len, dst, &count, true);
                impl.apply(right_data, right_off, right_len, dst, &count, false);
            } else {
                impl.apply(right_data, right_off, right_len, dst, &count, false);
                impl.apply(left_data, left_off, left_len, dst, &count, true);
            }
            current += count;
            dst.offsets_ptr->push_back(current);
            impl.reset();
        }
        return true;
    }
};

} // namespace doris::vectorized