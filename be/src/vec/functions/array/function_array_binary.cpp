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

#include "vec/common/hash_table/hash_set.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

using NullMapType = PaddedPODArray<UInt8>;

template <typename T>
struct OpenHashSet {
    using ElementNativeType = typename NativeType<typename T::value_type>::Type;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set set;

};

template <>
struct OpenHashSet<ColumnString> {
    using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, 4>;
};

template <typename T>
struct UnionActionImpl {
    using Set = OpenHashSet<T>::Set;
    Set set;
    bool containNull = false;

    void apply_array_left(const ColumnArrayExecutionData& src, size_t off, size_t len,
                          IColumn* dst_column, NullMapType* dst_null_map, size_t* count) {
        const auto& src_data = assert_cast<const T&>(*src->nested_col).get_data();
        const auto& dst_data = assert_cast<const T&>(*dst_column).get_data();
        for (size_t i = off; i < off + len; ++i) {
            if (src->nested_nullmap_data[i]) {
                if (!containNull) {
                    dst_data->push_back(typename T::value_type());
                    dst_null_map->push_back(1);
                    containNull = true;
                    continue;
                }
            } else  {
                if (!set.find(src_data[i])) {
                    dst_data->push_back(src_data[i]);
                    dst_null_map->push_back(0);
                    set.insert(src_data[i]);
                    continue;
                }
            }
            ++(*count);
        }
    }

    void apply_array_right(const ColumnArrayExecutionData& src, size_t off, size_t len,
                              IColumn* dst_column, NullMapType* dst_null_map, size_t* count) {
        apply_array_left(src, off, len, dst_column, dst_null_map, count);
    }
};

template <ConcreteActionImpl>
struct ArraySetLikeImpl {
public:
    static DataTypePtr get_return_type(const DataTypeArray& array1, const DataTypeArray& array2) {
        return array1;
    }

    static Status execute(MutableColumnPtr& res_ptr, const ColumnArrayExecutionData& left_data,
                          const ColumnArrayExecutionData& right_data) {

        NullMapType* dst_null_map = nullptr;
        MutableColumnPtr array_nested_column = nullptr;
        IColumn* dst_column;
        if (left_data.nested_nullmap_data || right_data.nested_nullmap_data) {
            auto dst_nested_column = ColumnNullable::create(
                    left_data.nested_col->clone_empty(), ColumnUInt8::create());
            array_nested_column = dst_nested_column->get_ptr();
            dst_column = dst_nested_column->get_nested_column_ptr();
            dst_null_map = &dst_nested_column->get_null_map_data();
        } else {
            auto dst_nested_column = left_data.nested_col->clone_empty();
            array_nested_column = dst_nested_column->get_ptr();
            dst_column = dst_nested_column;
        }

        auto dst_offsets_column = ColumnArray::ColumnOffsets::create();
        auto& dst_offsets = dst_offsets_column->get_data();

        if (_execute_expand<ColumnDate>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnDateTime>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnUInt8>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnInt8>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnInt16>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnInt32>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnInt64>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnInt128>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnFloat32>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnFloat64>(dst_column, dst_null_map, left_data, right_data) ||
                 _execute_expand<ColumnDecimal128>(dst_column, dst_null_map, left_data, right_data)) {
            // do nothing
        } else {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({}, {})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
    }

private:
    template <typename ColumnType>
    static Status _execute_internal(IColumn* dst_column, NullMapType* dst_null_map,
                                 const ColumnArrayExecutionData& left_data,
                                 const ColumnArrayExecutionData& right_data) {
        for (ssize_t row = 0; row < left_data.offsets_ptr->size(); ++row) {
            size_t count = 0;
            ConcreteActionImpl impl;

            size_t left_off = (*left_data.offsets_ptr)[row - 1];
            size_t left_len = (*left_data.offsets_ptr)[row] - left_off;
            impl.apply_array_left(left_data, left_off, left_len, dst_column, dst_null_map, &count);

            size_t right_off = (*right_data.offsets_ptr)[row - 1];
            size_t right_len = (*right_data.offsets_ptr)[row] - right_off;
            impl.apply_array_right(left_data, left_off, left_len, dst_column, dst_null_map, &count);
        }
    }

    template <typename ColumnType>
    static bool _execute_expand(IColumn* dst_column, NullMapType* dst_null_map,
                                       const ColumnArrayExecutionData& left_data,
                                       const ColumnArrayExecutionData& right_data) {
        if (!check_column<ColumnType>(*left_exec_data.nested_col)) {
            return false;
        }

        _execute_internal<ColumnType>(dst_column, dst_null_map, left_data, right_data);
        return true;
    }
};

} // namespace doris::vectorized
