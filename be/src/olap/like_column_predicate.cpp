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

#include "olap/like_column_predicate.h"

#include "runtime/define_primitive_type.h"
#include "udf/udf.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/common/string_ref.h"
#include "vec/functions/like.h"

namespace doris {

template <PrimitiveType T>
LikeColumnPredicate<T>::LikeColumnPredicate(bool opposite, uint32_t column_id,
                                            doris::FunctionContext* fn_ctx, doris::StringRef val)
        : ColumnPredicate(column_id, opposite), pattern(val) {
    static_assert(T == TYPE_VARCHAR || T == TYPE_CHAR || T == TYPE_STRING,
                  "LikeColumnPredicate only supports the following types: TYPE_VARCHAR, TYPE_CHAR, "
                  "TYPE_STRING");
    _state = reinterpret_cast<StateType*>(
            fn_ctx->get_function_state(doris::FunctionContext::THREAD_LOCAL));
    static_cast<void>(_state->search_state.clone(_like_state));
}

template <PrimitiveType T>
void LikeColumnPredicate<T>::evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                                          bool* flags) const {
    _evaluate_vec<false>(column, size, flags);
}

template <PrimitiveType T>
void LikeColumnPredicate<T>::evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                                              bool* flags) const {
    _evaluate_vec<true>(column, size, flags);
}

template <PrimitiveType T>
uint16_t LikeColumnPredicate<T>::_evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                                                 uint16_t size) const {
    uint16_t new_size = 0;
    if (column.is_nullable()) {
        auto* nullable_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        auto& nested_col = nullable_col->get_nested_column();
        if (nested_col.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);
            auto& data_array = nested_col_ptr->get_data();
            if (!nullable_col->has_null()) {
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    StringRef cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                    unsigned char flag = 0;
                    static_cast<void>((_state->scalar_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state),
                            StringRef(cell_value.data, cell_value.size), pattern, &flag));
                    new_size += _opposite ^ flag;
                }
            } else {
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    if (null_map_data[idx]) {
                        new_size += _opposite;
                        continue;
                    }

                    StringRef cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                    unsigned char flag = 0;
                    static_cast<void>((_state->scalar_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state),
                            StringRef(cell_value.data, cell_value.size), pattern, &flag));
                    new_size += _opposite ^ flag;
                }
            }
        } else {
            auto* str_col = vectorized::check_and_get_column<vectorized::PredicateColumnType<T>>(
                    nested_col);
            if (!nullable_col->has_null()) {
                vectorized::ColumnUInt8::Container res(size, 0);
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    unsigned char flag = 0;
                    static_cast<void>((_state->scalar_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state),
                            str_col->get_data_at(idx), pattern, &flag));
                    new_size += _opposite ^ flag;
                }
            } else {
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    if (null_map_data[idx]) {
                        new_size += _opposite;
                        continue;
                    }

                    StringRef cell_value = str_col->get_data_at(idx);
                    unsigned char flag = 0;
                    static_cast<void>((_state->scalar_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state),
                            StringRef(cell_value.data, cell_value.size), pattern, &flag));
                    new_size += _opposite ^ flag;
                }
            }
        }
    } else {
        if (column.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(column);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i != size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                StringRef cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                unsigned char flag = 0;
                static_cast<void>((_state->scalar_function)(
                        const_cast<vectorized::LikeSearchState*>(&_like_state),
                        StringRef(cell_value.data, cell_value.size), pattern, &flag));
                new_size += _opposite ^ flag;
            }
        } else {
            const vectorized::PredicateColumnType<T>* str_col =
                    vectorized::check_and_get_column<vectorized::PredicateColumnType<T>>(column);

            vectorized::ColumnUInt8::Container res(size, 0);
            for (uint16_t i = 0; i != size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                unsigned char flag = 0;
                static_cast<void>((_state->scalar_function)(
                        const_cast<vectorized::LikeSearchState*>(&_like_state),
                        str_col->get_data_at(idx), pattern, &flag));
                new_size += _opposite ^ flag;
            }
        }
    }
    return new_size;
}

template class LikeColumnPredicate<TYPE_CHAR>;
template class LikeColumnPredicate<TYPE_STRING>;

} //namespace doris
