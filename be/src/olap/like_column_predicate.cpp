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

#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "udf/udf.h"

namespace doris {

template <>
LikeColumnPredicate<true>::LikeColumnPredicate(bool opposite, uint32_t column_id,
                                               doris_udf::FunctionContext* fn_ctx,
                                               doris_udf::StringVal val)
        : ColumnPredicate(column_id, opposite),
          _fn_ctx(fn_ctx),
          pattern(reinterpret_cast<char*>(val.ptr), val.len) {
    _state = reinterpret_cast<StateType*>(
            _fn_ctx->get_function_state(doris_udf::FunctionContext::THREAD_LOCAL));
    _state->search_state.clone(_like_state);
}

template <>
LikeColumnPredicate<false>::LikeColumnPredicate(bool opposite, uint32_t column_id,
                                                doris_udf::FunctionContext* fn_ctx,
                                                doris_udf::StringVal val)
        : ColumnPredicate(column_id, opposite), _fn_ctx(fn_ctx), pattern(val) {
    _state = reinterpret_cast<StateType*>(
            _fn_ctx->get_function_state(doris_udf::FunctionContext::THREAD_LOCAL));
}

template <bool is_vectorized>
void LikeColumnPredicate<is_vectorized>::evaluate(ColumnBlock* block, uint16_t* sel,
                                                  uint16_t* size) const {
    if (block->is_nullable()) {
        _base_evaluate<true>(block, sel, size);
    } else {
        _base_evaluate<false>(block, sel, size);
    }
}

template <bool is_vectorized>
void LikeColumnPredicate<is_vectorized>::evaluate_vec(const vectorized::IColumn& column,
                                                      uint16_t size, bool* flags) const {
    _evaluate_vec<false>(column, size, flags);
}

template <bool is_vectorized>
void LikeColumnPredicate<is_vectorized>::evaluate_and_vec(const vectorized::IColumn& column,
                                                          uint16_t size, bool* flags) const {
    _evaluate_vec<true>(column, size, flags);
}

template <bool is_vectorized>
uint16_t LikeColumnPredicate<is_vectorized>::evaluate(const vectorized::IColumn& column,
                                                      uint16_t* sel, uint16_t size) const {
    uint16_t new_size = 0;
    if constexpr (is_vectorized) {
        if (column.is_nullable()) {
            auto* nullable_col =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
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
                        StringValue cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.ptr, cell_value.len), pattern, &flag);
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

                        StringValue cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.ptr, cell_value.len), pattern, &flag);
                        new_size += _opposite ^ flag;
                    }
                }
            } else {
                auto* str_col = vectorized::check_and_get_column<
                        vectorized::PredicateColumnType<TYPE_STRING>>(nested_col);
                if (!nullable_col->has_null()) {
                    vectorized::ColumnUInt8::Container res(size, 0);
                    (_state->predicate_like_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state), *str_col,
                            pattern, res, sel, size);
                    for (uint16_t i = 0; i != size; i++) {
                        uint16_t idx = sel[i];
                        sel[new_size] = idx;
                        new_size += _opposite ^ res[i];
                    }
                } else {
                    for (uint16_t i = 0; i != size; i++) {
                        uint16_t idx = sel[i];
                        sel[new_size] = idx;
                        if (null_map_data[idx]) {
                            new_size += _opposite;
                            continue;
                        }

                        StringValue cell_value = str_col->get_data()[idx];
                        unsigned char flag = 0;
                        (_state->scalar_function)(
                                const_cast<vectorized::LikeSearchState*>(&_like_state),
                                StringRef(cell_value.ptr, cell_value.len), pattern, &flag);
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
                    StringValue cell_value = nested_col_ptr->get_shrink_value(data_array[idx]);
                    unsigned char flag = 0;
                    (_state->scalar_function)(
                            const_cast<vectorized::LikeSearchState*>(&_like_state),
                            StringRef(cell_value.ptr, cell_value.len), pattern, &flag);
                    new_size += _opposite ^ flag;
                }
            } else {
                auto* str_col = vectorized::check_and_get_column<
                        vectorized::PredicateColumnType<TYPE_STRING>>(column);
                vectorized::ColumnUInt8::Container res(size, 0);
                (_state->predicate_like_function)(
                        const_cast<vectorized::LikeSearchState*>(&_like_state), *str_col, pattern,
                        res, sel, size);
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    new_size += _opposite ^ res[i];
                }
            }
        }
    }
    return new_size;
}

template class LikeColumnPredicate<true>;
template class LikeColumnPredicate<false>;

} //namespace doris
