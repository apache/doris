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

#include "storage/predicate/like_column_predicate.h"

#include <cstring>

#include "core/column/column_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "exprs/function/like.h"
#include "exprs/function_context.h"

namespace doris {

template <PrimitiveType T>
StringRef like_column_string_at(const StringRef* data, size_t row) {
    auto value = data[row];
    if constexpr (T == TYPE_CHAR) {
        value.size = strnlen(value.data, value.size);
    }
    return value;
}

template <PrimitiveType T>
LikeColumnPredicate<T>::LikeColumnPredicate(bool opposite, uint32_t column_id, std::string col_name,
                                            doris::FunctionContext* fn_ctx, doris::StringRef val)
        : ColumnPredicate(column_id, col_name, T, opposite), pattern(val) {
    static_assert(T == TYPE_VARCHAR || T == TYPE_CHAR || T == TYPE_STRING,
                  "LikeColumnPredicate only supports the following types: TYPE_VARCHAR, TYPE_CHAR, "
                  "TYPE_STRING");
    _state = reinterpret_cast<StateType*>(
            fn_ctx->get_function_state(doris::FunctionContext::THREAD_LOCAL));
    THROW_IF_ERROR(_state->search_state.clone(_like_state));
}

template <PrimitiveType T>
void LikeColumnPredicate<T>::evaluate_vec(const IColumn& column, uint16_t size, bool* flags) const {
    _evaluate_vec<false>(column, size, flags);
}

template <PrimitiveType T>
void LikeColumnPredicate<T>::evaluate_and_vec(const IColumn& column, uint16_t size,
                                              bool* flags) const {
    _evaluate_vec<true>(column, size, flags);
}

template <PrimitiveType T>
uint16_t LikeColumnPredicate<T>::_evaluate_inner(const IColumn& column, uint16_t* sel,
                                                 uint16_t size) const {
    uint16_t new_size = 0;
    if (column.is_nullable()) {
        auto* nullable_col = assert_cast<const ColumnNullable*>(&column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        auto& nested_col = nullable_col->get_nested_column();
        if (nested_col.is_column_dictionary()) {
            auto* nested_col_ptr = assert_cast<const ColumnDictI32*>(&nested_col);
            auto& data_array = nested_col_ptr->get_data();
            const auto& dict_res = _find_code_from_dictionary_column(*nested_col_ptr);
            if (!nullable_col->has_null()) {
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    unsigned char flag = dict_res[data_array[idx]];
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
                    unsigned char flag = dict_res[data_array[idx]];
                    new_size += _opposite ^ flag;
                }
            }
        } else {
            const auto& data_array = assert_cast<const ColumnString&>(nested_col).get_data();
            if (!nullable_col->has_null()) {
                ColumnUInt8::Container res(size, 0);
                for (uint16_t i = 0; i != size; i++) {
                    uint16_t idx = sel[i];
                    sel[new_size] = idx;
                    unsigned char flag = 0;
                    THROW_IF_ERROR((_state->scalar_function)(
                            &_like_state, like_column_string_at<T>(data_array.data(), idx), pattern,
                            &flag));
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

                    StringRef cell_value = like_column_string_at<T>(data_array.data(), idx);
                    unsigned char flag = 0;
                    THROW_IF_ERROR((_state->scalar_function)(
                            &_like_state, StringRef(cell_value.data, cell_value.size), pattern,
                            &flag));
                    new_size += _opposite ^ flag;
                }
            }
        }
    } else {
        if (column.is_column_dictionary()) {
            auto* nested_col_ptr = assert_cast<const ColumnDictI32*>(&column);
            const auto& dict_res = _find_code_from_dictionary_column(*nested_col_ptr);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i != size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                unsigned char flag = dict_res[data_array[idx]];
                new_size += _opposite ^ flag;
            }
        } else {
            ColumnUInt8::Container res(size, 0);
            const auto& data_array = assert_cast<const ColumnString&>(column).get_data();
            for (uint16_t i = 0; i != size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                unsigned char flag = 0;
                THROW_IF_ERROR((_state->scalar_function)(
                        &_like_state, like_column_string_at<T>(data_array.data(), idx), pattern,
                        &flag));
                new_size += _opposite ^ flag;
            }
        }
    }
    return new_size;
}

template class LikeColumnPredicate<TYPE_CHAR>;
template class LikeColumnPredicate<TYPE_STRING>;

} //namespace doris
