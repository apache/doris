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

LikeColumnPredicate::LikeColumnPredicate(bool opposite, uint32_t column_id,
                                         doris_udf::FunctionContext* fn_ctx,
                                         doris_udf::StringVal val)
        : ColumnPredicate(column_id, opposite), _fn_ctx(fn_ctx), pattern(val) {
    _state = reinterpret_cast<LikePredicateState*>(
            _fn_ctx->get_function_state(doris_udf::FunctionContext::THREAD_LOCAL));
}

void LikeColumnPredicate::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
    if (block->is_nullable()) {
        _base_evaluate<true>(block, sel, size);
    } else {
        _base_evaluate<false>(block, sel, size);
    }
}

void LikeColumnPredicate::evaluate(vectorized::IColumn& column, uint16_t* sel,
                                   uint16_t* size) const {
    uint16_t new_size = 0;

    if (column.is_nullable()) {
        auto* nullable_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        auto& nested_col = nullable_col->get_nested_column();
        if (nested_col.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                if (null_map_data[idx]) {
                    new_size += _opposite;
                    continue;
                }

                StringValue cell_value = nested_col_ptr->get_value(data_array[idx]);
                doris_udf::StringVal target;
                cell_value.to_string_val(&target);
                new_size += _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        } else {
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                if (null_map_data[idx]) {
                    new_size += _opposite;
                    continue;
                }

                StringRef cell_value = nested_col.get_data_at(idx);
                doris_udf::StringVal target = cell_value.to_string_val();
                new_size += _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        }
    } else {
        if (column.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(column);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                StringValue cell_value = nested_col_ptr->get_value(data_array[idx]);
                doris_udf::StringVal target;
                cell_value.to_string_val(&target);
                new_size += _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        } else {
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                StringRef cell_value = column.get_data_at(idx);
                doris_udf::StringVal target = cell_value.to_string_val();
                new_size += _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        }
    }

    *size = new_size;
}

void LikeColumnPredicate::evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                                       bool* flags) const {
    if (column.is_nullable()) {
        auto* nullable_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        auto& nested_col = nullable_col->get_nested_column();
        if (nested_col.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(nested_col);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i < size; i++) {
                if (null_map_data[i]) {
                    flags[i] = _opposite;
                    continue;
                }

                StringValue cell_value = nested_col_ptr->get_value(data_array[i]);
                doris_udf::StringVal target;
                cell_value.to_string_val(&target);
                flags[i] = _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        } else {
            for (uint16_t i = 0; i < size; i++) {
                if (null_map_data[i]) {
                    flags[i] = _opposite;
                    continue;
                }

                StringRef cell_value = nested_col.get_data_at(i);
                doris_udf::StringVal target = cell_value.to_string_val();
                flags[i] = _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        }
    } else {
        if (column.is_column_dictionary()) {
            auto* nested_col_ptr = vectorized::check_and_get_column<
                    vectorized::ColumnDictionary<vectorized::Int32>>(column);
            auto& data_array = nested_col_ptr->get_data();
            for (uint16_t i = 0; i < size; i++) {
                StringValue cell_value = nested_col_ptr->get_value(data_array[i]);
                doris_udf::StringVal target;
                cell_value.to_string_val(&target);
                flags[i] = _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        } else {
            for (uint16_t i = 0; i < size; i++) {
                StringRef cell_value = column.get_data_at(i);
                doris_udf::StringVal target = cell_value.to_string_val();
                flags[i] = _opposite ^ ((_state->function)(_fn_ctx, target, pattern).val);
            }
        }
    }
}

} //namespace doris
