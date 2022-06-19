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

#include "udf/udf.h"
#include "exprs/like_predicate.h"

#include "olap/like_column_predicate.h"
#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"

namespace doris {

LikeColumnPredicate::LikeColumnPredicate(bool opposite, uint32_t column_id, doris_udf::FunctionContext* fn_ctx, StringValue* val)
    : ColumnPredicate(column_id, opposite), _fn_ctx(fn_ctx) {
    val->to_string_val(&pattern);
    _state = reinterpret_cast<LikePredicateState*>(_fn_ctx->get_function_state(doris_udf::FunctionContext::THREAD_LOCAL));
}

void LikeColumnPredicate::evaluate(VectorizedRowBatch* batch) const {
    uint16_t n = batch->size();
    uint16_t* sel = batch->selected();
    if (!batch->selected_in_use()) {
        for (uint16_t i = 0; i != n; ++i) {
            sel[i] = i;
        }
    }
}

void LikeColumnPredicate::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
    uint16_t new_size = 0;
    if (block->is_nullable()) {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            if (block->cell(idx).is_null())
            {
                new_size += _opposite ? 1 : 0;
                continue;
            }

            const StringValue* str_value = reinterpret_cast<const StringValue*>(block->cell(idx).cell_ptr());
            doris_udf::StringVal target;
            str_value->to_string_val(&target);
            auto result = (_state->function)(_fn_ctx, target, pattern).val;
            new_size += _opposite ? !result : result;
        }
    } else {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const StringValue* str_value = reinterpret_cast<const StringValue*>(block->cell(idx).cell_ptr());
            doris_udf::StringVal target;
            str_value->to_string_val(&target);
            auto result = (_state->function)(_fn_ctx, target, pattern).val;
            new_size += _opposite ? !result : result;
        }
    }
    *size = new_size;
}

} //namespace doris
