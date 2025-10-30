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

#include "vec/common/sort/vsort_exec_exprs.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {

Status VSortExecExprs::init(const TSortInfo& sort_info, ObjectPool* pool) {
    return init(sort_info.ordering_exprs,
                sort_info.__isset.sort_tuple_slot_exprs ? &sort_info.sort_tuple_slot_exprs : NULL,
                pool);
}

Status VSortExecExprs::init(const std::vector<TExpr>& ordering_exprs,
                            const std::vector<TExpr>* sort_tuple_slot_exprs, ObjectPool* pool) {
    RETURN_IF_ERROR(VExpr::create_expr_trees(ordering_exprs, _ordering_expr_ctxs));
    if (sort_tuple_slot_exprs != NULL) {
        _materialize_tuple = true;
        RETURN_IF_ERROR(
                VExpr::create_expr_trees(*sort_tuple_slot_exprs, _sort_tuple_slot_expr_ctxs));
    } else {
        _materialize_tuple = false;
    }
    return Status::OK();
}

Status VSortExecExprs::prepare(RuntimeState* state, const RowDescriptor& child_row_desc,
                               const RowDescriptor& output_row_desc) {
    if (_materialize_tuple) {
        RETURN_IF_ERROR(VExpr::prepare(_sort_tuple_slot_expr_ctxs, state, child_row_desc));
    }
    RETURN_IF_ERROR(VExpr::prepare(_ordering_expr_ctxs, state, output_row_desc));
    return Status::OK();
}

Status VSortExecExprs::open(RuntimeState* state) {
    if (_materialize_tuple) {
        RETURN_IF_ERROR(VExpr::open(_sort_tuple_slot_expr_ctxs, state));
    }
    RETURN_IF_ERROR(VExpr::open(_ordering_expr_ctxs, state));
    return Status::OK();
}

void VSortExecExprs::close(RuntimeState* state) {}

Status VSortExecExprs::clone(RuntimeState* state, VSortExecExprs& new_exprs) {
    new_exprs._ordering_expr_ctxs.resize(_ordering_expr_ctxs.size());
    for (size_t i = 0; i < _ordering_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(_ordering_expr_ctxs[i]->clone(state, new_exprs._ordering_expr_ctxs[i]));
    }
    new_exprs._sort_tuple_slot_expr_ctxs.resize(_sort_tuple_slot_expr_ctxs.size());
    for (size_t i = 0; i < _sort_tuple_slot_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(_sort_tuple_slot_expr_ctxs[i]->clone(
                state, new_exprs._sort_tuple_slot_expr_ctxs[i]));
    }
    new_exprs._materialize_tuple = _materialize_tuple;
    return Status::OK();
}

} // namespace doris::vectorized
