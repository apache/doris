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

#include "exec/sort_exec_exprs.h"

namespace doris {

Status SortExecExprs::init(const TSortInfo& sort_info, ObjectPool* pool) {
    return init(
            sort_info.ordering_exprs,
            sort_info.__isset.sort_tuple_slot_exprs ? &sort_info.sort_tuple_slot_exprs : nullptr,
            pool);
}

Status SortExecExprs::init(const std::vector<TExpr>& ordering_exprs,
                           const std::vector<TExpr>* sort_tuple_slot_exprs, ObjectPool* pool) {
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, ordering_exprs, &_lhs_ordering_expr_ctxs));
    if (sort_tuple_slot_exprs != nullptr) {
        _materialize_tuple = true;
        RETURN_IF_ERROR(
                Expr::create_expr_trees(pool, *sort_tuple_slot_exprs, &_sort_tuple_slot_expr_ctxs));
    } else {
        _materialize_tuple = false;
    }
    return Status::OK();
}

Status SortExecExprs::init(const std::vector<ExprContext*>& lhs_ordering_expr_ctxs,
                           const std::vector<ExprContext*>& rhs_ordering_expr_ctxs) {
    _lhs_ordering_expr_ctxs = lhs_ordering_expr_ctxs;
    _rhs_ordering_expr_ctxs = rhs_ordering_expr_ctxs;
    return Status::OK();
}

Status SortExecExprs::prepare(RuntimeState* state, const RowDescriptor& child_row_desc,
                              const RowDescriptor& output_row_desc,
                              const std::shared_ptr<MemTracker>& expr_mem_tracker) {
    if (_materialize_tuple) {
        RETURN_IF_ERROR(
                Expr::prepare(_sort_tuple_slot_expr_ctxs, state, child_row_desc, expr_mem_tracker));
    }
    RETURN_IF_ERROR(
            Expr::prepare(_lhs_ordering_expr_ctxs, state, output_row_desc, expr_mem_tracker));
    return Status::OK();
}

Status SortExecExprs::open(RuntimeState* state) {
    if (_materialize_tuple) {
        RETURN_IF_ERROR(Expr::open(_sort_tuple_slot_expr_ctxs, state));
    }
    RETURN_IF_ERROR(Expr::open(_lhs_ordering_expr_ctxs, state));
    RETURN_IF_ERROR(
            Expr::clone_if_not_exists(_lhs_ordering_expr_ctxs, state, &_rhs_ordering_expr_ctxs));
    return Status::OK();
}

void SortExecExprs::close(RuntimeState* state) {
    if (_materialize_tuple) {
        Expr::close(_sort_tuple_slot_expr_ctxs, state);
    }
    Expr::close(_lhs_ordering_expr_ctxs, state);
    Expr::close(_rhs_ordering_expr_ctxs, state);
}

} //namespace doris
