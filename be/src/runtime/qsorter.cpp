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

#include "runtime/qsorter.h"

#include <algorithm>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"

namespace doris {

class TupleRowLessThan {
public:
    TupleRowLessThan(std::vector<ExprContext*>& lhs_expr_ctxs,
                     std::vector<ExprContext*>& rhs_expr_ctxs);
    bool operator()(TupleRow* const& lhs, TupleRow* const& rhs) const;

private:
    std::vector<ExprContext*>& _lhs_expr_ctxs;
    std::vector<ExprContext*>& _rhs_expr_ctxs;
};

TupleRowLessThan::TupleRowLessThan(std::vector<ExprContext*>& lhs_expr_ctxs,
                                   std::vector<ExprContext*>& rhs_expr_ctxs)
        : _lhs_expr_ctxs(lhs_expr_ctxs), _rhs_expr_ctxs(rhs_expr_ctxs) {}

// Return true only when lhs less than rhs
// nullptr is the positive infinite
bool TupleRowLessThan::operator()(TupleRow* const& lhs, TupleRow* const& rhs) const {
    for (int i = 0; i < _lhs_expr_ctxs.size(); ++i) {
        void* lhs_value = _lhs_expr_ctxs[i]->get_value(lhs);
        void* rhs_value = _rhs_expr_ctxs[i]->get_value(rhs);

        // nullptr's always go at the end regardless of asc/desc
        if (lhs_value == nullptr && rhs_value == nullptr) {
            continue;
        }
        if (rhs_value == nullptr) {
            return false;
        }
        if (lhs_value == nullptr) {
            return true;
        }

        int result = RawValue::compare(lhs_value, rhs_value, _lhs_expr_ctxs[i]->root()->type());
        if (result > 0) {
            return false;
        } else if (result < 0) {
            return true;
        } else {
            // Otherwise, try the next Expr
        }
    }

    // NOTE: must return false when two value equal with each other
    return false;
}

QSorter::QSorter(const RowDescriptor& row_desc, const std::vector<ExprContext*>& order_expr_ctxs,
                 RuntimeState* state)
        : _row_desc(row_desc),
          _order_expr_ctxs(order_expr_ctxs),
          _tuple_pool(new MemPool(state->instance_mem_tracker().get())) {}

Status QSorter::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_order_expr_ctxs, state, &_lhs_expr_ctxs));
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_order_expr_ctxs, state, &_rhs_expr_ctxs));
    return Status::OK();
}

// Insert if either not at the limit or it's a new TopN tuple_row
Status QSorter::insert_tuple_row(TupleRow* input_row) {
    TupleRow* insert_tuple_row =
            input_row->deep_copy(_row_desc.tuple_descriptors(), _tuple_pool.get());
    if (insert_tuple_row == nullptr) {
        return Status::InternalError("deep copy failed.");
    }
    _sorted_rows.push_back(insert_tuple_row);
    return Status::OK();
}

Status QSorter::add_batch(RowBatch* batch) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        RETURN_IF_ERROR(insert_tuple_row(batch->get_row(i)));
    }
    return Status::OK();
}

// Reverse result in priority_queue
Status QSorter::input_done() {
    std::sort(_sorted_rows.begin(), _sorted_rows.end(),
              TupleRowLessThan(_lhs_expr_ctxs, _rhs_expr_ctxs));
    _next_iter = _sorted_rows.begin();
    return Status::OK();
}

Status QSorter::get_next(RowBatch* batch, bool* eos) {
    while (!batch->is_full() && (_next_iter != _sorted_rows.end())) {
        int row_idx = batch->add_row();
        TupleRow* dst_row = batch->get_row(row_idx);
        TupleRow* src_row = *_next_iter;
        batch->copy_row(src_row, dst_row);
        ++_next_iter;
        batch->commit_last_row();
    }

    *eos = _next_iter == _sorted_rows.end();
    return Status::OK();
}

Status QSorter::close(RuntimeState* state) {
    _tuple_pool.reset();
    Expr::close(_lhs_expr_ctxs, state);
    Expr::close(_rhs_expr_ctxs, state);
    return Status::OK();
}

} // namespace doris
