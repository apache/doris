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

#include "exec/sort_node.h"
#include "exec/sort_exec_exprs.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

SortNode::SortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
      _num_rows_skipped(0) {
    Status status = init(tnode, nullptr);
    DCHECK(status.ok()) << "SortNode c'tor:init failed: \n" << status.get_error_msg();
}

SortNode::~SortNode() {
}

Status SortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    const vector<TExpr>* sort_tuple_slot_exprs =  tnode.sort_node.__isset.sort_tuple_slot_exprs ?
            &tnode.sort_node.sort_tuple_slot_exprs : NULL;
    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.ordering_exprs,
            sort_tuple_slot_exprs, _pool));
    _is_asc_order = tnode.sort_node.is_asc_order;
    _nulls_first = tnode.sort_node.nulls_first;
    return Status::OK();
}

Status SortNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(
            state, child(0)->row_desc(), _row_descriptor, expr_mem_tracker()));
    return Status::OK();
}

Status SortNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));

    TupleRowComparator less_than(
        _sort_exec_exprs.lhs_ordering_expr_ctxs(), _sort_exec_exprs.rhs_ordering_expr_ctxs(),
        _is_asc_order, _nulls_first);
    _sorter.reset(new MergeSorter(
                      less_than, _sort_exec_exprs.sort_tuple_slot_expr_ctxs(),
                      &_row_descriptor, runtime_profile(), state));

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in GetNext().
    RETURN_IF_ERROR(sort_input(state));

    // The child can be closed at this point.
    child(0)->close(state);
    return Status::OK();
}

Status SortNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    //RETURN_IF_ERROR(QueryMaintenance(state));

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    } else {
        *eos = false;
    }

    DCHECK_EQ(row_batch->num_rows(), 0);
    RETURN_IF_ERROR(_sorter->get_next(row_batch, eos));
    while ((_num_rows_skipped < _offset)) {
        _num_rows_skipped += row_batch->num_rows();
        // Throw away rows in the output batch until the offset is skipped.
        int rows_to_keep = _num_rows_skipped - _offset;
        if (rows_to_keep > 0) {
            row_batch->copy_rows(0, row_batch->num_rows() - rows_to_keep, rows_to_keep);
            row_batch->set_num_rows(rows_to_keep);
        } else {
            row_batch->set_num_rows(0);
        }
        if (rows_to_keep > 0 || *eos) {
            break;
        }
        RETURN_IF_ERROR(_sorter->get_next(row_batch, eos));
    }

    _num_rows_returned += row_batch->num_rows();
    if (reached_limit()) {
        row_batch->set_num_rows(row_batch->num_rows() - (_num_rows_returned - _limit));
        *eos = true;
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status SortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _sort_exec_exprs.close(state);
    _sorter.reset();
    return ExecNode::close(state);
}

void SortNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SortNode(";
         // << Expr::debug_string(_sort_exec_exprs.lhs_ordering_expr_ctxs());
    for (int i = 0; i < _is_asc_order.size(); ++i) {
        *out << (i > 0 ? " " : "")
             << (_is_asc_order[i] ? "asc" : "desc")
             << " nulls " << (_nulls_first[i] ? "first" : "last");
    }
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

Status SortNode::sort_input(RuntimeState* state) {
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    bool eos = false;
    do {
        batch.reset();
        RETURN_IF_ERROR(child(0)->get_next(state, &batch, &eos));
        RETURN_IF_ERROR(_sorter->add_batch(&batch));
        RETURN_IF_CANCELLED(state);
        RETURN_IF_LIMIT_EXCEEDED(state);
        // RETURN_IF_ERROR(QueryMaintenance(state));
    } while (!eos);
    RETURN_IF_ERROR(_sorter->input_done());
    return Status::OK();
}

}
