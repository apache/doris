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

#include "exec/spill_sort_node.h"

#include "exec/sort_exec_exprs.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/sorted_run_merger.h"
#include "util/debug_util.h"

namespace doris {

SpillSortNode::SpillSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _sorter(nullptr),
          _num_rows_skipped(0) {}

SpillSortNode::~SpillSortNode() {}

Status SpillSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;
    return Status::OK();
}

Status SpillSortNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor,
                                             expr_mem_tracker()));
    // AddExprCtxsToFree(_sort_exec_exprs);
    return Status::OK();
}

Status SpillSortNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Spill sort, while open."));
    RETURN_IF_ERROR(child(0)->open(state));

    // These objects must be created after opening the _sort_exec_exprs. Avoid creating
    // them after every reset()/open().
    if (_sorter.get() == nullptr) {
        TupleRowComparator less_than(_sort_exec_exprs, _is_asc_order, _nulls_first);
        // Create and initialize the external sort impl object
        _sorter.reset(new SpillSorter(less_than, _sort_exec_exprs.sort_tuple_slot_expr_ctxs(),
                                      &_row_descriptor, mem_tracker(), runtime_profile(), state));
        RETURN_IF_ERROR(_sorter->init());
    }

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in get_next().
    RETURN_IF_ERROR(sort_input(state));

    // Unless we are inside a subplan expecting to call open()/get_next() on the child
    // again, the child can be closed at this point.
    // if (!IsInSubplan()) {
    child(0)->close(state);
    // }
    return Status::OK();
}

Status SpillSortNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT, state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Spill sort, while getting next."));

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

Status SpillSortNode::reset(RuntimeState* state) {
    _num_rows_skipped = 0;
    if (_sorter.get() != nullptr) {
        _sorter->reset();
    }
    // return ExecNode::reset(state);
    return Status::OK();
}

Status SpillSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _sort_exec_exprs.close(state);
    _sorter.reset();
    ExecNode::close(state);
    return Status::OK();
}

void SpillSortNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SpillSortNode(" << Expr::debug_string(_sort_exec_exprs.lhs_ordering_expr_ctxs());
    for (int i = 0; i < _is_asc_order.size(); ++i) {
        *out << (i > 0 ? " " : "") << (_is_asc_order[i] ? "asc" : "desc") << " nulls "
             << (_nulls_first[i] ? "first" : "last");
    }
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

Status SpillSortNode::sort_input(RuntimeState* state) {
    RowBatch batch(child(0)->row_desc(), state->batch_size());
    bool eos = false;
    do {
        batch.reset();
        RETURN_IF_ERROR(child(0)->get_next(state, &batch, &eos));
        RETURN_IF_ERROR(_sorter->add_batch(&batch));
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("Spill sort, while sorting input."));
    } while (!eos);

    RETURN_IF_ERROR(_sorter->input_done());
    if (_sorter->is_spilled()) {
        add_runtime_exec_option("Spilled");
    }
    return Status::OK();
}

} // end namespace doris
