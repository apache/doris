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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/topn-node.cc
// and modified by Doris

#include "exec/topn_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/tuple_row_compare.h"

namespace doris {

TopNNode::TopNNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _materialized_tuple_desc(nullptr),
          _tuple_row_less_than(nullptr),
          _tuple_pool(nullptr),
          _num_rows_skipped(0),
          _priority_queue(nullptr) {}

TopNNode::~TopNNode() {}

Status TopNNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;

    DCHECK_EQ(_conjuncts.size(), 0) << "TopNNode should never have predicates to evaluate.";
    _abort_on_default_limit_exceeded = tnode.sort_node.is_default_limit;
    return Status::OK();
}

Status TopNNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    _tuple_pool.reset(new MemPool(mem_tracker()));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    // AddExprCtxsToFree(_sort_exec_exprs);

    _tuple_row_less_than.reset(
            new TupleRowComparator(_sort_exec_exprs, _is_asc_order, _nulls_first));

    _abort_on_default_limit_exceeded =
            _abort_on_default_limit_exceeded && state->abort_on_default_limit_exceeded();
    _materialized_tuple_desc = _row_descriptor.tuple_descriptors()[0];
    return Status::OK();
}

Status TopNNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Top n, before open."));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));

    // Avoid creating them after every Reset()/Open().
    // TODO: For some reason initializing _priority_queue in Prepare() causes a 30% perf
    // regression. Why??
    if (_priority_queue == nullptr) {
        _priority_queue.reset(new SortingHeap<Tuple*, std::vector<Tuple*>, TupleRowComparator>(
                *_tuple_row_less_than));
    }

    // Allocate memory for a temporary tuple.
    _tmp_tuple =
            reinterpret_cast<Tuple*>(_tuple_pool->allocate(_materialized_tuple_desc->byte_size()));
    RETURN_IF_ERROR(child(0)->open(state));

    // Limit of 0, no need to fetch anything from children.
    if (_limit != 0) {
        RowBatch batch(child(0)->row_desc(), state->batch_size());
        bool eos = false;

        do {
            batch.reset();
            RETURN_IF_ERROR(child(0)->get_next(state, &batch, &eos));

            if (_abort_on_default_limit_exceeded && child(0)->rows_returned() > _limit) {
                return Status::InternalError("DEFAULT_ORDER_BY_LIMIT has been exceeded.");
            }

            for (int i = 0; i < batch.num_rows(); ++i) {
                insert_tuple_row(batch.get_row(i));
            }
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(state->check_query_state("Top n, while getting next from child 0."));
        } while (!eos);
    }

    DCHECK_LE(_priority_queue->size(), _offset + _limit);
    prepare_for_output();

    // Unless we are inside a subplan expecting to call open()/get_next() on the child
    // again, the child can be closed at this point.
    // if (!is_in_subplan()) {
    child(0)->close(state);
    // }
    return Status::OK();
}

Status TopNNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Top n, before moving result to row_batch."));

    while (!row_batch->at_capacity() && (_get_next_iter != _sorted_top_n.end())) {
        if (_num_rows_skipped < _offset) {
            ++_get_next_iter;
            _num_rows_skipped++;
            continue;
        }

        int row_idx = row_batch->add_row();
        TupleRow* dst_row = row_batch->get_row(row_idx);
        Tuple* src_tuple = *_get_next_iter;
        TupleRow* src_row = reinterpret_cast<TupleRow*>(&src_tuple);
        row_batch->copy_row(src_row, dst_row);
        ++_get_next_iter;
        row_batch->commit_last_row();
        ++_num_rows_returned;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    if (VLOG_ROW_IS_ON) {
        VLOG_ROW << "TOPN-node output row: " << row_batch->to_string();
    }

    *eos = _get_next_iter == _sorted_top_n.end();
    // Transfer ownership of tuple data to output batch.
    // TODO: To improve performance for small inputs when this node is run multiple times
    // inside a subplan, we might choose to only selectively transfer, e.g., when the
    // block(s) in the pool are all full or when the pool has reached a certain size.
    if (*eos) {
        row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), false);
    }
    return Status::OK();
}

Status TopNNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_tuple_pool.get() != nullptr) {
        _tuple_pool->free_all();
    }
    _sort_exec_exprs.close(state);

    return ExecNode::close(state);
}

// Insert if either not at the limit or it's a new TopN tuple_row
void TopNNode::insert_tuple_row(TupleRow* input_row) {
    if (_priority_queue->size() < _offset + _limit) {
        auto insert_tuple = reinterpret_cast<Tuple*>(
                _tuple_pool->allocate(_materialized_tuple_desc->byte_size()));
        insert_tuple->materialize_exprs<false>(input_row, *_materialized_tuple_desc,
                                               _sort_exec_exprs.sort_tuple_slot_expr_ctxs(),
                                               _tuple_pool.get(), nullptr, nullptr);
        _priority_queue->push(insert_tuple);
    } else {
        DCHECK(!_priority_queue->empty());
        Tuple* top_tuple = _priority_queue->top();
        _tmp_tuple->materialize_exprs<false>(input_row, *_materialized_tuple_desc,
                                             _sort_exec_exprs.sort_tuple_slot_expr_ctxs(), nullptr,
                                             nullptr, nullptr);

        if ((*_tuple_row_less_than)(_tmp_tuple, top_tuple)) {
            // TODO: DeepCopy will allocate new buffers for the string data.  This needs
            // to be fixed to use a freelist
            _tmp_tuple->deep_copy(top_tuple, *_materialized_tuple_desc, _tuple_pool.get());
            auto insert_tuple = top_tuple;
            _priority_queue->replace_top(insert_tuple);
        }
    }
}

// Reverse the order of the tuples in the priority queue
void TopNNode::prepare_for_output() {
    _sorted_top_n = _priority_queue->sorted_seq();

    _get_next_iter = _sorted_top_n.begin();
}

void TopNNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "TopNNode("
         // << " ordering_exprs=" << Expr::debug_string(_lhs_ordering_expr_ctxs)
         << Expr::debug_string(_sort_exec_exprs.lhs_ordering_expr_ctxs()) << " sort_order=[";

    for (int i = 0; i < _is_asc_order.size(); ++i) {
        *out << (i > 0 ? " " : "") << (_is_asc_order[i] ? "asc" : "desc") << " nulls "
             << (_nulls_first[i] ? "first" : "last");
    }

    *out << "]";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

void TopNNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    std::list<ExprContext*>::iterator iter = expr_ctxs->begin();
    while (iter != expr_ctxs->end()) {
        if ((*iter)->root()->is_bound(&_tuple_ids)) {
            // LOG(INFO) << "push down success expr is " << (*iter)->debug_string();
            // (*iter)->get_child(0)->prepare(state, row_desc());
            (*iter)->prepare(state, row_desc());
            (*iter)->open(state);
            _conjunct_ctxs.push_back(*iter);
            iter = expr_ctxs->erase(iter);
        } else {
            ++iter;
        }
    }
}

} // namespace doris
