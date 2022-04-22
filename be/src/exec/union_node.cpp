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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/union-node.cc
// and modified by Doris

#include "exec/union_node.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
// #include "util/runtime_profile_counters.h"
#include "gen_cpp/PlanNodes_types.h"
#include "util/runtime_profile.h"

namespace doris {

UnionNode::UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _tuple_id(tnode.union_node.tuple_id),
          _tuple_desc(nullptr),
          _first_materialized_child_idx(tnode.union_node.first_materialized_child_idx),
          _child_idx(0),
          _child_batch(nullptr),
          _child_row_idx(0),
          _child_eos(false),
          _const_expr_list_idx(0),
          _to_close_child_idx(-1) {}

Status UnionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    // TODO(zc):
    // RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.union_node);
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    // Create const_expr_ctx_lists_ from thrift exprs.
    auto& const_texpr_lists = tnode.union_node.const_expr_lists;
    for (auto& texprs : const_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _const_expr_lists.push_back(ctxs);
    }
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.union_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}

Status UnionNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    DCHECK(_tuple_desc != nullptr);
    _materialize_exprs_evaluate_timer =
            ADD_TIMER(_runtime_profile, "MaterializeExprsEvaluateTimer");
    _codegend_union_materialize_batch_fns.resize(_child_expr_lists.size());
    // Prepare const expr lists.
    for (const std::vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::prepare(exprs, state, row_desc(), expr_mem_tracker()));
        // TODO(zc)
        // AddExprCtxsToFree(exprs);
        DCHECK_EQ(exprs.size(), _tuple_desc->slots().size());
    }

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::prepare(_child_expr_lists[i], state, child(i)->row_desc(),
                                      expr_mem_tracker()));
        // TODO(zc)
        // AddExprCtxsToFree(_child_expr_lists[i]);
        DCHECK_EQ(_child_expr_lists[i].size(), _tuple_desc->slots().size());
    }
    return Status::OK();
}

Status UnionNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    RETURN_IF_ERROR(ExecNode::open(state));
    // open const expr lists.
    for (const std::vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }
    // open result expr lists.
    for (const std::vector<ExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }

    // Ensures that rows are available for clients to fetch after this open() has
    // succeeded.
    if (!_children.empty()) RETURN_IF_ERROR(child(_child_idx)->open(state));

    return Status::OK();
}

Status UnionNode::get_next_pass_through(RuntimeState* state, RowBatch* row_batch) {
    DCHECK(!reached_limit());
    DCHECK(!is_in_subplan());
    DCHECK_LT(_child_idx, _children.size());
    DCHECK(is_child_passthrough(_child_idx));
    // TODO(zc)
    // DCHECK(child(_child_idx)->row_desc().LayoutEquals(row_batch->row_desc()));
    if (_child_eos) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
        _child_eos = false;
    }
    DCHECK_EQ(row_batch->num_rows(), 0);
    RETURN_IF_ERROR(child(_child_idx)->get_next(state, row_batch, &_child_eos));
    if (_child_eos) {
        // Even though the child is at eos, it's not OK to close() it here. Once we close
        // the child, the row batches that it produced are invalid. Marking the batch as
        // needing a deep copy let's us safely close the child in the next get_next() call.
        // TODO: Remove this as part of IMPALA-4179.
        row_batch->mark_needs_deep_copy();
        _to_close_child_idx = _child_idx;
        ++_child_idx;
    }
    return Status::OK();
}

Status UnionNode::get_next_materialized(RuntimeState* state, RowBatch* row_batch) {
    // Fetch from children, evaluate corresponding exprs and materialize.
    DCHECK(!reached_limit());
    DCHECK_LT(_child_idx, _children.size());
    int64_t tuple_buf_size;
    uint8_t* tuple_buf;
    RETURN_IF_ERROR(
            row_batch->resize_and_allocate_tuple_buffer(state, &tuple_buf_size, &tuple_buf));
    memset(tuple_buf, 0, tuple_buf_size);

    while (has_more_materialized() && !row_batch->at_capacity()) {
        // The loop runs until we are either done iterating over the children that require
        // materialization, or the row batch is at capacity.
        DCHECK(!is_child_passthrough(_child_idx));
        // Child row batch was either never set or we're moving on to a different child.
        if (_child_batch.get() == nullptr) {
            DCHECK_LT(_child_idx, _children.size());
            _child_batch.reset(new RowBatch(child(_child_idx)->row_desc(), state->batch_size()));
            _child_row_idx = 0;
            // open the current child unless it's the first child, which was already opened in
            // UnionNode::open().
            if (_child_eos) {
                RETURN_IF_ERROR(child(_child_idx)->open(state));
                _child_eos = false;
            }
            // The first batch from each child is always fetched here.
            RETURN_IF_ERROR(child(_child_idx)->get_next(state, _child_batch.get(), &_child_eos));
        }

        while (!row_batch->at_capacity()) {
            DCHECK(_child_batch.get() != nullptr);
            DCHECK_LE(_child_row_idx, _child_batch->num_rows());
            if (_child_row_idx == _child_batch->num_rows()) {
                // Move on to the next child if it is at eos.
                if (_child_eos) break;
                // Fetch more rows from the child.
                _child_batch->reset();
                _child_row_idx = 0;
                // All batches except the first batch from each child are fetched here.
                RETURN_IF_ERROR(
                        child(_child_idx)->get_next(state, _child_batch.get(), &_child_eos));
                // If we fetched an empty batch, go back to the beginning of this while loop, and
                // try again.
                if (_child_batch->num_rows() == 0) continue;
            }
            DCHECK_EQ(_codegend_union_materialize_batch_fns.size(), _children.size());
            if (_codegend_union_materialize_batch_fns[_child_idx] == nullptr) {
                SCOPED_TIMER(_materialize_exprs_evaluate_timer);
                materialize_batch(row_batch, &tuple_buf);
            } else {
                _codegend_union_materialize_batch_fns[_child_idx](this, row_batch, &tuple_buf);
            }
        }
        // It shouldn't be the case that we reached the limit because we shouldn't have
        // incremented '_num_rows_returned' yet.
        DCHECK(!reached_limit());

        if (_child_eos && _child_row_idx == _child_batch->num_rows()) {
            // Unless we are inside a subplan expecting to call open()/get_next() on the child
            // again, the child can be closed at this point.
            _child_batch.reset();
            if (!is_in_subplan()) child(_child_idx)->close(state);
            ++_child_idx;
        } else {
            // If we haven't finished consuming rows from the current child, we must have ended
            // up here because the row batch is at capacity.
            DCHECK(row_batch->at_capacity());
        }
    }

    DCHECK_LE(_child_idx, _children.size());
    return Status::OK();
}

Status UnionNode::get_next_const(RuntimeState* state, RowBatch* row_batch) {
    DCHECK_EQ(state->per_fragment_instance_idx(), 0);
    DCHECK_LT(_const_expr_list_idx, _const_expr_lists.size());
    // Create new tuple buffer for row_batch.
    int64_t tuple_buf_size;
    uint8_t* tuple_buf;
    RETURN_IF_ERROR(
            row_batch->resize_and_allocate_tuple_buffer(state, &tuple_buf_size, &tuple_buf));
    memset(tuple_buf, 0, tuple_buf_size);

    while (_const_expr_list_idx < _const_expr_lists.size() && !row_batch->at_capacity()) {
        materialize_exprs(_const_expr_lists[_const_expr_list_idx], nullptr, tuple_buf, row_batch);
        RETURN_IF_ERROR(get_error_msg(_const_expr_lists[_const_expr_list_idx]));
        tuple_buf += _tuple_desc->byte_size();
        ++_const_expr_list_idx;
    }

    return Status::OK();
}

Status UnionNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_EXISTED_MEM_TRACKER(mem_tracker());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    // TODO(zc)
    // RETURN_IF_ERROR(QueryMaintenance(state));

    if (_to_close_child_idx != -1) {
        // The previous child needs to be closed if passthrough was enabled for it. In the non
        // passthrough case, the child was already closed in the previous call to get_next().
        DCHECK(is_child_passthrough(_to_close_child_idx));
        DCHECK(!is_in_subplan());
        child(_to_close_child_idx)->close(state);
        _to_close_child_idx = -1;
    }

    // Save the number of rows in case get_next() is called with a non-empty batch, which can
    // happen in a subplan.
    int num_rows_before = row_batch->num_rows();

    if (has_more_passthrough()) {
        RETURN_IF_ERROR(get_next_pass_through(state, row_batch));
    } else if (has_more_materialized()) {
        RETURN_IF_ERROR(get_next_materialized(state, row_batch));
    } else if (has_more_const(state)) {
        RETURN_IF_ERROR(get_next_const(state, row_batch));
    }

    int num_rows_added = row_batch->num_rows() - num_rows_before;
    DCHECK_GE(num_rows_added, 0);
    if (_limit != -1 && _num_rows_returned + num_rows_added > _limit) {
        // Truncate the row batch if we went over the limit.
        num_rows_added = _limit - _num_rows_returned;
        row_batch->set_num_rows(num_rows_before + num_rows_added);
        DCHECK_GE(num_rows_added, 0);
    }
    _num_rows_returned += num_rows_added;

    *eos = reached_limit() ||
           (!has_more_passthrough() && !has_more_materialized() && !has_more_const(state));

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status UnionNode::close(RuntimeState* state) {
    if (is_closed()) return Status::OK();
    _child_batch.reset();
    for (auto& exprs : _const_expr_lists) {
        Expr::close(exprs, state);
    }
    for (auto& exprs : _child_expr_lists) {
        Expr::close(exprs, state);
    }
    return ExecNode::close(state);
}

void UnionNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "_union(_first_materialized_child_idx=" << _first_materialized_child_idx
         << " _row_descriptor=[" << row_desc().debug_string() << "] "
         << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << Expr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}

void UnionNode::materialize_exprs(const std::vector<ExprContext*>& exprs, TupleRow* row,
                                  uint8_t* tuple_buf, RowBatch* dst_batch) {
    DCHECK(!dst_batch->at_capacity());
    Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
    TupleRow* dst_row = dst_batch->get_row(dst_batch->add_row());
    // dst_tuple->materialize_exprs<false, false>(row, *_tuple_desc, exprs,
    dst_tuple->materialize_exprs<false>(row, *_tuple_desc, exprs, dst_batch->tuple_data_pool(),
                                        nullptr, nullptr);
    dst_row->set_tuple(0, dst_tuple);
    dst_batch->commit_last_row();
}

void UnionNode::materialize_batch(RowBatch* dst_batch, uint8_t** tuple_buf) {
    // Take all references to member variables out of the loop to reduce the number of
    // loads and stores.
    RowBatch* child_batch = _child_batch.get();
    int tuple_byte_size = _tuple_desc->byte_size();
    uint8_t* cur_tuple = *tuple_buf;
    const std::vector<ExprContext*>& child_exprs = _child_expr_lists[_child_idx];

    int num_rows_to_process = std::min(child_batch->num_rows() - _child_row_idx,
                                       dst_batch->capacity() - dst_batch->num_rows());
    FOREACH_ROW_LIMIT(child_batch, _child_row_idx, num_rows_to_process, batch_iter) {
        TupleRow* child_row = batch_iter.get();
        materialize_exprs(child_exprs, child_row, cur_tuple, dst_batch);
        cur_tuple += tuple_byte_size;
    }

    _child_row_idx += num_rows_to_process;
    *tuple_buf = cur_tuple;
}

Status UnionNode::get_error_msg(const std::vector<ExprContext*>& exprs) {
    for (auto expr_ctx : exprs) {
        std::string expr_error = expr_ctx->get_error_msg();
        if (!expr_error.empty()) {
            return Status::RuntimeError(expr_error);
        }
    }
    return Status::OK();
}

} // namespace doris
