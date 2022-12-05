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
// https://github.com/cloudera/Impala/blob/v0.7refresh/be/src/exec/merge-node.cc
// and modified by Doris

#include "exec/merge_node.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

using std::vector;

namespace doris {

MergeNode::MergeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _tuple_id(tnode.merge_node.tuple_id),
          _const_result_expr_idx(0),
          _child_idx(INVALID_CHILD_IDX),
          _child_row_batch(nullptr),
          _child_eos(false),
          _child_row_idx(0) {}

Status MergeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.merge_node);
    // Create _const_expr_lists from thrift exprs.
    const std::vector<vector<TExpr>>& const_texpr_lists = tnode.merge_node.const_expr_lists;
    for (int i = 0; i < const_texpr_lists.size(); ++i) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, const_texpr_lists[i], &ctxs));
        _const_result_expr_ctx_lists.push_back(ctxs);
    }
    // Create _result_expr__ctx_lists from thrift exprs.
    const std::vector<vector<TExpr>>& result_texpr_lists = tnode.merge_node.result_expr_lists;
    for (int i = 0; i < result_texpr_lists.size(); ++i) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, result_texpr_lists[i], &ctxs));
        _result_expr_ctx_lists.push_back(ctxs);
    }

    return Status::OK();
}

Status MergeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    DCHECK(_tuple_desc != nullptr);

    // Prepare const expr lists.
    for (int i = 0; i < _const_result_expr_ctx_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::prepare(_const_result_expr_ctx_lists[i], state, row_desc()));
        DCHECK_EQ(_const_result_expr_ctx_lists[i].size(), _tuple_desc->slots().size());
    }

    // prepare materialized_slots_
    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _tuple_desc->slots()[i];
        if (desc->is_materialized()) {
            _materialized_slots.push_back(desc);
        }
    }

    // Prepare result expr lists.
    for (int i = 0; i < _result_expr_ctx_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::prepare(_result_expr_ctx_lists[i], state, child(i)->row_desc()));
        // DCHECK_EQ(_result_expr_ctx_lists[i].size(), _tuple_desc->slots().size());
        DCHECK_EQ(_result_expr_ctx_lists[i].size(), _materialized_slots.size());
    }

    return Status::OK();
}

Status MergeNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    // Prepare const expr lists.
    for (int i = 0; i < _const_result_expr_ctx_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_const_result_expr_ctx_lists[i], state));
    }

    // Prepare result expr lists.
    for (int i = 0; i < _result_expr_ctx_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_result_expr_ctx_lists[i], state));
    }

    return Status::OK();
}

Status MergeNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    // Create new tuple buffer for row_batch.
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = row_batch->tuple_data_pool()->allocate(tuple_buffer_size);
    bzero(tuple_buffer, tuple_buffer_size);
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

    // Evaluate and materialize the const expr lists exactly once.
    while (_const_result_expr_idx < _const_result_expr_ctx_lists.size()) {
        // Materialize expr results into row_batch.
        eval_and_materialize_exprs(_const_result_expr_ctx_lists[_const_result_expr_idx], true,
                                   &tuple, row_batch);
        ++_const_result_expr_idx;
        *eos = reached_limit();

        if (*eos || row_batch->is_full()) {
            return Status::OK();
        }
    }

    if (_child_idx == INVALID_CHILD_IDX) {
        _child_idx = 0;
    }

    // Fetch from children, evaluate corresponding exprs and materialize.
    while (_child_idx < _children.size()) {
        // Row batch was either never set or we're moving on to a different child.
        if (_child_row_batch.get() == nullptr) {
            RETURN_IF_CANCELLED(state);
            _child_row_batch.reset(
                    new RowBatch(child(_child_idx)->row_desc(), state->batch_size()));
            // Open child and fetch the first row batch.
            RETURN_IF_ERROR(child(_child_idx)->open(state));
            RETURN_IF_ERROR(
                    child(_child_idx)->get_next(state, _child_row_batch.get(), &_child_eos));
            _child_row_idx = 0;
        }

        // Start (or continue) consuming row batches from current child.
        while (true) {
            // Continue materializing exprs on _child_row_batch into row batch.
            if (eval_and_materialize_exprs(_result_expr_ctx_lists[_child_idx], false, &tuple,
                                           row_batch)) {
                *eos = reached_limit();

                if (*eos) {
                    _child_idx = INVALID_CHILD_IDX;
                }

                return Status::OK();
            }

            // Fetch new batch if one is available, otherwise move on to next child.
            if (_child_eos) {
                break;
            }

            RETURN_IF_CANCELLED(state);
            _child_row_batch->reset();
            RETURN_IF_ERROR(
                    child(_child_idx)->get_next(state, _child_row_batch.get(), &_child_eos));
            _child_row_idx = 0;
        }

        // Close current child and move on to next one.
        ++_child_idx;
        _child_row_batch.reset(nullptr);
    }

    _child_idx = INVALID_CHILD_IDX;
    *eos = true;
    return Status::OK();
}

Status MergeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    // don't call ExecNode::close(), it always closes all children
    _child_row_batch.reset(nullptr);
    for (int i = 0; i < _const_result_expr_ctx_lists.size(); ++i) {
        Expr::close(_const_result_expr_ctx_lists[i], state);
    }
    for (int i = 0; i < _result_expr_ctx_lists.size(); ++i) {
        Expr::close(_result_expr_ctx_lists[i], state);
    }

    return ExecNode::close(state);
}

bool MergeNode::eval_and_materialize_exprs(const std::vector<ExprContext*>& ctxs, bool const_exprs,
                                           Tuple** tuple, RowBatch* row_batch) {
    // Make sure there are rows left in the batch.
    if (!const_exprs && _child_row_idx >= _child_row_batch->num_rows()) {
        return false;
    }

    // Execute the body at least once.
    bool done = true;
    ExprContext* const* conjunct_ctxs = &_conjunct_ctxs[0];
    int num_conjunct_ctxs = _conjunct_ctxs.size();

    do {
        TupleRow* child_row = nullptr;

        if (!const_exprs) {
            DCHECK(_child_row_batch != nullptr);
            // Non-const expr list. Fetch next row from batch.
            child_row = _child_row_batch->get_row(_child_row_idx);
            ++_child_row_idx;
            done = _child_row_idx >= _child_row_batch->num_rows();
        }

        // Add a new row to the batch.
        int row_idx = row_batch->add_row();
        DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
        TupleRow* row = row_batch->get_row(row_idx);
        row->set_tuple(0, *tuple);

        // Materialize expr results into tuple.
        for (int i = 0; i < ctxs.size(); ++i) {
            SlotDescriptor* slot_desc = _tuple_desc->slots()[i];
            RawValue::write(ctxs[i]->get_value(child_row), *tuple, slot_desc,
                            row_batch->tuple_data_pool());
        }

        if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, row)) {
            row_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            char* new_tuple = reinterpret_cast<char*>(*tuple);
            new_tuple += _tuple_desc->byte_size();
            *tuple = reinterpret_cast<Tuple*>(new_tuple);
        } else {
            // Make sure to reset null indicators since we're overwriting
            // the tuple assembled for the previous row.
            (*tuple)->init(_tuple_desc->byte_size());
        }

        if (row_batch->is_full() || row_batch->at_resource_limit() || reached_limit()) {
            return true;
        }
    } while (!done);

    return false;
}

} // namespace doris
