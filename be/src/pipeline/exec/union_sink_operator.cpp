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

#include "union_sink_operator.h"

#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::pipeline {

Status UnionSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<Parent>();
    _shared_state->data_queue.set_sink_dependency(_dependency, p._cur_child_id);
    return Status::OK();
}

Status UnionSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<Parent>();
    _child_expr.resize(p._child_expr.size());
    for (size_t i = 0; i < p._child_expr.size(); i++) {
        RETURN_IF_ERROR(p._child_expr[i]->clone(state, _child_expr[i]));
    }
    _shared_state->data_queue.set_max_blocks_in_sub_queue(state->data_queue_max_blocks());
    return Status::OK();
}

UnionSinkOperatorX::UnionSinkOperatorX(int child_id, int sink_id, ObjectPool* pool,
                                       const TPlanNode& tnode, const DescriptorTbl& descs)
        : Base(sink_id, tnode.node_id, tnode.node_id),
          _first_materialized_child_idx(tnode.union_node.first_materialized_child_idx),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _cur_child_id(child_id),
          _child_size(tnode.num_children) {}

Status UnionSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.union_node);
    {
        // Create result_expr_ctx_lists_ from thrift exprs.
        auto& result_texpr_lists = tnode.union_node.result_expr_lists;
        auto& texprs = result_texpr_lists[_cur_child_id];
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texprs, ctxs));
        _child_expr = ctxs;
    }
    return Status::OK();
}

Status UnionSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_expr, state, _child_x->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::check_expr_output_type(_child_expr, _row_descriptor));
    return Status::OK();
}

Status UnionSinkOperatorX::open(RuntimeState* state) {
    // open const expr lists.
    RETURN_IF_ERROR(vectorized::VExpr::open(_const_expr, state));

    // open result expr lists.
    RETURN_IF_ERROR(vectorized::VExpr::open(_child_expr, state));

    return Status::OK();
}

Status UnionSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (local_state._output_block == nullptr) {
        local_state._output_block =
                local_state._shared_state->data_queue.get_free_block(_cur_child_id);
    }
    if (_cur_child_id < _get_first_materialized_child_idx()) { //pass_through
        if (in_block->rows() > 0) {
            local_state._output_block->swap(*in_block);
            local_state._shared_state->data_queue.push_block(std::move(local_state._output_block),
                                                             _cur_child_id);
        }
    } else if (_get_first_materialized_child_idx() != children_count() &&
               _cur_child_id < children_count()) { //need materialized
        RETURN_IF_ERROR(materialize_child_block(state, _cur_child_id, in_block,
                                                local_state._output_block.get()));
    } else {
        return Status::InternalError("maybe can't reach here, execute const expr: {}, {}, {}",
                                     _cur_child_id, _get_first_materialized_child_idx(),
                                     children_count());
    }
    if (UNLIKELY(eos)) {
        //if _cur_child_id eos, need check to push block
        //Now here can't check _output_block rows, even it's row==0, also need push block
        //because maybe sink is eos and queue have none data, if not push block
        //the source can't can_read again and can't set source finished
        if (local_state._output_block) {
            local_state._shared_state->data_queue.push_block(std::move(local_state._output_block),
                                                             _cur_child_id);
        }

        local_state._shared_state->data_queue.set_finish(_cur_child_id);
        return Status::OK();
    }
    // not eos and block rows is enough to output,so push block
    if (local_state._output_block && (local_state._output_block->rows() >= state->batch_size())) {
        local_state._shared_state->data_queue.push_block(std::move(local_state._output_block),
                                                         _cur_child_id);
    }
    return Status::OK();
}

} // namespace doris::pipeline
