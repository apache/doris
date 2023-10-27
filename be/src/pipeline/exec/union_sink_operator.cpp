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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

UnionSinkOperatorBuilder::UnionSinkOperatorBuilder(int32_t id, int child_id, ExecNode* node,
                                                   std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "UnionSinkOperator", node),
          _cur_child_id(child_id),
          _data_queue(queue) {};

UnionSinkOperator::UnionSinkOperator(OperatorBuilderBase* operator_builder, int child_id,
                                     ExecNode* node, std::shared_ptr<DataQueue> queue)
        : StreamingOperator(operator_builder, node), _cur_child_id(child_id), _data_queue(queue) {};

OperatorPtr UnionSinkOperatorBuilder::build_operator() {
    return std::make_shared<UnionSinkOperator>(this, _cur_child_id, _node, _data_queue);
}

Status UnionSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                               SourceState source_state) {
    if (_output_block == nullptr) {
        _output_block = _data_queue->get_free_block(_cur_child_id);
    }

    if (_cur_child_id < _node->get_first_materialized_child_idx()) { //pass_through
        if (in_block->rows() > 0) {
            _output_block->swap(*in_block);
            _data_queue->push_block(std::move(_output_block), _cur_child_id);
        }
    } else if (_node->get_first_materialized_child_idx() != _node->children_count() &&
               _cur_child_id < _node->children_count()) { //need materialized
        RETURN_IF_ERROR(this->_node->materialize_child_block(state, _cur_child_id, in_block,
                                                             _output_block.get()));
    } else {
        return Status::InternalError("maybe can't reach here, execute const expr: {}, {}, {}",
                                     _cur_child_id, _node->get_first_materialized_child_idx(),
                                     _node->children_count());
    }

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        //if _cur_child_id eos, need check to push block
        //Now here can't check _output_block rows, even it's row==0, also need push block
        //because maybe sink is eos and queue have none data, if not push block
        //the source can't can_read again and can't set source finished
        if (_output_block) {
            _data_queue->push_block(std::move(_output_block), _cur_child_id);
        }
        _data_queue->set_finish(_cur_child_id);
        return Status::OK();
    }
    // not eos and block rows is enough to output,so push block
    if (_output_block && (_output_block->rows() >= state->batch_size())) {
        _data_queue->push_block(std::move(_output_block), _cur_child_id);
    }
    return Status::OK();
}

Status UnionSinkOperator::close(RuntimeState* state) {
    if (_data_queue && !_data_queue->is_finish(_cur_child_id)) {
        // finish should be set, if not set here means error.
        _data_queue->set_canceled(_cur_child_id);
    }
    return StreamingOperator::close(state);
}

Status UnionSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<Parent>();
    _child_expr.resize(p._child_expr.size());
    for (size_t i = 0; i < p._child_expr.size(); i++) {
        RETURN_IF_ERROR(p._child_expr[i]->clone(state, _child_expr[i]));
    }
    return Status::OK();
};

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
    return Status::OK();
}

Status UnionSinkOperatorX::open(RuntimeState* state) {
    // open const expr lists.
    RETURN_IF_ERROR(vectorized::VExpr::open(_const_expr, state));

    // open result expr lists.
    RETURN_IF_ERROR(vectorized::VExpr::open(_child_expr, state));

    return Status::OK();
}

Status UnionSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                SourceState source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
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
    if (UNLIKELY(source_state == SourceState::FINISHED)) {
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
