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

#include "pipeline/exec/union_source_operator.h"

#include <functional>
#include <utility>

#include "common/status.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

UnionSourceOperatorBuilder::UnionSourceOperatorBuilder(int32_t id, ExecNode* node,
                                                       std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "UnionSourceOperator", node), _data_queue(queue) {};

OperatorPtr UnionSourceOperatorBuilder::build_operator() {
    return std::make_shared<UnionSourceOperator>(this, _node, _data_queue);
}

UnionSourceOperator::UnionSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* node,
                                         std::shared_ptr<DataQueue> queue)
        : SourceOperator(operator_builder, node),
          _data_queue(queue),
          _need_read_for_const_expr(true) {};

bool UnionSourceOperator::_has_data() {
    return _need_read_for_const_expr || _data_queue->remaining_has_data();
}

// we assumed it can read to process const expr， Although we don't know whether there is
// ,and queue have data, could read also
bool UnionSourceOperator::can_read() {
    return _has_data() || _data_queue->is_all_finish();
}

Status UnionSourceOperator::pull_data(RuntimeState* state, vectorized::Block* block, bool* eos) {
    // here we precess const expr firstly
    if (_need_read_for_const_expr) {
        if (_node->has_more_const(state)) {
            _node->get_next_const(state, block);
        }
        _need_read_for_const_expr = _node->has_more_const(state);
    } else {
        std::unique_ptr<vectorized::Block> output_block;
        int child_idx = 0;
        _data_queue->get_block_from_queue(&output_block, &child_idx);
        if (!output_block) {
            return Status::OK();
        }
        block->swap(*output_block);
        output_block->clear_column_data(_node->row_desc().num_materialized_slots());
        _data_queue->push_free_block(std::move(output_block), child_idx);
    }

    _node->reached_limit(block, eos);
    return Status::OK();
}

Status UnionSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    bool eos = false;
    RETURN_IF_ERROR(_node->get_next_after_projects(
            state, block, &eos,
            std::bind(&UnionSourceOperator::pull_data, this, std::placeholders::_1,
                      std::placeholders::_2, std::placeholders::_3)));
    //have exectue const expr, queue have no data any more, and child could be colsed
    if (eos || (!_has_data() && _data_queue->is_all_finish())) {
        source_state = SourceState::FINISHED;
    } else if (_has_data()) {
        source_state = SourceState::MORE_DATA;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }

    return Status::OK();
}

Status UnionSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<Parent>();
    std::shared_ptr<DataQueue> data_queue =
            std::make_shared<DataQueue>(p._child_size, nullptr, _dependency);
    _shared_state->_data_queue.swap(data_queue);
    return Status::OK();
}

Status UnionSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                       SourceState& source_state) {
    auto& local_state = state->get_local_state(id())->cast<UnionSourceLocalState>();
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    std::unique_ptr<vectorized::Block> output_block = vectorized::Block::create_unique();
    int child_idx = 0;
    local_state._shared_state->_data_queue->get_block_from_queue(&output_block, &child_idx);
    if (!output_block) {
        return Status::OK();
    }
    block->swap(*output_block);
    output_block->clear_column_data(row_desc().num_materialized_slots());
    local_state._shared_state->_data_queue->push_free_block(std::move(output_block), child_idx);

    local_state.reached_limit(block, source_state);
    //have exectue const expr, queue have no data any more, and child could be colsed
    if ((!_has_data(state) && local_state._shared_state->_data_queue->is_all_finish())) {
        source_state = SourceState::FINISHED;
    } else if (_has_data(state)) {
        source_state = SourceState::MORE_DATA;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
