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
// The source operator's run dependences on Node's alloc_resource, which is called in Sink's open.
// So hang until SinkOperator was scheduled to open.
bool UnionSourceOperator::can_read() {
    return _node->resource_allocated() && (_has_data() || _data_queue->is_all_finish());
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
        output_block->clear_column_data(_node->intermediate_row_desc().num_materialized_slots());
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
    //have executing const expr, queue have no data anymore, and child could be closed.
    if (eos) { // reach limit
        source_state = SourceState::FINISHED;
    } else if (_has_data()) {
        source_state = SourceState::MORE_DATA;
    } else if (_data_queue->is_all_finish()) {
        // Here, check the value of `_has_data(state)` again after `data_queue.is_all_finish()` is TRUE
        // as there may be one or more blocks when `data_queue.is_all_finish()` is TRUE.
        source_state = _has_data() ? SourceState::MORE_DATA : SourceState::FINISHED;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }

    return Status::OK();
}
} // namespace pipeline
} // namespace doris