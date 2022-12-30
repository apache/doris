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

#include <opentelemetry/common/threadlocal.h>

#include "common/status.h"
#include "pipeline/exec/data_queue.h"

namespace doris {
namespace vectorized {
class Block;
}

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

// we assumed it can read to process const expr， Although we don't know whether there is
// ,and queue have data, could read also
bool UnionSourceOperator::can_read() {
    return _need_read_for_const_expr || _data_queue->remaining_has_data();
}

Status UnionSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    // here we precess const expr firstly
    if (_need_read_for_const_expr) {
        if (this->_node->has_more_const(state)) {
            this->_node->get_next_const(state, block);
        }
        _need_read_for_const_expr = this->_node->has_more_const(state);
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

    bool reached_limit = false;
    this->_node->reached_limit(block, &reached_limit);
    //have exectue const expr, queue have no data any more, and child could be colsed
    source_state = ((!_need_read_for_const_expr && !_data_queue->remaining_has_data() &&
                     _data_queue->is_all_finish()) ||
                    reached_limit)
                           ? SourceState::FINISHED
                           : SourceState::DEPEND_ON_SOURCE;
    return Status::OK();
}
} // namespace pipeline
} // namespace doris