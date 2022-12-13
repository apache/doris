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

#include "common/status.h"

namespace doris::pipeline {

UnionSinkOperatorBuilder::UnionSinkOperatorBuilder(int32_t id, int child_id, ExecNode* node,
                                                   std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "UnionSinkOperatorBuilder", node),
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
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    auto output_block = std::make_unique<vectorized::Block>();
    if (_cur_child_id < _node->get_first_materialized_child_idx()) { //pass_through
        output_block->swap(*in_block);
    } else if (_node->get_first_materialized_child_idx() !=
                       _node->children_count() && //need materialized
               _cur_child_id < _node->children_count()) {
        this->_node->materialize_child_block(state, _cur_child_id, in_block, output_block.get());
    } else {
        LOG(WARNING) << "maybe can't reach here, execute const expr: " << _cur_child_id << " "
                     << _node->get_first_materialized_child_idx() << " " << _node->children_count();
    }
    _data_queue->push_block(_cur_child_id, std::move(output_block));

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        this->_node->set_child_close(_cur_child_id);
        //check last child idx and doing const expr
        if ((this->_node->children_count() == (_cur_child_id + 1)) &&
            (this->_node->has_more_const(state))) {
            output_block.reset(new vectorized::Block());
            this->_node->get_next_const(state, output_block.get());
            _data_queue->push_block(_cur_child_id, std::move(output_block));
        }
    }
    return Status::OK();
}

} // namespace doris::pipeline