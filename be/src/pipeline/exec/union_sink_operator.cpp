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
    SCOPED_TIMER(_runtime_profile->total_time_counter());
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
        this->_node->materialize_child_block(state, _cur_child_id, in_block, _output_block.get());
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

} // namespace doris::pipeline