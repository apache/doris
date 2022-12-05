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

#include "repeat_operator.h"

#include "vec/exec/vrepeat_node.h"

namespace doris {
namespace pipeline {

RepeatOperator::RepeatOperator(RepeatOperatorBuilder* operator_builder,
                               vectorized::VRepeatNode* repeat_node)
        : Operator(operator_builder), _repeat_node(repeat_node) {}

Status RepeatOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(Operator::open(state));
    _child_block.reset(new vectorized::Block);
    return _repeat_node->alloc_resource(state);
}

Status RepeatOperator::close(RuntimeState* state) {
    _fresh_exec_timer(_repeat_node);
    _repeat_node->release_resource(state);
    Operator::close(state);
    return Status::OK();
}

Status RepeatOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                 SourceState& source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_repeat_node->need_more_input_data()) {
        RETURN_IF_ERROR(_child->get_block(state, _child_block.get(), _child_source_state));
        source_state = _child_source_state;
        if (_child_block->rows() == 0) {
            return Status::OK();
        }

        _repeat_node->push(state, _child_block.get(), source_state == SourceState::FINISHED);
    }

    bool eos = false;
    RETURN_IF_ERROR(_repeat_node->pull(state, block, &eos));
    if (eos) {
        source_state = SourceState::FINISHED;
        _child_block->clear_column_data();
    } else if (!_repeat_node->need_more_input_data()) {
        source_state = SourceState::MORE_DATA;
    } else {
        _child_block->clear_column_data();
    }
    return Status::OK();
}

RepeatOperatorBuilder::RepeatOperatorBuilder(int32_t id, vectorized::VRepeatNode* repeat_node)
        : OperatorBuilder(id, "RepeatOperatorBuilder", repeat_node), _repeat_node(repeat_node) {}

OperatorPtr RepeatOperatorBuilder::build_operator() {
    return std::make_shared<RepeatOperator>(this, _repeat_node);
}
} // namespace pipeline
} // namespace doris
