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

#include "sort_source_operator.h"

#include <string>

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(SortSourceOperator, SourceOperator)

SortLocalState::SortLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<SortDependency>(state, parent), _get_next_timer(nullptr) {}

Status SortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<SortDependency>::init(state, info));
    _get_next_timer = ADD_TIMER(profile(), "GetResultTime");
    return Status::OK();
}

SortSourceOperatorX::SortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : OperatorX<SortLocalState>(pool, tnode, descs) {}

Status SortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    auto& local_state = state->get_local_state(id())->cast<SortLocalState>();
    SCOPED_TIMER(local_state._get_next_timer);
    bool eos;
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
            local_state._shared_state->sorter->get_next(state, block, &eos));
    local_state.reached_limit(block, &eos);
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    return Status::OK();
}

Dependency* SortSourceOperatorX::wait_for_dependency(RuntimeState* state) {
    auto& local_state = state->get_local_state(id())->cast<SortLocalState>();
    return local_state._dependency->read_blocked_by();
}

Status SortLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    _shared_state->sorter = nullptr;
    return PipelineXLocalState<SortDependency>::close(state);
}

} // namespace doris::pipeline
