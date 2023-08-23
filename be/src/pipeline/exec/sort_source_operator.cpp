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
        : PipelineXLocalState(state, parent), _get_next_timer(nullptr) {}

Status SortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState::init(state, info));
    _dependency = (SortDependency*)info.dependency;
    _shared_state = (SortSharedState*)_dependency->shared_state();
    _get_next_timer = ADD_TIMER(profile(), "GetResultTime");
    return Status::OK();
}

SortSourceOperatorX::SortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs, std::string op_name)
        : OperatorXBase(pool, tnode, descs, op_name) {}

Status SortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    auto& local_state = state->get_local_state(id())->cast<SortLocalState>();
    SCOPED_TIMER(local_state._get_next_timer);
    bool eos;
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
            local_state._shared_state->sorter->get_next(state, block, &eos));
    local_state.reached_limit(block, &eos);
    if (eos) {
        _runtime_profile->add_info_string(
                "Spilled", local_state._shared_state->sorter->is_spilled() ? "true" : "false");
        source_state = SourceState::FINISHED;
    }
    return Status::OK();
}

bool SortSourceOperatorX::can_read(RuntimeState* state) {
    auto& local_state = state->get_local_state(id())->cast<SortLocalState>();
    return local_state._dependency->done();
}

Status SortSourceOperatorX::setup_local_state(RuntimeState* state, LocalStateInfo& info) {
    auto local_state = SortLocalState::create_shared(state, this);
    state->emplace_local_state(id(), local_state);
    return local_state->init(state, info);
}

Status SortSourceOperatorX::close(doris::RuntimeState* state) {
    auto& local_state = state->get_local_state(id())->cast<SortLocalState>();
    local_state._shared_state->sorter = nullptr;
    return Status::OK();
}

} // namespace doris::pipeline
