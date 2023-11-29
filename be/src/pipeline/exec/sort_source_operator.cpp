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
        : PipelineXLocalState<SortSourceDependency>(state, parent) {}

SortSourceOperatorX::SortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                         const DescriptorTbl& descs)
        : OperatorX<SortLocalState>(pool, tnode, operator_id, descs) {}

Status SortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    bool eos = false;
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
            local_state._shared_state->sorter->get_next(state, block, &eos));
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    local_state.reached_limit(block, source_state);
    return Status::OK();
}

} // namespace doris::pipeline
