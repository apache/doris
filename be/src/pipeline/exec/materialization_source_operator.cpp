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

#include "pipeline/exec/materialization_source_operator.h"

#include <utility>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

Status MaterializationSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    ((DataQueueSharedState*)_dependency->shared_state())
            ->data_queue.set_source_dependency(_shared_state->source_deps.front());
    return Status::OK();
}

Status MaterializationSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

Status MaterializationSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                 bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    std::unique_ptr<vectorized::Block> output_block;
    int child_idx = 0;
    RETURN_IF_ERROR(
            local_state._shared_state->data_queue.get_block_from_queue(&output_block, &child_idx));
    *eos = !_has_data(state) && local_state._shared_state->data_queue.is_all_finish();

    if (!output_block) {
        return Status::OK();
    }

    *block = std::move(*output_block);
    return Status::OK();
}

} // namespace pipeline
} // namespace doris