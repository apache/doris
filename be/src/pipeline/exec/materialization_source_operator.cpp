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
#include "vec/core/block.h"

namespace doris::pipeline {

Status MaterializationSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                 bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (!local_state._shared_state->rpc_status.ok()) {
        return local_state._shared_state->rpc_status;
    }

    // clear origin block, do merge response to build a ret block
    block->clear();
    if (local_state._shared_state->need_merge_block) {
        SCOPED_TIMER(local_state._merge_response_timer);
        RETURN_IF_ERROR(local_state._shared_state->merge_multi_response(block));
    }
    *eos = local_state._shared_state->last_block;

    if (!*eos) {
        local_state._shared_state->sink_deps.back()->set_ready();

        ((CountedFinishDependency*)(local_state._shared_state->source_deps.back().get()))
                ->add(local_state._shared_state->rpc_struct_map.size());
    } else {
        uint64_t max_rpc_time = 0;
        for (auto& [_, rpc_struct] : local_state._shared_state->rpc_struct_map) {
            max_rpc_time = std::max(max_rpc_time, rpc_struct.rpc_timer.elapsed_time());
        }
        COUNTER_SET(local_state._max_rpc_timer, (int64_t)max_rpc_time);
    }

    return Status::OK();
}

} // namespace doris::pipeline