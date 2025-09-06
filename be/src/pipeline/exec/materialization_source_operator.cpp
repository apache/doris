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
#include "common/compile_check_avoid_begin.h"

Status MaterializationSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                 bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (!local_state._shared_state->rpc_status.ok()) {
        return local_state._shared_state->rpc_status.status();
    }

    // clear origin block, do merge response to build a ret block
    block->clear();
    if (local_state._shared_state->need_merge_block) {
        SCOPED_TIMER(local_state._merge_response_timer);
        RETURN_IF_ERROR(local_state._shared_state->merge_multi_response(block));
    }
    *eos = local_state._shared_state->last_block;

    if (!*eos) {
        ((CountedFinishDependency*)(local_state._shared_state->source_deps.back().get()))
                ->add(local_state._shared_state->rpc_struct_map.size());

        local_state._shared_state->sink_deps.back()->set_ready();
    } else {
        uint64_t max_rpc_time = 0;
        for (auto& [_, rpc_struct] : local_state._shared_state->rpc_struct_map) {
            max_rpc_time = std::max(max_rpc_time, rpc_struct.rpc_timer.elapsed_time());
        }
        COUNTER_SET(local_state._max_rpc_timer, (int64_t)max_rpc_time);

        for (const auto& [backend_id, child_info] :
             local_state._shared_state->backend_profile_info_string) {
            auto child_profile = local_state.operator_profile()->create_child(
                    "RowIDFetcher: BackendId:" + std::to_string(backend_id));
            for (const auto& [info_key, info_value] :
                 local_state._shared_state->backend_profile_info_string[backend_id]) {
                child_profile->add_info_string(info_key, "{" + fmt::to_string(info_value) + "}");
            }
            local_state.operator_profile()->add_child(child_profile, true);
        }
    }

    return Status::OK();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris::pipeline