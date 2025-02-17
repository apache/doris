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

Status MaterializationSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                 bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    if (!local_state._shared_state->rpc_status.ok()) {
        return local_state._shared_state->rpc_status;
    }
    RETURN_IF_ERROR(local_state._shared_state->merge_multi_response(block));
    *eos = local_state._shared_state->last_block;
    if (!*eos) {
        local_state._shared_state->sink_deps.back()->ready();
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris