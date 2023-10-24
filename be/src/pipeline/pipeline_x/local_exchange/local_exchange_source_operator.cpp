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

#include "pipeline/pipeline_x/local_exchange/local_exchange_source_operator.h"

namespace doris::pipeline {

Status LocalExchangeSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _dependency->set_shared_state(info.local_exchange_state);
    _shared_state = (LocalExchangeSharedState*)_dependency->shared_state();
    DCHECK(_shared_state != nullptr);
    _channel_id = info.task_idx;
    _dependency->set_channel_id(_channel_id);
    _get_block_failed_counter =
            ADD_COUNTER_WITH_LEVEL(profile(), "GetBlockFailedTime", TUnit::UNIT, 1);
    return Status::OK();
}

Status LocalExchangeSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                               SourceState& source_state) {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    if (!local_state._shared_state->data_queue[local_state._channel_id].try_dequeue(*block)) {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        if (local_state._shared_state->running_sink_operators == 0) {
            source_state = SourceState::FINISHED;
        }
    }

    local_state.reached_limit(block, source_state);

    return Status::OK();
}

} // namespace doris::pipeline
