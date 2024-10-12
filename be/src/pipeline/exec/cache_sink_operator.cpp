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

#include "cache_sink_operator.h"

#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::pipeline {

Status CacheSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _shared_state->data_queue.set_sink_dependency(_dependency, 0);
    return Status::OK();
}

Status CacheSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    //    auto& p = _parent->cast<Parent>();

    _shared_state->data_queue.set_max_blocks_in_sub_queue(state->data_queue_max_blocks());
    return Status::OK();
}

CacheSinkOperatorX::CacheSinkOperatorX(int sink_id, int child_id)
        : Base(sink_id, child_id, child_id) {
    _name = "CACHE_SINK_OPERATOR";
}

Status CacheSinkOperatorX::open(RuntimeState* state) {
    return Status::OK();
}

Status CacheSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (in_block->rows() > 0) {
        local_state._shared_state->data_queue.push_block(
                vectorized::Block::create_unique(std::move(*in_block)), 0);
    }
    if (UNLIKELY(eos)) {
        local_state._shared_state->data_queue.set_finish(0);
    }
    return Status::OK();
}

} // namespace doris::pipeline
