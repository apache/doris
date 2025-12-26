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

#include "blackhole_sink_operator.h"

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

BlackholeSinkOperatorX::BlackholeSinkOperatorX(int operator_id) : Base(operator_id, 0, 0) {}

Status BlackholeSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorXBase::prepare(state));
    return Status::OK();
}

Status BlackholeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorXBase::init(tsink));
    return Status::OK();
}

Status BlackholeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());

    if (block && block->rows() > 0) {
        RETURN_IF_ERROR(_process_block(state, block));
    }

    return Status::OK();
}

Status BlackholeSinkOperatorX::_process_block(RuntimeState* state, vectorized::Block* block) {
    auto& local_state = get_local_state(state);

    // Update metrics - count rows and bytes processed
    local_state._rows_processed += block->rows();
    local_state._bytes_processed += block->bytes();

    // Update runtime counters
    if (local_state._rows_processed_timer) {
        COUNTER_UPDATE(local_state._rows_processed_timer, block->rows());
    }
    if (local_state._bytes_processed_timer) {
        COUNTER_UPDATE(local_state._bytes_processed_timer, block->bytes());
    }

    // The BLACKHOLE discards the data
    // We don't write the block anywhere - it's effectively sent to /dev/null
    return Status::OK();
}

Status BlackholeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    // Initialize performance counters
    _rows_processed_timer = ADD_COUNTER(custom_profile(), "RowsProcessed", TUnit::UNIT);
    _bytes_processed_timer = ADD_COUNTER(custom_profile(), "BytesProcessed", TUnit::BYTES);

    return Status::OK();
}

Status BlackholeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    return Status::OK();
}

Status BlackholeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    return Base::close(state, exec_status);
}

Status BlackholeSinkOperatorX::close(RuntimeState* state) {
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
