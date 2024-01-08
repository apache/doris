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

#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"

namespace doris::pipeline {

void LocalExchangeSourceDependency::block() {
    if (((LocalExchangeSharedState*)_shared_state.get())->exchanger->_running_sink_operators == 0) {
        return;
    }
    std::unique_lock<std::mutex> lc(((LocalExchangeSharedState*)_shared_state.get())->le_lock);
    if (((LocalExchangeSharedState*)_shared_state.get())->exchanger->_running_sink_operators == 0) {
        return;
    }
    Dependency::block();
}

Status LocalExchangeSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _channel_id = info.task_idx;
    _shared_state->set_dep_by_channel_id(info.dependency, _channel_id);
    _shared_state->mem_trackers[_channel_id] = _mem_tracker.get();
    _exchanger = _shared_state->exchanger.get();
    DCHECK(_exchanger != nullptr);
    _get_block_failed_counter =
            ADD_COUNTER_WITH_LEVEL(profile(), "GetBlockFailedTime", TUnit::UNIT, 1);
    if (_exchanger->get_type() == ExchangeType::HASH_SHUFFLE ||
        _exchanger->get_type() == ExchangeType::BUCKET_HASH_SHUFFLE) {
        _copy_data_timer = ADD_TIMER(profile(), "CopyDataTime");
    }

    return Status::OK();
}

std::string LocalExchangeSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, _channel_id: {}, _num_partitions: {}, _num_senders: {}, _num_sources: {}",
                   Base::debug_string(indentation_level), _channel_id, _exchanger->_num_partitions,
                   _exchanger->_num_senders, _exchanger->_num_sources,
                   _exchanger->_running_sink_operators);
    return fmt::to_string(debug_string_buffer);
}

Status LocalExchangeSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                               SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._exchanger->get_block(state, block, source_state, local_state));
    local_state.reached_limit(block, source_state);
    return Status::OK();
}

} // namespace doris::pipeline
