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

#include "pipeline/local_exchange/local_exchange_source_operator.h"

#include "pipeline/local_exchange/local_exchanger.h"

namespace doris::pipeline {

Status LocalExchangeSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _channel_id = info.task_idx;
    _shared_state->mem_trackers[_channel_id] = _mem_tracker.get();
    return Status::OK();
}

Status LocalExchangeSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

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

Status LocalExchangeSourceLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }

    if (_exchanger) {
        _exchanger->close(*this);
    }
    if (_shared_state) {
        _shared_state->sub_running_source_operators();
    }

    return Base::close(state);
}

std::vector<Dependency*> LocalExchangeSourceLocalState::dependencies() const {
    auto deps = Base::dependencies();
    auto exchanger_deps = _exchanger->local_state_dependency(_channel_id);
    for (auto* dep : exchanger_deps) {
        deps.push_back(dep);
    }
    return deps;
}

std::string LocalExchangeSourceLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, _channel_id: {}, _num_partitions: {}, _num_senders: {}, _num_sources: {}, "
                   "_running_sink_operators: {}, _running_source_operators: {}, mem_usage: {}",
                   Base::debug_string(indentation_level), _channel_id, _exchanger->_num_partitions,
                   _exchanger->_num_senders, _exchanger->_num_sources,
                   _exchanger->_running_sink_operators, _exchanger->_running_source_operators,
                   _shared_state->mem_usage.load());
    size_t i = 0;
    fmt::format_to(debug_string_buffer, ", MemTrackers: ");
    for (auto* mem_tracker : _shared_state->mem_trackers) {
        fmt::format_to(debug_string_buffer, "{}: {}, ", i, mem_tracker->consumption());
        i++;
    }
    return fmt::to_string(debug_string_buffer);
}

Status LocalExchangeSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                               bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._exchanger->get_block(state, block, eos, local_state));
    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris::pipeline
