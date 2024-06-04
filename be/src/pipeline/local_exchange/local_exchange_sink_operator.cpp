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

#include "pipeline/local_exchange/local_exchange_sink_operator.h"

#include "pipeline/local_exchange/local_exchanger.h"

namespace doris::pipeline {

Status LocalExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _compute_hash_value_timer = ADD_TIMER(profile(), "ComputeHashValueTime");
    _distribute_timer = ADD_TIMER(profile(), "DistributeDataTime");
    return Status::OK();
}

Status LocalExchangeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    _exchanger = _shared_state->exchanger.get();
    DCHECK(_exchanger != nullptr);

    if (_exchanger->get_type() == ExchangeType::HASH_SHUFFLE ||
        _exchanger->get_type() == ExchangeType::BUCKET_HASH_SHUFFLE) {
        auto& p = _parent->cast<LocalExchangeSinkOperatorX>();
        RETURN_IF_ERROR(p._partitioner->clone(state, _partitioner));
    }

    return Status::OK();
}

Status LocalExchangeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(Base::close(state, exec_status));
    if (exec_status.ok()) {
        DCHECK(_release_count) << "Do not finish correctly! " << debug_string(0)
                               << " state: { cancel = " << state->is_cancelled() << ", "
                               << state->cancel_reason().to_string() << "} query ctx: { cancel = "
                               << state->get_query_ctx()->is_cancelled() << ", "
                               << state->get_query_ctx()->exec_status().to_string() << "}";
    }
    return Status::OK();
}

std::string LocalExchangeSinkLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, _channel_id: {}, _num_partitions: {}, _num_senders: {}, _num_sources: {}, "
                   "_running_sink_operators: {}, _running_source_operators: {}, _release_count: {}",
                   Base::debug_string(indentation_level), _channel_id, _exchanger->_num_partitions,
                   _exchanger->_num_senders, _exchanger->_num_sources,
                   _exchanger->_running_sink_operators, _exchanger->_running_source_operators,
                   _release_count);
    return fmt::to_string(debug_string_buffer);
}

Status LocalExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    RETURN_IF_ERROR(local_state._exchanger->sink(state, in_block, eos, local_state));

    // If all exchange sources ended due to limit reached, current task should also finish
    if (local_state._exchanger->_running_source_operators == 0) {
        local_state._release_count = true;
        local_state._shared_state->sub_running_sink_operators();
        return Status::EndOfFile("receiver eof");
    }
    if (eos) {
        local_state._shared_state->sub_running_sink_operators();
        local_state._release_count = true;
    }

    return Status::OK();
}

} // namespace doris::pipeline
