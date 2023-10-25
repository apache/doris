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

#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h"

namespace doris::pipeline {

Status LocalExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _compute_hash_value_timer = ADD_TIMER(profile(), "ComputeHashValueTime");
    _distribute_timer = ADD_TIMER(profile(), "DistributeDataTime");
    auto& p = _parent->cast<LocalExchangeSinkOperatorX>();
    RETURN_IF_ERROR(p._partitioner->clone(state, _partitioner));
    _mutable_block.resize(p._num_partitions);
    _shared_state->running_sink_operators++;
    return Status::OK();
}

Status LocalExchangeSinkLocalState::channel_add_rows(RuntimeState* state,
                                                     const uint32_t* __restrict channel_ids,
                                                     vectorized::Block* block,
                                                     SourceState source_state) {
    auto& data_queue = _shared_state->data_queue;
    std::vector<int> channel2rows[data_queue.size()];

    auto rows = block->rows();
    for (int i = 0; i < rows; i++) {
        channel2rows[channel_ids[i]].emplace_back(i);
    }
    for (size_t i = 0; i < data_queue.size(); i++) {
        if (_mutable_block[i] == nullptr) {
            _mutable_block[i] = vectorized::MutableBlock::create_unique(block->clone_empty());
        }

        const int* begin = channel2rows[i].data();
        _mutable_block[i]->add_rows(block, begin, begin + channel2rows[i].size());
        if (_mutable_block[i]->rows() > state->batch_size() ||
            source_state == SourceState::FINISHED) {
            data_queue[i].enqueue(_mutable_block[i]->to_block());
            _mutable_block[i].reset(nullptr);
        }
    }

    return Status::OK();
}

Status LocalExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        SourceState source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    {
        SCOPED_TIMER(local_state._compute_hash_value_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, in_block,
                                                                  local_state.mem_tracker()));
    }
    {
        SCOPED_TIMER(local_state._distribute_timer);
        RETURN_IF_ERROR(local_state.channel_add_rows(
                state, (const uint32_t*)local_state._partitioner->get_channel_ids(), in_block,
                source_state));
    }

    if (source_state == SourceState::FINISHED) {
        local_state._shared_state->running_sink_operators--;
    }

    return Status::OK();
}

} // namespace doris::pipeline
