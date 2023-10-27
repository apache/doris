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
    _shared_state->running_sink_operators++;
    return Status::OK();
}

Status LocalExchangeSinkLocalState::split_rows(RuntimeState* state,
                                               const uint32_t* __restrict channel_ids,
                                               vectorized::Block* block, SourceState source_state) {
    auto& data_queue = _shared_state->data_queue;
    const auto num_partitions = data_queue.size();
    const auto rows = block->rows();
    auto row_idx = std::make_shared<std::vector<int>>(rows);
    {
        _partition_rows_histogram.assign(num_partitions + 1, 0);
        for (size_t i = 0; i < rows; ++i) {
            _partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= num_partitions; ++i) {
            _partition_rows_histogram[i] += _partition_rows_histogram[i - 1];
        }

        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[_partition_rows_histogram[channel_ids[i]] - 1] = i;
            _partition_rows_histogram[channel_ids[i]]--;
        }
    }
    auto new_block = vectorized::Block::create_shared(block->clone_empty());
    new_block->swap(*block);
    for (size_t i = 0; i < num_partitions; i++) {
        size_t start = _partition_rows_histogram[i];
        size_t size = _partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            data_queue[i].enqueue({new_block, {row_idx, start, size}});
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
        RETURN_IF_ERROR(local_state.split_rows(
                state, (const uint32_t*)local_state._partitioner->get_channel_ids(), in_block,
                source_state));
    }

    if (source_state == SourceState::FINISHED) {
        local_state._shared_state->running_sink_operators--;
    }

    return Status::OK();
}

} // namespace doris::pipeline
