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

#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"

#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_source_operator.h"

namespace doris::pipeline {

Status ShuffleExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                              SourceState source_state, LocalExchangeSinkLocalState& local_state) {
    {
        SCOPED_TIMER(local_state._compute_hash_value_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, in_block,
                                                                  local_state.mem_tracker()));
    }
    {
        SCOPED_TIMER(local_state._distribute_timer);
        RETURN_IF_ERROR(_split_rows(state,
                                    (const uint32_t*)local_state._partitioner->get_channel_ids(),
                                    in_block, source_state, local_state));
    }

    return Status::OK();
}

Status ShuffleExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                   SourceState& source_state,
                                   LocalExchangeSourceLocalState& local_state) {
    PartitionedBlock partitioned_block;
    std::unique_ptr<vectorized::MutableBlock> mutable_block = nullptr;

    auto get_data = [&](vectorized::Block* result_block) {
        do {
            const auto* offset_start = &((
                    *std::get<0>(partitioned_block.second))[std::get<1>(partitioned_block.second)]);
            mutable_block->add_rows(partitioned_block.first.get(), offset_start,
                                    offset_start + std::get<2>(partitioned_block.second));
        } while (mutable_block->rows() < state->batch_size() &&
                 _data_queue[local_state._channel_id].try_dequeue(partitioned_block));
        *result_block = mutable_block->to_block();
    };
    if (running_sink_operators == 0) {
        if (_data_queue[local_state._channel_id].try_dequeue(partitioned_block)) {
            SCOPED_TIMER(local_state._copy_data_timer);
            mutable_block =
                    vectorized::MutableBlock::create_unique(partitioned_block.first->clone_empty());
            get_data(block);
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[local_state._channel_id].try_dequeue(partitioned_block)) {
        SCOPED_TIMER(local_state._copy_data_timer);
        mutable_block =
                vectorized::MutableBlock::create_unique(partitioned_block.first->clone_empty());
        get_data(block);
    } else {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return Status::OK();
}

Status ShuffleExchanger::_split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                                     vectorized::Block* block, SourceState source_state,
                                     LocalExchangeSinkLocalState& local_state) {
    auto& data_queue = _data_queue;
    const auto rows = block->rows();
    auto row_idx = std::make_shared<std::vector<uint32_t>>(rows);
    {
        local_state._partition_rows_histogram.assign(_num_instances + 1, 0);
        for (size_t i = 0; i < rows; ++i) {
            local_state._partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_instances; ++i) {
            local_state._partition_rows_histogram[i] +=
                    local_state._partition_rows_histogram[i - 1];
        }

        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[local_state._partition_rows_histogram[channel_ids[i]] - 1] = i;
            local_state._partition_rows_histogram[channel_ids[i]]--;
        }
    }
    auto new_block = vectorized::Block::create_shared(block->clone_empty());
    new_block->swap(*block);
    for (size_t i = 0; i < _num_instances; i++) {
        size_t start = local_state._partition_rows_histogram[i];
        size_t size = local_state._partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            data_queue[i].enqueue({new_block, {row_idx, start, size}});
            local_state._shared_state->set_ready_for_read(i);
        }
    }

    return Status::OK();
}

Status PassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                  SourceState source_state,
                                  LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block(in_block->clone_empty());
    new_block.swap(*in_block);
    auto channel_id = (local_state._channel_id++) % _num_instances;
    _data_queue[channel_id].enqueue(std::move(new_block));
    local_state._shared_state->set_ready_for_read(channel_id);

    return Status::OK();
}

Status PassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                       SourceState& source_state,
                                       LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (running_sink_operators == 0) {
        if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
            *block = std::move(next_block);
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
        *block = std::move(next_block);
    } else {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return Status::OK();
}

} // namespace doris::pipeline
