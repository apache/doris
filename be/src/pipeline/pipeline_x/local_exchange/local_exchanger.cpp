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
            auto block_wrapper = partitioned_block.first;
            local_state._shared_state->sub_mem_usage(
                    local_state._channel_id, block_wrapper->data_block.allocated_bytes(), false);
            mutable_block->add_rows(&block_wrapper->data_block, offset_start,
                                    offset_start + std::get<2>(partitioned_block.second));
            block_wrapper->unref(local_state._shared_state);
        } while (mutable_block->rows() < state->batch_size() &&
                 _data_queue[local_state._channel_id].try_dequeue(partitioned_block));
        *result_block = mutable_block->to_block();
    };
    if (_running_sink_operators == 0) {
        if (_data_queue[local_state._channel_id].try_dequeue(partitioned_block)) {
            SCOPED_TIMER(local_state._copy_data_timer);
            mutable_block = vectorized::MutableBlock::create_unique(
                    partitioned_block.first->data_block.clone_empty());
            get_data(block);
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[local_state._channel_id].try_dequeue(partitioned_block)) {
        SCOPED_TIMER(local_state._copy_data_timer);
        mutable_block = vectorized::MutableBlock::create_unique(
                partitioned_block.first->data_block.clone_empty());
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
        local_state._partition_rows_histogram.assign(_num_partitions + 1, 0);
        for (size_t i = 0; i < rows; ++i) {
            local_state._partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_partitions; ++i) {
            local_state._partition_rows_histogram[i] +=
                    local_state._partition_rows_histogram[i - 1];
        }

        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[local_state._partition_rows_histogram[channel_ids[i]] - 1] = i;
            local_state._partition_rows_histogram[channel_ids[i]]--;
        }
    }

    vectorized::Block data_block;
    std::shared_ptr<ShuffleBlockWrapper> new_block_wrapper;
    if (_free_blocks.try_enqueue(data_block)) {
        new_block_wrapper = ShuffleBlockWrapper::create_shared(std::move(data_block));
    } else {
        new_block_wrapper = ShuffleBlockWrapper::create_shared(block->clone_empty());
    }

    new_block_wrapper->data_block.swap(*block);
    if (new_block_wrapper->data_block.empty()) {
        return Status::OK();
    }
    local_state._shared_state->add_total_mem_usage(new_block_wrapper->data_block.allocated_bytes());
    new_block_wrapper->ref(_num_partitions);
    if (get_type() == ExchangeType::HASH_SHUFFLE) {
        for (size_t i = 0; i < _num_partitions; i++) {
            size_t start = local_state._partition_rows_histogram[i];
            size_t size = local_state._partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                local_state._shared_state->add_mem_usage(
                        i, new_block_wrapper->data_block.allocated_bytes(), false);
                data_queue[i].enqueue({new_block_wrapper, {row_idx, start, size}});
                local_state._shared_state->set_ready_to_read(i);
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    } else if (_num_senders != _num_sources || _ignore_source_data_distribution) {
        for (size_t i = 0; i < _num_partitions; i++) {
            size_t start = local_state._partition_rows_histogram[i];
            size_t size = local_state._partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                local_state._shared_state->add_mem_usage(
                        i % _num_sources, new_block_wrapper->data_block.allocated_bytes(), false);
                data_queue[i % _num_sources].enqueue({new_block_wrapper, {row_idx, start, size}});
                local_state._shared_state->set_ready_to_read(i % _num_sources);
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    } else {
        auto map =
                local_state._parent->cast<LocalExchangeSinkOperatorX>()._bucket_seq_to_instance_idx;
        for (size_t i = 0; i < _num_partitions; i++) {
            size_t start = local_state._partition_rows_histogram[i];
            size_t size = local_state._partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                local_state._shared_state->add_mem_usage(
                        map[i], new_block_wrapper->data_block.allocated_bytes(), false);
                data_queue[map[i]].enqueue({new_block_wrapper, {row_idx, start, size}});
                local_state._shared_state->set_ready_to_read(map[i]);
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    }

    return Status::OK();
}

Status PassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                  SourceState source_state,
                                  LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = (local_state._channel_id++) % _num_partitions;
    local_state._shared_state->add_mem_usage(channel_id, new_block.allocated_bytes());
    _data_queue[channel_id].enqueue(std::move(new_block));
    local_state._shared_state->set_ready_to_read(channel_id);

    return Status::OK();
}

Status PassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                       SourceState& source_state,
                                       LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_running_sink_operators == 0) {
        if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
            block->swap(next_block);
            _free_blocks.enqueue(std::move(next_block));
            local_state._shared_state->sub_mem_usage(local_state._channel_id,
                                                     block->allocated_bytes());
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
        block->swap(next_block);
        _free_blocks.enqueue(std::move(next_block));
        local_state._shared_state->sub_mem_usage(local_state._channel_id, block->allocated_bytes());
    } else {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return Status::OK();
}

Status PassToOneExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                SourceState source_state,
                                LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block(in_block->clone_empty());
    new_block.swap(*in_block);
    _data_queue[0].enqueue(std::move(new_block));
    local_state._shared_state->set_ready_to_read(0);

    return Status::OK();
}

Status PassToOneExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                     SourceState& source_state,
                                     LocalExchangeSourceLocalState& local_state) {
    if (local_state._channel_id != 0) {
        source_state = SourceState::FINISHED;
        return Status::OK();
    }
    vectorized::Block next_block;
    if (_running_sink_operators == 0) {
        if (_data_queue[0].try_dequeue(next_block)) {
            *block = std::move(next_block);
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[0].try_dequeue(next_block)) {
        *block = std::move(next_block);
    } else {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return Status::OK();
}

Status BroadcastExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                SourceState source_state,
                                LocalExchangeSinkLocalState& local_state) {
    for (size_t i = 0; i < _num_partitions; i++) {
        auto mutable_block = vectorized::MutableBlock::create_unique(in_block->clone_empty());
        mutable_block->add_rows(in_block, 0, in_block->rows());
        _data_queue[i].enqueue(mutable_block->to_block());
        local_state._shared_state->set_ready_to_read(i);
    }

    return Status::OK();
}

Status BroadcastExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                     SourceState& source_state,
                                     LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_running_sink_operators == 0) {
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

Status AdaptivePassthroughExchanger::_passthrough_sink(RuntimeState* state,
                                                       vectorized::Block* in_block,
                                                       SourceState source_state,
                                                       LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = (local_state._channel_id++) % _num_partitions;
    local_state._shared_state->add_mem_usage(channel_id, new_block.allocated_bytes());
    _data_queue[channel_id].enqueue(std::move(new_block));
    local_state._shared_state->set_ready_to_read(channel_id);

    return Status::OK();
}

Status AdaptivePassthroughExchanger::_shuffle_sink(RuntimeState* state, vectorized::Block* block,
                                                   SourceState source_state,
                                                   LocalExchangeSinkLocalState& local_state) {
    std::vector<uint32_t> channel_ids;
    const auto num_rows = block->rows();
    channel_ids.resize(num_rows, 0);
    if (num_rows <= _num_partitions) {
        std::iota(channel_ids.begin(), channel_ids.end(), 0);
    } else {
        size_t i = 0;
        for (; i < num_rows - _num_partitions; i += _num_partitions) {
            std::iota(channel_ids.begin() + i, channel_ids.begin() + i + _num_partitions, 0);
        }
        if (i < num_rows - 1) {
            std::iota(channel_ids.begin() + i, channel_ids.end(), 0);
        }
    }
    return _split_rows(state, channel_ids.data(), block, source_state, local_state);
}

Status AdaptivePassthroughExchanger::_split_rows(RuntimeState* state,
                                                 const uint32_t* __restrict channel_ids,
                                                 vectorized::Block* block, SourceState source_state,
                                                 LocalExchangeSinkLocalState& local_state) {
    auto& data_queue = _data_queue;
    const auto rows = block->rows();
    auto row_idx = std::make_shared<std::vector<uint32_t>>(rows);
    {
        local_state._partition_rows_histogram.assign(_num_partitions + 1, 0);
        for (size_t i = 0; i < rows; ++i) {
            local_state._partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_partitions; ++i) {
            local_state._partition_rows_histogram[i] +=
                    local_state._partition_rows_histogram[i - 1];
        }

        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[local_state._partition_rows_histogram[channel_ids[i]] - 1] = i;
            local_state._partition_rows_histogram[channel_ids[i]]--;
        }
    }
    for (size_t i = 0; i < _num_partitions; i++) {
        const size_t start = local_state._partition_rows_histogram[i];
        const size_t size = local_state._partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            std::unique_ptr<vectorized::MutableBlock> mutable_block =
                    vectorized::MutableBlock::create_unique(block->clone_empty());
            mutable_block->add_rows(block, start, size);
            auto new_block = mutable_block->to_block();
            local_state._shared_state->add_mem_usage(i, new_block.allocated_bytes());
            data_queue[i].enqueue(std::move(new_block));
        }
        local_state._shared_state->set_ready_to_read(i);
    }
    return Status::OK();
}

Status AdaptivePassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                          SourceState source_state,
                                          LocalExchangeSinkLocalState& local_state) {
    if (_is_pass_through) {
        return _passthrough_sink(state, in_block, source_state, local_state);
    } else {
        if (_total_block++ > _num_partitions) {
            _is_pass_through = true;
        }
        return _shuffle_sink(state, in_block, source_state, local_state);
    }
}

Status AdaptivePassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                               SourceState& source_state,
                                               LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_running_sink_operators == 0) {
        if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
            block->swap(next_block);
            _free_blocks.enqueue(std::move(next_block));
            local_state._shared_state->sub_mem_usage(local_state._channel_id,
                                                     block->allocated_bytes());
        } else {
            COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
            source_state = SourceState::FINISHED;
        }
    } else if (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
        block->swap(next_block);
        _free_blocks.enqueue(std::move(next_block));
        local_state._shared_state->sub_mem_usage(local_state._channel_id, block->allocated_bytes());
    } else {
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return Status::OK();
}

} // namespace doris::pipeline
