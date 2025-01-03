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

#include "common/status.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_source_operator.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {

template <typename BlockType>
bool Exchanger<BlockType>::_enqueue_data_and_set_ready(int channel_id,
                                                       LocalExchangeSinkLocalState& local_state,
                                                       BlockType&& block) {
    std::unique_lock l(*_m[channel_id]);
    if (_data_queue[channel_id].enqueue(std::move(block))) {
        local_state._shared_state->set_ready_to_read(channel_id);
        return true;
    }
    return false;
}

template <typename BlockType>
bool Exchanger<BlockType>::_dequeue_data(LocalExchangeSourceLocalState& local_state,
                                         BlockType& block, bool* eos) {
    bool all_finished = _running_sink_operators == 0;
    if (_data_queue[local_state._channel_id].try_dequeue(block)) {
        return true;
    } else if (all_finished) {
        *eos = true;
    } else {
        std::unique_lock l(*_m[local_state._channel_id]);
        if (_data_queue[local_state._channel_id].try_dequeue(block)) {
            return true;
        }
        COUNTER_UPDATE(local_state._get_block_failed_counter, 1);
        local_state._dependency->block();
    }
    return false;
}

Status ShuffleExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                              LocalExchangeSinkLocalState& local_state) {
    {
        SCOPED_TIMER(local_state._compute_hash_value_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, in_block,
                                                                  local_state.mem_tracker()));
    }
    {
        SCOPED_TIMER(local_state._distribute_timer);
        RETURN_IF_ERROR(_split_rows(state,
                                    local_state._partitioner->get_channel_ids().get<uint32_t>(),
                                    in_block, eos, local_state));
    }

    return Status::OK();
}

void ShuffleExchanger::close(LocalExchangeSourceLocalState& local_state) {
    PartitionedBlock partitioned_block;
    _data_queue[local_state._channel_id].set_eos();
    while (_data_queue[local_state._channel_id].try_dequeue(partitioned_block)) {
        auto block_wrapper = partitioned_block.first;
        local_state._shared_state->sub_mem_usage(
                local_state._channel_id, block_wrapper->data_block.allocated_bytes(), false);
        block_wrapper->unref(local_state._shared_state);
    }
}

Status ShuffleExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                   LocalExchangeSourceLocalState& local_state) {
    PartitionedBlock partitioned_block;
    vectorized::MutableBlock mutable_block;

    auto get_data = [&](vectorized::Block* result_block) -> Status {
        do {
            const auto* offset_start = partitioned_block.second.row_idxs->data() +
                                       partitioned_block.second.offset_start;
            auto block_wrapper = partitioned_block.first;
            local_state._shared_state->sub_mem_usage(
                    local_state._channel_id, block_wrapper->data_block.allocated_bytes(), false);
            RETURN_IF_ERROR(mutable_block.add_rows(&block_wrapper->data_block, offset_start,
                                                   offset_start + partitioned_block.second.length));
            block_wrapper->unref(local_state._shared_state);
        } while (mutable_block.rows() < state->batch_size() &&
                 _data_queue[local_state._channel_id].try_dequeue(partitioned_block));
        return Status::OK();
    };

    if (_dequeue_data(local_state, partitioned_block, eos)) {
        SCOPED_TIMER(local_state._copy_data_timer);
        mutable_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
                block, partitioned_block.first->data_block);
        RETURN_IF_ERROR(get_data(block));
    }
    return Status::OK();
}

Status ShuffleExchanger::_split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                                     vectorized::Block* block, bool eos,
                                     LocalExchangeSinkLocalState& local_state) {
    const auto rows = block->rows();
    auto row_idx = std::make_shared<vectorized::PODArray<uint32_t>>(rows);
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
    if (_free_blocks.try_dequeue(data_block)) {
        new_block_wrapper = ShuffleBlockWrapper::create_shared(std::move(data_block));
    } else {
        new_block_wrapper = ShuffleBlockWrapper::create_shared(block->clone_empty());
    }

    new_block_wrapper->data_block.swap(*block);
    if (new_block_wrapper->data_block.empty()) {
        return Status::OK();
    }
    local_state._shared_state->add_total_mem_usage(new_block_wrapper->data_block.allocated_bytes());
    auto bucket_seq_to_instance_idx =
            local_state._parent->cast<LocalExchangeSinkOperatorX>()._bucket_seq_to_instance_idx;
    if (get_type() == ExchangeType::HASH_SHUFFLE) {
        /**
         * If type is `HASH_SHUFFLE`, data are hash-shuffled and distributed to all instances of
         * all BEs. So we need a shuffleId-To-InstanceId mapping.
         * For example, row 1 get a hash value 1 which means we should distribute to instance 1 on
         * BE 1 and row 2 get a hash value 2 which means we should distribute to instance 1 on BE 3.
         */
        const auto& map = local_state._parent->cast<LocalExchangeSinkOperatorX>()
                                  ._shuffle_idx_to_instance_idx;
        new_block_wrapper->ref(map.size());
        for (const auto& it : map) {
            DCHECK(it.second >= 0 && it.second < _num_partitions)
                    << it.first << " : " << it.second << " " << _num_partitions;
            uint32_t start = local_state._partition_rows_histogram[it.first];
            uint32_t size = local_state._partition_rows_histogram[it.first + 1] - start;
            if (size > 0) {
                local_state._shared_state->add_mem_usage(
                        it.second, new_block_wrapper->data_block.allocated_bytes(), false);

                if (!_enqueue_data_and_set_ready(it.second, local_state,
                                                 {new_block_wrapper, {row_idx, start, size}})) {
                    local_state._shared_state->sub_mem_usage(
                            it.second, new_block_wrapper->data_block.allocated_bytes(), false);
                    new_block_wrapper->unref(local_state._shared_state);
                }
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    } else if (_num_senders != _num_sources || _ignore_source_data_distribution) {
        // In this branch, data just should be distributed equally into all instances.
        new_block_wrapper->ref(_num_partitions);
        for (size_t i = 0; i < _num_partitions; i++) {
            uint32_t start = local_state._partition_rows_histogram[i];
            uint32_t size = local_state._partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                local_state._shared_state->add_mem_usage(
                        i % _num_sources, new_block_wrapper->data_block.allocated_bytes(), false);
                if (!_enqueue_data_and_set_ready(i % _num_sources, local_state,
                                                 {new_block_wrapper, {row_idx, start, size}})) {
                    local_state._shared_state->sub_mem_usage(
                            i % _num_sources, new_block_wrapper->data_block.allocated_bytes(),
                            false);
                    new_block_wrapper->unref(local_state._shared_state);
                }
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    } else {
        DCHECK(!bucket_seq_to_instance_idx.empty());
        new_block_wrapper->ref(_num_partitions);
        for (size_t i = 0; i < _num_partitions; i++) {
            uint32_t start = local_state._partition_rows_histogram[i];
            uint32_t size = local_state._partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                _enqueue_data_and_set_ready(bucket_seq_to_instance_idx[i], local_state,
                                            {new_block_wrapper, {row_idx, start, size}});
            } else {
                new_block_wrapper->unref(local_state._shared_state);
            }
        }
    }

    return Status::OK();
}

Status PassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                  LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = (local_state._channel_id++) % _num_partitions;
    size_t memory_usage = new_block.allocated_bytes();
    local_state._shared_state->add_mem_usage(channel_id, memory_usage);
    if (!_enqueue_data_and_set_ready(channel_id, local_state, std::move(new_block))) {
        local_state._shared_state->sub_mem_usage(channel_id, memory_usage);
    }

    return Status::OK();
}

void PassthroughExchanger::close(LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    _data_queue[local_state._channel_id].set_eos();
    while (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
        local_state._shared_state->sub_mem_usage(local_state._channel_id,
                                                 next_block.allocated_bytes());
    }
}

Status PassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                       LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_dequeue_data(local_state, next_block, eos)) {
        block->swap(next_block);
        local_state._shared_state->sub_mem_usage(local_state._channel_id, block->allocated_bytes());
        if (_free_block_limit == 0 ||
            _free_blocks.size_approx() < _free_block_limit * _num_sources) {
            _free_blocks.enqueue(std::move(next_block));
        }
    }
    return Status::OK();
}

Status PassToOneExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block(in_block->clone_empty());
    new_block.swap(*in_block);
    _enqueue_data_and_set_ready(0, local_state, std::move(new_block));

    return Status::OK();
}

Status PassToOneExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                     LocalExchangeSourceLocalState& local_state) {
    if (local_state._channel_id != 0) {
        *eos = true;
        return Status::OK();
    }
    vectorized::Block next_block;
    if (_dequeue_data(local_state, next_block, eos)) {
        *block = std::move(next_block);
    }
    return Status::OK();
}

Status BroadcastExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                LocalExchangeSinkLocalState& local_state) {
    for (size_t i = 0; i < _num_partitions; i++) {
        auto mutable_block = vectorized::MutableBlock::create_unique(in_block->clone_empty());
        RETURN_IF_ERROR(mutable_block->add_rows(in_block, 0, in_block->rows()));
        _enqueue_data_and_set_ready(i, local_state, mutable_block->to_block());
    }

    return Status::OK();
}

void BroadcastExchanger::close(LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    _data_queue[local_state._channel_id].set_eos();
    while (_data_queue[local_state._channel_id].try_dequeue(next_block)) {
        // do nothing
    }
}

Status BroadcastExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                     LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_dequeue_data(local_state, next_block, eos)) {
        *block = std::move(next_block);
    }
    return Status::OK();
}

Status AdaptivePassthroughExchanger::_passthrough_sink(RuntimeState* state,
                                                       vectorized::Block* in_block, bool eos,
                                                       LocalExchangeSinkLocalState& local_state) {
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = (local_state._channel_id++) % _num_partitions;
    size_t memory_usage = new_block.allocated_bytes();
    local_state._shared_state->add_mem_usage(channel_id, memory_usage);

    if (!_enqueue_data_and_set_ready(channel_id, local_state, std::move(new_block))) {
        local_state._shared_state->sub_mem_usage(channel_id, memory_usage);
    }

    return Status::OK();
}

Status AdaptivePassthroughExchanger::_shuffle_sink(RuntimeState* state, vectorized::Block* block,
                                                   bool eos,
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
    return _split_rows(state, channel_ids.data(), block, eos, local_state);
}

Status AdaptivePassthroughExchanger::_split_rows(RuntimeState* state,
                                                 const uint32_t* __restrict channel_ids,
                                                 vectorized::Block* block, bool eos,
                                                 LocalExchangeSinkLocalState& local_state) {
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
            RETURN_IF_ERROR(mutable_block->add_rows(block, start, size));
            auto new_block = mutable_block->to_block();

            size_t memory_usage = new_block.allocated_bytes();
            local_state._shared_state->add_mem_usage(i, memory_usage);
            if (!_enqueue_data_and_set_ready(i, local_state, std::move(new_block))) {
                local_state._shared_state->sub_mem_usage(i, memory_usage);
            }
        }
    }
    return Status::OK();
}

Status AdaptivePassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                          bool eos, LocalExchangeSinkLocalState& local_state) {
    if (_is_pass_through) {
        return _passthrough_sink(state, in_block, eos, local_state);
    } else {
        if (_total_block++ > _num_partitions) {
            _is_pass_through = true;
        }
        return _shuffle_sink(state, in_block, eos, local_state);
    }
}

Status AdaptivePassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                               bool* eos,
                                               LocalExchangeSourceLocalState& local_state) {
    vectorized::Block next_block;
    if (_dequeue_data(local_state, next_block, eos)) {
        block->swap(next_block);
        if (_free_block_limit == 0 ||
            _free_blocks.size_approx() < _free_block_limit * _num_sources) {
            _free_blocks.enqueue(std::move(next_block));
        }
        local_state._shared_state->sub_mem_usage(local_state._channel_id, block->allocated_bytes());
    }
    return Status::OK();
}

} // namespace doris::pipeline
