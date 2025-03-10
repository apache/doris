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

#include "pipeline/local_exchange/local_exchanger.h"

#include "common/cast_set.h"
#include "common/status.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/local_exchange/local_exchange_source_operator.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
template <typename BlockType>
void Exchanger<BlockType>::_enqueue_data_and_set_ready(int channel_id,
                                                       LocalExchangeSinkLocalState* local_state,
                                                       BlockType&& block) {
    if (local_state == nullptr) {
        _enqueue_data_and_set_ready(channel_id, std::move(block));
        return;
    }
    // PartitionedBlock is used by shuffle exchanger.
    // PartitionedBlock will be push into multiple queues with different row ranges, so it will be
    // referenced multiple times. Otherwise, we only ref the block once because it is only push into
    // one queue.
    std::unique_lock l(*_m[channel_id]);
    if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                  std::is_same_v<BroadcastBlock, BlockType>) {
        block.first->record_channel_id(channel_id);
    } else {
        block->record_channel_id(channel_id);
    }

    if (_data_queue[channel_id].enqueue(std::move(block))) {
        local_state->_shared_state->set_ready_to_read(channel_id);
    }
}

template <typename BlockType>
bool Exchanger<BlockType>::_dequeue_data(LocalExchangeSourceLocalState* local_state,
                                         BlockType& block, bool* eos, vectorized::Block* data_block,
                                         int channel_id) {
    if (local_state == nullptr) {
        return _dequeue_data(block, eos, data_block, channel_id);
    }
    bool all_finished = _running_sink_operators == 0;
    if (_data_queue[channel_id].try_dequeue(block)) {
        if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                      std::is_same_v<BroadcastBlock, BlockType>) {
            local_state->_shared_state->sub_mem_usage(channel_id, block.first->_allocated_bytes);
        } else {
            local_state->_shared_state->sub_mem_usage(channel_id, block->_allocated_bytes);
            data_block->swap(block->_data_block);
        }
        return true;
    } else if (all_finished) {
        *eos = true;
    } else {
        std::unique_lock l(*_m[channel_id]);
        if (_data_queue[channel_id].try_dequeue(block)) {
            if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                          std::is_same_v<BroadcastBlock, BlockType>) {
                local_state->_shared_state->sub_mem_usage(channel_id,
                                                          block.first->_allocated_bytes);
            } else {
                local_state->_shared_state->sub_mem_usage(channel_id, block->_allocated_bytes);
                data_block->swap(block->_data_block);
            }
            return true;
        }
        COUNTER_UPDATE(local_state->_get_block_failed_counter, 1);
        local_state->_dependency->block();
    }
    return false;
}

template <typename BlockType>
void Exchanger<BlockType>::_enqueue_data_and_set_ready(int channel_id, BlockType&& block) {
    if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                  std::is_same_v<BroadcastBlock, BlockType>) {
        block.first->record_channel_id(channel_id);
    } else {
        block->record_channel_id(channel_id);
    }
    _data_queue[channel_id].enqueue(std::move(block));
}

template <typename BlockType>
bool Exchanger<BlockType>::_dequeue_data(BlockType& block, bool* eos, vectorized::Block* data_block,
                                         int channel_id) {
    if (_data_queue[channel_id].try_dequeue(block)) {
        if constexpr (!std::is_same_v<PartitionedBlock, BlockType> &&
                      !std::is_same_v<BroadcastBlock, BlockType>) {
            data_block->swap(block->_data_block);
        }
        return true;
    }
    return false;
}

Status ShuffleExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                              Profile&& profile, SinkInfo&& sink_info) {
    if (in_block->empty()) {
        return Status::OK();
    }
    {
        SCOPED_TIMER(profile.compute_hash_value_timer);
        RETURN_IF_ERROR(sink_info.partitioner->do_partitioning(state, in_block));
    }
    {
        SCOPED_TIMER(profile.distribute_timer);
        RETURN_IF_ERROR(_split_rows(state, sink_info.partitioner->get_channel_ids().get<uint32_t>(),
                                    in_block, *sink_info.channel_id, sink_info.local_state,
                                    sink_info.shuffle_idx_to_instance_idx));
    }

    sink_info.local_state->_memory_used_counter->set(
            sink_info.local_state->_shared_state->mem_usage);
    return Status::OK();
}

void ShuffleExchanger::close(SourceInfo&& source_info) {
    PartitionedBlock partitioned_block;
    bool eos;
    vectorized::Block block;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, partitioned_block, &eos, &block,
                         source_info.channel_id)) {
        // do nothing
    }
}

Status ShuffleExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                   Profile&& profile, SourceInfo&& source_info) {
    PartitionedBlock partitioned_block;
    vectorized::MutableBlock mutable_block;

    auto get_data = [&]() -> Status {
        do {
            const auto* offset_start = partitioned_block.second.row_idxs->data() +
                                       partitioned_block.second.offset_start;
            auto block_wrapper = partitioned_block.first;
            RETURN_IF_ERROR(mutable_block.add_rows(&block_wrapper->_data_block, offset_start,
                                                   offset_start + partitioned_block.second.length));
        } while (mutable_block.rows() < state->batch_size() && !*eos &&
                 _dequeue_data(source_info.local_state, partitioned_block, eos, block,
                               source_info.channel_id));
        return Status::OK();
    };

    if (_dequeue_data(source_info.local_state, partitioned_block, eos, block,
                      source_info.channel_id)) {
        SCOPED_TIMER(profile.copy_data_timer);
        mutable_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
                block, partitioned_block.first->_data_block);
        RETURN_IF_ERROR(get_data());
    }
    return Status::OK();
}

Status ShuffleExchanger::_split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                                     vectorized::Block* block, int channel_id,
                                     LocalExchangeSinkLocalState* local_state,
                                     std::map<int, int>* shuffle_idx_to_instance_idx) {
    if (local_state == nullptr) {
        return _split_rows(state, channel_ids, block, channel_id);
    }
    const auto rows = cast_set<int32_t>(block->rows());
    auto row_idx = std::make_shared<vectorized::PODArray<uint32_t>>(rows);
    auto& partition_rows_histogram = _partition_rows_histogram[channel_id];
    {
        partition_rows_histogram.assign(_num_partitions + 1, 0);
        for (int32_t i = 0; i < rows; ++i) {
            partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_partitions; ++i) {
            partition_rows_histogram[i] += partition_rows_histogram[i - 1];
        }
        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[partition_rows_histogram[channel_ids[i]] - 1] = i;
            partition_rows_histogram[channel_ids[i]]--;
        }
    }

    vectorized::Block data_block;
    std::shared_ptr<BlockWrapper> new_block_wrapper;
    if (!_free_blocks.try_dequeue(data_block)) {
        data_block = block->clone_empty();
    }
    data_block.swap(*block);
    new_block_wrapper =
            BlockWrapper::create_shared(std::move(data_block), local_state->_shared_state, -1);
    if (new_block_wrapper->_data_block.empty()) {
        return Status::OK();
    }
    /**
     * Data are hash-shuffled and distributed to all instances of
     * all BEs. So we need a shuffleId-To-InstanceId mapping.
     * For example, row 1 get a hash value 1 which means we should distribute to instance 1 on
     * BE 1 and row 2 get a hash value 2 which means we should distribute to instance 1 on BE 3.
     */
    DCHECK(shuffle_idx_to_instance_idx && shuffle_idx_to_instance_idx->size() > 0);
    const auto& map = *shuffle_idx_to_instance_idx;
    int32_t enqueue_rows = 0;
    for (const auto& it : map) {
        DCHECK(it.second >= 0 && it.second < _num_partitions)
                << it.first << " : " << it.second << " " << _num_partitions;
        uint32_t start = partition_rows_histogram[it.first];
        uint32_t size = partition_rows_histogram[it.first + 1] - start;
        if (size > 0) {
            enqueue_rows += size;
            _enqueue_data_and_set_ready(it.second, local_state,
                                        {new_block_wrapper, {row_idx, start, size}});
        }
    }
    if (enqueue_rows != rows) [[unlikely]] {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "Type: {}, Local Exchange Id: {}, Shuffled Map: ",
                       get_exchange_type_name(get_type()), local_state->parent()->node_id());
        for (const auto& it : map) {
            fmt::format_to(debug_string_buffer, "[{}:{}], ", it.first, it.second);
        }
        return Status::InternalError(
                "Rows mismatched! Data may be lost. [Expected enqueue rows={}, Real enqueue "
                "rows={}, Detail: {}]",
                rows, enqueue_rows, fmt::to_string(debug_string_buffer));
    }

    return Status::OK();
}

Status ShuffleExchanger::_split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                                     vectorized::Block* block, int channel_id) {
    const auto rows = cast_set<int32_t>(block->rows());
    auto row_idx = std::make_shared<vectorized::PODArray<uint32_t>>(rows);
    auto& partition_rows_histogram = _partition_rows_histogram[channel_id];
    {
        partition_rows_histogram.assign(_num_partitions + 1, 0);
        for (int32_t i = 0; i < rows; ++i) {
            partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_partitions; ++i) {
            partition_rows_histogram[i] += partition_rows_histogram[i - 1];
        }
        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[partition_rows_histogram[channel_ids[i]] - 1] = i;
            partition_rows_histogram[channel_ids[i]]--;
        }
    }

    vectorized::Block data_block;
    std::shared_ptr<BlockWrapper> new_block_wrapper;
    if (!_free_blocks.try_dequeue(data_block)) {
        data_block = block->clone_empty();
    }
    data_block.swap(*block);
    new_block_wrapper = BlockWrapper::create_shared(std::move(data_block), nullptr, -1);
    if (new_block_wrapper->_data_block.empty()) {
        return Status::OK();
    }
    for (int i = 0; i < _num_partitions; i++) {
        uint32_t start = partition_rows_histogram[i];
        uint32_t size = partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            _enqueue_data_and_set_ready(i, {new_block_wrapper, {row_idx, start, size}});
        }
    }

    return Status::OK();
}

Status PassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                  Profile&& profile, SinkInfo&& sink_info) {
    if (in_block->empty()) {
        return Status::OK();
    }
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = ((*sink_info.channel_id)++) % _num_partitions;
    BlockWrapperSPtr wrapper = BlockWrapper::create_shared(
            std::move(new_block),
            sink_info.local_state ? sink_info.local_state->_shared_state : nullptr, channel_id);

    _enqueue_data_and_set_ready(channel_id, sink_info.local_state, std::move(wrapper));

    sink_info.local_state->_memory_used_counter->set(
            sink_info.local_state->_shared_state->mem_usage);

    return Status::OK();
}

void PassthroughExchanger::close(SourceInfo&& source_info) {
    vectorized::Block next_block;
    BlockWrapperSPtr wrapper;
    bool eos;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, wrapper, &eos, &next_block,
                         source_info.channel_id)) {
        // do nothing
    }
}

void PassToOneExchanger::close(SourceInfo&& source_info) {
    vectorized::Block next_block;
    BlockWrapperSPtr wrapper;
    bool eos;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, wrapper, &eos, &next_block,
                         source_info.channel_id)) {
        // do nothing
    }
}

Status PassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                       Profile&& profile, SourceInfo&& source_info) {
    BlockWrapperSPtr next_block;
    _dequeue_data(source_info.local_state, next_block, eos, block, source_info.channel_id);
    return Status::OK();
}

Status PassToOneExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                Profile&& profile, SinkInfo&& sink_info) {
    if (in_block->empty()) {
        return Status::OK();
    }
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);

    BlockWrapperSPtr wrapper = BlockWrapper::create_shared(
            std::move(new_block),
            sink_info.local_state ? sink_info.local_state->_shared_state : nullptr, 0);
    _enqueue_data_and_set_ready(0, sink_info.local_state, std::move(wrapper));

    sink_info.local_state->_memory_used_counter->set(
            sink_info.local_state->_shared_state->mem_usage);

    return Status::OK();
}

Status PassToOneExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                     Profile&& profile, SourceInfo&& source_info) {
    if (source_info.channel_id != 0) {
        *eos = true;
        return Status::OK();
    }
    BlockWrapperSPtr next_block;
    _dequeue_data(source_info.local_state, next_block, eos, block, source_info.channel_id);
    return Status::OK();
}

void ExchangerBase::finalize() {
    DCHECK(_running_source_operators == 0);
    vectorized::Block block;
    while (_free_blocks.try_dequeue(block)) {
        // do nothing
    }
}

Status BroadcastExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                Profile&& profile, SinkInfo&& sink_info) {
    if (in_block->empty()) {
        return Status::OK();
    }
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto wrapper = BlockWrapper::create_shared(
            std::move(new_block),
            sink_info.local_state ? sink_info.local_state->_shared_state : nullptr, -1);
    for (int i = 0; i < _num_partitions; i++) {
        _enqueue_data_and_set_ready(i, sink_info.local_state,
                                    {wrapper, {0, wrapper->_data_block.rows()}});
    }

    return Status::OK();
}

void BroadcastExchanger::close(SourceInfo&& source_info) {
    BroadcastBlock partitioned_block;
    bool eos;
    vectorized::Block block;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, partitioned_block, &eos, &block,
                         source_info.channel_id)) {
        // do nothing
    }
}

Status BroadcastExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                     Profile&& profile, SourceInfo&& source_info) {
    BroadcastBlock partitioned_block;

    if (_dequeue_data(source_info.local_state, partitioned_block, eos, block,
                      source_info.channel_id)) {
        SCOPED_TIMER(profile.copy_data_timer);
        vectorized::MutableBlock mutable_block =
                vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
                        block, partitioned_block.first->_data_block);
        auto block_wrapper = partitioned_block.first;
        RETURN_IF_ERROR(mutable_block.add_rows(&block_wrapper->_data_block,
                                               partitioned_block.second.offset_start,
                                               partitioned_block.second.length));
    }

    return Status::OK();
}

Status AdaptivePassthroughExchanger::_passthrough_sink(RuntimeState* state,
                                                       vectorized::Block* in_block,
                                                       SinkInfo&& sink_info) {
    vectorized::Block new_block;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    auto channel_id = ((*sink_info.channel_id)++) % _num_partitions;
    _enqueue_data_and_set_ready(
            channel_id, sink_info.local_state,
            BlockWrapper::create_shared(
                    std::move(new_block),
                    sink_info.local_state ? sink_info.local_state->_shared_state : nullptr,
                    channel_id));

    sink_info.local_state->_memory_used_counter->set(
            sink_info.local_state->_shared_state->mem_usage);
    return Status::OK();
}

Status AdaptivePassthroughExchanger::_shuffle_sink(RuntimeState* state, vectorized::Block* block,
                                                   SinkInfo&& sink_info) {
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

    sink_info.local_state->_memory_used_counter->set(
            sink_info.local_state->_shared_state->mem_usage);
    RETURN_IF_ERROR(_split_rows(state, channel_ids.data(), block, std::move(sink_info)));
    return Status::OK();
}

Status AdaptivePassthroughExchanger::_split_rows(RuntimeState* state,
                                                 const uint32_t* __restrict channel_ids,
                                                 vectorized::Block* block, SinkInfo&& sink_info) {
    const auto rows = cast_set<int32_t>(block->rows());
    auto row_idx = std::make_shared<std::vector<uint32_t>>(rows);
    auto& partition_rows_histogram = _partition_rows_histogram[*sink_info.channel_id];
    {
        partition_rows_histogram.assign(_num_partitions + 1, 0);
        for (int32_t i = 0; i < rows; ++i) {
            partition_rows_histogram[channel_ids[i]]++;
        }
        for (int32_t i = 1; i <= _num_partitions; ++i) {
            partition_rows_histogram[i] += partition_rows_histogram[i - 1];
        }

        for (int32_t i = rows - 1; i >= 0; --i) {
            (*row_idx)[partition_rows_histogram[channel_ids[i]] - 1] = i;
            partition_rows_histogram[channel_ids[i]]--;
        }
    }
    for (int32_t i = 0; i < _num_partitions; i++) {
        const size_t start = partition_rows_histogram[i];
        const size_t size = partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            std::unique_ptr<vectorized::MutableBlock> mutable_block =
                    vectorized::MutableBlock::create_unique(block->clone_empty());
            RETURN_IF_ERROR(mutable_block->add_rows(block, start, size));
            auto new_block = mutable_block->to_block();

            _enqueue_data_and_set_ready(
                    i, sink_info.local_state,
                    BlockWrapper::create_shared(
                            std::move(new_block),
                            sink_info.local_state ? sink_info.local_state->_shared_state : nullptr,
                            i));
        }
    }
    return Status::OK();
}

Status AdaptivePassthroughExchanger::sink(RuntimeState* state, vectorized::Block* in_block,
                                          bool eos, Profile&& profile, SinkInfo&& sink_info) {
    if (in_block->empty()) {
        return Status::OK();
    }
    if (_is_pass_through) {
        return _passthrough_sink(state, in_block, std::move(sink_info));
    } else {
        if (++_total_block >= _num_partitions) {
            _is_pass_through = true;
        }
        return _shuffle_sink(state, in_block, std::move(sink_info));
    }
}

Status AdaptivePassthroughExchanger::get_block(RuntimeState* state, vectorized::Block* block,
                                               bool* eos, Profile&& profile,
                                               SourceInfo&& source_info) {
    BlockWrapperSPtr next_block;
    _dequeue_data(source_info.local_state, next_block, eos, block, source_info.channel_id);
    return Status::OK();
}

void AdaptivePassthroughExchanger::close(SourceInfo&& source_info) {
    vectorized::Block next_block;
    bool eos;
    BlockWrapperSPtr wrapper;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, wrapper, &eos, &next_block,
                         source_info.channel_id)) {
        // do nothing
    }
}

} // namespace doris::pipeline
