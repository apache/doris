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
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
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
    size_t allocated_bytes = 0;
    // PartitionedBlock is used by shuffle exchanger.
    // PartitionedBlock will be push into multiple queues with different row ranges, so it will be
    // referenced multiple times. Otherwise, we only ref the block once because it is only push into
    // one queue.
    if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                  std::is_same_v<BroadcastBlock, BlockType>) {
        allocated_bytes = block.first->data_block.allocated_bytes();
    } else {
        block->ref(1);
        allocated_bytes = block->data_block.allocated_bytes();
    }
    std::unique_lock l(_m);
    local_state->_shared_state->add_mem_usage(channel_id, allocated_bytes,
                                              !std::is_same_v<PartitionedBlock, BlockType> &&
                                                      !std::is_same_v<BroadcastBlock, BlockType>);
    if (_data_queue[channel_id].enqueue(std::move(block))) {
        local_state->_shared_state->set_ready_to_read(channel_id);
    } else {
        local_state->_shared_state->sub_mem_usage(channel_id, allocated_bytes);
        // `enqueue(block)` return false iff this queue's source operator is already closed so we
        // just unref the block.
        if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                      std::is_same_v<BroadcastBlock, BlockType>) {
            block.first->unref(local_state->_shared_state, allocated_bytes, channel_id);
        } else {
            block->unref(local_state->_shared_state, allocated_bytes, channel_id);
            DCHECK_EQ(block->ref_value(), 0);
        }
    }
}

template <typename BlockType>
bool Exchanger<BlockType>::_dequeue_data(LocalExchangeSourceLocalState* local_state,
                                         BlockType& block, bool* eos, vectorized::Block* data_block,
                                         int channel_id) {
    if (local_state == nullptr) {
        if (!_dequeue_data(block, eos, data_block, channel_id)) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Exchanger has no data: {}",
                            data_queue_debug_string(channel_id));
        }
        return true;
    }
    bool all_finished = _running_sink_operators == 0;
    if (_data_queue[channel_id].try_dequeue(block)) {
        if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                      std::is_same_v<BroadcastBlock, BlockType>) {
            local_state->_shared_state->sub_mem_usage(channel_id,
                                                      block.first->data_block.allocated_bytes());
        } else {
            local_state->_shared_state->sub_mem_usage(channel_id,
                                                      block->data_block.allocated_bytes());
            data_block->swap(block->data_block);
            block->unref(local_state->_shared_state, data_block->allocated_bytes(), channel_id);
            DCHECK_EQ(block->ref_value(), 0);
        }
        return true;
    } else if (all_finished) {
        *eos = true;
    } else {
        std::unique_lock l(_m);
        if (_data_queue[channel_id].try_dequeue(block)) {
            if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                          std::is_same_v<BroadcastBlock, BlockType>) {
                local_state->_shared_state->sub_mem_usage(
                        channel_id, block.first->data_block.allocated_bytes());
            } else {
                local_state->_shared_state->sub_mem_usage(channel_id,
                                                          block->data_block.allocated_bytes());
                data_block->swap(block->data_block);
                block->unref(local_state->_shared_state, data_block->allocated_bytes(), channel_id);
                DCHECK_EQ(block->ref_value(), 0);
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
    if constexpr (!std::is_same_v<PartitionedBlock, BlockType> &&
                  !std::is_same_v<BroadcastBlock, BlockType>) {
        block->ref(1);
    }
    if (!_data_queue[channel_id].enqueue(std::move(block))) {
        if constexpr (std::is_same_v<PartitionedBlock, BlockType> ||
                      std::is_same_v<BroadcastBlock, BlockType>) {
            block.first->unref();
        } else {
            block->unref();
            DCHECK_EQ(block->ref_value(), 0);
        }
    }
}

template <typename BlockType>
bool Exchanger<BlockType>::_dequeue_data(BlockType& block, bool* eos, vectorized::Block* data_block,
                                         int channel_id) {
    if (_data_queue[channel_id].try_dequeue(block)) {
        if constexpr (!std::is_same_v<PartitionedBlock, BlockType> &&
                      !std::is_same_v<BroadcastBlock, BlockType>) {
            data_block->swap(block->data_block);
            block->unref();
            DCHECK_EQ(block->ref_value(), 0);
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
                                    in_block, *sink_info.channel_id, sink_info.local_state));
    }

    return Status::OK();
}

void ShuffleExchanger::close(SourceInfo&& source_info) {
    PartitionedBlock partitioned_block;
    bool eos;
    vectorized::Block block;
    _data_queue[source_info.channel_id].set_eos();
    while (_dequeue_data(source_info.local_state, partitioned_block, &eos, &block,
                         source_info.channel_id)) {
        partitioned_block.first->unref(
                source_info.local_state ? source_info.local_state->_shared_state : nullptr,
                source_info.channel_id);
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
            RETURN_IF_ERROR(mutable_block.add_rows(&block_wrapper->data_block, offset_start,
                                                   offset_start + partitioned_block.second.length));
            block_wrapper->unref(
                    source_info.local_state ? source_info.local_state->_shared_state : nullptr,
                    source_info.channel_id);
        } while (mutable_block.rows() < state->batch_size() && !*eos &&
                 _dequeue_data(source_info.local_state, partitioned_block, eos, block,
                               source_info.channel_id));
        return Status::OK();
    };

    if (_dequeue_data(source_info.local_state, partitioned_block, eos, block,
                      source_info.channel_id)) {
        SCOPED_TIMER(profile.copy_data_timer);
        mutable_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
                block, partitioned_block.first->data_block);
        RETURN_IF_ERROR(get_data());
    }
    return Status::OK();
}

Status ShuffleExchanger::_split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                                     vectorized::Block* block, int channel_id,
                                     LocalExchangeSinkLocalState* local_state) {
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
    if (_free_blocks.try_dequeue(data_block)) {
        new_block_wrapper = BlockWrapper::create_shared(std::move(data_block));
    } else {
        new_block_wrapper = BlockWrapper::create_shared(block->clone_empty());
    }

    new_block_wrapper->data_block.swap(*block);
    if (new_block_wrapper->data_block.empty()) {
        return Status::OK();
    }
    local_state->_shared_state->add_total_mem_usage(new_block_wrapper->data_block.allocated_bytes(),
                                                    channel_id);
    auto bucket_seq_to_instance_idx =
            local_state->_parent->cast<LocalExchangeSinkOperatorX>()._bucket_seq_to_instance_idx;
    if (get_type() == ExchangeType::HASH_SHUFFLE) {
        /**
         * If type is `HASH_SHUFFLE`, data are hash-shuffled and distributed to all instances of
         * all BEs. So we need a shuffleId-To-InstanceId mapping.
         * For example, row 1 get a hash value 1 which means we should distribute to instance 1 on
         * BE 1 and row 2 get a hash value 2 which means we should distribute to instance 1 on BE 3.
         */
        const auto& map = local_state->_parent->cast<LocalExchangeSinkOperatorX>()
                                  ._shuffle_idx_to_instance_idx;
        new_block_wrapper->ref(cast_set<int>(map.size()));
        for (const auto& it : map) {
            DCHECK(it.second >= 0 && it.second < _num_partitions)
                    << it.first << " : " << it.second << " " << _num_partitions;
            uint32_t start = partition_rows_histogram[it.first];
            uint32_t size = partition_rows_histogram[it.first + 1] - start;
            if (size > 0) {
                _enqueue_data_and_set_ready(it.second, local_state,
                                            {new_block_wrapper, {row_idx, start, size}});
            } else {
                new_block_wrapper->unref(local_state->_shared_state, channel_id);
            }
        }
    } else {
        DCHECK(!bucket_seq_to_instance_idx.empty());
        new_block_wrapper->ref(_num_partitions);
        for (int i = 0; i < _num_partitions; i++) {
            uint32_t start = partition_rows_histogram[i];
            uint32_t size = partition_rows_histogram[i + 1] - start;
            if (size > 0) {
                _enqueue_data_and_set_ready(bucket_seq_to_instance_idx[i], local_state,
                                            {new_block_wrapper, {row_idx, start, size}});
            } else {
                new_block_wrapper->unref(local_state->_shared_state, channel_id);
            }
        }
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
    if (_free_blocks.try_dequeue(data_block)) {
        new_block_wrapper = BlockWrapper::create_shared(std::move(data_block));
    } else {
        new_block_wrapper = BlockWrapper::create_shared(block->clone_empty());
    }

    new_block_wrapper->data_block.swap(*block);
    if (new_block_wrapper->data_block.empty()) {
        return Status::OK();
    }
    new_block_wrapper->ref(cast_set<int>(_num_partitions));
    for (int i = 0; i < _num_partitions; i++) {
        uint32_t start = partition_rows_histogram[i];
        uint32_t size = partition_rows_histogram[i + 1] - start;
        if (size > 0) {
            _enqueue_data_and_set_ready(i, {new_block_wrapper, {row_idx, start, size}});
        } else {
            new_block_wrapper->unref();
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
    BlockWrapperSPtr wrapper;
    if (!_free_blocks.try_dequeue(new_block)) {
        new_block = {in_block->clone_empty()};
    }
    new_block.swap(*in_block);
    wrapper = BlockWrapper::create_shared(std::move(new_block));
    auto channel_id = ((*sink_info.channel_id)++) % _num_partitions;
    _enqueue_data_and_set_ready(channel_id, sink_info.local_state, std::move(wrapper));

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

    BlockWrapperSPtr wrapper = BlockWrapper::create_shared(std::move(new_block));
    _enqueue_data_and_set_ready(0, sink_info.local_state, std::move(wrapper));

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

Status LocalMergeSortExchanger::sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                                     Profile&& profile, SinkInfo&& sink_info) {
    if (!in_block->empty()) {
        vectorized::Block new_block;
        if (!_free_blocks.try_dequeue(new_block)) {
            new_block = {in_block->clone_empty()};
        }
        DCHECK_LE(*sink_info.channel_id, _data_queue.size());

        new_block.swap(*in_block);
        _enqueue_data_and_set_ready(*sink_info.channel_id, sink_info.local_state,
                                    BlockWrapper::create_shared(std::move(new_block)));
    }
    if (eos && sink_info.local_state) {
        sink_info.local_state->_shared_state->source_deps[*sink_info.channel_id]
                ->set_always_ready();
    }
    return Status::OK();
}

void ExchangerBase::finalize() {
    DCHECK(_running_source_operators == 0);
    vectorized::Block block;
    while (_free_blocks.try_dequeue(block)) {
        // do nothing
    }
}

void LocalMergeSortExchanger::finalize() {
    BlockWrapperSPtr next_block;
    vectorized::Block block;
    bool eos;
    int id = 0;
    for (auto& data_queue : _data_queue) {
        data_queue.set_eos();
        while (_dequeue_data(next_block, &eos, &block, id)) {
            block = vectorized::Block();
        }
        id++;
    }
    ExchangerBase::finalize();
}

Status LocalMergeSortExchanger::build_merger(RuntimeState* state,
                                             LocalExchangeSourceLocalState* local_state) {
    RETURN_IF_ERROR(_sort_source->build_merger(state, _merger, local_state->profile()));
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    for (int channel_id = 0; channel_id < _num_partitions; channel_id++) {
        vectorized::BlockSupplier block_supplier = [&, local_state, id = channel_id](
                                                           vectorized::Block* block, bool* eos) {
            BlockWrapperSPtr next_block;
            _dequeue_data(local_state, next_block, eos, block, id);
            return Status::OK();
        };
        child_block_suppliers.push_back(block_supplier);
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

/*
before
    sort(8) --> datasink(8) [0,7].  ---->
    sort(8) --> datasink(8) [8,15]. ---->        [0,23]global merge ---->   Exchange(1)
    sort(8) --> datasink(8) [16,23].---->

now

    sort(8) --> local merge(1) ---> datasink(1) [0] ---->
    sort(8) --> local merge(1) ---> datasink(1) [1] ---->     [0,2]global merge ---->   Exchange(1)
    sort(8) --> local merge(1) ---> datasink(1) [2] ---->
*/
Status LocalMergeSortExchanger::get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                                          Profile&& profile, SourceInfo&& source_info) {
    if (source_info.channel_id != 0) {
        *eos = true;
        return Status::OK();
    }
    if (!_merger) {
        DCHECK(source_info.local_state);
        RETURN_IF_ERROR(build_merger(state, source_info.local_state));
    }
    RETURN_IF_ERROR(_merger->get_next(block, eos));
    return Status::OK();
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
    auto wrapper = BlockWrapper::create_shared(std::move(new_block));
    if (sink_info.local_state) {
        sink_info.local_state->_shared_state->add_total_mem_usage(
                wrapper->data_block.allocated_bytes(), *sink_info.channel_id);
    }

    wrapper->ref(_num_partitions);
    for (int i = 0; i < _num_partitions; i++) {
        _enqueue_data_and_set_ready(i, sink_info.local_state,
                                    {wrapper, {0, wrapper->data_block.rows()}});
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
        partitioned_block.first->unref(
                source_info.local_state ? source_info.local_state->_shared_state : nullptr,
                source_info.channel_id);
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
                        block, partitioned_block.first->data_block);
        auto block_wrapper = partitioned_block.first;
        RETURN_IF_ERROR(mutable_block.add_rows(&block_wrapper->data_block,
                                               partitioned_block.second.offset_start,
                                               partitioned_block.second.length));
        block_wrapper->unref(
                source_info.local_state ? source_info.local_state->_shared_state : nullptr,
                source_info.channel_id);
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
    _enqueue_data_and_set_ready(channel_id, sink_info.local_state,
                                BlockWrapper::create_shared(std::move(new_block)));

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
    return _split_rows(state, channel_ids.data(), block, std::move(sink_info));
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

            _enqueue_data_and_set_ready(i, sink_info.local_state,
                                        BlockWrapper::create_shared(std::move(new_block)));
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
        if (_total_block++ > _num_partitions) {
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
