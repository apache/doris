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

#pragma once

#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"

namespace doris::pipeline {

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;
struct BlockWrapper;
class SortSourceOperatorX;

/**
 * One exchanger is hold by one `LocalExchangeSharedState`. And one `LocalExchangeSharedState` is
 * shared by all local exchange sink operators and source operators with the same id.
 *
 * In exchanger, two block queues is maintained, one is data block queue and another is free block queue.
 *
 * In details, data block queue has queues as many as source operators. Each source operator will get
 * data block from the corresponding queue. Data blocks is push into the queue by sink operators. One
 * sink operator will push blocks into one or more queues.
 *
 * Free block is used to reuse the allocated memory. To reduce the memory limit, we also use a conf
 * to limit the size of free block queue.
 */
class ExchangerBase {
public:
    ExchangerBase(int running_sink_operators, int num_partitions, int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_partitions),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_partitions),
              _free_block_limit(free_block_limit) {}
    ExchangerBase(int running_sink_operators, int num_sources, int num_partitions,
                  int free_block_limit)
            : _running_sink_operators(running_sink_operators),
              _running_source_operators(num_partitions),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_sources),
              _free_block_limit(free_block_limit) {}
    virtual ~ExchangerBase() = default;
    virtual Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                             LocalExchangeSourceLocalState& local_state) = 0;
    virtual Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                        LocalExchangeSinkLocalState& local_state) = 0;
    virtual ExchangeType get_type() const = 0;
    // Called if a local exchanger source operator are closed. Free the unused data block in data_queue.
    virtual void close(LocalExchangeSourceLocalState& local_state) = 0;
    // Called if all local exchanger source operators are closed. We free the memory in
    // `_free_blocks` here.
    virtual void finalize(LocalExchangeSourceLocalState& local_state);

    virtual std::string data_queue_debug_string(int i) = 0;

protected:
    friend struct LocalExchangeSharedState;
    friend struct BlockWrapper;
    friend class LocalExchangeSourceLocalState;
    friend class LocalExchangeSinkOperatorX;
    friend class LocalExchangeSinkLocalState;
    std::atomic<int> _running_sink_operators = 0;
    std::atomic<int> _running_source_operators = 0;
    const int _num_partitions;
    const int _num_senders;
    const int _num_sources;
    const int _free_block_limit = 0;
    moodycamel::ConcurrentQueue<vectorized::Block> _free_blocks;
};

struct PartitionedRowIdxs {
    std::shared_ptr<vectorized::PODArray<uint32_t>> row_idxs;
    uint32_t offset_start;
    uint32_t length;
};

using PartitionedBlock = std::pair<std::shared_ptr<BlockWrapper>, PartitionedRowIdxs>;

struct RowRange {
    uint32_t offset_start;
    size_t length;
};
using BroadcastBlock = std::pair<std::shared_ptr<BlockWrapper>, RowRange>;

template <typename BlockType>
struct BlockQueue {
    std::atomic<bool> eos = false;
    moodycamel::ConcurrentQueue<BlockType> data_queue;
    BlockQueue() : eos(false), data_queue(moodycamel::ConcurrentQueue<BlockType>()) {}
    BlockQueue(BlockQueue<BlockType>&& other)
            : eos(other.eos.load()), data_queue(std::move(other.data_queue)) {}
    inline bool enqueue(BlockType const& item) {
        if (!eos) {
            data_queue.enqueue(item);
            return true;
        }
        return false;
    }

    inline bool enqueue(BlockType&& item) {
        if (!eos) {
            data_queue.enqueue(std::move(item));
            return true;
        }
        return false;
    }

    bool try_dequeue(BlockType& item) { return data_queue.try_dequeue(item); }

    void set_eos() { eos = true; }
};

using BlockWrapperSPtr = std::shared_ptr<BlockWrapper>;

template <typename BlockType>
class Exchanger : public ExchangerBase {
public:
    Exchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_partitions, free_block_limit) {}
    Exchanger(int running_sink_operators, int num_sources, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_sources, num_partitions, free_block_limit) {
    }
    ~Exchanger() override = default;
    std::string data_queue_debug_string(int i) override {
        return fmt::format("Data Queue {}: [size approx = {}, eos = {}]", i,
                           _data_queue[i].data_queue.size_approx(), _data_queue[i].eos);
    }

protected:
    // Enqueue data block and set downstream source operator to read.
    void _enqueue_data_and_set_ready(int channel_id, LocalExchangeSinkLocalState& local_state,
                                     BlockType&& block);
    bool _dequeue_data(LocalExchangeSourceLocalState& local_state, BlockType& block, bool* eos,
                       vectorized::Block* data_block);
    bool _dequeue_data(LocalExchangeSourceLocalState& local_state, BlockType& block, bool* eos,
                       vectorized::Block* data_block, int channel_id);
    std::vector<BlockQueue<BlockType>> _data_queue;

private:
    std::mutex _m;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

/**
 * `BlockWrapper` is used to wrap a data block with a reference count.
 *
 * In function `unref()`, if `ref_count` decremented to 0, which means this block is not needed by
 * operators, so we put it into `_free_blocks` to reuse its memory if needed and refresh memory usage
 * in current queue.
 *
 * Note: `ref_count` will be larger than 1 only if this block is shared between multiple queues in
 * shuffle exchanger.
 */
struct BlockWrapper {
    ENABLE_FACTORY_CREATOR(BlockWrapper);
    BlockWrapper(vectorized::Block&& data_block_) : data_block(std::move(data_block_)) {}
    ~BlockWrapper() { DCHECK_EQ(ref_count.load(), 0); }
    void ref(int delta) { ref_count += delta; }
    void unref(LocalExchangeSharedState* shared_state, size_t allocated_bytes, int channel_id) {
        if (ref_count.fetch_sub(1) == 1) {
            DCHECK_GT(allocated_bytes, 0);
            shared_state->sub_total_mem_usage(allocated_bytes, channel_id);
            if (shared_state->exchanger->_free_block_limit == 0 ||
                shared_state->exchanger->_free_blocks.size_approx() <
                        shared_state->exchanger->_free_block_limit *
                                shared_state->exchanger->_num_sources) {
                data_block.clear_column_data();
                shared_state->exchanger->_free_blocks.enqueue(std::move(data_block));
            }
        }
    }

    void unref(LocalExchangeSharedState* shared_state, int channel_id) {
        unref(shared_state, data_block.allocated_bytes(), channel_id);
    }
    int ref_value() const { return ref_count.load(); }
    std::atomic<int> ref_count = 0;
    vectorized::Block data_block;
};

class ShuffleExchanger : public Exchanger<PartitionedBlock> {
public:
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<PartitionedBlock>(running_sink_operators, num_partitions,
                                          free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    ~ShuffleExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    void close(LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::HASH_SHUFFLE; }

protected:
    ShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                     bool ignore_source_data_distribution, int free_block_limit)
            : Exchanger<PartitionedBlock>(running_sink_operators, num_sources, num_partitions,
                                          free_block_limit),
              _ignore_source_data_distribution(ignore_source_data_distribution) {
        _data_queue.resize(num_partitions);
    }
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, LocalExchangeSinkLocalState& local_state);

    const bool _ignore_source_data_distribution = false;
};

class BucketShuffleExchanger final : public ShuffleExchanger {
    ENABLE_FACTORY_CREATOR(BucketShuffleExchanger);
    BucketShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                           bool ignore_source_data_distribution, int free_block_limit)
            : ShuffleExchanger(running_sink_operators, num_sources, num_partitions,
                               ignore_source_data_distribution, free_block_limit) {}
    ~BucketShuffleExchanger() override = default;
    ExchangeType get_type() const override { return ExchangeType::BUCKET_HASH_SHUFFLE; }
};

class PassthroughExchanger final : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    ~PassthroughExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASSTHROUGH; }
    void close(LocalExchangeSourceLocalState& local_state) override;
};

class PassToOneExchanger final : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(PassToOneExchanger);
    PassToOneExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }
    void close(LocalExchangeSourceLocalState& local_state) override;
};

class LocalMergeSortExchanger final : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(LocalMergeSortExchanger);
    LocalMergeSortExchanger(std::shared_ptr<SortSourceOperatorX> sort_source,
                            int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions, free_block_limit),
              _sort_source(std::move(sort_source)) {
        _data_queue.resize(num_partitions);
    }
    ~LocalMergeSortExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::LOCAL_MERGE_SORT; }

    Status build_merger(RuntimeState* statem, LocalExchangeSourceLocalState& local_state);

    void close(LocalExchangeSourceLocalState& local_state) override {}
    void finalize(LocalExchangeSourceLocalState& local_state) override;

private:
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;
    std::shared_ptr<SortSourceOperatorX> _sort_source;
    std::vector<std::atomic_int64_t> _queues_mem_usege;
};

class BroadcastExchanger final : public Exchanger<BroadcastBlock> {
public:
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<BroadcastBlock>(running_sink_operators, num_partitions, free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    ~BroadcastExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::BROADCAST; }
    void close(LocalExchangeSourceLocalState& local_state) override;
};

//The code in AdaptivePassthroughExchanger is essentially
// a copy of ShuffleExchanger and PassthroughExchanger.
class AdaptivePassthroughExchanger : public Exchanger<BlockWrapperSPtr> {
public:
    ENABLE_FACTORY_CREATOR(AdaptivePassthroughExchanger);
    AdaptivePassthroughExchanger(int running_sink_operators, int num_partitions,
                                 int free_block_limit)
            : Exchanger<BlockWrapperSPtr>(running_sink_operators, num_partitions,
                                          free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::ADAPTIVE_PASSTHROUGH; }

    void close(LocalExchangeSourceLocalState& local_state) override;

private:
    Status _passthrough_sink(RuntimeState* state, vectorized::Block* in_block,
                             LocalExchangeSinkLocalState& local_state);
    Status _shuffle_sink(RuntimeState* state, vectorized::Block* in_block,
                         LocalExchangeSinkLocalState& local_state);
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, LocalExchangeSinkLocalState& local_state);

    std::atomic_bool _is_pass_through = false;
    std::atomic_int32_t _total_block = 0;
};

} // namespace doris::pipeline
