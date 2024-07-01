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
struct ShuffleBlockWrapper;
class SortSourceOperatorX;

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
    virtual void close(LocalExchangeSourceLocalState& local_state) = 0;

    virtual std::vector<Dependency*> local_sink_state_dependency(int channel_id) { return {}; }
    virtual std::vector<Dependency*> local_state_dependency(int channel_id) { return {}; }

protected:
    friend struct LocalExchangeSharedState;
    friend struct ShuffleBlockWrapper;
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

using PartitionedBlock = std::pair<std::shared_ptr<ShuffleBlockWrapper>, PartitionedRowIdxs>;

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

template <typename BlockType>
class Exchanger : public ExchangerBase {
public:
    Exchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_partitions, free_block_limit) {}
    Exchanger(int running_sink_operators, int num_sources, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_sources, num_partitions, free_block_limit) {
    }
    ~Exchanger() override = default;

protected:
    std::vector<BlockQueue<BlockType>> _data_queue;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

struct ShuffleBlockWrapper {
    ENABLE_FACTORY_CREATOR(ShuffleBlockWrapper);
    ShuffleBlockWrapper(vectorized::Block&& data_block_) : data_block(std::move(data_block_)) {}
    void ref(int delta) { ref_count += delta; }
    void unref(LocalExchangeSharedState* shared_state) {
        if (ref_count.fetch_sub(1) == 1) {
            shared_state->sub_total_mem_usage(data_block.allocated_bytes());
            if (shared_state->exchanger->_free_block_limit == 0 ||
                shared_state->exchanger->_free_blocks.size_approx() <
                        shared_state->exchanger->_free_block_limit *
                                shared_state->exchanger->_num_sources) {
                data_block.clear_column_data();
                shared_state->exchanger->_free_blocks.enqueue(std::move(data_block));
            }
        }
    }
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
                       vectorized::Block* block, bool eos,
                       LocalExchangeSinkLocalState& local_state);

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

class PassthroughExchanger final : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
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

class PassToOneExchanger final : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(PassToOneExchanger);
    PassToOneExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
                                           free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }
    void close(LocalExchangeSourceLocalState& local_state) override {}
};

class LocalMergeSortExchanger final : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(LocalMergeSortExchanger);
    LocalMergeSortExchanger(std::shared_ptr<SortSourceOperatorX> sort_source,
                            int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
                                           free_block_limit),
              _sort_source(std::move(sort_source)),
              _queues_mem_usege(num_partitions),
              _each_queue_limit(config::local_exchange_buffer_mem_limit / num_partitions) {
        _data_queue.resize(num_partitions);
        for (size_t i = 0; i < num_partitions; i++) {
            _queues_mem_usege[i] = 0;
            _sink_deps.push_back(
                    std::make_shared<Dependency>(0, 0, "LOCAL_MERGE_SORT_SINK_DEPENDENCY", true));
            _queue_deps.push_back(
                    std::make_shared<Dependency>(0, 0, "LOCAL_MERGE_SORT_QUEUE_DEPENDENCY"));
            _queue_deps.back()->block();
        }
    }
    ~LocalMergeSortExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::LOCAL_MERGE_SORT; }

    Status build_merger(RuntimeState* statem, LocalExchangeSourceLocalState& local_state);

    std::vector<Dependency*> local_sink_state_dependency(int channel_id) override;

    std::vector<Dependency*> local_state_dependency(int channel_id) override;

    void add_mem_usage(LocalExchangeSinkLocalState& local_state, int64_t delta);
    void sub_mem_usage(LocalExchangeSinkLocalState& local_state, int64_t delta);
    void sub_mem_usage(LocalExchangeSourceLocalState& local_state, int channel_id, int64_t delta);
    void close(LocalExchangeSourceLocalState& local_state) override {}

private:
    // only channel_id = 0 , build _merger and use it

    std::unique_ptr<vectorized::VSortedRunMerger> _merger;
    std::shared_ptr<SortSourceOperatorX> _sort_source;
    std::vector<DependencySPtr> _sink_deps;
    std::vector<std::atomic_int64_t> _queues_mem_usege;
    // if cur queue is empty, block this queue
    std::vector<DependencySPtr> _queue_deps;
    const int64_t _each_queue_limit;
};

class BroadcastExchanger final : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
                                           free_block_limit) {
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
class AdaptivePassthroughExchanger : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(AdaptivePassthroughExchanger);
    AdaptivePassthroughExchanger(int running_sink_operators, int num_partitions,
                                 int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
                                           free_block_limit) {
        _data_queue.resize(num_partitions);
    }
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::ADAPTIVE_PASSTHROUGH; }

    void close(LocalExchangeSourceLocalState& local_state) override {}

private:
    Status _passthrough_sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                             LocalExchangeSinkLocalState& local_state);
    Status _shuffle_sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                         LocalExchangeSinkLocalState& local_state);
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, bool eos,
                       LocalExchangeSinkLocalState& local_state);

    std::atomic_bool _is_pass_through = false;
    std::atomic_int32_t _total_block = 0;
};

} // namespace doris::pipeline
