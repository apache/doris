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

#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"

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

    virtual DependencySPtr get_local_state_dependency(int _channel_id) { return nullptr; }

    virtual std::string data_queue_debug_string(int i) = 0;

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
            if (!data_queue.enqueue(item)) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Exception occurs in data queue [size = {}] of local exchange.",
                                data_queue.size_approx());
            }
            return true;
        }
        return false;
    }

    inline bool enqueue(BlockType&& item) {
        if (!eos) {
            if (!data_queue.enqueue(std::move(item))) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Exception occurs in data queue [size = {}] of local exchange.",
                                data_queue.size_approx());
            }
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
            : ExchangerBase(running_sink_operators, num_partitions, free_block_limit) {
        _data_queue.resize(num_partitions);
        _m.resize(num_partitions);
        for (size_t i = 0; i < num_partitions; i++) {
            _m[i] = std::make_unique<std::mutex>();
        }
    }
    Exchanger(int running_sink_operators, int num_sources, int num_partitions, int free_block_limit)
            : ExchangerBase(running_sink_operators, num_sources, num_partitions, free_block_limit) {
        _data_queue.resize(num_partitions);
        _m.resize(num_partitions);
        for (size_t i = 0; i < num_partitions; i++) {
            _m[i] = std::make_unique<std::mutex>();
        }
    }
    ~Exchanger() override = default;
    std::string data_queue_debug_string(int i) override {
        return fmt::format("Data Queue {}: [size approx = {}, eos = {}]", i,
                           _data_queue[i].data_queue.size_approx(), _data_queue[i].eos);
    }

protected:
    bool _enqueue_data_and_set_ready(int channel_id, LocalExchangeSinkLocalState& local_state,
                                     BlockType&& block);
    bool _dequeue_data(LocalExchangeSourceLocalState& local_state, BlockType& block, bool* eos);
    std::vector<BlockQueue<BlockType>> _data_queue;
    std::vector<std::unique_ptr<std::mutex>> _m;
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
                // Free blocks is used to improve memory efficiency. Failure during pushing back
                // free block will not incur any bad result so just ignore the return value.
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
                                          free_block_limit) {}
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
              _ignore_source_data_distribution(ignore_source_data_distribution) {}
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
                                           free_block_limit) {}
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
                                           free_block_limit) {}
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }
    void close(LocalExchangeSourceLocalState& local_state) override {}
};

class BroadcastExchanger final : public Exchanger<vectorized::Block> {
public:
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions, int free_block_limit)
            : Exchanger<vectorized::Block>(running_sink_operators, num_partitions,
                                           free_block_limit) {}
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
                                           free_block_limit) {}
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
