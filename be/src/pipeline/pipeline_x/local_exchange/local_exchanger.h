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

class Exchanger {
public:
    Exchanger(int running_sink_operators, int num_partitions)
            : _running_sink_operators(running_sink_operators),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_partitions) {}
    Exchanger(int running_sink_operators, int num_sources, int num_partitions)
            : _running_sink_operators(running_sink_operators),
              _num_partitions(num_partitions),
              _num_senders(running_sink_operators),
              _num_sources(num_sources) {}
    virtual ~Exchanger() = default;
    virtual Status get_block(RuntimeState* state, vectorized::Block* block,
                             SourceState& source_state,
                             LocalExchangeSourceLocalState& local_state) = 0;
    virtual Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                        LocalExchangeSinkLocalState& local_state) = 0;
    virtual ExchangeType get_type() const = 0;

protected:
    friend struct LocalExchangeSourceDependency;
    friend struct LocalExchangeSharedState;
    friend struct ShuffleBlockWrapper;
    friend class LocalExchangeSourceLocalState;
    friend class LocalExchangeSinkLocalState;
    std::atomic<int> _running_sink_operators = 0;
    const int _num_partitions;
    const int _num_senders;
    const int _num_sources;
    moodycamel::ConcurrentQueue<vectorized::Block> _free_blocks;
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
            data_block.clear_column_data();
            shared_state->exchanger->_free_blocks.enqueue(std::move(data_block));
        }
    }
    std::atomic<int> ref_count = 0;
    vectorized::Block data_block;
};

class ShuffleExchanger : public Exchanger {
    using PartitionedBlock =
            std::pair<std::shared_ptr<ShuffleBlockWrapper>,
                      std::tuple<std::shared_ptr<std::vector<uint32_t>>, size_t, size_t>>;

public:
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int running_sink_operators, int num_partitions)
            : Exchanger(running_sink_operators, num_partitions) {
        _data_queue.resize(num_partitions);
    }
    ~ShuffleExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::HASH_SHUFFLE; }

protected:
    ShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                     bool ignore_source_data_distribution)
            : Exchanger(running_sink_operators, num_sources, num_partitions),
              _ignore_source_data_distribution(ignore_source_data_distribution) {
        _data_queue.resize(num_partitions);
    }
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, SourceState source_state,
                       LocalExchangeSinkLocalState& local_state);

    std::vector<moodycamel::ConcurrentQueue<PartitionedBlock>> _data_queue;

    const bool _ignore_source_data_distribution = false;
};

class BucketShuffleExchanger : public ShuffleExchanger {
    ENABLE_FACTORY_CREATOR(BucketShuffleExchanger);
    BucketShuffleExchanger(int running_sink_operators, int num_sources, int num_partitions,
                           bool ignore_source_data_distribution)
            : ShuffleExchanger(running_sink_operators, num_sources, num_partitions,
                               ignore_source_data_distribution) {}
    ~BucketShuffleExchanger() override = default;
    ExchangeType get_type() const override { return ExchangeType::BUCKET_HASH_SHUFFLE; }
};

class PassthroughExchanger final : public Exchanger {
public:
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int running_sink_operators, int num_partitions)
            : Exchanger(running_sink_operators, num_partitions) {
        _data_queue.resize(num_partitions);
    }
    ~PassthroughExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASSTHROUGH; }

private:
    std::vector<moodycamel::ConcurrentQueue<vectorized::Block>> _data_queue;
};

class PassToOneExchanger final : public Exchanger {
public:
    ENABLE_FACTORY_CREATOR(PassToOneExchanger);
    PassToOneExchanger(int running_sink_operators, int num_partitions)
            : Exchanger(running_sink_operators, num_partitions) {
        _data_queue.resize(num_partitions);
    }
    ~PassToOneExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::PASS_TO_ONE; }

private:
    std::vector<moodycamel::ConcurrentQueue<vectorized::Block>> _data_queue;
};

class BroadcastExchanger final : public Exchanger {
public:
    ENABLE_FACTORY_CREATOR(BroadcastExchanger);
    BroadcastExchanger(int running_sink_operators, int num_partitions)
            : Exchanger(running_sink_operators, num_partitions) {
        _data_queue.resize(num_partitions);
    }
    ~BroadcastExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::BROADCAST; }

private:
    std::vector<moodycamel::ConcurrentQueue<vectorized::Block>> _data_queue;
};

//The code in AdaptivePassthroughExchanger is essentially
// a copy of ShuffleExchanger and PassthroughExchanger.
class AdaptivePassthroughExchanger : public Exchanger {
public:
    ENABLE_FACTORY_CREATOR(AdaptivePassthroughExchanger);
    AdaptivePassthroughExchanger(int running_sink_operators, int num_partitions)
            : Exchanger(running_sink_operators, num_partitions) {
        _data_queue.resize(num_partitions);
    }
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::ADAPTIVE_PASSTHROUGH; }

private:
    Status _passthrough_sink(RuntimeState* state, vectorized::Block* in_block,
                             SourceState source_state, LocalExchangeSinkLocalState& local_state);
    Status _shuffle_sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                         LocalExchangeSinkLocalState& local_state);
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, SourceState source_state,
                       LocalExchangeSinkLocalState& local_state);
    std::vector<moodycamel::ConcurrentQueue<vectorized::Block>> _data_queue;

    std::atomic_bool _is_pass_through = false;
    std::atomic_int32_t _total_block = 0;
};

} // namespace doris::pipeline
