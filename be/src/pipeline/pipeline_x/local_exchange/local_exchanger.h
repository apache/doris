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

class Exchanger {
public:
    Exchanger(int num_partitions)
            : _running_sink_operators(num_partitions), _num_partitions(num_partitions) {}
    Exchanger(int running_sink_operators, int num_partitions)
            : _running_sink_operators(running_sink_operators), _num_partitions(num_partitions) {}
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
    std::atomic<int> _running_sink_operators = 0;
    const int _num_partitions;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

class ShuffleExchanger : public Exchanger {
    using PartitionedBlock =
            std::pair<std::shared_ptr<vectorized::Block>,
                      std::tuple<std::shared_ptr<std::vector<uint32_t>>, size_t, size_t>>;

public:
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int num_partitions) : Exchanger(num_partitions) {
        _data_queue.resize(num_partitions);
    }
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
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, SourceState source_state,
                       LocalExchangeSinkLocalState& local_state);

    std::vector<moodycamel::ConcurrentQueue<PartitionedBlock>> _data_queue;
};

class BucketShuffleExchanger : public ShuffleExchanger {
    ENABLE_FACTORY_CREATOR(BucketShuffleExchanger);
    BucketShuffleExchanger(int running_sink_operators, int num_buckets)
            : ShuffleExchanger(running_sink_operators, num_buckets) {}
    ~BucketShuffleExchanger() override = default;
    ExchangeType get_type() const override { return ExchangeType::BUCKET_HASH_SHUFFLE; }
};

class PassthroughExchanger final : public Exchanger {
public:
    ENABLE_FACTORY_CREATOR(PassthroughExchanger);
    PassthroughExchanger(int num_instances) : Exchanger(num_instances) {
        _data_queue.resize(num_instances);
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

} // namespace doris::pipeline
