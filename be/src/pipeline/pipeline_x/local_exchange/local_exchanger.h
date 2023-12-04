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
    Exchanger(int num_instances)
            : running_sink_operators(num_instances), _num_instances(num_instances) {}
    virtual ~Exchanger() = default;
    virtual Status get_block(RuntimeState* state, vectorized::Block* block,
                             SourceState& source_state,
                             LocalExchangeSourceLocalState& local_state) = 0;
    virtual Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                        LocalExchangeSinkLocalState& local_state) = 0;
    virtual ExchangeType get_type() const = 0;

    std::atomic<int> running_sink_operators = 0;

protected:
    const int _num_instances;
};

class LocalExchangeSourceLocalState;
class LocalExchangeSinkLocalState;

class ShuffleExchanger final : public Exchanger {
    using PartitionedBlock =
            std::pair<std::shared_ptr<vectorized::Block>,
                      std::tuple<std::shared_ptr<std::vector<uint32_t>>, size_t, size_t>>;

public:
    ENABLE_FACTORY_CREATOR(ShuffleExchanger);
    ShuffleExchanger(int num_instances) : Exchanger(num_instances) {
        _data_queue.resize(num_instances);
    }
    ~ShuffleExchanger() override = default;
    Status sink(RuntimeState* state, vectorized::Block* in_block, SourceState source_state,
                LocalExchangeSinkLocalState& local_state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, SourceState& source_state,
                     LocalExchangeSourceLocalState& local_state) override;
    ExchangeType get_type() const override { return ExchangeType::SHUFFLE; }

private:
    Status _split_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                       vectorized::Block* block, SourceState source_state,
                       LocalExchangeSinkLocalState& local_state);

    std::vector<moodycamel::ConcurrentQueue<PartitionedBlock>> _data_queue;
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
