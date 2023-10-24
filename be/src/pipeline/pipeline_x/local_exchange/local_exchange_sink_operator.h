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

class LocalExchangeSinkOperatorX;
class LocalExchangeSinkLocalState final : public PipelineXSinkLocalState<LocalExchangeDependency> {
public:
    using Base = PipelineXSinkLocalState<LocalExchangeDependency>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSinkLocalState);

    LocalExchangeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~LocalExchangeSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status channel_add_rows(RuntimeState* state, const uint32_t* __restrict channel_ids,
                            vectorized::Block* block, SourceState source_state);

private:
    friend class LocalExchangeSinkOperatorX;

    RuntimeProfile::Counter* _compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* _distribute_timer = nullptr;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> _mutable_block;
};

// A single 32-bit division on a recent x64 processor has a throughput of one instruction every six cycles with a latency of 26 cycles.
// In contrast, a multiplication has a throughput of one instruction every cycle and a latency of 3 cycles.
// So we prefer to this algorithm instead of modulo.
// Reference: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
struct LocalExchangeChannelIds {
    static constexpr auto SHIFT_BITS = 32;
    uint32_t operator()(uint32_t l, uint32_t r) {
        return ((uint64_t)l * (uint64_t)r) >> SHIFT_BITS;
    }
};

class LocalExchangeSinkOperatorX final : public DataSinkOperatorX<LocalExchangeSinkLocalState> {
public:
    using Base = DataSinkOperatorX<LocalExchangeSinkLocalState>;
    LocalExchangeSinkOperatorX(int sink_id, int num_partitions, const std::vector<TExpr>& texprs)
            : Base(sink_id, -1), _num_partitions(num_partitions), _texprs(texprs) {}

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init() override {
        _name = "LOCAL_EXCHANGE_SINK_OPERATOR";
        _partitioner.reset(
                new vectorized::Crc32HashPartitioner<LocalExchangeChannelIds>(_num_partitions));
        RETURN_IF_ERROR(_partitioner->init(_texprs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(_partitioner->prepare(state, _child_x->row_desc()));
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(_partitioner->open(state));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

private:
    friend class LocalExchangeSinkLocalState;
    const int _num_partitions;
    const std::vector<TExpr>& _texprs;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
};

} // namespace doris::pipeline
