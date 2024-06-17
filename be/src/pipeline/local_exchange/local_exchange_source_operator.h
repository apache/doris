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

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

class ExchangerBase;
class ShuffleExchanger;
class PassthroughExchanger;
class BroadcastExchanger;
class PassToOneExchanger;
class LocalMergeSortExchanger;
class LocalExchangeSourceOperatorX;
class LocalExchangeSourceLocalState final : public PipelineXLocalState<LocalExchangeSharedState> {
public:
    using Base = PipelineXLocalState<LocalExchangeSharedState>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSourceLocalState);
    LocalExchangeSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {}

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const override;

    std::vector<Dependency*> dependencies() const override;

private:
    friend class LocalExchangeSourceOperatorX;
    friend class ExchangerBase;
    friend class ShuffleExchanger;
    friend class PassthroughExchanger;
    friend class BroadcastExchanger;
    friend class PassToOneExchanger;
    friend class LocalMergeSortExchanger;
    friend class AdaptivePassthroughExchanger;

    ExchangerBase* _exchanger = nullptr;
    int _channel_id;
    RuntimeProfile::Counter* _get_block_failed_counter = nullptr;
    RuntimeProfile::Counter* _copy_data_timer = nullptr;
};

class LocalExchangeSourceOperatorX final : public OperatorX<LocalExchangeSourceLocalState> {
public:
    using Base = OperatorX<LocalExchangeSourceLocalState>;
    LocalExchangeSourceOperatorX(ObjectPool* pool, int id) : Base(pool, id, id) {}
    Status init(ExchangeType type) override {
        _op_name = "LOCAL_EXCHANGE_OPERATOR (" + get_exchange_type_name(type) + ")";
        _exchange_type = type;
        return Status::OK();
    }
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    const RowDescriptor& intermediate_row_desc() const override {
        return _child_x->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override { return _child_x->row_descriptor(); }
    const RowDescriptor& row_desc() const override { return _child_x->row_desc(); }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    // If input data distribution is ignored by this fragment, this first local exchange source in this fragment will re-assign all data.
    bool ignore_data_distribution() const override { return false; }

private:
    friend class LocalExchangeSourceLocalState;

    ExchangeType _exchange_type;
};

} // namespace doris::pipeline
