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
class LocalExchangeSourceOperatorX;
class LocalExchangeSourceLocalState final : public PipelineXLocalState<LocalExchangeSharedState> {
public:
    using Base = PipelineXLocalState<LocalExchangeSharedState>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSourceLocalState);
    LocalExchangeSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {}

    Status init(RuntimeState* state, LocalStateInfo& info) override;
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
    friend class AdaptivePassthroughExchanger;
    template <typename BlockType>
    friend class Exchanger;

    ExchangerBase* _exchanger = nullptr;
    int _channel_id;
    RuntimeProfile::Counter* _get_block_failed_counter = nullptr;
    RuntimeProfile::Counter* _copy_data_timer = nullptr;
    std::vector<RuntimeProfile::Counter*> _deps_counter;
    std::vector<DependencySPtr> _local_merge_deps;
};

class LocalExchangeSourceOperatorX final : public OperatorX<LocalExchangeSourceLocalState> {
public:
    using Base = OperatorX<LocalExchangeSourceLocalState>;
    LocalExchangeSourceOperatorX(ObjectPool* pool, int id) : Base(pool, id, id) {}
    LocalExchangeSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs),
              _exchange_type(tnode.local_exchange_node.partition_type),
              _planned_by_fe(true) {}
#ifdef BE_TEST
    LocalExchangeSourceOperatorX() = default;
#endif
    Status init(TLocalPartitionType::type type) override {
        DCHECK(!_planned_by_fe);
        _op_name = "LOCAL_EXCHANGE_OPERATOR(" + get_exchange_type_name(type) + ")";
        _exchange_type = type;
        return Status::OK();
    }
    Status prepare(RuntimeState* state) override {
        if (_planned_by_fe) {
            RETURN_IF_ERROR(Base::prepare(state));
            // Base::prepare() resets _op_name from tnode node_type; restore the type-qualified name.
            _op_name = "LOCAL_EXCHANGE_OPERATOR(" + get_exchange_type_name(_exchange_type) + ")";
            return Status::OK();
        }
        return Status::OK();
    }
    const RowDescriptor& intermediate_row_desc() const override {
        if (_planned_by_fe) {
            return Base::intermediate_row_desc();
        }
        return _child->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override {
        if (_planned_by_fe) {
            return Base::row_descriptor();
        }
        return _child->row_descriptor();
    }
    const RowDescriptor& row_desc() const override {
        if (_planned_by_fe) {
            return Base::row_desc();
        }
        return _child->row_desc();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

private:
    friend class LocalExchangeSourceLocalState;

    TLocalPartitionType::type _exchange_type;
    const bool _planned_by_fe = false;
};

} // namespace doris::pipeline
