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

#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vunion_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
class DataQueue;

class UnionSourceOperatorBuilder final : public OperatorBuilder<vectorized::VUnionNode> {
public:
    UnionSourceOperatorBuilder(int32_t id, ExecNode* node, std::shared_ptr<DataQueue>);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class UnionSourceOperator final : public SourceOperator<UnionSourceOperatorBuilder> {
public:
    UnionSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* node,
                        std::shared_ptr<DataQueue>);

    // this operator in source open directly return, do this work in sink
    Status open(RuntimeState* /*state*/) override { return Status::OK(); }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;
    bool can_read() override;

    Status pull_data(RuntimeState* state, vectorized::Block* output_block, bool* eos);

private:
    bool _has_data();

    std::shared_ptr<DataQueue> _data_queue;
    bool _need_read_for_const_expr;
};

class UnionSourceOperatorX;
class UnionSourceLocalState final : public PipelineXLocalState<UnionDependency> {
public:
    ENABLE_FACTORY_CREATOR(UnionSourceLocalState);
    using Base = PipelineXLocalState<UnionDependency>;
    using Parent = UnionSourceOperatorX;
    UnionSourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    friend class UnionSourceOperatorX;
    bool _need_read_for_const_expr {false};
};

class UnionSourceOperatorX final : public OperatorX<UnionSourceLocalState> {
public:
    using Base = OperatorX<UnionSourceLocalState>;
    UnionSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : Base(pool, tnode, descs), _child_size(tnode.num_children) {};
    ~UnionSourceOperatorX() override = default;
    Dependency* wait_for_dependency(RuntimeState* state) override {
        auto& local_state = state->get_local_state(id())->cast<UnionSourceLocalState>();
        return local_state._dependency->read_blocked_by();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    bool _has_data(RuntimeState* state) {
        auto& local_state = state->get_local_state(id())->cast<UnionSourceLocalState>();
        return local_state._shared_state->_data_queue->remaining_has_data();
    }
    bool has_more_const(const RuntimeState* state) const {
        return state->per_fragment_instance_idx() == 0;
    }
    friend class UnionSourceLocalState;
    const int _child_size;
};

} // namespace pipeline
} // namespace doris