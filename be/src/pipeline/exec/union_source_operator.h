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
    std::shared_ptr<UnionSharedState> create_shared_state();

private:
    friend class UnionSourceOperatorX;
    friend class OperatorX<UnionSourceLocalState>;
    bool _need_read_for_const_expr {true};
    int _const_expr_list_idx {0};
    std::vector<vectorized::VExprContextSPtrs> _const_expr_lists;
};

class UnionSourceOperatorX final : public OperatorX<UnionSourceLocalState> {
public:
    using Base = OperatorX<UnionSourceLocalState>;
    UnionSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : Base(pool, tnode, descs), _child_size(tnode.num_children) {};
    ~UnionSourceOperatorX() override = default;
    Dependency* wait_for_dependency(RuntimeState* state) override {
        if (_child_size == 0) {
            return nullptr;
        }
        CREATE_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
        return local_state._dependency->read_blocked_by();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        RETURN_IF_ERROR(Base::init(tnode, state));
        DCHECK(tnode.__isset.union_node);
        // Create const_expr_ctx_lists_ from thrift exprs.
        auto& const_texpr_lists = tnode.union_node.const_expr_lists;
        for (auto& texprs : const_texpr_lists) {
            vectorized::VExprContextSPtrs ctxs;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texprs, ctxs));
            _const_expr_lists.push_back(ctxs);
        }
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        static_cast<void>(Base::prepare(state));
        // Prepare const expr lists.
        for (const vectorized::VExprContextSPtrs& exprs : _const_expr_lists) {
            RETURN_IF_ERROR(vectorized::VExpr::prepare(exprs, state, _row_descriptor));
        }
        return Status::OK();
    }
    Status open(RuntimeState* state) override {
        static_cast<void>(Base::open(state));
        // open const expr lists.
        for (const auto& exprs : _const_expr_lists) {
            RETURN_IF_ERROR(vectorized::VExpr::open(exprs, state));
        }
        return Status::OK();
    }
    [[nodiscard]] int get_child_count() const { return _child_size; }

private:
    bool _has_data(RuntimeState* state) {
        auto& local_state = state->get_local_state(id())->cast<UnionSourceLocalState>();
        if (_child_size == 0) {
            return local_state._need_read_for_const_expr;
        }
        return local_state._shared_state->data_queue.remaining_has_data();
    }
    bool has_more_const(RuntimeState* state) const {
        auto& local_state = state->get_local_state(id())->cast<UnionSourceLocalState>();
        return state->per_fragment_instance_idx() == 0 &&
               local_state._const_expr_list_idx < local_state._const_expr_lists.size();
    }
    friend class UnionSourceLocalState;
    const int _child_size;
    Status get_next_const(RuntimeState* state, vectorized::Block* block);
    std::vector<vectorized::VExprContextSPtrs> _const_expr_lists;
};

} // namespace pipeline
} // namespace doris