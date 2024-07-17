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

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
class DataQueue;

class UnionSourceOperatorX;
class UnionSourceLocalState final : public PipelineXLocalState<UnionSharedState> {
public:
    ENABLE_FACTORY_CREATOR(UnionSourceLocalState);
    using Base = PipelineXLocalState<UnionSharedState>;
    using Parent = UnionSourceOperatorX;
    UnionSourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

private:
    friend class UnionSourceOperatorX;
    friend class OperatorX<UnionSourceLocalState>;
    bool _need_read_for_const_expr {true};
    int _const_expr_list_idx {0};
    std::vector<vectorized::VExprContextSPtrs> _const_expr_lists;

    // If this operator has no children, there is no shared state which owns dependency. So we
    // use this local state to hold this dependency.
    DependencySPtr _only_const_dependency = nullptr;
};

class UnionSourceOperatorX final : public OperatorX<UnionSourceLocalState> {
public:
    using Base = OperatorX<UnionSourceLocalState>;
    UnionSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                         const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs), _child_size(tnode.num_children) {};
    ~UnionSourceOperatorX() override = default;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

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
        RETURN_IF_ERROR(Base::prepare(state));
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
    bool _has_data(RuntimeState* state) const {
        auto& local_state = get_local_state(state);
        if (_child_size == 0) {
            return local_state._need_read_for_const_expr;
        }
        return local_state._shared_state->data_queue.remaining_has_data();
    }
    bool has_more_const(RuntimeState* state) const {
        auto& local_state = get_local_state(state);
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