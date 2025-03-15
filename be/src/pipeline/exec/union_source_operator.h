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
#include "common/compile_check_begin.h"
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

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

private:
    friend class UnionSourceOperatorX;
    friend class OperatorX<UnionSourceLocalState>;
    bool _need_read_for_const_expr {true};
    int _const_expr_list_idx {0};

    // If this operator has no children, there is no shared state which owns dependency. So we
    // use this local state to hold this dependency.
    DependencySPtr _only_const_dependency = nullptr;
};

/*
There are two cases for union node: one is only constant expressions, and the other is having other child nodes besides constant expressions.
Unlike other union operators, the union node only merges data without deduplication.

|   0:VUNION(66)                                                                                                           |
|      constant exprs:                                                                                                     |
|          1 | 2 | 3 | 4                                                                                                   |
|          5 | 6 | 7 | 8                                                                                                   |
|      tuple ids: 0                                                                                                        | 

|   4:VUNION(179)                                                                                                           |
|   |  constant exprs:                                                                                                      |
|   |      1 | 2 | 3 | 4                                                                                                    |
|   |      5 | 6 | 7 | 8                                                                                                    |
|   |  child exprs:                                                                                                         |
|   |      k1[#0] | k2[#1] | k3[#2] | k4[#3]                                                                                |
|   |      k1[#4] | k2[#5] | k3[#6] | k4[#7]                                                                                |
|   |  tuple ids: 2                                                                                                         |
*/

class UnionSourceOperatorX MOCK_REMOVE(final) : public OperatorX<UnionSourceLocalState> {
public:
    using Base = OperatorX<UnionSourceLocalState>;
    UnionSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                         const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs), _child_size(tnode.num_children) {}

#ifdef BE_TEST
    UnionSourceOperatorX(int child_size) : _child_size(child_size) {}
#endif
    ~UnionSourceOperatorX() override = default;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    [[nodiscard]] int get_child_count() const { return _child_size; }
    bool require_shuffled_data_distribution() const override {
        return _followed_by_shuffled_operator;
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        if (local_state._shared_state) {
            local_state._shared_state->data_queue.set_low_memory_mode();
        }
    }

    bool is_shuffled_operator() const override { return _followed_by_shuffled_operator; }
    Status set_child(OperatorPtr child) override {
        Base::_child = child;
        return Status::OK();
    }

private:
    bool has_data(RuntimeState* state) const {
        auto& local_state = get_local_state(state);
        if (_child_size == 0) {
            return local_state._need_read_for_const_expr;
        }
        return local_state._shared_state->data_queue.remaining_has_data();
    }
    bool has_more_const(RuntimeState* state) const {
        // For constant expressions, only one instance will execute the expression
        auto& local_state = get_local_state(state);
        return state->per_fragment_instance_idx() == 0 &&
               local_state._const_expr_list_idx < _const_expr_lists.size();
    }
    friend class UnionSourceLocalState;
    const int _child_size;
    Status get_next_const(RuntimeState* state, vectorized::Block* block);
    std::vector<vectorized::VExprContextSPtrs> _const_expr_lists;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris