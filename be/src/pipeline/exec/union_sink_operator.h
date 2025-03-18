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
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {
class DataQueue;

class UnionSinkOperatorX;
class UnionSinkLocalState final : public PipelineXSinkLocalState<UnionSharedState> {
public:
    ENABLE_FACTORY_CREATOR(UnionSinkLocalState);
    UnionSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {}
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    friend class UnionSinkOperatorX;
    using Base = PipelineXSinkLocalState<UnionSharedState>;
    using Parent = UnionSinkOperatorX;

private:
    std::unique_ptr<vectorized::Block> _output_block;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    vectorized::VExprContextSPtrs _child_expr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
};

class UnionSinkOperatorX MOCK_REMOVE(final) : public DataSinkOperatorX<UnionSinkLocalState> {
public:
    using Base = DataSinkOperatorX<UnionSinkLocalState>;

    friend class UnionSinkLocalState;
    UnionSinkOperatorX(int child_id, int sink_id, int dest_id, ObjectPool* pool,
                       const TPlanNode& tnode, const DescriptorTbl& descs);
#ifdef BE_TEST
    UnionSinkOperatorX(int child_size, int cur_child_id, int first_materialized_child_idx)
            : _first_materialized_child_idx(first_materialized_child_idx),
              _cur_child_id(cur_child_id),
              _child_size(child_size) {}
#endif
    ~UnionSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     DataSinkOperatorX<UnionSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    std::shared_ptr<BasicSharedState> create_shared_state() const override {
        if (_cur_child_id > 0) {
            return nullptr;
        } else {
            std::shared_ptr<BasicSharedState> ss = std::make_shared<UnionSharedState>(_child_size);
            ss->id = operator_id();
            for (auto& dest : dests_id()) {
                ss->related_op_ids.insert(dest);
            }
            return ss;
        }
    }

    bool require_shuffled_data_distribution() const override {
        return _followed_by_shuffled_operator;
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state._shared_state->data_queue.set_low_memory_mode();
    }

    bool is_shuffled_operator() const override { return _followed_by_shuffled_operator; }

    MOCK_FUNCTION const RowDescriptor& row_descriptor() { return _row_descriptor; }

private:
    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    vectorized::VExprContextSPtrs _child_expr;
    /// Index of the first non-passthrough child; i.e. a child that needs materialization.
    /// 0 when all children are materialized, '_children.size()' when no children are
    /// materialized.
    const int _first_materialized_child_idx;

    const RowDescriptor _row_descriptor;
    const int _cur_child_id;
    const int _child_size;

    bool is_child_passthrough(int child_idx) const {
        DCHECK_LT(child_idx, _child_size);
        return child_idx < _first_materialized_child_idx;
    }
    Status materialize_child_block(RuntimeState* state, vectorized::Block* input_block,
                                   vectorized::Block* output_block);
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris