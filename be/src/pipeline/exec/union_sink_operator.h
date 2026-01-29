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
#include "vec/core/column_with_type_and_name.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

inline Status materialize_block(const vectorized::VExprContextSPtrs& exprs,
                                vectorized::Block* src_block, vectorized::Block* res_block,
                                bool need_clone) {
    vectorized::ColumnsWithTypeAndName columns;
    auto rows = src_block->rows();
    for (const auto& expr : exprs) {
        vectorized::ColumnWithTypeAndName result_data;
        RETURN_IF_ERROR(expr->execute(src_block, result_data));
        if (need_clone) {
            result_data.column = result_data.column->clone_resized(rows);
        }
        columns.emplace_back(result_data);
    }
    *res_block = {columns};
    return Status::OK();
}

namespace pipeline {
class DataQueue;

class UnionSinkOperatorX;
class UnionSinkLocalState final : public PipelineXSinkLocalState<UnionSharedState> {
public:
    ENABLE_FACTORY_CREATOR(UnionSinkLocalState);
    UnionSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _child_row_idx(0) {}
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    friend class UnionSinkOperatorX;
    using Base = PipelineXSinkLocalState<UnionSharedState>;
    using Parent = UnionSinkOperatorX;

private:
    std::unique_ptr<vectorized::Block> _output_block;

    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    vectorized::VExprContextSPtrs _const_expr;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    vectorized::VExprContextSPtrs _child_expr;

    /// Index of current row in child_row_block_.
    int _child_row_idx;
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

    MOCK_FUNCTION const RowDescriptor& row_descriptor() { return _row_descriptor; }

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

    DataDistribution required_data_distribution(RuntimeState* state) const override {
        if (_require_bucket_distribution) {
            return DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _distribute_exprs);
        }
        if (_followed_by_shuffled_operator) {
            return DataDistribution(ExchangeType::HASH_SHUFFLE, _distribute_exprs);
        }
        return Base::required_data_distribution(state);
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state._shared_state->data_queue.set_low_memory_mode();
    }

private:
    int _get_first_materialized_child_idx() const { return _first_materialized_child_idx; }

    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    vectorized::VExprContextSPtrs _const_expr;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    vectorized::VExprContextSPtrs _child_expr;
    /// Index of the first non-passthrough child; i.e. a child that needs materialization.
    /// 0 when all children are materialized, '_children.size()' when no children are
    /// materialized.
    const int _first_materialized_child_idx;

    const RowDescriptor _row_descriptor;
    const int _cur_child_id;
    const int _child_size;
    const std::vector<TExpr> _distribute_exprs;
    int children_count() const { return _child_size; }
    bool is_child_passthrough(int child_idx) const {
        DCHECK_LT(child_idx, _child_size);
        return child_idx < _first_materialized_child_idx;
    }
    Status materialize_child_block(RuntimeState* state, int child_id,
                                   vectorized::Block* input_block,
                                   vectorized::Block* output_block) {
        DCHECK_LT(child_id, _child_size);
        DCHECK(!is_child_passthrough(child_id));
        if (input_block->rows() > 0) {
            vectorized::MutableBlock mblock =
                    vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                               row_descriptor());
            vectorized::Block res;
            auto& local_state = get_local_state(state);
            {
                SCOPED_TIMER(local_state._expr_timer);
                RETURN_IF_ERROR(
                        materialize_block(local_state._child_expr, input_block, &res, false));
            }
            local_state._child_row_idx += res.rows();
            RETURN_IF_ERROR(mblock.merge(res));
        }
        return Status::OK();
    }
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris