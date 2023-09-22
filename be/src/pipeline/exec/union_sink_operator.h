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
#include "vec/core/block.h"
#include "vec/exec/vunion_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
class DataQueue;

class UnionSinkOperatorBuilder final : public OperatorBuilder<vectorized::VUnionNode> {
public:
    UnionSinkOperatorBuilder(int32_t id, int child_id, ExecNode* node,
                             std::shared_ptr<DataQueue> queue);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; }

private:
    int _cur_child_id;
    std::shared_ptr<DataQueue> _data_queue;
};

class UnionSinkOperator final : public StreamingOperator<UnionSinkOperatorBuilder> {
public:
    UnionSinkOperator(OperatorBuilderBase* operator_builder, int child_id, ExecNode* node,
                      std::shared_ptr<DataQueue> queue);

    bool can_write() override { return true; }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    Status close(RuntimeState* state) override;

private:
    int _cur_child_id;
    std::shared_ptr<DataQueue> _data_queue;
    std::unique_ptr<vectorized::Block> _output_block;
};

class UnionSinkOperatorX;
class UnionSinkLocalState final : public PipelineXSinkLocalState<UnionDependency> {
public:
    ENABLE_FACTORY_CREATOR(UnionSinkLocalState);
    UnionSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _child_row_idx(0) {}
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    friend class UnionSinkOperatorX;
    using Base = PipelineXSinkLocalState<UnionDependency>;
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
};

class UnionSinkOperatorX final : public DataSinkOperatorX<UnionSinkLocalState> {
public:
    using Base = DataSinkOperatorX<UnionSinkLocalState>;

    friend class UnionSinkLocalState;
    UnionSinkOperatorX(int child_id, int sink_id, ObjectPool* pool, const TPlanNode& tnode,
                       const DescriptorTbl& descs);
    ~UnionSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink");
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

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
                                                                               _row_descriptor);
            vectorized::Block res;
            RETURN_IF_ERROR(materialize_block(state, input_block, child_id, &res));
            RETURN_IF_ERROR(mblock.merge(res));
        }
        return Status::OK();
    }

    Status materialize_block(RuntimeState* state, vectorized::Block* src_block, int child_idx,
                             vectorized::Block* res_block) {
        CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
        const auto& child_exprs = local_state._child_expr;
        vectorized::ColumnsWithTypeAndName colunms;
        for (size_t i = 0; i < child_exprs.size(); ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(child_exprs[i]->execute(src_block, &result_column_id));
            colunms.emplace_back(src_block->get_by_position(result_column_id));
        }
        local_state._child_row_idx += src_block->rows();
        *res_block = {colunms};
        return Status::OK();
    }
};

} // namespace pipeline
} // namespace doris