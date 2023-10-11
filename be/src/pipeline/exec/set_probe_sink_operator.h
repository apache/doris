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

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
template <class HashTableContext, bool is_intersected>
struct HashTableProbeX;
} // namespace vectorized

namespace pipeline {

template <bool is_intersect>
class SetProbeSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::VSetOperationNode<is_intersect>> {
private:
    constexpr static auto builder_name =
            is_intersect ? "IntersectProbeSinkOperator" : "ExceptProbeSinkOperator";

public:
    SetProbeSinkOperatorBuilder(int32_t id, int child_id, ExecNode* set_node);
    [[nodiscard]] bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;

private:
    int _child_id;
};

template <bool is_intersect>
class SetProbeSinkOperator : public StreamingOperator<SetProbeSinkOperatorBuilder<is_intersect>> {
public:
    SetProbeSinkOperator(OperatorBuilderBase* operator_builder, int child_id, ExecNode* set_node);

    bool can_write() override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;
    Status open(RuntimeState* /*state*/) override { return Status::OK(); }

private:
    int _child_id;
};

template <bool is_intersect>
class SetProbeSinkOperatorX;

template <bool is_intersect>
class SetProbeSinkLocalState final : public PipelineXSinkLocalState<SetDependency> {
public:
    ENABLE_FACTORY_CREATOR(SetProbeSinkLocalState);
    using Base = PipelineXSinkLocalState<SetDependency>;
    using Parent = SetProbeSinkOperatorX<is_intersect>;

    SetProbeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class SetProbeSinkOperatorX<is_intersect>;
    template <class HashTableContext, bool is_intersected>
    friend struct vectorized::HashTableProbeX;

    RuntimeProfile::Counter* _probe_timer; // time to probe
    //record insert column id during probe
    std::vector<uint16_t> _probe_column_inserted_id;
    vectorized::ColumnRawPtrs _probe_columns;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
};

template <bool is_intersect>
class SetProbeSinkOperatorX final : public DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>> {
public:
    using Base = DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>;
    using DataSinkOperatorXBase::id;
    using typename Base::LocalState;

    friend class SetProbeSinkLocalState<is_intersect>;
    SetProbeSinkOperatorX(int child_id, int sink_id, ObjectPool* pool, const TPlanNode& tnode,
                          const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id, tnode.node_id), _cur_child_id(child_id) {}
    ~SetProbeSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError(
                "{} should not init with TDataSink",
                DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        const std::vector<std::vector<TExpr>>* result_texpr_lists;

        // Create result_expr_ctx_lists_ from thrift exprs.
        if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
            result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
        } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
            result_texpr_lists = &(tnode.except_node.result_expr_lists);
        } else {
            return Status::NotSupported("Not Implemented, Check The Operation Node.");
        }

        const auto& texpr = (*result_texpr_lists)[_cur_child_id];
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texpr, _child_exprs));

        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::prepare(state));
        return vectorized::VExpr::prepare(_child_exprs, state, _child_x->row_desc());
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::open(state));
        return vectorized::VExpr::open(_child_exprs, state);
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    WriteDependency* wait_for_dependency(RuntimeState* state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
        return ((SetSharedState*)local_state._dependency->shared_state())
                               ->probe_finished_children_index[_cur_child_id - 1]
                       ? nullptr
                       : local_state._dependency;
    }

private:
    void _finalize_probe(SetProbeSinkLocalState<is_intersect>& local_state);
    Status _extract_probe_column(SetProbeSinkLocalState<is_intersect>& local_state,
                                 vectorized::Block& block, vectorized::ColumnRawPtrs& raw_ptrs,
                                 int child_id);
    void _refresh_hash_table(SetProbeSinkLocalState<is_intersect>& local_state);
    const int _cur_child_id;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
    using OperatorBase::_child_x;
};

} // namespace pipeline
} // namespace doris
