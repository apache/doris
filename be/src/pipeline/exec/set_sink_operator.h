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

#include "olap/olap_common.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;

namespace vectorized {
template <class HashTableContext, bool is_intersected>
struct HashTableBuildX;
}

namespace pipeline {

template <bool is_intersect>
class SetSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::VSetOperationNode<is_intersect>> {
private:
    constexpr static auto builder_name =
            is_intersect ? "IntersectSinkOperator" : "ExceptSinkOperator";

public:
    SetSinkOperatorBuilder(int32_t id, ExecNode* set_node);
    [[nodiscard]] bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;
};

template <bool is_intersect>
class SetSinkOperator : public StreamingOperator<SetSinkOperatorBuilder<is_intersect>> {
public:
    SetSinkOperator(OperatorBuilderBase* operator_builder,
                    vectorized::VSetOperationNode<is_intersect>* set_node);

    bool can_write() override { return true; }

private:
    vectorized::VSetOperationNode<is_intersect>* _set_node;
};

template <bool is_intersect>
class SetSinkOperatorX;

template <bool is_intersect>
class SetSinkLocalState final : public PipelineXSinkLocalState<SetDependency> {
public:
    ENABLE_FACTORY_CREATOR(SetSinkLocalState);
    using Base = PipelineXSinkLocalState<SetDependency>;
    using Parent = SetSinkOperatorX<is_intersect>;

    SetSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXSinkLocalState<SetDependency>::init(state, info));
        _build_timer = ADD_TIMER(_profile, "BuildTime");

        Parent& parent = _parent->cast<Parent>();
        _shared_state->_childs = parent._child_quantity;
        _shared_state->_probe_finished_children_index.assign(parent._child_quantity, false);

        auto& child_exprs_lists = _shared_state->_child_exprs_lists;
        DCHECK(child_exprs_lists.size() == 0 || child_exprs_lists.size() == parent._child_quantity);
        if (child_exprs_lists.size() == 0) {
            child_exprs_lists.resize(parent._child_quantity);
        }
        child_exprs_lists[parent._cur_child_id] = parent._child_exprs;
        return Status::OK();
    }

private:
    friend class SetSinkOperatorX<is_intersect>;
    template <class HashTableContext, bool is_intersected>
    friend struct vectorized::HashTableBuildX;

    RuntimeProfile::Counter* _build_timer; // time to build hash table
    vectorized::MutableBlock _mutable_block;
};

template <bool is_intersect>
class SetSinkOperatorX final : public DataSinkOperatorX<SetSinkLocalState<is_intersect>> {
public:
    using Base = DataSinkOperatorX<SetSinkLocalState<is_intersect>>;
    using DataSinkOperatorXBase::id;
    using typename Base::LocalState;

    friend class SetSinkLocalState<is_intersect>;
    SetSinkOperatorX(int child_id, int sink_id, ObjectPool* pool, const TPlanNode& tnode,
                     const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id), _cur_child_id(child_id) {}
    ~SetSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     DataSinkOperatorX<SetSinkLocalState<is_intersect>>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        const std::vector<std::vector<TExpr>>* result_texpr_lists;

        // Create result_expr_ctx_lists_ from thrift exprs.
        if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
            result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
            _child_quantity = tnode.intersect_node.result_expr_lists.size();
        } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
            result_texpr_lists = &(tnode.except_node.result_expr_lists);
            _child_quantity = tnode.except_node.result_expr_lists.size();
        } else {
            return Status::NotSupported("Not Implemented, Check The Operation Node.");
        }

        const auto& texpr = (*result_texpr_lists)[_cur_child_id];
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texpr, _child_exprs));

        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SetSinkOperatorX<is_intersect>>::prepare(state));
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_exprs, state, _child_x->row_desc()));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

private:
    template <class HashTableContext, bool is_intersected>
    friend struct HashTableBuild;

    Status process_build_block(SetSinkLocalState<is_intersect>& local_state,
                               vectorized::Block& block, uint8_t offset, RuntimeState* state);
    Status extract_build_column(SetSinkLocalState<is_intersect>& local_state,
                                vectorized::Block& block, vectorized::ColumnRawPtrs& raw_ptrs);

    const int _cur_child_id;
    int _child_quantity;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
    using OperatorBase::_child_x;
};

} // namespace pipeline
} // namespace doris
