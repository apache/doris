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

#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/join/vjoin_node_base.h"

namespace doris {

namespace pipeline {
template <typename LocalStateType>
class JoinProbeOperatorX;
template <typename DependencyType, typename Derived>
class JoinProbeLocalState : public PipelineXLocalState<DependencyType> {
public:
    using Base = PipelineXLocalState<DependencyType>;
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;
    virtual void add_tuple_is_null_column(vectorized::Block* block) = 0;

protected:
    template <typename LocalStateType>
    friend class StatefulOperatorX;
    JoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent),
              _child_block(vectorized::Block::create_unique()),
              _child_source_state(SourceState::DEPEND_ON_SOURCE) {}
    ~JoinProbeLocalState() override = default;
    void _construct_mutable_join_block();
    Status _build_output_block(vectorized::Block* origin_block, vectorized::Block* output_block,
                               bool keep_origin = true);
    void _reset_tuple_is_null_column();
    // output expr
    vectorized::VExprContextSPtrs _output_expr_ctxs;
    vectorized::Block _join_block;
    vectorized::MutableColumnPtr _tuple_is_null_left_flag_column;
    vectorized::MutableColumnPtr _tuple_is_null_right_flag_column;

    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::Counter* _join_filter_timer;
    RuntimeProfile::Counter* _build_output_block_timer;

    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
};

template <typename LocalStateType>
class JoinProbeOperatorX : public StatefulOperatorX<LocalStateType> {
public:
    using Base = StatefulOperatorX<LocalStateType>;
    JoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(doris::RuntimeState* state) override;
    [[nodiscard]] const RowDescriptor& row_desc() override { return *_output_row_desc; }

    [[nodiscard]] const RowDescriptor& intermediate_row_desc() const override {
        return *_intermediate_row_desc;
    }

    [[nodiscard]] bool is_source() const override { return false; }

    void set_build_side_child(OperatorXPtr& build_side_child) {
        _build_side_child = build_side_child;
    }

    Status set_child(OperatorXPtr child) override {
        if (OperatorX<LocalStateType>::_child_x) {
            // when there already (probe) child, others is build child.
            set_build_side_child(child);
        } else {
            // first child which is probe side is in this pipeline
            OperatorX<LocalStateType>::_child_x = std::move(child);
        }
        return Status::OK();
    }

protected:
    template <typename DependencyType, typename Derived>
    friend class JoinProbeLocalState;

    TJoinOp::type _join_op;
    const bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    const bool _build_unique;    // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_left_semi_anti;
    const bool _is_outer_join;
    const bool _is_mark_join;

    std::unique_ptr<RowDescriptor> _output_row_desc;
    std::unique_ptr<RowDescriptor> _intermediate_row_desc;
    // output expr
    vectorized::VExprContextSPtrs _output_expr_ctxs;
    OperatorXPtr _build_side_child;
    const bool _short_circuit_for_null_in_build_side;
};

} // namespace pipeline
} // namespace doris
