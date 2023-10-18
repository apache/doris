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

#include "nested_loop_join_build_operator.h"

#include <string>

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(NestLoopJoinBuildOperator, StreamingOperator)

NestedLoopJoinBuildSinkLocalState::NestedLoopJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                                     RuntimeState* state)
        : JoinBuildSinkLocalState<NestedLoopJoinDependency, NestedLoopJoinBuildSinkLocalState>(
                  parent, state) {}

Status NestedLoopJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<NestedLoopJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;
    _filter_src_expr_ctxs.resize(p._filter_src_expr_ctxs.size());
    for (size_t i = 0; i < _filter_src_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._filter_src_expr_ctxs[i]->clone(state, _filter_src_expr_ctxs[i]));
    }
    for (size_t i = 0; i < p._runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_producer_filter(
                p._runtime_filter_descs[i], state->query_options()));
    }
    return Status::OK();
}

const std::vector<TRuntimeFilterDesc>& NestedLoopJoinBuildSinkLocalState::runtime_filter_descs() {
    return _parent->cast<NestedLoopJoinBuildSinkOperatorX>()._runtime_filter_descs;
}

NestedLoopJoinBuildSinkOperatorX::NestedLoopJoinBuildSinkOperatorX(ObjectPool* pool,
                                                                   const TPlanNode& tnode,
                                                                   const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>(pool, tnode, descs),
          _runtime_filter_descs(tnode.runtime_filters),
          _is_output_left_side_only(tnode.nested_loop_join_node.__isset.is_output_left_side_only &&
                                    tnode.nested_loop_join_node.is_output_left_side_only),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples) {}

Status NestedLoopJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>::init(tnode, state));

    std::vector<TExpr> filter_src_exprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        filter_src_exprs.push_back(_runtime_filter_descs[i].src_expr);
    }
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(filter_src_exprs, _filter_src_expr_ctxs));
    return Status::OK();
}

Status NestedLoopJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    // pre-compute the tuple index of build tuples in the output row
    int num_build_tuples = _child_x->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = _child_x->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_filter_src_expr_ctxs, state, _child_x->row_desc()));
    return Status::OK();
}

Status NestedLoopJoinBuildSinkOperatorX::open(RuntimeState* state) {
    return vectorized::VExpr::open(_filter_src_expr_ctxs, state);
}

Status NestedLoopJoinBuildSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* block,
                                              SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_STATUS_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());
    auto rows = block->rows();
    auto mem_usage = block->allocated_bytes();

    if (rows != 0) {
        local_state._build_rows += rows;
        local_state._total_mem_usage += mem_usage;
        local_state._shared_state->build_blocks.emplace_back(std::move(*block));
        if (_match_all_build || _is_right_semi_anti) {
            local_state._shared_state->build_side_visited_flags.emplace_back(
                    vectorized::ColumnUInt8::create(rows, 0));
        }
    }

    if (source_state == SourceState::FINISHED) {
        COUNTER_UPDATE(local_state._build_rows_counter, local_state._build_rows);
        vectorized::RuntimeFilterBuild<NestedLoopJoinBuildSinkLocalState> rf_ctx(&local_state);
        RETURN_IF_ERROR(rf_ctx(state));

        // optimize `in bitmap`, see https://github.com/apache/doris/issues/14338
        if (_is_output_left_side_only && ((_join_op == TJoinOp::type::LEFT_SEMI_JOIN &&
                                           local_state._shared_state->build_blocks.empty()) ||
                                          (_join_op == TJoinOp::type::LEFT_ANTI_JOIN &&
                                           !local_state._shared_state->build_blocks.empty()))) {
            local_state._shared_state->left_side_eos = true;
        }
        local_state._dependency->set_ready_for_read();
    }

    return Status::OK();
}

} // namespace doris::pipeline
