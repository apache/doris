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

#include "exprs/runtime_filter_slots_cross.h"
#include "pipeline/exec/operator.h"

namespace doris::pipeline {

struct RuntimeFilterBuild {
    RuntimeFilterBuild(NestedLoopJoinBuildSinkLocalState* parent) : _parent(parent) {}
    Status operator()(RuntimeState* state) {
        if (_parent->runtime_filters().empty()) {
            return Status::OK();
        }
        VRuntimeFilterSlotsCross runtime_filter_slots(_parent->runtime_filters(),
                                                      _parent->filter_src_expr_ctxs());

        RETURN_IF_ERROR(runtime_filter_slots.init(state));

        if (!runtime_filter_slots.empty() && !_parent->build_blocks().empty()) {
            SCOPED_TIMER(_parent->runtime_filter_compute_timer());
            for (auto& build_block : _parent->build_blocks()) {
                RETURN_IF_ERROR(runtime_filter_slots.insert(&build_block));
            }
        }
        {
            SCOPED_TIMER(_parent->publish_runtime_filter_timer());
            RETURN_IF_ERROR(runtime_filter_slots.publish());
        }

        return Status::OK();
    }

private:
    NestedLoopJoinBuildSinkLocalState* _parent = nullptr;
};

NestedLoopJoinBuildSinkLocalState::NestedLoopJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                                     RuntimeState* state)
        : JoinBuildSinkLocalState<NestedLoopJoinSharedState, NestedLoopJoinBuildSinkLocalState>(
                  parent, state) {}

Status NestedLoopJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<NestedLoopJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;
    _runtime_filters.resize(p._runtime_filter_descs.size());
    for (size_t i = 0; i < p._runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->register_producer_runtime_filter(
                p._runtime_filter_descs[i], p._need_local_merge, &_runtime_filters[i], false));
    }
    return Status::OK();
}

Status NestedLoopJoinBuildSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinBuildSinkLocalState::open(state));
    auto& p = _parent->cast<NestedLoopJoinBuildSinkOperatorX>();
    _filter_src_expr_ctxs.resize(p._filter_src_expr_ctxs.size());
    for (size_t i = 0; i < _filter_src_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._filter_src_expr_ctxs[i]->clone(state, _filter_src_expr_ctxs[i]));
    }
    return Status::OK();
}

NestedLoopJoinBuildSinkOperatorX::NestedLoopJoinBuildSinkOperatorX(ObjectPool* pool,
                                                                   int operator_id,
                                                                   const TPlanNode& tnode,
                                                                   const DescriptorTbl& descs,
                                                                   bool need_local_merge)
        : JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>(pool, operator_id, tnode,
                                                                    descs),
          _need_local_merge(need_local_merge),
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

Status NestedLoopJoinBuildSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>::open(state));
    int num_build_tuples = _child->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = _child->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_filter_src_expr_ctxs, state, _child->row_desc()));
    return vectorized::VExpr::open(_filter_src_expr_ctxs, state);
}

Status NestedLoopJoinBuildSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* block,
                                              bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
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

    if (eos) {
        COUNTER_UPDATE(local_state._build_rows_counter, local_state._build_rows);
        RuntimeFilterBuild rf_ctx(&local_state);
        RETURN_IF_ERROR(rf_ctx(state));

        // optimize `in bitmap`, see https://github.com/apache/doris/issues/14338
        if (_is_output_left_side_only && ((_join_op == TJoinOp::type::LEFT_SEMI_JOIN &&
                                           local_state._shared_state->build_blocks.empty()) ||
                                          (_join_op == TJoinOp::type::LEFT_ANTI_JOIN &&
                                           !local_state._shared_state->build_blocks.empty()))) {
            local_state._shared_state->left_side_eos = true;
        }
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

} // namespace doris::pipeline
