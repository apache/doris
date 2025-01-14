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

#include "join_probe_operator.h"

#include <memory>

#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
template <typename SharedStateArg, typename Derived>
Status JoinProbeLocalState<SharedStateArg, Derived>::init(RuntimeState* state,
                                                          LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));

    _join_filter_timer = ADD_TIMER(Base::profile(), "JoinFilterTimer");
    _build_output_block_timer = ADD_TIMER(Base::profile(), "BuildOutputBlock");
    _probe_rows_counter = ADD_COUNTER_WITH_LEVEL(Base::profile(), "ProbeRows", TUnit::UNIT, 1);
    _finish_probe_phase_timer = ADD_TIMER(Base::profile(), "FinishProbePhaseTime");
    return Status::OK();
}

template <typename SharedStateArg, typename Derived>
Status JoinProbeLocalState<SharedStateArg, Derived>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<typename Derived::Parent>();
    _output_expr_ctxs.resize(p._output_expr_ctxs.size());
    for (size_t i = 0; i < _output_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_expr_ctxs[i]->clone(state, _output_expr_ctxs[i]));
    }

    return Status::OK();
}

template <typename SharedStateArg, typename Derived>
Status JoinProbeLocalState<SharedStateArg, Derived>::close(RuntimeState* state) {
    if (Base::_closed) {
        return Status::OK();
    }
    _join_block.clear();
    return Base::close(state);
}

template <typename SharedStateArg, typename Derived>
void JoinProbeLocalState<SharedStateArg, Derived>::_construct_mutable_join_block() {
    auto& p = Base::_parent->template cast<typename Derived::Parent>();
    const auto& mutable_block_desc = p._intermediate_row_desc;
    for (const auto tuple_desc : mutable_block_desc->tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }

    if (p._is_mark_join) {
        _mark_column_id = _join_block.columns() - 1;
#ifndef NDEBUG
        const auto& mark_column = assert_cast<const vectorized::ColumnNullable&>(
                *_join_block.get_by_position(_mark_column_id).column);
        auto& nested_column = mark_column.get_nested_column();
        DCHECK(check_and_get_column<vectorized::ColumnUInt8>(nested_column) != nullptr);
#endif
    }
}

template <typename SharedStateArg, typename Derived>
Status JoinProbeLocalState<SharedStateArg, Derived>::_build_output_block(
        vectorized::Block* origin_block, vectorized::Block* output_block) {
    if (!output_block->mem_reuse()) {
        output_block->swap(origin_block->clone_empty());
    }
    output_block->swap(*origin_block);
    return Status::OK();
}

template <typename LocalStateType>
JoinProbeOperatorX<LocalStateType>::JoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                       int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _join_op(tnode.__isset.hash_join_node ? tnode.hash_join_node.join_op
                                                : (tnode.__isset.nested_loop_join_node
                                                           ? tnode.nested_loop_join_node.join_op
                                                           : TJoinOp::CROSS_JOIN)),
          _have_other_join_conjunct(tnode.__isset.hash_join_node &&
                                    ((tnode.hash_join_node.__isset.other_join_conjuncts &&
                                      !tnode.hash_join_node.other_join_conjuncts.empty()) ||
                                     tnode.hash_join_node.__isset.vother_join_conjunct)),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(!_have_other_join_conjunct &&
                        (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_ANTI_JOIN ||
                         _join_op == TJoinOp::LEFT_SEMI_JOIN)),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_left_semi_anti(_join_op == TJoinOp::LEFT_ANTI_JOIN ||
                             _join_op == TJoinOp::LEFT_SEMI_JOIN ||
                             _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe),
          _is_mark_join(tnode.__isset.nested_loop_join_node
                                ? (tnode.nested_loop_join_node.__isset.is_mark
                                           ? tnode.nested_loop_join_node.is_mark
                                           : false)
                        : tnode.hash_join_node.__isset.is_mark ? tnode.hash_join_node.is_mark
                                                               : false),
          _short_circuit_for_null_in_build_side(_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                                                !_is_mark_join),
          _use_specific_projections(
                  tnode.__isset.hash_join_node
                          ? (tnode.hash_join_node.__isset.use_specific_projections
                                     ? tnode.hash_join_node.use_specific_projections
                                     : true)
                          : (tnode.nested_loop_join_node.__isset.use_specific_projections
                                     ? tnode.nested_loop_join_node.use_specific_projections
                                     : true)

          ) {
    Base::_is_serial_operator = tnode.__isset.is_serial_operator && tnode.is_serial_operator;
    if (tnode.__isset.hash_join_node) {
        _intermediate_row_desc = std::make_unique<RowDescriptor>(
                descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size()));
        if (!Base::_output_row_descriptor) {
            _output_row_desc = std::make_unique<RowDescriptor>(
                    descs, std::vector<TTupleId> {tnode.hash_join_node.voutput_tuple_id},
                    std::vector<bool> {false});
        }
    } else if (tnode.__isset.nested_loop_join_node) {
        _intermediate_row_desc = std::make_unique<RowDescriptor>(
                descs, tnode.nested_loop_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.nested_loop_join_node.vintermediate_tuple_id_list.size()));
        if (!Base::_output_row_descriptor) {
            _output_row_desc = std::make_unique<RowDescriptor>(
                    descs, std::vector<TTupleId> {tnode.nested_loop_join_node.voutput_tuple_id},
                    std::vector<bool> {false});
        }
    } else {
        // Iff BE has been upgraded and FE has not yet, we should keep origin logics for CROSS JOIN.
        DCHECK_EQ(_join_op, TJoinOp::CROSS_JOIN);
    }
}

template <typename LocalStateType>
Status JoinProbeOperatorX<LocalStateType>::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    if (tnode.__isset.hash_join_node || tnode.__isset.nested_loop_join_node) {
        const auto& output_exprs = tnode.__isset.hash_join_node
                                           ? tnode.hash_join_node.srcExprList
                                           : tnode.nested_loop_join_node.srcExprList;
        for (const auto& expr : output_exprs) {
            vectorized::VExprContextSPtr ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(expr, ctx));
            _output_expr_ctxs.push_back(ctx);
        }
    }

    return Status::OK();
}

template <typename LocalStateType>
Status JoinProbeOperatorX<LocalStateType>::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    return vectorized::VExpr::open(_output_expr_ctxs, state);
}

template class JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>;
template class JoinProbeOperatorX<HashJoinProbeLocalState>;

template class JoinProbeLocalState<NestedLoopJoinSharedState, NestedLoopJoinProbeLocalState>;
template class JoinProbeOperatorX<NestedLoopJoinProbeLocalState>;

template class JoinProbeLocalState<PartitionedHashJoinSharedState,
                                   PartitionedHashJoinProbeLocalState>;
template class JoinProbeOperatorX<PartitionedHashJoinProbeLocalState>;

} // namespace doris::pipeline
