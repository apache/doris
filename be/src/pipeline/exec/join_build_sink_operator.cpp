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

#include "join_build_sink_operator.h"

#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/pipeline_x/operator.h"

namespace doris::pipeline {

template <typename DependencyType, typename Derived>
Status JoinBuildSinkLocalState<DependencyType, Derived>::init(RuntimeState* state,
                                                              LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<DependencyType>::init(state, info));
    auto& p = PipelineXSinkLocalState<DependencyType>::_parent
                      ->template cast<typename Derived::Parent>();

    PipelineXSinkLocalState<DependencyType>::profile()->add_info_string("JoinType",
                                                                        to_string(p._join_op));
    _build_rows_counter = ADD_COUNTER(PipelineXSinkLocalState<DependencyType>::profile(),
                                      "BuildRows", TUnit::UNIT);

    _push_down_timer = ADD_TIMER(PipelineXSinkLocalState<DependencyType>::profile(),
                                 "PublishRuntimeFilterTime");
    _push_compute_timer =
            ADD_TIMER(PipelineXSinkLocalState<DependencyType>::profile(), "PushDownComputeTime");

    return Status::OK();
}

template <typename LocalStateType>
JoinBuildSinkOperatorX<LocalStateType>::JoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                               const TPlanNode& tnode,
                                                               const DescriptorTbl& descs)
        : DataSinkOperatorX<LocalStateType>(operator_id, tnode.node_id),
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
          _short_circuit_for_null_in_build_side(_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    _init_join_op();
    if (_is_mark_join) {
        DCHECK(_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN ||
               _join_op == TJoinOp::CROSS_JOIN || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN)
                << "Mark join is only supported for null aware left semi/anti join and cross join "
                   "but this is "
                << _join_op;
    }
}

#define APPLY_FOR_JOINOP_VARIANTS(M) \
    M(INNER_JOIN)                    \
    M(LEFT_SEMI_JOIN)                \
    M(LEFT_ANTI_JOIN)                \
    M(LEFT_OUTER_JOIN)               \
    M(FULL_OUTER_JOIN)               \
    M(RIGHT_OUTER_JOIN)              \
    M(CROSS_JOIN)                    \
    M(RIGHT_SEMI_JOIN)               \
    M(RIGHT_ANTI_JOIN)               \
    M(NULL_AWARE_LEFT_ANTI_JOIN)

template <typename LocalStateType>
void JoinBuildSinkOperatorX<LocalStateType>::_init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                            \
    case TJoinOp::NAME:                                                                    \
        _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>(); \
        break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
    default:
        //do nothing
        break;
    }
}

template class JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>;
template class JoinBuildSinkLocalState<HashJoinDependency, HashJoinBuildSinkLocalState>;
template class JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>;
template class JoinBuildSinkLocalState<NestedLoopJoinDependency, NestedLoopJoinBuildSinkLocalState>;

} // namespace doris::pipeline
