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

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

JoinProbeOperatorX::JoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
        : OperatorXBase(pool, tnode, descs),
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
                                                               : false) {
    if (tnode.__isset.hash_join_node) {
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())));
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}));
    } else if (tnode.__isset.nested_loop_join_node) {
        _intermediate_row_desc.reset(new RowDescriptor(
                descs, tnode.nested_loop_join_node.vintermediate_tuple_id_list,
                std::vector<bool>(tnode.nested_loop_join_node.vintermediate_tuple_id_list.size())));
        _output_row_desc.reset(
                new RowDescriptor(descs, {tnode.nested_loop_join_node.voutput_tuple_id}, {false}));
    } else {
        // Iff BE has been upgraded and FE has not yet, we should keep origin logics for CROSS JOIN.
        DCHECK_EQ(_join_op, TJoinOp::CROSS_JOIN);
    }
}

Status JoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
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

Status JoinProbeOperatorX::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::open(state));
    return vectorized::VExpr::open(_output_expr_ctxs, state);
}

} // namespace doris::pipeline
