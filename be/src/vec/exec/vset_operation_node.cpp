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

#include "vec/exec/vset_operation_node.h"

#include "vec/exprs/vexpr.h"

namespace doris {

namespace vectorized {

VSetOperationNode::VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _const_expr_list_idx(0),
          _child_idx(0),
          _child_row_idx(0),
          _child_eos(false),
          _to_close_child_idx(-1) {}

Status VSetOperationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    for (auto& exprs : _const_expr_lists) {
        VExpr::close(exprs, state);
    }
    for (auto& exprs : _child_expr_lists) {
        VExpr::close(exprs, state);
    }
    return ExecNode::close(state);
}
Status VSetOperationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    // Create const_expr_ctx_lists_ from thrift exprs.
    auto& const_texpr_lists = tnode.union_node.const_expr_lists;
    for (auto& texprs : const_texpr_lists) {
        std::vector<VExprContext*> ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, texprs, &ctxs));
        _const_expr_lists.push_back(ctxs);
    }
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.union_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<VExprContext*> ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}

Status VSetOperationNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    // open const expr lists.
    for (const std::vector<VExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    // open result expr lists.
    for (const std::vector<VExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }

    // Ensures that rows are available for clients to fetch after this open() has
    // succeeded.
    if (!_children.empty()) RETURN_IF_ERROR(child(_child_idx)->open(state));

    return Status::OK();
}
Status VSetOperationNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _materialize_exprs_evaluate_timer =
            ADD_TIMER(_runtime_profile, "MaterializeExprsEvaluateTimer");
    // Prepare const expr lists.
    for (const std::vector<VExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::prepare(exprs, state, row_desc(), expr_mem_tracker()));
    }

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc(),
                                       expr_mem_tracker()));
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
