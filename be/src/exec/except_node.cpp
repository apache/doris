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

#include "exec/except_node.h"

#include "exprs/expr.h"

namespace doris {
// TODO(yangzhengguo) implememt this class
ExceptNode::ExceptNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      _tuple_id(tnode.except_node.tuple_id),
      _tuple_desc(nullptr),
      _first_materialized_child_idx(tnode.except_node.first_materialized_child_idx),
      _child_idx(0),
      _child_batch(nullptr),
      _child_row_idx(0),
      _child_eos(false),
      _const_expr_list_idx(0),
      _to_close_child_idx(-1) {
}

Status ExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    // RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.except_node);
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    // Create const_expr_ctx_lists_ from thrift exprs.
    auto& const_texpr_lists = tnode.except_node.const_expr_lists;
    for (auto& texprs : const_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _const_expr_lists.push_back(ctxs);
    }
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.except_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}
}