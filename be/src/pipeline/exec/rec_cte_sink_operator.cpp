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

#include "pipeline/exec/rec_cte_sink_operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status RecCTESinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<Parent>();
    _child_expr.resize(p._child_expr.size());
    for (size_t i = 0; i < p._child_expr.size(); i++) {
        RETURN_IF_ERROR(p._child_expr[i]->clone(state, _child_expr[i]));
    }
    return Status::OK();
}

Status RecCTESinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    DCHECK(tnode.__isset.rec_cte_node);
    {
        const auto& texprs = tnode.rec_cte_node.result_expr_lists[1];
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texprs, ctxs));
        _child_expr = ctxs;
    }
    return Status::OK();
}

Status RecCTESinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_expr, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::check_expr_output_type(_child_expr, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_child_expr, state));
    return Status::OK();
}

} // namespace doris::pipeline
