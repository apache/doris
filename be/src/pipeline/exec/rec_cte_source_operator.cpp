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

#include "pipeline/exec/rec_cte_source_operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

RecCTESourceLocalState::RecCTESourceLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status RecCTESourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

Status RecCTESourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _shared_state->max_recursion_depth =
            _parent->cast<RecCTESourceOperatorX>()._max_recursion_depth;
    _shared_state->source_dep = _dependency;

    auto& p = _parent->cast<Parent>();
    _child_expr.resize(p._child_expr.size());
    for (size_t i = 0; i < p._child_expr.size(); i++) {
        RETURN_IF_ERROR(p._child_expr[i]->clone(state, _child_expr[i]));
    }
    if (!_parent->cast<RecCTESourceOperatorX>()._is_union_all) {
        _shared_state->agg_data = std::make_unique<DistinctDataVariants>();
        RETURN_IF_ERROR(init_hash_method<DistinctDataVariants>(_shared_state->agg_data.get(),
                                                               get_data_types(_child_expr), false));
    }

    _shared_state->hash_table_compute_timer =
            ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _shared_state->hash_table_emplace_timer =
            ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _shared_state->hash_table_input_counter =
            ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
    return Status::OK();
}

Status RecCTESourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    DCHECK(tnode.__isset.rec_cte_node);

    _max_recursion_depth = state->cte_max_recursion_depth();

    {
        const auto& texprs = tnode.rec_cte_node.result_expr_lists[1];
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texprs, ctxs));
        _child_expr = ctxs;
    }
    return Status::OK();
}

Status RecCTESourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_expr, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::check_expr_output_type(_child_expr, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_child_expr, state));
    return Status::OK();
}

} // namespace doris::pipeline
