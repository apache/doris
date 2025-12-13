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
    _shared_state->targets = _parent->cast<RecCTESourceOperatorX>()._targets;
    _shared_state->max_recursion_depth =
            _parent->cast<RecCTESourceOperatorX>()._max_recursion_depth;
    _shared_state->source_dep = _dependency;
    _anchor_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                   _parent->get_name() + "_ANCHOR_DEPENDENCY");
    _shared_state->anchor_dep = _anchor_dependency.get();

    auto& p = _parent->cast<Parent>();
    if (!_parent->cast<RecCTESourceOperatorX>()._is_union_all) {
        _shared_state->agg_data = std::make_unique<DistinctDataVariants>();
        RETURN_IF_ERROR(init_hash_method<DistinctDataVariants>(_shared_state->agg_data.get(),
                                                               p._hash_table_key_types, false));
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
        _hash_table_key_types = get_data_types(ctxs);
    }
    return Status::OK();
}

Status RecCTESourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (auto fragment : _fragments_to_reset) {
        if (state->fragment_id() == fragment.fragment_id) {
            return Status::InternalError("Fragment {} contains a recursive CTE node",
                                         fragment.fragment_id);
        }
    }
    return Status::OK();
}

} // namespace doris::pipeline
