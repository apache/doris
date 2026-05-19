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

#include "exec/operator/groupjoin_probe_operator.h"

#include "core/data_type/data_type.h"
#include "exec/operator/operator.h"
#include "runtime/descriptors.h"

namespace doris {

GroupJoinProbeOperatorX::GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs)
        : HashJoinProbeOperatorX(pool, tnode, operator_id, descs) {}

Status GroupJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperatorX::init(tnode, state));
    if (tnode.__isset.group_join_node && !tnode.group_join_node.aggregate_functions.empty()) {
        for (const auto& agg_expr : tnode.group_join_node.aggregate_functions) {
            AggFnEvaluator* evaluator = nullptr;
            RETURN_IF_ERROR(AggFnEvaluator::create(state->obj_pool(), agg_expr, TSortInfo(), false,
                                                   false, &evaluator));
            _aggregate_evaluators.push_back(evaluator);
        }
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::prepare(RuntimeState* state) {
    // Call JoinProbeOperatorX::prepare() directly to skip HashJoinProbeOperatorX::prepare()'s
    // validation loop, which would fail for GroupJoin because _intermediate_row_desc does not
    // include the appended aggregate columns.
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::prepare(state));

    // Prepare other join conjuncts (mirrors HashJoinProbeOperatorX::prepare()).
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }
    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }

    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));

    // Prepare aggregate evaluators to obtain their final result types.
    if (!_aggregate_evaluators.empty()) {
        const SlotDescriptor* dummy_slot = nullptr;
        if (!_build_side_child->row_desc().tuple_descriptors().empty() &&
            !_build_side_child->row_desc().tuple_descriptors()[0]->slots().empty()) {
            dummy_slot = _build_side_child->row_desc().tuple_descriptors()[0]->slots()[0];
        }
        for (auto* evaluator : _aggregate_evaluators) {
            RETURN_IF_ERROR(evaluator->prepare(state, _build_side_child->row_desc(), dummy_slot,
                                               dummy_slot));
            RETURN_IF_ERROR(evaluator->open(state));
            _aggregate_data_types.push_back(evaluator->function()->get_return_type());
            _aggregate_column_names.push_back(evaluator->debug_string());
        }
    }

    DCHECK(_build_side_child != nullptr);
    _right_table_data_types = VectorizedUtils::get_data_types(_build_side_child->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(_child->row_desc());
    _right_table_column_names = VectorizedUtils::get_column_names(_build_side_child->row_desc());

    _left_output_slot_flags.assign(_left_table_data_types.size(), true);
    _right_output_slot_flags.assign(_right_table_data_types.size(), false);

    // Append aggregate types and column names.
    for (size_t i = 0; i < _aggregate_data_types.size(); i++) {
        _right_table_data_types.push_back(_aggregate_data_types[i]);
        _right_table_column_names.push_back(_aggregate_column_names[i]);
        _right_output_slot_flags.push_back(true);
    }

    _right_col_idx = (_is_right_semi_anti && !_have_other_join_conjunct &&
                      (!_is_mark_join || _mark_join_conjuncts.empty()))
                             ? 0
                             : _left_table_data_types.size();

    _build_side_child.reset();

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    return Status::OK();
}

Status GroupJoinProbeOperatorX::setup_local_state(RuntimeState* state, LocalStateInfo& info) {
    auto local_state = GroupJoinProbeLocalState::create_unique(state, this);
    RETURN_IF_ERROR(local_state->init(state, info));
    state->emplace_local_state(operator_id(), std::move(local_state));
    return Status::OK();
}

Status GroupJoinProbeLocalState::open(RuntimeState* state) {
    // Rebuild _join_block to include aggregate columns before ProcessHashTableProbe is
    // constructed in HashJoinProbeLocalState::open().
    auto& p = _parent->cast<GroupJoinProbeOperatorX>();
    _join_block.clear();
    for (size_t i = 0; i < p._left_table_data_types.size(); i++) {
        _join_block.insert(
                {p._left_table_data_types[i]->create_column(), p._left_table_data_types[i], ""});
    }
    for (size_t i = 0; i < p._right_table_data_types.size(); i++) {
        _join_block.insert(
                {p._right_table_data_types[i]->create_column(), p._right_table_data_types[i], ""});
    }

    return HashJoinProbeLocalState::open(state);
}

} // namespace doris
