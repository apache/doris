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

#include <algorithm>

#include "common/cast_set.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/common/util.hpp"
#include "exec/operator/groupjoin_operator_utils.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

Status GroupJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = _parent->cast<GroupJoinProbeOperatorX>();
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    _aggregate_evaluators.reserve(p._aggregate_evaluators.size());
    for (auto* evaluator : p._aggregate_evaluators) {
        _aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    RETURN_IF_ERROR(groupjoin::register_agg_state_layout(
            _shared_state, p._aggregate_sides, p._sizes_of_aggregate_states,
            p._aligns_of_aggregate_states, p._aggregate_indices, p._aggregate_evaluators));
    return Status::OK();
}

GroupJoinProbeOperatorX::GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _join_distribution(tnode.group_join_node.__isset.dist_type
                                     ? tnode.group_join_node.dist_type
                                     : TJoinDistributionType::NONE),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _output_tuple_id(tnode.group_join_node.output_tuple_id) {
    _op_name = "GROUP_JOIN_PROBE_OPERATOR";
    _output_row_desc =
            std::make_unique<RowDescriptor>(descs, std::vector<TupleId> {_output_tuple_id});
}

Status GroupJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    RETURN_IF_ERROR(groupjoin::validate_group_join_node(tnode));
    for (const auto& eq_join_conjunct : tnode.group_join_node.eq_join_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);
    }
    const auto& group_join_node = tnode.group_join_node;
    _aggregate_sides.resize(group_join_node.aggregate_functions.size());
    _sizes_of_aggregate_states.assign(group_join_node.aggregate_functions.size(), 0);
    _aligns_of_aggregate_states.assign(group_join_node.aggregate_functions.size(), 1);

    TSortInfo dummy;
    for (int i = 0; i < group_join_node.aggregate_functions.size(); ++i) {
        const auto& aggregate_function = group_join_node.aggregate_functions[i];
        _aggregate_sides[i] = aggregate_function.input_side;
        if (aggregate_function.input_side != TGroupJoinAggSide::PROBE) {
            continue;
        }
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, aggregate_function.aggregate_function, dummy,
                                               false, false, &evaluator));
        _aggregate_evaluators.push_back(evaluator);
        _aggregate_indices.push_back(i);
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK(_output_tuple_desc != nullptr);
    const auto key_size = _probe_expr_ctxs.size();
    for (size_t i = 0; i < key_size; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        const auto agg_idx = _aggregate_indices[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[key_size + agg_idx];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, _child->row_desc(),
                                                          output_slot_desc, output_slot_desc));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
        _sizes_of_aggregate_states[agg_idx] = _aggregate_evaluators[i]->function()->size_of_data();
        _aligns_of_aggregate_states[agg_idx] =
                _aggregate_evaluators[i]->function()->align_of_data();
    }
    for (auto* evaluator : _aggregate_evaluators) {
        RETURN_IF_ERROR(evaluator->open(state));
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::push(RuntimeState* state, Block* input_block, bool eos) const {
    auto& local_state = get_local_state(state);
    if (input_block->rows() > 0) {
        const auto rows = cast_set<uint32_t>(input_block->rows());
        RETURN_IF_ERROR(groupjoin::do_evaluate(*input_block, local_state._probe_expr_ctxs,
                                               local_state._key_columns_holder));
        RETURN_IF_ERROR(groupjoin::extract_key_columns(
                input_block->rows(), local_state._key_columns_holder,
                local_state._probe_key_not_nullable_columns, local_state._null_map_column));
        const uint8_t* null_map = local_state._null_map_column
                                          ? local_state._null_map_column->get_data().data()
                                          : nullptr;
        local_state._places.resize(rows);
        int64_t matched_rows = 0;
        uint32_t matched_probe_rows = 0;
        RETURN_IF_ERROR(groupjoin::update_probe_counts(
                local_state._shared_state, *local_state._shared_state->arena,
                local_state._probe_key_not_nullable_columns, rows, null_map, _aggregate_indices,
                local_state._places.data(), matched_rows, matched_probe_rows));
        local_state._shared_state->total_match_count += matched_rows;
        for (size_t i = 0; i < local_state._aggregate_evaluators.size(); ++i) {
            const auto offset =
                    local_state._shared_state->offsets_of_aggregate_states[_aggregate_indices[i]];
            // Probe rows can have nullptr places not only because of NULL join keys, but also
            // because an inner join probe key may not exist in the build hash table.
            if (matched_probe_rows == rows) {
                RETURN_IF_ERROR(local_state._aggregate_evaluators[i]->execute_batch_add(
                        input_block, offset, local_state._places.data(),
                        *local_state._shared_state->arena));
            } else {
                RETURN_IF_ERROR(local_state._aggregate_evaluators[i]->execute_batch_add_selected(
                        input_block, offset, local_state._places.data(),
                        *local_state._shared_state->arena));
            }
        }
    }
    if (eos) {
        local_state._shared_state->probe_eos = true;
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::pull(RuntimeState* state, Block* output_block, bool* eos) const {
    auto& local_state = get_local_state(state);
    auto* shared_state = local_state._shared_state;
    if (shared_state->result_emitted) {
        output_block->clear_column_data();
        *eos = true;
        return Status::OK();
    }

    auto columns_with_schema = VectorizedUtils::create_columns_with_type_and_name(row_desc());
    const size_t key_size = local_state._probe_expr_ctxs.size();
    const size_t agg_size = shared_state->aggregate_evaluators.size();
    DCHECK_EQ(columns_with_schema.size(), key_size + agg_size);

    MutableColumns key_columns;
    key_columns.reserve(key_size);
    for (size_t i = 0; i < key_size; ++i) {
        const auto output_key_need_nullable =
                std::ranges::find(_make_nullable_keys, i) != _make_nullable_keys.end();
        key_columns.emplace_back((output_key_need_nullable
                                          ? remove_nullable(columns_with_schema[i].type)
                                          : columns_with_schema[i].type)
                                         ->create_column());
    }

    MutableColumns value_columns;
    value_columns.reserve(agg_size);
    for (size_t i = 0; i < agg_size; ++i) {
        DCHECK(shared_state->aggregate_evaluators[i] != nullptr);
        value_columns.emplace_back(columns_with_schema[key_size + i].type->create_column());
    }

    bool output_eos = false;
    RETURN_IF_ERROR(groupjoin::drain_groupjoin_result(shared_state, state->batch_size(),
                                                      local_state, key_columns, value_columns,
                                                      output_eos));

    for (size_t i = 0; i < key_size; ++i) {
        columns_with_schema[i].column = std::move(key_columns[i]);
    }
    for (size_t i = 0; i < agg_size; ++i) {
        columns_with_schema[key_size + i].column = std::move(value_columns[i]);
    }
    *output_block = Block(std::move(columns_with_schema));
    if (output_block->rows() != 0) {
        for (auto cid : _make_nullable_keys) {
            output_block->get_by_position(cid).column =
                    make_nullable(output_block->get_by_position(cid).column);
            output_block->get_by_position(cid).type =
                    make_nullable(output_block->get_by_position(cid).type);
        }
    }
    shared_state->result_emitted = output_eos;
    *eos = output_eos;
    return Status::OK();
}

bool GroupJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return !local_state._shared_state->probe_eos;
}

DataDistribution GroupJoinProbeOperatorX::required_data_distribution(RuntimeState* state) const {
    // Keep the same distribution rule as hash join's non-broadcast path. GroupJoin currently
    // supports only inner equi join, so broadcast/null-aware special branches are not needed.
    return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                           _join_distribution == TJoinDistributionType::COLOCATE
                   ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                   : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
}

bool GroupJoinProbeOperatorX::is_shuffled_operator() const {
    return _join_distribution == TJoinDistributionType::PARTITIONED ||
           _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _join_distribution == TJoinDistributionType::COLOCATE;
}

bool GroupJoinProbeOperatorX::is_colocated_operator() const {
    return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _join_distribution == TJoinDistributionType::COLOCATE;
}

bool GroupJoinProbeOperatorX::followed_by_shuffled_operator() const {
    return (is_shuffled_operator() && !is_colocated_operator()) || _followed_by_shuffled_operator;
}

} // namespace doris
