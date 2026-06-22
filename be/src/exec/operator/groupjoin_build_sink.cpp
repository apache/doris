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

#include "exec/operator/groupjoin_build_sink.h"

#include <variant>

#include "common/cast_set.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/util.hpp"
#include "exec/operator/groupjoin_operator_utils.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

GroupJoinBuildSinkLocalState::GroupJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                           RuntimeState* state)
        : Base(parent, state) {
    _finish_dependency = std::make_shared<CountedFinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY");
}

Status GroupJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = _parent->cast<GroupJoinBuildSinkOperatorX>();
    _build_expr_ctxs.resize(p._build_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(p._build_expr_ctxs[i]->clone(state, _build_expr_ctxs[i]));
    }
    _aggregate_evaluators.reserve(p._aggregate_evaluators.size());
    for (auto* evaluator : p._aggregate_evaluators) {
        _aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    RETURN_IF_ERROR(groupjoin::register_agg_state_layout(
            _shared_state, p._aggregate_sides, p._sizes_of_aggregate_states,
            p._aligns_of_aggregate_states, p._aggregate_indices, p._aggregate_evaluators));
    DCHECK(std::holds_alternative<std::monostate>(_shared_state->data_variants->method_variant));
    std::vector<DataTypePtr> data_types;
    data_types.reserve(_build_expr_ctxs.size());
    for (const auto& ctx : _build_expr_ctxs) {
        data_types.emplace_back(remove_nullable(ctx->root()->data_type()));
    }
    RETURN_IF_ERROR(init_hash_method<GroupJoinDataVariants>(_shared_state->data_variants.get(),
                                                            data_types, true));
    if (!p._runtime_filter_descs.empty()) {
        _runtime_filter_producer_helper = std::make_shared<RuntimeFilterProducerHelperGroupJoin>();
        RETURN_IF_ERROR(_runtime_filter_producer_helper->init(
                state, _build_expr_ctxs, p._runtime_filter_descs, p._child->row_desc()));
    }
    _dependency->set_ready();
    return Status::OK();
}

Status GroupJoinBuildSinkLocalState::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    if (_runtime_filter_producer_helper) {
        RETURN_IF_ERROR(_runtime_filter_producer_helper->skip_process(state));
    }
    return Base::terminate(state);
}

Status GroupJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    try {
        if (!_terminated && _runtime_filter_producer_helper && !state->is_cancelled()) {
            if (_runtime_filter_size_sent && exec_status.ok()) {
                RETURN_IF_ERROR(_runtime_filter_producer_helper->build_and_publish(state));
            } else {
                RETURN_IF_ERROR(_runtime_filter_producer_helper->skip_process(state));
            }
        }
    } catch (Exception& e) {
        return Status::InternalError("GroupJoin runtime filter process meet error: {}",
                                     e.to_string());
    }
    if (_runtime_filter_producer_helper) {
        _runtime_filter_producer_helper->collect_realtime_profile(custom_profile());
    }
    return Base::close(state, exec_status);
}

Status GroupJoinBuildSinkLocalState::_append_runtime_filter_columns(Block* block) {
    auto& p = _parent->cast<GroupJoinBuildSinkOperatorX>();
    if (p._runtime_filter_descs.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_runtime_filter_producer_helper->append_block(block));
    return Status::OK();
}

GroupJoinBuildSinkOperatorX::GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         int dest_id, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs)
        : Base(operator_id, tnode, dest_id),
          _join_distribution(tnode.group_join_node.__isset.dist_type
                                     ? tnode.group_join_node.dist_type
                                     : TJoinDistributionType::NONE),
          _pool(pool),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _runtime_filter_descs(tnode.runtime_filters) {}

Status GroupJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    RETURN_IF_ERROR(groupjoin::validate_group_join_node(tnode));
    for (const auto& eq_join_conjunct : tnode.group_join_node.eq_join_conjuncts) {
        VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, build_ctx));
        {
            VExprContextSPtr probe_ctx;
            RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, probe_ctx));
            auto build_side_expr_type = build_ctx->root()->data_type();
            auto probe_side_expr_type = probe_ctx->root()->data_type();
            if (!make_nullable(build_side_expr_type)
                         ->equals(*make_nullable(probe_side_expr_type))) {
                return Status::InternalError(
                        "GroupJoin build side type {}, not match probe side type {}, node={}",
                        build_side_expr_type->get_name(), probe_side_expr_type->get_name(),
                        debug_string(0));
            }
        }
        _build_expr_ctxs.push_back(build_ctx);
    }
    const auto& group_join_node = tnode.group_join_node;
    _aggregate_sides.resize(group_join_node.aggregate_functions.size());
    _sizes_of_aggregate_states.assign(group_join_node.aggregate_functions.size(), 0);
    _aligns_of_aggregate_states.assign(group_join_node.aggregate_functions.size(), 1);
    _output_tuple_id = group_join_node.output_tuple_id;

    TSortInfo dummy;
    for (int i = 0; i < group_join_node.aggregate_functions.size(); ++i) {
        const auto& aggregate_function = group_join_node.aggregate_functions[i];
        _aggregate_sides[i] = aggregate_function.input_side;
        if (aggregate_function.input_side != TGroupJoinAggSide::BUILD) {
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

Status GroupJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK(_output_tuple_desc != nullptr);
    const auto key_size = _build_expr_ctxs.size();
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

Status GroupJoinBuildSinkOperatorX::sink_impl(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), static_cast<int64_t>(in_block->rows()));
    if (in_block->rows() > 0) {
        const auto rows = cast_set<uint32_t>(in_block->rows());
        RETURN_IF_ERROR(groupjoin::do_evaluate(*in_block, local_state._build_expr_ctxs,
                                               local_state._key_columns_holder));
        RETURN_IF_ERROR(groupjoin::extract_key_columns(
                in_block->rows(), local_state._key_columns_holder,
                local_state._build_key_not_nullable_columns, local_state._null_map_column));
        const uint8_t* null_map = local_state._null_map_column
                                          ? local_state._null_map_column->get_data().data()
                                          : nullptr;
        local_state._places.resize(rows);
        RETURN_IF_ERROR(groupjoin::add_build_counts_by_key(
                local_state._shared_state, *local_state._shared_state->arena,
                local_state._build_key_not_nullable_columns, rows, null_map, _aggregate_indices,
                local_state._places.data()));
        for (size_t i = 0; i < local_state._aggregate_evaluators.size(); ++i) {
            const auto offset =
                    local_state._shared_state->offsets_of_aggregate_states[_aggregate_indices[i]];
            // If there is no nullable join key, every build row has a valid place because build
            // always creates/fetches a hash-table entry. If null_map exists, rows with NULL join
            // keys are skipped for normal inner equal join and keep places[row] as nullptr.
            if (null_map == nullptr) {
                RETURN_IF_ERROR(local_state._aggregate_evaluators[i]->execute_batch_add(
                        in_block, offset, local_state._places.data(),
                        *local_state._shared_state->arena));
            } else {
                RETURN_IF_ERROR(local_state._aggregate_evaluators[i]->execute_batch_add_selected(
                        in_block, offset, local_state._places.data(),
                        *local_state._shared_state->arena));
            }
        }
        RETURN_IF_ERROR(local_state._append_runtime_filter_columns(in_block));
    }
    if (eos) {
        if (local_state._runtime_filter_producer_helper) {
            RETURN_IF_ERROR(local_state._runtime_filter_producer_helper->send_filter_size(
                    state, local_state._runtime_filter_producer_helper->build_rows(),
                    local_state._finish_dependency));
            local_state._runtime_filter_size_sent = true;
        }
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

DataDistribution GroupJoinBuildSinkOperatorX::required_data_distribution(
        RuntimeState* state) const {
    // Keep the same distribution rule as hash join's non-broadcast path. GroupJoin currently
    // supports only inner equi join, so broadcast/null-aware special branches are not needed.
    return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                           _join_distribution == TJoinDistributionType::COLOCATE
                   ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                   : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
}

bool GroupJoinBuildSinkOperatorX::is_shuffled_operator() const {
    return _join_distribution == TJoinDistributionType::PARTITIONED ||
           _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _join_distribution == TJoinDistributionType::COLOCATE;
}

bool GroupJoinBuildSinkOperatorX::is_colocated_operator() const {
    return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _join_distribution == TJoinDistributionType::COLOCATE;
}

bool GroupJoinBuildSinkOperatorX::followed_by_shuffled_operator() const {
    return (is_shuffled_operator() && !is_colocated_operator()) || _followed_by_shuffled_operator;
}

} // namespace doris
