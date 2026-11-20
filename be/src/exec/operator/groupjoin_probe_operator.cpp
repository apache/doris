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
#include <set>

#include "core/column/column_nullable.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/hash_table/join_hash_table_utils.h"
#include "exec/common/join_utils.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exec/operator/groupjoin_operator_utils.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {
GroupJoinProbeLocalState::GroupJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}
GroupJoinProbeLocalState::~GroupJoinProbeLocalState() = default;
Status GroupJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& parent = _parent->cast<GroupJoinProbeOperatorX>();
    for (const auto& expr : parent._key_exprs) {
        VExprContextSPtr clone;
        RETURN_IF_ERROR(expr->clone(state, clone));
        _key_exprs.push_back(std::move(clone));
    }
    for (auto* evaluator : parent._evaluators) {
        _evaluators.push_back(evaluator->clone(state, parent._pool));
    }
    _match_timer = ADD_TIMER(custom_profile(), "ProbeMatchTime");
    _update_timer = ADD_TIMER(custom_profile(), "AggregateUpdateTime");
    _drain_timer = ADD_TIMER(custom_profile(), "ResultDrainTime");
    _matched_rows = ADD_COUNTER(custom_profile(), "MatchedRows", TUnit::UNIT);
    _group_count = ADD_COUNTER(custom_profile(), "MatchedGroups", TUnit::UNIT);
    return Status::OK();
}

Status GroupJoinProbeLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    auto& parent = _parent->cast<GroupJoinProbeOperatorX>();
    for (size_t group_id = 0; group_id < _group_build_rows.size(); ++group_id) {
        auto* const place = _group_states[group_id];
        for (size_t i = 0; i < _evaluators.size(); ++i) {
            _evaluators[i]->destroy(place + parent._offsets[i]);
        }
    }
    decltype(_group_build_rows) {}.swap(_group_build_rows);
    decltype(_state_by_build_row) {}.swap(_state_by_build_row);
    decltype(_group_states) {}.swap(_group_states);
    decltype(_result_places) {}.swap(_result_places);
    _release_probe_resources();
    _arena.clear(true);
    COUNTER_SET(_memory_used_counter, 0);
    return Base::close(state);
}

void GroupJoinProbeLocalState::_release_probe_resources() {
    _argument_columns.clear();
    _probe_arguments.clear();
    _build_arguments.clear();
    _probe_context.reset();
    decltype(_match_places) {}.swap(_match_places);
    decltype(_match_build_rows) {}.swap(_match_build_rows);
    decltype(_match_probe_rows) {}.swap(_match_probe_rows);
    _child_block->clear();
}

void GroupJoinProbeLocalState::_update_memory_usage() {
    // Structural buffers only: shared build columns and aggregate-owned heap allocations
    // are not included here. Query memory enforcement uses the task's MemTracker.
    size_t probe_key_bytes = 0;
    if (_probe_context) {
        probe_key_bytes =
                std::visit(Overload {[](std::monostate&) -> size_t { return 0; },
                                     [](auto& method) {
                                         return method.serialized_keys_size(false) +
                                                method.bucket_nums.capacity() * sizeof(uint32_t);
                                     }},
                           _probe_context->method_variant);
    }
    COUNTER_SET(_memory_used_counter,
                _arena.size() + _state_by_build_row.capacity() * sizeof(AggregateDataPtr) +
                        _group_states.capacity() * sizeof(AggregateDataPtr) +
                        _result_places.capacity() * sizeof(AggregateDataPtr) + probe_key_bytes +
                        _group_build_rows.capacity() * sizeof(uint32_t) +
                        _match_places.capacity() * sizeof(AggregateDataPtr) +
                        (_match_build_rows.capacity() + _match_probe_rows.capacity()) *
                                sizeof(uint32_t));
}

GroupJoinProbeOperatorX::GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& node,
                                                 int operator_id, const DescriptorTbl& descs)
        : Base(pool, node, operator_id, descs),
          _output_tuple_id(node.group_join_node.output_tuple_id),
          _distribution(node.group_join_node.dist_type),
          _partition_exprs(node.__isset.distribute_expr_lists ? node.distribute_expr_lists[0]
                                                              : std::vector<TExpr> {}) {
    _op_name = "GROUP_JOIN_OPERATOR";
    _row_descriptor = RowDescriptor(descs, std::vector<TupleId> {_output_tuple_id});
}

Status GroupJoinProbeOperatorX::init(const TPlanNode& node, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(node, state));
    RETURN_IF_ERROR(groupjoin::validate_group_join_node(node));
    for (const auto& conjunct : node.group_join_node.eq_join_conjuncts) {
        VExprContextSPtr expr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(conjunct.left, expr));
        _key_exprs.push_back(std::move(expr));
    }
    TSortInfo sort_info;
    for (const auto& aggregate : node.group_join_node.aggregate_functions) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, aggregate.aggregate_function, sort_info,
                                               false, false, &evaluator));
        if (evaluator->is_merge()) {
            return Status::InternalError("GroupJoin requires input-to-result aggregates");
        }
        _has_joined_arguments |= aggregate.input_side == TGroupJoinAggSide::JOINED;
        _evaluators.push_back(evaluator);
        _sides.push_back(aggregate.input_side);
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (const auto& expr : _key_exprs) {
        RETURN_IF_ERROR(expr->prepare(state, _child->operator_row_desc_after_projection()));
        RETURN_IF_ERROR(expr->open(state));
    }
    _joined_row_desc = RowDescriptor(_child->operator_row_desc_after_projection(),
                                     _build_side_child->operator_row_desc_after_projection());
    _joined_columns.assign(_joined_row_desc.num_slots(), 0);
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        RETURN_IF_ERROR(_prepare_evaluator(state, i));
    }
    _states_size = (_states_size + _states_alignment - 1) / _states_alignment * _states_alignment;
    return Status::OK();
}

Status GroupJoinProbeOperatorX::_prepare_evaluator(RuntimeState* state, size_t index) {
    const auto& input =
            _sides[index] == TGroupJoinAggSide::JOINED
                    ? _joined_row_desc
                    : (_sides[index] == TGroupJoinAggSide::BUILD ? _build_side_child : _child)
                              ->operator_row_desc_after_projection();
    auto* output = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    auto* slot = output->slots()[_key_exprs.size() + index];
    auto* evaluator = _evaluators[index];
    RETURN_IF_ERROR(evaluator->prepare(state, input, slot, slot));
    evaluator->set_version(state->be_exec_version());
    RETURN_IF_ERROR(evaluator->open(state));
    if (_sides[index] == TGroupJoinAggSide::JOINED) {
        std::set<int> columns;
        for (const auto& expr : evaluator->input_exprs_ctxs()) {
            expr->root()->collect_slot_column_ids(columns);
        }
        for (const auto column : columns) {
            DCHECK_GE(column, 0);
            DCHECK_LT(static_cast<size_t>(column), _joined_columns.size());
            _joined_columns[column] = 1;
        }
    }
    const auto alignment = evaluator->align_of_data();
    _states_alignment = std::max(_states_alignment, alignment);
    _states_size = (_states_size + alignment - 1) / alignment * alignment;
    _offsets.push_back(_states_size);
    _states_size += evaluator->size_of_data();
    return Status::OK();
}

AggregateDataPtr GroupJoinProbeOperatorX::_create_group_state(GroupJoinProbeLocalState& local,
                                                              uint32_t head) const {
    const auto group_count_before_create = local._group_build_rows.size();
    auto* data = local._arena.aligned_alloc(_states_size, _states_alignment);
    size_t constructed = 0;
    try {
        for (; constructed < local._evaluators.size(); ++constructed) {
            local._evaluators[constructed]->create(data + _offsets[constructed]);
        }
        // Register only fully constructed states so close() can destroy them after any error.
        local._group_build_rows.push_back(head);
        local._group_states.push_back(data);
    } catch (...) {
        if (local._group_states.size() > local._group_build_rows.size()) {
            local._group_states.pop_back();
        }
        if (local._group_build_rows.size() > group_count_before_create) {
            local._group_build_rows.pop_back();
        }
        for (size_t i = 0; i < constructed; ++i) {
            local._evaluators[i]->destroy(data + _offsets[i]);
        }
        throw;
    }
    local._state_by_build_row[head] = data;
    return data;
}

Status GroupJoinProbeOperatorX::_initialize_probe(GroupJoinProbeLocalState& local) const {
    const auto& shared = *local._shared_state;
    local._state_by_build_row.assign(shared.build_block.rows(), nullptr);
    local._probe_context = make_join_probe_context(*shared.build_context);
    local._build_arguments.resize(_evaluators.size());
    local._probe_arguments.resize(_evaluators.size());
    local._argument_columns.resize(_evaluators.size());
    // Share source columns, but keep expression result slots private to this probe.
    Block build_expressions = shared.build_block;
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        if (_sides[i] != TGroupJoinAggSide::BUILD) {
            continue;
        }
        for (const auto& expr : local._evaluators[i]->input_exprs_ctxs()) {
            ColumnPtr argument;
            RETURN_IF_ERROR(groupjoin::evaluate_column(build_expressions, expr, argument));
            local._argument_columns[i].push_back(argument.get());
            local._build_arguments[i].push_back(std::move(argument));
        }
    }
    local._initialized = true;
    return Status::OK();
}

Status GroupJoinProbeOperatorX::_prepare_probe(Block& block,
                                               GroupJoinProbeLocalState& local) const {
    const auto probe_rows = block.rows();
    std::vector<ColumnPtr> key_columns;
    RETURN_IF_ERROR(groupjoin::do_evaluate(block, local._key_exprs, key_columns));
    ColumnRawPtrs raw_keys;
    ColumnUInt8::MutablePtr null_map;
    RETURN_IF_ERROR(groupjoin::extract_key_columns(probe_rows, key_columns, raw_keys, null_map));
    const auto* nulls = null_map ? null_map->get_data().data() : nullptr;
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        if (_sides[i] == TGroupJoinAggSide::PROBE) {
            auto& arguments = local._probe_arguments[i];
            auto& columns = local._argument_columns[i];
            arguments.clear();
            columns.clear();
            for (const auto& expr : local._evaluators[i]->input_exprs_ctxs()) {
                ColumnPtr argument;
                RETURN_IF_ERROR(groupjoin::evaluate_column(block, expr, argument));
                columns.push_back(argument.get());
                arguments.push_back(std::move(argument));
            }
        }
    }
    {
        SCOPED_TIMER(local._match_timer);
        std::visit(Overload {[](std::monostate&) { DORIS_CHECK(false); },
                             [&](auto& method) {
                                 prepare_join_probe(method, raw_keys,
                                                    static_cast<uint32_t>(probe_rows), nulls);
                             }},
                   local._probe_context->method_variant);
    }
    const size_t match_batch_size = local.state()->batch_size();
    local._match_places.resize(match_batch_size);
    local._match_build_rows.resize(match_batch_size);
    local._match_probe_rows.resize(match_batch_size);
    return Status::OK();
}

Block GroupJoinProbeOperatorX::_make_joined_block(const Block& probe,
                                                  const GroupJoinProbeLocalState& local,
                                                  size_t matches) const {
    Block joined;
    auto append = [&](const Block& input, const uint32_t* rows, size_t columns) {
        for (size_t i = 0; i < columns; ++i) {
            const auto& source = input.get_by_position(i);
            ColumnPtr column;
            if (_joined_columns[joined.columns()]) {
                auto materialized = source.column->clone_empty();
                materialized->insert_indices_from(*source.column, rows, rows + matches);
                column = std::move(materialized);
            } else {
                column = source.type->create_column_const_with_default_value(matches);
            }
            joined.insert({std::move(column), source.type, source.name});
        }
    };
    append(probe, local._match_probe_rows.data(),
           _child->operator_row_desc_after_projection().num_slots());
    append(local._shared_state->build_block, local._match_build_rows.data(),
           _build_side_child->operator_row_desc_after_projection().num_slots());
    return joined;
}

Status GroupJoinProbeOperatorX::_aggregate_batch(Block& probe, GroupJoinProbeLocalState& local,
                                                 size_t matches) const {
    Block joined;
    if (_has_joined_arguments) {
        joined = _make_joined_block(probe, local, matches);
    }
    for (size_t i = 0; i < local._evaluators.size(); ++i) {
        if (_sides[i] == TGroupJoinAggSide::JOINED) {
            RETURN_IF_ERROR(local._evaluators[i]->execute_batch_add(
                    &joined, _offsets[i], local._match_places.data(), local._arena));
            continue;
        }
        const auto& rows = _sides[i] == TGroupJoinAggSide::BUILD ? local._match_build_rows
                                                                 : local._match_probe_rows;
        local._evaluators[i]->add_batch_indexed(matches, local._match_places.data(), _offsets[i],
                                                local._argument_columns[i].data(), rows.data(),
                                                local._arena);
    }
    return Status::OK();
}

void GroupJoinProbeOperatorX::_assign_group_states(GroupJoinProbeLocalState& local,
                                                   const uint32_t* representatives, size_t matches,
                                                   uint32_t& previous_head,
                                                   AggregateDataPtr& previous_place) const {
    for (size_t i = 0; i < matches; ++i) {
        constexpr size_t prefetch_distance = 16;
        if (i + prefetch_distance < matches) {
            const auto ahead = representatives[local._match_probe_rows[i + prefetch_distance]];
            if (auto* ahead_place = local._state_by_build_row[ahead]; ahead_place != nullptr) {
                __builtin_prefetch(ahead_place, 1, 1);
            }
        }
        const auto head = representatives[local._match_probe_rows[i]];
        if (head != previous_head) {
            previous_place = local._state_by_build_row[head] == nullptr
                                     ? _create_group_state(local, head)
                                     : local._state_by_build_row[head];
            previous_head = head;
        }
        local._match_places[i] = previous_place;
    }
}

Status GroupJoinProbeOperatorX::_aggregate_matches(RuntimeState* state, Block* block,
                                                   GroupJoinProbeLocalState& local) const {
    const auto probe_rows = block->rows();
    // Evaluated columns are owned by block throughout matching; only key pointers are borrowed.
    RETURN_IF_ERROR(_prepare_probe(*block, local));
    int64_t matches = 0;
    uint32_t probe_row = 0;
    uint32_t build_row = 0;
    uint32_t previous_head = 0;
    AggregateDataPtr previous_place = nullptr;
    RETURN_IF_ERROR(std::visit(
            Overload {[](std::monostate&) -> Status {
                          return Status::InternalError("Uninitialized GroupJoin probe context");
                      },
                      [&](auto& method) -> Status {
                          while (probe_row < probe_rows) {
                              RETURN_IF_CANCELLED(state);
                              size_t buffered;
                              {
                                  SCOPED_TIMER(local._match_timer);
                                  buffered = groupjoin::match_batch(
                                          method, static_cast<uint32_t>(probe_rows), probe_row,
                                          build_row, local._match_probe_rows.data(),
                                          local._match_build_rows.data());
                              }
                              SCOPED_TIMER(local._update_timer);
                              _assign_group_states(local, method.bucket_nums.data(), buffered,
                                                   previous_head, previous_place);
                              matches += buffered;
                              if (buffered != 0) {
                                  RETURN_IF_ERROR(_aggregate_batch(*block, local, buffered));
                              }
                          }
                          return Status::OK();
                      }},
            local._probe_context->method_variant));
    for (size_t i = 0; i < _evaluators.size(); ++i) {
        if (_sides[i] == TGroupJoinAggSide::PROBE) {
            local._argument_columns[i].clear();
            local._probe_arguments[i].clear();
        }
    }
    COUNTER_UPDATE(local._matched_rows, matches);
    COUNTER_SET(local._group_count, static_cast<int64_t>(local._group_build_rows.size()));
    return Status::OK();
}

Status GroupJoinProbeOperatorX::push(RuntimeState* state, Block* block, bool eos) const {
    auto& local = get_local_state(state);
    auto* shared = local._shared_state;
    if (shared->short_circuit_for_probe) {
        local._probe_eos = true;
        return Status::OK();
    }
    if (!local._initialized) {
        RETURN_IF_ERROR(_initialize_probe(local));
    }
    if (block->rows() > 0) {
        RETURN_IF_ERROR(_aggregate_matches(state, block, local));
    }
    local._update_memory_usage();
    local._probe_eos = eos;
    if (eos) {
        // Draining needs only group states and build keys, not cached arguments or probe keys.
        local._release_probe_resources();
        local._update_memory_usage();
    }
    return Status::OK();
}

Status GroupJoinProbeOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local = get_local_state(state);
    if (local._shared_state->short_circuit_for_probe) {
        *eos = true;
        return Status::OK();
    }
    SCOPED_TIMER(local._drain_timer);
    auto columns = VectorizedUtils::create_columns_with_type_and_name(
            operator_row_desc_before_projection());
    MutableColumns result;
    for (size_t i = 0; i < columns.size(); ++i) {
        result.push_back(
                (i < _key_exprs.size() ? remove_nullable(columns[i].type) : columns[i].type)
                        ->create_column());
    }
    DCHECK(local._probe_eos);
    const auto end = std::min(local._group_build_rows.size(),
                              local._drain_position + static_cast<size_t>(state->batch_size()));
    ColumnRawPtrs build_keys;
    for (const auto& key : local._shared_state->build_keys) {
        build_keys.push_back(remove_nullable(key).get());
    }
    RETURN_IF_CANCELLED(state);
    const auto rows = end - local._drain_position;
    if (rows != 0) {
        const auto* build_rows = local._group_build_rows.data() + local._drain_position;
        for (size_t key = 0; key < _key_exprs.size(); ++key) {
            result[key]->insert_indices_from(*build_keys[key], build_rows, build_rows + rows);
        }
        local._result_places.resize(rows);
        for (size_t row = 0; row < rows; ++row) {
            local._result_places[row] = local._group_states[local._drain_position + row];
        }
        for (size_t i = 0; i < local._evaluators.size(); ++i) {
            local._evaluators[i]->insert_result_info_vec(local._result_places, _offsets[i],
                                                         result[i + _key_exprs.size()].get(), rows);
        }
        local._drain_position = end;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i].column = std::move(result[i]);
    }
    for (size_t key = 0; key < _key_exprs.size(); ++key) {
        if (columns[key].type->is_nullable()) {
            columns[key].column = make_nullable(columns[key].column);
        }
    }
    *block = Block(std::move(columns));
    *eos = local._drain_position == local._group_build_rows.size();
    local._update_memory_usage();
    return Status::OK();
}

bool GroupJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    const auto& local = get_local_state(state);
    return !local._probe_eos && !local._shared_state->short_circuit_for_probe;
}

size_t GroupJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local = get_local_state(state);
    if (local._shared_state->short_circuit_for_probe) {
        return 0;
    }
    size_t reserve = Base::get_reserve_mem_size(state);
    if (!local._probe_eos) {
        reserve +=
                state->batch_size() * (_states_size + sizeof(uint32_t) + sizeof(AggregateDataPtr));
    }
    if (!local._initialized) {
        // The row-to-state array and BUILD argument results are allocated only once.
        const auto build_rows = local._shared_state->build_block.rows();
        reserve += build_rows * sizeof(AggregateDataPtr);
        for (size_t i = 0; i < local._evaluators.size(); ++i) {
            if (_sides[i] != TGroupJoinAggSide::BUILD) {
                continue;
            }
            for (const auto& expr : local._evaluators[i]->input_exprs_ctxs()) {
                reserve += expr->root()->estimate_memory(build_rows);
            }
        }
    }
    return reserve;
}

Status GroupJoinProbeOperatorX::set_child(OperatorPtr child) {
    if (_child && !_build_side_child) {
        _build_side_child = child;
        return Status::OK();
    }
    return Base::set_child(child);
}

DataDistribution GroupJoinProbeOperatorX::required_data_distribution(RuntimeState*) const {
    return groupjoin::distribution(_distribution, _partition_exprs);
}
bool GroupJoinProbeOperatorX::is_shuffled_operator() const {
    return _distribution != TJoinDistributionType::NONE;
}
bool GroupJoinProbeOperatorX::is_colocated_operator() const {
    return _distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _distribution == TJoinDistributionType::COLOCATE;
}
bool GroupJoinProbeOperatorX::followed_by_shuffled_operator() const {
    return (is_shuffled_operator() && !is_colocated_operator()) || _followed_by_shuffled_operator;
}
} // namespace doris
