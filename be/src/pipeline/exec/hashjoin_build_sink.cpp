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

#include "hashjoin_build_sink.h"

#include <string>

#include "exprs/bloom_filter_func.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
HashJoinBuildSinkLocalState::HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                         RuntimeState* state)
        : JoinBuildSinkLocalState(parent, state) {
    _finish_dependency = std::make_shared<CountedFinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY");
}

Status HashJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;

    _shared_state->is_null_safe_eq_join = p._is_null_safe_eq_join;
    _shared_state->serialize_null_into_key = p._serialize_null_into_key;
    _build_expr_ctxs.resize(p._build_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._build_expr_ctxs[i]->clone(state, _build_expr_ctxs[i]));
    }
    _shared_state->build_exprs_size = _build_expr_ctxs.size();

    _should_build_hash_table = true;
    profile()->add_info_string("BroadcastJoin", std::to_string(p._is_broadcast_join));
    if (p._is_broadcast_join) {
        if (state->enable_share_hash_table_for_broadcast_join()) {
            _should_build_hash_table = info.task_idx == 0;
            if (_should_build_hash_table) {
                p._shared_hashtable_controller->set_builder_and_consumers(
                        state->fragment_instance_id(), p.node_id());
            }
        }
    }
    profile()->add_info_string("BuildShareHashTable", std::to_string(_should_build_hash_table));
    profile()->add_info_string("ShareHashTableEnabled",
                               std::to_string(state->enable_share_hash_table_for_broadcast_join()));
    if (!_should_build_hash_table) {
        _dependency->block();
        _finish_dependency->block();
        p._shared_hashtable_controller->append_dependency(p.node_id(),
                                                          _dependency->shared_from_this(),
                                                          _finish_dependency->shared_from_this());
    }

    _runtime_filter_init_timer = ADD_TIMER(profile(), "RuntimeFilterInitTime");
    _build_blocks_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile(), "MemoryUsageBuildBlocks", TUnit::BYTES, 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _build_arena_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile(), "MemoryUsageBuildKeyArena", TUnit::BYTES, 1);

    // Build phase
    auto* record_profile = _should_build_hash_table ? profile() : faker_runtime_profile();
    _build_table_timer = ADD_TIMER(profile(), "BuildHashTableTime");
    _build_side_merge_block_timer = ADD_TIMER(profile(), "MergeBuildBlockTime");
    _build_table_insert_timer = ADD_TIMER(record_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(record_profile, "BuildExprCallTime");

    // Hash Table Init
    RETURN_IF_ERROR(_hash_table_init(state));
    _runtime_filters.resize(p._runtime_filter_descs.size());
    for (size_t i = 0; i < p._runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->register_producer_runtime_filter(p._runtime_filter_descs[i],
                                                                &_runtime_filters[i]));
    }

    _runtime_filter_slots =
            std::make_shared<VRuntimeFilterSlots>(_build_expr_ctxs, runtime_filters());

    return Status::OK();
}

Status HashJoinBuildSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinBuildSinkLocalState::open(state));
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    auto p = _parent->cast<HashJoinBuildSinkOperatorX>();
    Defer defer {[&]() {
        if (!_should_build_hash_table) {
            return;
        }
        // The build side hash key column maybe no need output, but we need to keep the column in block
        // because it is used to compare with probe side hash key column

        if (p._should_keep_hash_key_column && _build_col_ids.size() == 1) {
            p._should_keep_column_flags[_build_col_ids[0]] = true;
        }

        if (_shared_state->build_block) {
            // release the memory of unused column in probe stage
            _shared_state->build_block->clear_column_mem_not_keep(
                    p._should_keep_column_flags, bool(p._shared_hashtable_controller));
        }

        if (p._shared_hashtable_controller) {
            p._shared_hashtable_controller->signal_finish(p.node_id());
        }
    }};

    if (!_runtime_filter_slots || _runtime_filters.empty() || state->is_cancelled()) {
        return Base::close(state, exec_status);
    }

    try {
        if (state->get_task()->wake_up_by_downstream()) {
            if (_should_build_hash_table) {
                // partitial ignore rf to make global rf work
                RETURN_IF_ERROR(
                        _runtime_filter_slots->send_filter_size(state, 0, _finish_dependency));
                RETURN_IF_ERROR(_runtime_filter_slots->ignore_all_filters());
            } else {
                // do not publish filter coz local rf not inited and useless
                return Base::close(state, exec_status);
            }
        } else if (_should_build_hash_table) {
            if (p._shared_hashtable_controller &&
                !p._shared_hash_table_context->complete_build_stage) {
                return Status::InternalError("close before sink meet eos");
            }
            auto* block = _shared_state->build_block.get();
            uint64_t hash_table_size = block ? block->rows() : 0;
            {
                SCOPED_TIMER(_runtime_filter_init_timer);
                RETURN_IF_ERROR(_runtime_filter_slots->init_filters(state, hash_table_size));
                RETURN_IF_ERROR(_runtime_filter_slots->ignore_filters(state));
            }
            if (hash_table_size > 1) {
                SCOPED_TIMER(_runtime_filter_compute_timer);
                _runtime_filter_slots->insert(block);
            }
        } else if ((p._shared_hashtable_controller && !p._shared_hash_table_context->signaled) ||
                   (p._shared_hash_table_context &&
                    !p._shared_hash_table_context->complete_build_stage)) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "build_sink::close meet error state");
        } else {
            RETURN_IF_ERROR(
                    _runtime_filter_slots->copy_from_shared_context(p._shared_hash_table_context));
        }

        SCOPED_TIMER(_publish_runtime_filter_timer);
        RETURN_IF_ERROR(_runtime_filter_slots->publish(state, !_should_build_hash_table));
    } catch (Exception& e) {
        return Status::InternalError(
                "rf process meet error: {}, wake_up_by_downstream: {}, should_build_hash_table: "
                "{}, _finish_dependency: {}, complete_build_stage: {}, shared_hash_table_signaled: "
                "{}",
                e.to_string(), state->get_task()->wake_up_by_downstream(), _should_build_hash_table,
                _finish_dependency->debug_string(),
                p._shared_hash_table_context && !p._shared_hash_table_context->complete_build_stage,
                p._shared_hashtable_controller && !p._shared_hash_table_context->signaled);
    }
    return Base::close(state, exec_status);
}

bool HashJoinBuildSinkLocalState::build_unique() const {
    return _parent->cast<HashJoinBuildSinkOperatorX>()._build_unique;
}

void HashJoinBuildSinkLocalState::init_short_circuit_for_probe() {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    bool empty_block =
            !_shared_state->build_block ||
            !(_shared_state->build_block->rows() > 1); // build size always mock a row into block
    _shared_state->short_circuit_for_probe =
            ((_shared_state->_has_null_in_build_side &&
              p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) ||
             (empty_block &&
              (p._join_op == TJoinOp::INNER_JOIN || p._join_op == TJoinOp::LEFT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_ANTI_JOIN))) &&
            !p._is_mark_join;

    //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
    //we could get the result is probe table + null-column(if need output)
    _shared_state->empty_right_table_need_probe_dispose =
            (empty_block && !p._have_other_join_conjunct && !p._is_mark_join) &&
            (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
             p._join_op == TJoinOp::LEFT_ANTI_JOIN);
}

Status HashJoinBuildSinkLocalState::_do_evaluate(vectorized::Block& block,
                                                 vectorized::VExprContextSPtrs& exprs,
                                                 RuntimeProfile::Counter& expr_call_timer,
                                                 std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }
    return Status::OK();
}

std::vector<uint16_t> HashJoinBuildSinkLocalState::_convert_block_to_null(
        vectorized::Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

Status HashJoinBuildSinkLocalState::_extract_join_column(
        vectorized::Block& block, vectorized::ColumnUInt8::MutablePtr& null_map,
        vectorized::ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids) {
    auto& shared_state = *_shared_state;
    for (size_t i = 0; i < shared_state.build_exprs_size; ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (!column->is_nullable() && shared_state.serialize_null_into_key[i]) {
            _key_columns_holder.emplace_back(
                    vectorized::make_nullable(block.get_by_position(res_col_ids[i]).column));
            raw_ptrs[i] = _key_columns_holder.back().get();
        } else if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column);
                   !shared_state.serialize_null_into_key[i] && nullable) {
            // update nulllmap and split nested out of ColumnNullable when serialize_null_into_key is false and column is nullable
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            DCHECK(null_map != nullptr);
            vectorized::VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
            raw_ptrs[i] = &col_nested;
        } else {
            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::process_build_block(RuntimeState* state,
                                                        vectorized::Block& block) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (UNLIKELY(rows == 0)) {
        return Status::OK();
    }
    block.replace_if_overflow();

    vectorized::ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    vectorized::ColumnUInt8::MutablePtr null_map_val;
    if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN) {
        _convert_block_to_null(block);
        // first row is mocked
        for (int i = 0; i < block.columns(); i++) {
            auto [column, is_const] = unpack_if_const(block.safe_get_by_position(i).column);
            assert_cast<vectorized::ColumnNullable*>(column->assume_mutable().get())
                    ->get_null_map_column()
                    .get_data()
                    .data()[0] = 1;
        }
    }

    _set_build_side_has_external_nullmap(block, _build_col_ids);
    if (_build_side_has_external_nullmap) {
        null_map_val = vectorized::ColumnUInt8::create();
        null_map_val->get_data().assign(rows, (uint8_t)0);
    }

    // Get the key column that needs to be built
    Status st = _extract_join_column(block, null_map_val, raw_ptrs, _build_col_ids);

    st = std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg, auto join_op,
                        auto short_circuit_for_null_in_build_side,
                        auto with_other_conjuncts) -> Status {
                        LOG(FATAL) << "FATAL: uninited hash table";
                        __builtin_unreachable();
                        return Status::OK();
                    },
                    [&](auto&& arg, auto&& join_op, auto short_circuit_for_null_in_build_side,
                        auto with_other_conjuncts) -> Status {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        using JoinOpType = std::decay_t<decltype(join_op)>;
                        ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                                rows, raw_ptrs, this, state->batch_size(), state);
                        auto st = hash_table_build_process.template run<
                                JoinOpType::value, short_circuit_for_null_in_build_side,
                                with_other_conjuncts>(
                                arg, null_map_val ? &null_map_val->get_data() : nullptr,
                                &_shared_state->_has_null_in_build_side);
                        COUNTER_SET(_memory_used_counter,
                                    _build_blocks_memory_usage->value() +
                                            (int64_t)(arg.hash_table->get_byte_size() +
                                                      arg.serialized_keys_size(true)));
                        return st;
                    }},
            _shared_state->hash_table_variants->method_variant, _shared_state->join_op_variants,
            vectorized::make_bool_variant(p._short_circuit_for_null_in_build_side),
            vectorized::make_bool_variant((p._have_other_join_conjunct)));

    return st;
}

void HashJoinBuildSinkLocalState::_set_build_side_has_external_nullmap(
        vectorized::Block& block, const std::vector<int>& res_col_ids) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    if (p._short_circuit_for_null_in_build_side) {
        _build_side_has_external_nullmap = true;
        return;
    }
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (column->is_nullable() && !_shared_state->serialize_null_into_key[i]) {
            _build_side_has_external_nullmap = true;
            return;
        }
    }
}

Status HashJoinBuildSinkLocalState::_hash_table_init(RuntimeState* state) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    std::vector<vectorized::DataTypePtr> data_types;
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        auto& ctx = _build_expr_ctxs[i];
        auto data_type = ctx->root()->data_type();

        /// For 'null safe equal' join,
        /// the build key column maybe be converted to nullable from non-nullable.
        if (p._serialize_null_into_key[i]) {
            data_type = vectorized::make_nullable(data_type);
        }
        data_types.emplace_back(std::move(data_type));
    }
    if (_build_expr_ctxs.size() == 1) {
        p._should_keep_hash_key_column = true;
    }
    return init_hash_method<JoinDataVariants>(_shared_state->hash_table_variants.get(), data_types,
                                              true);
}

HashJoinBuildSinkOperatorX::HashJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX(pool, operator_id, tnode, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                   ? tnode.distribute_expr_lists[1]
                                   : std::vector<TExpr> {}) {}

Status HashJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);

    if (tnode.hash_join_node.__isset.hash_output_slot_ids) {
        _hash_output_slot_ids = tnode.hash_join_node.hash_output_slot_ids;
    }

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, build_ctx));
        {
            // for type check
            vectorized::VExprContextSPtr probe_ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, probe_ctx));
            auto build_side_expr_type = build_ctx->root()->data_type();
            auto probe_side_expr_type = probe_ctx->root()->data_type();
            if (!vectorized::make_nullable(build_side_expr_type)
                         ->equals(*vectorized::make_nullable(probe_side_expr_type))) {
                return Status::InternalError(
                        "build side type {}, not match probe side type {} , node info "
                        "{}",
                        build_side_expr_type->get_name(), probe_side_expr_type->get_name(),
                        this->debug_string(0));
            }
        }
        _build_expr_ctxs.push_back(build_ctx);

        const auto vexpr = _build_expr_ctxs.back()->root();

        /// null safe equal means null = null is true, the operator in SQL should be: <=>.
        const bool is_null_safe_equal =
                eq_join_conjunct.__isset.opcode &&
                (eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) &&
                // For a null safe equal join, FE may generate a plan that
                // both sides of the conjuct are not nullable, we just treat it
                // as a normal equal join conjunct.
                (eq_join_conjunct.right.nodes[0].is_nullable ||
                 eq_join_conjunct.left.nodes[0].is_nullable);

        _is_null_safe_eq_join.push_back(is_null_safe_equal);

        if (eq_join_conjuncts.size() == 1) {
            // single column key serialize method must use nullmap for represent null to instead serialize null into key
            _serialize_null_into_key.emplace_back(false);
        } else if (is_null_safe_equal) {
            // use serialize null into key to represent multi column null value
            _serialize_null_into_key.emplace_back(true);
        } else {
            // on normal conditions, because null!=null, it can be expressed directly with nullmap.
            _serialize_null_into_key.emplace_back(false);
        }
    }

    return Status::OK();
}

Status HashJoinBuildSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::open(state));
    if (_is_broadcast_join) {
        if (state->enable_share_hash_table_for_broadcast_join()) {
            _shared_hashtable_controller =
                    state->get_query_ctx()->get_shared_hash_table_controller();
            _shared_hash_table_context = _shared_hashtable_controller->get_context(node_id());
        }
    }
    auto init_keep_column_flags = [&](auto& tuple_descs, auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        _hash_output_slot_ids.empty() ||
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
            }
        }
    };
    init_keep_column_flags(row_desc().tuple_descriptors(), _should_keep_column_flags);
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_build_expr_ctxs, state, _child->row_desc()));
    return vectorized::VExpr::open(_build_expr_ctxs, state);
}

Status HashJoinBuildSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    local_state._eos = eos;
    if (local_state._should_build_hash_table) {
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from probe side.

        if (local_state._build_side_mutable_block.empty()) {
            auto tmp_build_block = vectorized::VectorizedUtils::create_empty_columnswithtypename(
                    _child->row_desc());
            tmp_build_block = *(tmp_build_block.create_same_struct_block(1, false));
            local_state._build_col_ids.resize(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(tmp_build_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     local_state._build_col_ids));
            local_state._build_side_mutable_block =
                    vectorized::MutableBlock::build_mutable_block(&tmp_build_block);
        }

        if (!in_block->empty()) {
            std::vector<int> res_col_ids(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(*in_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     res_col_ids));
            local_state._build_side_rows += in_block->rows();
            if (local_state._build_side_rows > std::numeric_limits<uint32_t>::max()) {
                return Status::NotSupported(
                        "Hash join do not support build table rows over: {}, you should enable "
                        "join spill to avoid this issue",
                        std::to_string(std::numeric_limits<uint32_t>::max()));
            }

            SCOPED_TIMER(local_state._build_side_merge_block_timer);
            RETURN_IF_ERROR(local_state._build_side_mutable_block.merge_ignore_overflow(
                    std::move(*in_block)));
            int64_t blocks_mem_usage = local_state._build_side_mutable_block.allocated_bytes();
            COUNTER_SET(local_state._memory_used_counter, blocks_mem_usage);
            COUNTER_SET(local_state._build_blocks_memory_usage, blocks_mem_usage);
        }
    }

    if (local_state._should_build_hash_table && eos) {
        DCHECK(!local_state._build_side_mutable_block.empty());
        local_state._shared_state->build_block = std::make_shared<vectorized::Block>(
                local_state._build_side_mutable_block.to_block());

        RETURN_IF_ERROR(local_state._runtime_filter_slots->send_filter_size(
                state, local_state._shared_state->build_block->rows(),
                local_state._finish_dependency));
        RETURN_IF_ERROR(
                local_state.process_build_block(state, (*local_state._shared_state->build_block)));
        if (_shared_hashtable_controller) {
            _shared_hash_table_context->status = Status::OK();
            _shared_hash_table_context->complete_build_stage = true;
            // arena will be shared with other instances.
            _shared_hash_table_context->arena = local_state._shared_state->arena;
            _shared_hash_table_context->hash_table_variants =
                    local_state._shared_state->hash_table_variants;
            _shared_hash_table_context->short_circuit_for_null_in_probe_side =
                    local_state._shared_state->_has_null_in_build_side;
            _shared_hash_table_context->block = local_state._shared_state->build_block;
            _shared_hash_table_context->build_indexes_null =
                    local_state._shared_state->build_indexes_null;
            local_state._runtime_filter_slots->copy_to_shared_context(_shared_hash_table_context);
        }
    } else if (!local_state._should_build_hash_table &&
               _shared_hash_table_context->complete_build_stage) {
        DCHECK(_shared_hashtable_controller != nullptr);
        DCHECK(_shared_hash_table_context != nullptr);
        // the instance which is not build hash table, it's should wait the signal of hash table build finished.
        // but if it's running and signaled == false, maybe the source operator have closed caused by some short circuit,
        if (!_shared_hash_table_context->signaled) {
            return Status::Error<ErrorCode::END_OF_FILE>("source have closed");
        }

        if (!_shared_hash_table_context->status.ok()) {
            return _shared_hash_table_context->status;
        }

        local_state.profile()->add_info_string(
                "SharedHashTableFrom",
                print_id(
                        _shared_hashtable_controller->get_builder_fragment_instance_id(node_id())));
        local_state._shared_state->_has_null_in_build_side =
                _shared_hash_table_context->short_circuit_for_null_in_probe_side;
        std::visit(
                [](auto&& dst, auto&& src) {
                    if constexpr (!std::is_same_v<std::monostate, std::decay_t<decltype(dst)>> &&
                                  std::is_same_v<std::decay_t<decltype(src)>,
                                                 std::decay_t<decltype(dst)>>) {
                        dst.hash_table = src.hash_table;
                    }
                },
                local_state._shared_state->hash_table_variants->method_variant,
                std::static_pointer_cast<JoinDataVariants>(
                        _shared_hash_table_context->hash_table_variants)
                        ->method_variant);

        local_state._shared_state->build_block = _shared_hash_table_context->block;
        local_state._shared_state->build_indexes_null =
                _shared_hash_table_context->build_indexes_null;
    }

    if (eos) {
        local_state.init_short_circuit_for_probe();
        // Since the comparison of null values is meaningless, null aware left anti/semi join should not output null
        // when the build side is not empty.
        if (local_state._shared_state->build_block &&
            (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
             _join_op == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN)) {
            local_state._shared_state->probe_ignore_null = true;
        }
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

} // namespace doris::pipeline
