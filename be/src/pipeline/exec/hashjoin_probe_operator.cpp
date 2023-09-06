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

#include "hashjoin_probe_operator.h"

#include <string>

#include "pipeline/exec/operator.h"

namespace doris {
namespace pipeline {

OPERATOR_CODE_GENERATOR(HashJoinProbeOperator, StatefulOperator)

HashJoinProbeLocalState::HashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
        : JoinProbeLocalState<HashJoinDependency, HashJoinProbeLocalState>(state, parent) {}

Status HashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));
    auto& p = _parent->cast<HashJoinProbeOperatorX>();
    _probe_ignore_null = p._probe_ignore_null;
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    _other_join_conjuncts.resize(p._other_join_conjuncts.size());
    for (size_t i = 0; i < _other_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._other_join_conjuncts[i]->clone(state, _other_join_conjuncts[i]));
    }
    // Since the comparison of null values is meaningless, null aware left anti join should not output null
    // when the build side is not empty.
    if (!_shared_state->build_blocks->empty() && p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        _probe_ignore_null = true;
    }
    _construct_mutable_join_block();
    _probe_column_disguise_null.reserve(_probe_expr_ctxs.size());
    _probe_arena_memory_usage =
            profile()->AddHighWaterMarkCounter("ProbeKeyArena", TUnit::BYTES, "MemoryUsage");
    // Probe phase
    auto probe_phase_profile = _probe_phase_profile;
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _search_hashtable_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenSearchHashTableTime", "ProbeTime");
    _build_side_output_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenBuildSideOutputTime", "ProbeTime");
    _probe_side_output_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenProbeSideOutputTime", "ProbeTime");
    _probe_process_hashtable_timer =
            ADD_CHILD_TIMER(probe_phase_profile, "ProbeWhenProcessHashTableTime", "ProbeTime");
    _process_other_join_conjunct_timer = ADD_TIMER(profile(), "OtherJoinConjunctTime");
    return Status::OK();
}

void HashJoinProbeLocalState::prepare_for_next() {
    _probe_index = 0;
    _prepare_probe_block();
}

Status HashJoinProbeLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    if (_process_hashtable_ctx_variants) {
        std::visit(vectorized::Overload {[&](std::monostate&) {},
                                         [&](auto&& process_hashtable_ctx) {
                                             if (process_hashtable_ctx._arena) {
                                                 process_hashtable_ctx._arena.reset();
                                             }

                                             if (process_hashtable_ctx._serialize_key_arena) {
                                                 process_hashtable_ctx._serialize_key_arena.reset();
                                                 process_hashtable_ctx._serialized_key_buffer_size =
                                                         0;
                                             }
                                         }},
                   *_process_hashtable_ctx_variants);
    }
    _shared_state->arena = nullptr;
    _shared_state->hash_table_variants.reset();
    _process_hashtable_ctx_variants = nullptr;
    _null_map_column = nullptr;
    _tuple_is_null_left_flag_column = nullptr;
    _tuple_is_null_right_flag_column = nullptr;
    _probe_block.clear();
    return JoinProbeLocalState<HashJoinDependency, HashJoinProbeLocalState>::close(state);
}

bool HashJoinProbeLocalState::_need_probe_null_map(vectorized::Block& block,
                                                   const std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        if (!_shared_state->is_null_safe_eq_join[i]) {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (check_and_get_column<vectorized::ColumnNullable>(*column)) {
                return true;
            }
        }
    }
    return false;
}

void HashJoinProbeLocalState::init_for_probe(RuntimeState* state) {
    if (_probe_inited) {
        return;
    }
    _process_hashtable_ctx_variants = std::make_unique<vectorized::HashTableCtxVariants>();
    std::visit(
            [&](auto&& join_op_variants) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                _probe_context.reset(new vectorized::HashJoinProbeContext(this));
                _process_hashtable_ctx_variants
                        ->emplace<vectorized::ProcessHashTableProbe<JoinOpType::value>>(
                                _probe_context.get(), state->batch_size());
            },
            _shared_state->join_op_variants);
    _probe_inited = true;
}

void HashJoinProbeLocalState::add_tuple_is_null_column(vectorized::Block* block) {
    DCHECK(_parent->cast<HashJoinProbeOperatorX>()._is_outer_join);
    auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
    auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
    auto& left_null_map = reinterpret_cast<vectorized::ColumnUInt8&>(*p0);
    auto& right_null_map = reinterpret_cast<vectorized::ColumnUInt8&>(*p1);
    auto left_size = left_null_map.size();
    auto right_size = right_null_map.size();

    if (left_size == 0) {
        DCHECK_EQ(right_size, block->rows());
        left_null_map.get_data().resize_fill(right_size, 0);
    }
    if (right_size == 0) {
        DCHECK_EQ(left_size, block->rows());
        right_null_map.get_data().resize_fill(left_size, 0);
    }

    block->insert(
            {std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(), "left_tuples_is_null"});
    block->insert(
            {std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(), "right_tuples_is_null"});
}

void HashJoinProbeLocalState::_prepare_probe_block() {
    // clear_column_data of _probe_block
    if (!_probe_column_disguise_null.empty()) {
        for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
            auto column_to_erase = _probe_column_disguise_null[i];
            _probe_block.erase(column_to_erase - i);
        }
        _probe_column_disguise_null.clear();
    }

    // remove add nullmap of probe columns
    for (auto index : _probe_column_convert_to_null) {
        auto& column_type = _probe_block.safe_get_by_position(index);
        DCHECK(column_type.column->is_nullable() || is_column_const(*(column_type.column.get())));
        DCHECK(column_type.type->is_nullable());

        column_type.column = remove_nullable(column_type.column);
        column_type.type = remove_nullable(column_type.type);
    }
    _probe_block.clear_column_data(_parent->get_child()->row_desc().num_materialized_slots());
}

HashJoinProbeOperatorX::HashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               const DescriptorTbl& descs)
        : JoinProbeOperatorX<HashJoinProbeLocalState>(pool, tnode, descs),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}) {}

Status HashJoinProbeOperatorX::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                    SourceState& source_state) const {
    auto& local_state = state->get_local_state(id())->cast<HashJoinProbeLocalState>();
    local_state.init_for_probe(state);
    SCOPED_TIMER(local_state._probe_timer);
    if (local_state._shared_state->short_circuit_for_probe) {
        // If we use a short-circuit strategy, should return empty block directly.
        source_state = SourceState::FINISHED;
        return Status::OK();
    }
    local_state._join_block.clear_column_data();

    vectorized::MutableBlock mutable_join_block(&local_state._join_block);
    vectorized::Block temp_block;

    Status st;
    if (local_state._probe_index < local_state._probe_block.rows()) {
        DCHECK(local_state._has_set_need_null_map_for_probe);
        RETURN_IF_CATCH_EXCEPTION({
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx, auto need_null_map_for_probe,
                        auto ignore_null) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                if (_have_other_join_conjunct) {
                                    st = process_hashtable_ctx
                                                 .template do_process_with_other_join_conjuncts<
                                                         need_null_map_for_probe, ignore_null>(
                                                         arg,
                                                         need_null_map_for_probe
                                                                 ? &local_state._null_map_column
                                                                            ->get_data()
                                                                 : nullptr,
                                                         mutable_join_block, &temp_block,
                                                         local_state._probe_block.rows(),
                                                         _is_mark_join);
                                } else {
                                    st = process_hashtable_ctx.template do_process<
                                            need_null_map_for_probe, ignore_null>(
                                            arg,
                                            need_null_map_for_probe
                                                    ? &local_state._null_map_column->get_data()
                                                    : nullptr,
                                            mutable_join_block, &temp_block,
                                            local_state._probe_block.rows(), _is_mark_join);
                                }
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table probe";
                        }
                    },
                    *local_state._shared_state->hash_table_variants,
                    *local_state._process_hashtable_ctx_variants,
                    vectorized::make_bool_variant(local_state._need_null_map_for_probe),
                    vectorized::make_bool_variant(local_state._probe_ignore_null));
        });
    } else if (local_state._probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                bool eos;
                                st = process_hashtable_ctx.process_data_in_hashtable(
                                        arg, mutable_join_block, &temp_block, &eos);
                                source_state = eos ? SourceState::FINISHED : source_state;
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table probe";
                        }
                    },
                    *local_state._shared_state->hash_table_variants,
                    *local_state._process_hashtable_ctx_variants);
        } else {
            source_state = SourceState::FINISHED;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }
    if (!st) {
        return st;
    }
    if (_is_outer_join) {
        local_state.add_tuple_is_null_column(&temp_block);
    }
    auto output_rows = temp_block.rows();
    DCHECK(output_rows <= state->batch_size());
    {
        SCOPED_TIMER(local_state._join_filter_timer);
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, &temp_block,
                                                               temp_block.columns()));
    }

    // Here make _join_block release the columns' ptr
    local_state._join_block.set_columns(local_state._join_block.clone_empty_columns());
    mutable_join_block.clear();

    RETURN_IF_ERROR(local_state._build_output_block(&temp_block, output_block, false));
    local_state._reset_tuple_is_null_column();
    local_state.reached_limit(output_block, source_state);
    return Status::OK();
}

bool HashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = state->get_local_state(id())->cast<HashJoinProbeLocalState>();
    return (local_state._probe_block.rows() == 0 ||
            local_state._probe_index == local_state._probe_block.rows()) &&
           !local_state._probe_eos && !local_state._shared_state->short_circuit_for_probe;
}

Status HashJoinProbeOperatorX::_do_evaluate(vectorized::Block& block,
                                            vectorized::VExprContextSPtrs& exprs,
                                            RuntimeProfile::Counter& expr_call_timer,
                                            std::vector<int>& res_col_ids) const {
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

Status HashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                    SourceState source_state) const {
    auto& local_state = state->get_local_state(id())->cast<HashJoinProbeLocalState>();
    local_state.prepare_for_next();
    local_state._probe_eos = source_state == SourceState::FINISHED;
    if (input_block->rows() > 0) {
        COUNTER_UPDATE(local_state._probe_rows_counter, input_block->rows());
        int probe_expr_ctxs_sz = local_state._probe_expr_ctxs.size();
        local_state._probe_columns.resize(probe_expr_ctxs_sz);

        std::vector<int> res_col_ids(probe_expr_ctxs_sz);
        RETURN_IF_ERROR(_do_evaluate(*input_block, local_state._probe_expr_ctxs,
                                     *local_state._probe_expr_call_timer, res_col_ids));
        if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
            local_state._probe_column_convert_to_null =
                    local_state._dependency->convert_block_to_null(*input_block);
        }
        // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
        //  so we have to initialize this flag by the first probe block.
        if (!local_state._has_set_need_null_map_for_probe) {
            local_state._has_set_need_null_map_for_probe = true;
            local_state._need_null_map_for_probe =
                    local_state._need_probe_null_map(*input_block, res_col_ids);
        }
        if (local_state._need_null_map_for_probe) {
            if (local_state._null_map_column == nullptr) {
                local_state._null_map_column = vectorized::ColumnUInt8::create();
            }
            local_state._null_map_column->get_data().assign(input_block->rows(), (uint8_t)0);
        }

        RETURN_IF_ERROR(local_state._dependency->extract_join_column<false>(
                *input_block, local_state._null_map_column, local_state._probe_columns,
                res_col_ids));
        if (&local_state._probe_block != input_block) {
            input_block->swap(local_state._probe_block);
        }
    }
    return Status::OK();
}

Status HashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const bool probe_dispose_null =
            _match_all_probe || _build_unique || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
            _join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN;
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<bool> probe_not_ignore_null(eq_join_conjuncts.size());
    size_t conjuncts_index = 0;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);
        bool null_aware = eq_join_conjunct.__isset.opcode &&
                          eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL;
        probe_not_ignore_null[conjuncts_index] =
                null_aware ||
                (_probe_expr_ctxs.back()->root()->is_nullable() && probe_dispose_null);
        conjuncts_index++;
    }
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        _probe_ignore_null |= !probe_not_ignore_null[i];
    }
    if (tnode.hash_join_node.__isset.other_join_conjuncts &&
        !tnode.hash_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.other_join_conjuncts, _other_join_conjuncts));

        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    } else if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _other_join_conjuncts.resize(1);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                tnode.hash_join_node.vother_join_conjunct, _other_join_conjuncts[0]));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    std::vector<SlotId> hash_output_slot_ids;
    // init left/right output slots flags, only column of slot_id in _hash_output_slot_ids need
    // insert to output block of hash join.
    // _left_output_slots_flags : column of left table need to output set flag = true
    // _rgiht_output_slots_flags : column of right table need to output set flag = true
    // if _hash_output_slot_ids is empty, means all column of left/right table need to output.
    auto init_output_slots_flags = [&hash_output_slot_ids](auto& tuple_descs,
                                                           auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        hash_output_slot_ids.empty() ||
                        std::find(hash_output_slot_ids.begin(), hash_output_slot_ids.end(),
                                  slot_desc->id()) != hash_output_slot_ids.end());
            }
        }
    };
    init_output_slots_flags(_child_x->row_desc().tuple_descriptors(), _left_output_slot_flags);
    init_output_slots_flags(_build_side_child->row_desc().tuple_descriptors(),
                            _right_output_slot_flags);
    return Status::OK();
}

Status HashJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child_x->row_desc()));
    // right table data types
    _right_table_data_types =
            vectorized::VectorizedUtils::get_data_types(_build_side_child->row_desc());
    _left_table_data_types = vectorized::VectorizedUtils::get_data_types(_child_x->row_desc());
    _build_side_child.reset();
    return Status::OK();
}

Status HashJoinProbeOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    return Status::OK();
}

bool HashJoinProbeOperatorX::can_read(RuntimeState* state) {
    auto& local_state = state->get_local_state(id())->cast<HashJoinProbeLocalState>();
    return local_state._dependency->done();
}

} // namespace pipeline
} // namespace doris
