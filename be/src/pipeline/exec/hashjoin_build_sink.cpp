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
#include "vec/exec/join/vhash_join_node.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(HashJoinBuildSink, StreamingOperator)

template <typename... Callables>
struct Overload : Callables... {
    using Callables::operator()...;
};

template <typename... Callables>
Overload(Callables&&... callables) -> Overload<Callables...>;

HashJoinBuildSinkLocalState::HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                         RuntimeState* state)
        : JoinBuildSinkLocalState(parent, state) {}

Status HashJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _shared_hash_table_dependency = SharedHashTableDependency::create_shared(_parent->id());
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;
    if (p._is_broadcast_join && state->enable_share_hash_table_for_broadcast_join()) {
        _shared_state->build_block = p._shared_hash_table_context->block;
    }
    _shared_state->is_null_safe_eq_join = p._is_null_safe_eq_join;
    _shared_state->store_null_in_hash_table = p._store_null_in_hash_table;
    _build_expr_ctxs.resize(p._build_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._build_expr_ctxs[i]->clone(state, _build_expr_ctxs[i]));
    }
    _shared_state->build_exprs_size = _build_expr_ctxs.size();

    _should_build_hash_table = true;
    if (p._is_broadcast_join) {
        profile()->add_info_string("BroadcastJoin", "true");
        if (state->enable_share_hash_table_for_broadcast_join()) {
            profile()->add_info_string("ShareHashTableEnabled", "true");
            _should_build_hash_table = p._shared_hashtable_controller->should_build_hash_table(
                    state->fragment_instance_id(), p.id());
        } else {
            profile()->add_info_string("ShareHashTableEnabled", "false");
        }
    }
    if (!_should_build_hash_table) {
        _shared_hash_table_dependency->block_writing();
        p._shared_hashtable_controller->append_dependency(p.id(), _shared_hash_table_dependency);
    }

    _memory_usage_counter = ADD_LABEL_COUNTER(profile(), "MemoryUsage");

    _build_blocks_memory_usage =
            ADD_CHILD_COUNTER(profile(), "BuildBlocks", TUnit::BYTES, "MemoryUsage");
    _hash_table_memory_usage =
            ADD_CHILD_COUNTER(profile(), "HashTable", TUnit::BYTES, "MemoryUsage");
    _build_arena_memory_usage =
            profile()->AddHighWaterMarkCounter("BuildKeyArena", TUnit::BYTES, "MemoryUsage");

    // Build phase
    auto record_profile = _should_build_hash_table ? profile() : faker_runtime_profile();
    _build_table_timer = ADD_TIMER(profile(), "BuildTableTime");
    _build_side_merge_block_timer = ADD_TIMER(profile(), "BuildSideMergeBlockTime");
    _build_table_insert_timer = ADD_TIMER(record_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(record_profile, "BuildExprCallTime");
    _build_table_expanse_timer = ADD_TIMER(record_profile, "BuildTableExpanseTime");
    _build_table_convert_timer = ADD_TIMER(record_profile, "BuildTableConvertToPartitionedTime");
    _build_side_compute_hash_timer = ADD_TIMER(record_profile, "BuildSideHashComputingTime");
    _build_runtime_filter_timer = ADD_TIMER(record_profile, "BuildRuntimeFilterTime");

    _allocate_resource_timer = ADD_TIMER(profile(), "AllocateResourceTime");

    _build_buckets_counter = ADD_COUNTER(profile(), "BuildBuckets", TUnit::UNIT);
    _build_buckets_fill_counter = ADD_COUNTER(profile(), "FilledBuckets", TUnit::UNIT);

    _build_collisions_counter = ADD_COUNTER(profile(), "BuildCollisions", TUnit::UNIT);
    // Hash Table Init
    _hash_table_init(state);

    _runtime_filters.resize(p._runtime_filter_descs.size());
    for (size_t i = 0; i < p._runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_producer_filter(
                p._runtime_filter_descs[i], state->query_options(), _build_expr_ctxs.size() == 1));
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(
                p._runtime_filter_descs[i].filter_id, &_runtime_filters[i]));
    }

    return Status::OK();
}

Status HashJoinBuildSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinBuildSinkLocalState::open(state));
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();

    for (size_t i = 0; i < p._runtime_filter_descs.size(); i++) {
        if (auto bf = _runtime_filters[i]->get_bloomfilter()) {
            RETURN_IF_ERROR(bf->init_with_fixed_length());
        }
    }
    return Status::OK();
}

bool HashJoinBuildSinkLocalState::build_unique() const {
    return _parent->cast<HashJoinBuildSinkOperatorX>()._build_unique;
}

std::vector<TRuntimeFilterDesc>& HashJoinBuildSinkLocalState::runtime_filter_descs() const {
    return _parent->cast<HashJoinBuildSinkOperatorX>()._runtime_filter_descs;
}

void HashJoinBuildSinkLocalState::init_short_circuit_for_probe() {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    _shared_state->short_circuit_for_probe =
            (_shared_state->_has_null_in_build_side &&
             p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && !p._is_mark_join) ||
            (!_shared_state->build_block && p._join_op == TJoinOp::INNER_JOIN &&
             !p._is_mark_join) ||
            (!_shared_state->build_block && p._join_op == TJoinOp::LEFT_SEMI_JOIN &&
             !p._is_mark_join) ||
            (!_shared_state->build_block && p._join_op == TJoinOp::RIGHT_OUTER_JOIN) ||
            (!_shared_state->build_block && p._join_op == TJoinOp::RIGHT_SEMI_JOIN) ||
            (!_shared_state->build_block && p._join_op == TJoinOp::RIGHT_ANTI_JOIN);

    //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
    //we could get the result is probe table + null-column(if need output)
    _shared_state->empty_right_table_need_probe_dispose =
            (!_shared_state->build_block && !p._have_other_join_conjunct && !p._is_mark_join) &&
            (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
             p._join_op == TJoinOp::LEFT_ANTI_JOIN);
}

Status HashJoinBuildSinkLocalState::process_build_block(RuntimeState* state,
                                                        vectorized::Block& block) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (UNLIKELY(rows == 0)) {
        return Status::OK();
    }
    COUNTER_UPDATE(_build_rows_counter, rows);

    vectorized::ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    vectorized::ColumnUInt8::MutablePtr null_map_val;
    std::vector<int> res_col_ids(_build_expr_ctxs.size());
    RETURN_IF_ERROR(_dependency->do_evaluate(block, _build_expr_ctxs, *_build_expr_call_timer,
                                             res_col_ids));
    if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN) {
        _dependency->convert_block_to_null(block);
    }
    // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
    //  so we have to initialize this flag by the first build block.
    if (!_has_set_need_null_map_for_build) {
        _has_set_need_null_map_for_build = true;
        _set_build_ignore_flag(block, res_col_ids);
    }
    if (p._short_circuit_for_null_in_build_side || _build_side_ignore_null) {
        null_map_val = vectorized::ColumnUInt8::create();
        null_map_val->get_data().assign(rows, (uint8_t)0);
    }

    // Get the key column that needs to be built
    Status st = _dependency->extract_join_column<true>(block, null_map_val, raw_ptrs, res_col_ids);

    st = std::visit(
            Overload {[&](std::monostate& arg, auto join_op, auto has_null_value,
                          auto short_circuit_for_null_in_build_side) -> Status {
                          LOG(FATAL) << "FATAL: uninited hash table";
                          __builtin_unreachable();
                          return Status::OK();
                      },
                      [&](auto&& arg, auto&& join_op, auto has_null_value,
                          auto short_circuit_for_null_in_build_side) -> Status {
                          using HashTableCtxType = std::decay_t<decltype(arg)>;
                          using JoinOpType = std::decay_t<decltype(join_op)>;
                          vectorized::ProcessHashTableBuild<HashTableCtxType,
                                                            HashJoinBuildSinkLocalState>
                                  hash_table_build_process(rows, block, raw_ptrs, this,
                                                           state->batch_size(), state);
                          return hash_table_build_process
                                  .template run<JoinOpType::value, has_null_value,
                                                short_circuit_for_null_in_build_side>(
                                          arg,
                                          has_null_value || short_circuit_for_null_in_build_side
                                                  ? &null_map_val->get_data()
                                                  : nullptr,
                                          &_shared_state->_has_null_in_build_side);
                      }},
            *_shared_state->hash_table_variants, _shared_state->join_op_variants,
            vectorized::make_bool_variant(_build_side_ignore_null),
            vectorized::make_bool_variant(p._short_circuit_for_null_in_build_side));

    return st;
}

void HashJoinBuildSinkLocalState::_set_build_ignore_flag(vectorized::Block& block,
                                                         const std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        if (!_shared_state->is_null_safe_eq_join[i]) {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if (check_and_get_column<vectorized::ColumnNullable>(*column)) {
                _build_side_ignore_null |= (_parent->cast<HashJoinBuildSinkOperatorX>()._join_op !=
                                                    TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                                            !_shared_state->store_null_in_hash_table[i]);
            }
        }
    }
}

void HashJoinBuildSinkLocalState::_hash_table_init(RuntimeState* state) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    std::visit(
            [&](auto&& join_op_variants, auto have_other_join_conjunct) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                using RowRefListType = std::conditional_t<
                        have_other_join_conjunct, vectorized::RowRefListWithFlags,
                        std::conditional_t<JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                                   JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                                   JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                                                   JoinOpType::value == TJoinOp::FULL_OUTER_JOIN,
                                           vectorized::RowRefListWithFlag, vectorized::RowRefList>>;
                if (_build_expr_ctxs.size() == 1 && !p._store_null_in_hash_table[0]) {
                    // Single column optimization
                    switch (_build_expr_ctxs[0]->root()->result_type()) {
                    case TYPE_BOOLEAN:
                    case TYPE_TINYINT:
                        _shared_state->hash_table_variants
                                ->emplace<vectorized::I8HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_SMALLINT:
                        _shared_state->hash_table_variants
                                ->emplace<vectorized::I16HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_INT:
                    case TYPE_FLOAT:
                    case TYPE_DATEV2:
                        _shared_state->hash_table_variants
                                ->emplace<vectorized::I32HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_BIGINT:
                    case TYPE_DOUBLE:
                    case TYPE_DATETIME:
                    case TYPE_DATE:
                    case TYPE_DATETIMEV2:
                        _shared_state->hash_table_variants
                                ->emplace<vectorized::I64HashTableContext<RowRefListType>>();
                        break;
                    case TYPE_LARGEINT:
                    case TYPE_DECIMALV2:
                    case TYPE_DECIMAL32:
                    case TYPE_DECIMAL64:
                    case TYPE_DECIMAL128I: {
                        vectorized::DataTypePtr& type_ptr =
                                _build_expr_ctxs[0]->root()->data_type();
                        vectorized::TypeIndex idx =
                                _build_expr_ctxs[0]->root()->is_nullable()
                                        ? assert_cast<const vectorized::DataTypeNullable&>(
                                                  *type_ptr)
                                                  .get_nested_type()
                                                  ->get_type_id()
                                        : type_ptr->get_type_id();
                        vectorized::WhichDataType which(idx);
                        if (which.is_decimal32()) {
                            _shared_state->hash_table_variants
                                    ->emplace<vectorized::I32HashTableContext<RowRefListType>>();
                        } else if (which.is_decimal64()) {
                            _shared_state->hash_table_variants
                                    ->emplace<vectorized::I64HashTableContext<RowRefListType>>();
                        } else {
                            _shared_state->hash_table_variants
                                    ->emplace<vectorized::I128HashTableContext<RowRefListType>>();
                        }
                        break;
                    }
                    default:
                        _shared_state->hash_table_variants
                                ->emplace<vectorized::SerializedHashTableContext<RowRefListType>>();
                    }
                    return;
                }
                if (!try_get_hash_map_context_fixed<JoinFixedHashMap, HashCRC32, RowRefListType>(
                            *_shared_state->hash_table_variants, _build_expr_ctxs)) {
                    _shared_state->hash_table_variants
                            ->emplace<vectorized::SerializedHashTableContext<RowRefListType>>();
                }
            },
            _shared_state->join_op_variants,
            vectorized::make_bool_variant(p._have_other_join_conjunct));

    DCHECK(!std::holds_alternative<std::monostate>(*_shared_state->hash_table_variants));
}

HashJoinBuildSinkOperatorX::HashJoinBuildSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX(pool, tnode, descs),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join) {
    _runtime_filter_descs = tnode.runtime_filters;
}

Status HashJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    if (_is_broadcast_join) {
        if (state->enable_share_hash_table_for_broadcast_join()) {
            _shared_hashtable_controller =
                    state->get_query_ctx()->get_shared_hash_table_controller();
            _shared_hash_table_context = _shared_hashtable_controller->get_context(id());
        }
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_build_expr_ctxs, state, _child_x->row_desc()));
    return Status::OK();
}

Status HashJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);

    const bool build_stores_null = _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                                   _join_op == TJoinOp::FULL_OUTER_JOIN ||
                                   _join_op == TJoinOp::RIGHT_ANTI_JOIN;

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_expr_ctxs.push_back(ctx);

        const auto vexpr = _build_expr_ctxs.back()->root();

        bool null_aware = eq_join_conjunct.__isset.opcode &&
                          eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL;

        _is_null_safe_eq_join.push_back(null_aware);

        // if is null aware, build join column and probe join column both need dispose null value
        _store_null_in_hash_table.emplace_back(
                null_aware ||
                (_build_expr_ctxs.back()->root()->is_nullable() && build_stores_null));
    }
    return Status::OK();
}

Status HashJoinBuildSinkOperatorX::open(RuntimeState* state) {
    return vectorized::VExpr::open(_build_expr_ctxs, state);
}

Status HashJoinBuildSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (local_state._shared_state->_has_null_in_build_side) {
        // TODO: if _has_null_in_build_side is true we should finish current pipeline task.
        DCHECK(state->enable_pipeline_exec());
        return Status::OK();
    }
    if (local_state._should_build_hash_table) {
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from probe side.
        local_state._build_side_mem_used += in_block->allocated_bytes();

        if (in_block->rows() != 0) {
            SCOPED_TIMER(local_state._build_side_merge_block_timer);
            if (local_state._build_side_mutable_block.empty()) {
                RETURN_IF_ERROR(local_state._build_side_mutable_block.merge(
                        *(in_block->create_same_struct_block(1, false))));
            }
            RETURN_IF_ERROR(local_state._build_side_mutable_block.merge(*in_block));
            if (local_state._build_side_mutable_block.rows() >
                std::numeric_limits<uint32_t>::max()) {
                return Status::NotSupported(
                        "Hash join do not support build table rows"
                        " over:" +
                        std::to_string(std::numeric_limits<uint32_t>::max()));
            }
        }
    }

    if (local_state._should_build_hash_table && source_state == SourceState::FINISHED) {
        if (!local_state._build_side_mutable_block.empty()) {
            local_state._shared_state->build_block = std::make_shared<vectorized::Block>(
                    local_state._build_side_mutable_block.to_block());
            COUNTER_UPDATE(local_state._build_blocks_memory_usage,
                           (*local_state._shared_state->build_block).bytes());
            RETURN_IF_ERROR(local_state.process_build_block(
                    state, (*local_state._shared_state->build_block)));
        }
        auto ret = std::visit(
                Overload {[&](std::monostate&) -> Status {
                              LOG(FATAL) << "FATAL: uninited hash table";
                              __builtin_unreachable();
                          },
                          [&](auto&& arg) -> Status {
                              vectorized::ProcessRuntimeFilterBuild runtime_filter_build_process;
                              return runtime_filter_build_process(state, arg, &local_state);
                          }},
                *local_state._shared_state->hash_table_variants);
        if (!ret.ok()) {
            if (_shared_hashtable_controller) {
                _shared_hash_table_context->status = ret;
                _shared_hashtable_controller->signal(id());
            }
            return ret;
        }
        if (_shared_hashtable_controller) {
            _shared_hash_table_context->status = Status::OK();
            // arena will be shared with other instances.
            _shared_hash_table_context->arena = local_state._shared_state->arena;
            _shared_hash_table_context->hash_table_variants =
                    local_state._shared_state->hash_table_variants;
            _shared_hash_table_context->short_circuit_for_null_in_probe_side =
                    local_state._shared_state->_has_null_in_build_side;
            if (local_state._runtime_filter_slots) {
                local_state._runtime_filter_slots->copy_to_shared_context(
                        _shared_hash_table_context);
            }
            _shared_hashtable_controller->signal(id());
        }
    } else if (!local_state._should_build_hash_table) {
        DCHECK(_shared_hashtable_controller != nullptr);
        DCHECK(_shared_hash_table_context != nullptr);
        auto wait_timer = ADD_TIMER(local_state.profile(), "WaitForSharedHashTableTime");
        SCOPED_TIMER(wait_timer);
        RETURN_IF_ERROR(
                _shared_hashtable_controller->wait_for_signal(state, _shared_hash_table_context));

        local_state.profile()->add_info_string(
                "SharedHashTableFrom",
                print_id(_shared_hashtable_controller->get_builder_fragment_instance_id(id())));
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
                *local_state._shared_state->hash_table_variants,
                *std::static_pointer_cast<vectorized::HashTableVariants>(
                        _shared_hash_table_context->hash_table_variants));

        if (!_shared_hash_table_context->runtime_filters.empty()) {
            auto ret = std::visit(
                    Overload {
                            [&](std::monostate&) -> Status {
                                LOG(FATAL) << "FATAL: uninited hash table";
                                __builtin_unreachable();
                            },
                            [&](auto&& arg) -> Status {
                                if (_runtime_filter_descs.empty()) {
                                    return Status::OK();
                                }
                                local_state._runtime_filter_slots =
                                        std::make_shared<VRuntimeFilterSlots>(
                                                _build_expr_ctxs, _runtime_filter_descs);

                                RETURN_IF_ERROR(local_state._runtime_filter_slots->init(
                                        state, arg.hash_table->size(), 0));
                                RETURN_IF_ERROR(
                                        local_state._runtime_filter_slots->copy_from_shared_context(
                                                _shared_hash_table_context));
                                RETURN_IF_ERROR(local_state._runtime_filter_slots->publish());
                                return Status::OK();
                            }},
                    *local_state._shared_state->hash_table_variants);
            RETURN_IF_ERROR(ret);
        }
    }

    local_state.init_short_circuit_for_probe();
    if (source_state == SourceState::FINISHED) {
        // Since the comparison of null values is meaningless, null aware left anti join should not output null
        // when the build side is not empty.
        if (!local_state._shared_state->build_block &&
            _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            local_state._shared_state->probe_ignore_null = true;
        }
        local_state._dependency->set_ready_for_read();
    }

    return Status::OK();
}

} // namespace doris::pipeline
