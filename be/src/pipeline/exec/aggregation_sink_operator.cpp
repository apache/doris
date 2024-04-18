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

#include "aggregation_sink_operator.h"

#include <string>

#include "pipeline/exec/distinct_streaming_aggregation_sink_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "runtime/primitive_type.h"
#include "vec/common/hash_table/hash.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(AggSinkOperator, StreamingOperator)

/// The minimum reduction factor (input rows divided by output rows) to grow hash tables
/// in a streaming preaggregation, given that the hash tables are currently the given
/// size or above. The sizes roughly correspond to hash table sizes where the bucket
/// arrays will fit in  a cache level. Intuitively, we don't want the working set of the
/// aggregation to expand to the next level of cache unless we're reducing the input
/// enough to outweigh the increased memory latency we'll incur for each hash table
/// lookup.
///
/// Note that the current reduction achieved is not always a good estimate of the
/// final reduction. It may be biased either way depending on the ordering of the
/// input. If the input order is random, we will underestimate the final reduction
/// factor because the probability of a row having the same key as a previous row
/// increases as more input is processed.  If the input order is correlated with the
/// key, skew may bias the estimate. If high cardinality keys appear first, we
/// may overestimate and if low cardinality keys appear first, we underestimate.
/// To estimate the eventual reduction achieved, we estimate the final reduction
/// using the planner's estimated input cardinality and the assumption that input
/// is in a random order. This means that we assume that the reduction factor will
/// increase over time.
AggSinkLocalState::AggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : Base(parent, state),
          _hash_table_compute_timer(nullptr),
          _hash_table_input_counter(nullptr),
          _build_timer(nullptr),
          _expr_timer(nullptr),
          _serialize_key_timer(nullptr),
          _merge_timer(nullptr),
          _serialize_data_timer(nullptr),
          _deserialize_data_timer(nullptr),
          _max_row_size_counter(nullptr) {}

Status AggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _agg_data = Base::_shared_state->agg_data.get();
    _agg_arena_pool = Base::_shared_state->agg_arena_pool.get();
    _hash_table_memory_usage = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "HashTable",
                                                            TUnit::BYTES, "MemoryUsage", 1);
    _serialize_key_arena_memory_usage = Base::profile()->AddHighWaterMarkCounter(
            "SerializeKeyArena", TUnit::BYTES, "MemoryUsage", 1);

    _build_timer = ADD_TIMER(Base::profile(), "BuildTime");
    _serialize_key_timer = ADD_TIMER(Base::profile(), "SerializeKeyTime");
    _exec_timer = ADD_TIMER(Base::profile(), "ExecTime");
    _merge_timer = ADD_TIMER(Base::profile(), "MergeTime");
    _expr_timer = ADD_TIMER(Base::profile(), "ExprTime");
    _serialize_data_timer = ADD_TIMER(Base::profile(), "SerializeDataTime");
    _deserialize_data_timer = ADD_TIMER(Base::profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);
    _max_row_size_counter = ADD_COUNTER(Base::profile(), "MaxRowSizeInBytes", TUnit::UNIT);

    return Status::OK();
}

Status AggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<AggSinkOperatorX>();
    Base::_shared_state->align_aggregate_states = p._align_aggregate_states;
    Base::_shared_state->total_size_of_aggregate_states = p._total_size_of_aggregate_states;
    Base::_shared_state->offsets_of_aggregate_states = p._offsets_of_aggregate_states;
    Base::_shared_state->make_nullable_keys = p._make_nullable_keys;
    for (auto& evaluator : p._aggregate_evaluators) {
        Base::_shared_state->aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    Base::_shared_state->probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < Base::_shared_state->probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                p._probe_expr_ctxs[i]->clone(state, Base::_shared_state->probe_expr_ctxs[i]));
    }
    for (auto& evaluator : Base::_shared_state->aggregate_evaluators) {
        evaluator->set_timer(_merge_timer, _expr_timer);
    }
    Base::_shared_state->agg_profile_arena = std::make_unique<vectorized::Arena>();

    if (Base::_shared_state->probe_expr_ctxs.empty()) {
        _agg_data->without_key = reinterpret_cast<vectorized::AggregateDataPtr>(
                Base::_shared_state->agg_profile_arena->alloc(p._total_size_of_aggregate_states));

        if (p._is_merge) {
            _executor = std::make_unique<Executor<true, true>>();
        } else {
            _executor = std::make_unique<Executor<true, false>>();
        }
    } else {
        _init_hash_method(Base::_shared_state->probe_expr_ctxs);

        std::visit(
                [&](auto&& agg_method) {
                    using HashTableType = std::decay_t<decltype(agg_method)>;
                    using KeyType = typename HashTableType::Key;

                    /// some aggregate functions (like AVG for decimal) have align issues.
                    Base::_shared_state->aggregate_data_container.reset(
                            new vectorized::AggregateDataContainer(
                                    sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                       p._align_aggregate_states - 1) /
                                                      p._align_aggregate_states) *
                                                             p._align_aggregate_states));
                },
                _agg_data->method_variant);
        if (p._is_merge) {
            _executor = std::make_unique<Executor<false, true>>();
        } else {
            _executor = std::make_unique<Executor<false, false>>();
        }

        _should_limit_output = p._limit != -1 &&       // has limit
                               (!p._have_conjuncts) && // no having conjunct
                               p._needs_finalize;      // agg's finalize step
    }
    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (Base::_shared_state->probe_expr_ctxs.empty()) {
        // _create_agg_status may acquire a lot of memory, may allocate failed when memory is very few
        RETURN_IF_CATCH_EXCEPTION(static_cast<void>(_create_agg_status(_agg_data->without_key)));
    }
    Base::_shared_state->ready_to_execute = true;
    return Status::OK();
}

Status AggSinkLocalState::_create_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        try {
            shared_state.aggregate_evaluators[i]->create(
                    data + shared_state.offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                shared_state.aggregate_evaluators[j]->destroy(
                        data + shared_state.offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status AggSinkLocalState::_execute_without_key(vectorized::Block* block) {
    DCHECK(_agg_data->without_key != nullptr);
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(Base::_shared_state->aggregate_evaluators[i]->execute_single_add(
                block,
                _agg_data->without_key + Base::_parent->template cast<AggSinkOperatorX>()
                                                 ._offsets_of_aggregate_states[i],
                _agg_arena_pool));
    }
    return Status::OK();
}

Status AggSinkLocalState::_merge_with_serialized_key(vectorized::Block* block) {
    if (_reach_limit) {
        return _merge_with_serialized_key_helper<true, false>(block);
    } else {
        return _merge_with_serialized_key_helper<false, false>(block);
    }
}

size_t AggSinkLocalState::_memory_usage() const {
    if (0 == _get_hash_table_size()) {
        return 0;
    }
    size_t usage = 0;
    if (_agg_arena_pool) {
        usage += _agg_arena_pool->size();
    }

    if (Base::_shared_state->aggregate_data_container) {
        usage += Base::_shared_state->aggregate_data_container->memory_usage();
    }

    std::visit(
            [&](auto&& agg_method) -> void {
                auto data = agg_method.hash_table;
                usage += data->get_buffer_size_in_bytes();
            },
            _agg_data->method_variant);

    return usage;
}

void AggSinkLocalState::_update_memusage_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = *agg_method.hash_table;
                auto arena_memory_usage =
                        _agg_arena_pool->size() +
                        Base::_shared_state->aggregate_data_container->memory_usage() -
                        Base::_shared_state->mem_usage_record.used_in_arena;
                Base::_mem_tracker->consume(arena_memory_usage);
                Base::_mem_tracker->consume(data.get_buffer_size_in_bytes() -
                                            Base::_shared_state->mem_usage_record.used_in_state);
                _serialize_key_arena_memory_usage->add(arena_memory_usage);
                COUNTER_UPDATE(_hash_table_memory_usage,
                               data.get_buffer_size_in_bytes() -
                                       Base::_shared_state->mem_usage_record.used_in_state);
                Base::_shared_state->mem_usage_record.used_in_state =
                        data.get_buffer_size_in_bytes();
                Base::_shared_state->mem_usage_record.used_in_arena =
                        _agg_arena_pool->size() +
                        Base::_shared_state->aggregate_data_container->memory_usage();
            },
            _agg_data->method_variant);
}

Status AggSinkLocalState::_destroy_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->destroy(
                data + shared_state.offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

template <bool limit, bool for_spill>
Status AggSinkLocalState::_merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);

    size_t key_size = Base::_shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        if constexpr (for_spill) {
            key_columns[i] = block->get_by_position(i).column.get();
        } else {
            int result_column_id = -1;
            RETURN_IF_ERROR(
                    Base::_shared_state->probe_expr_ctxs[i]->execute(block, &result_column_id));
            block->replace_by_position_if_const(result_column_id);
            key_columns[i] = block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if constexpr (limit) {
        _find_in_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            if (Base::_shared_state->aggregate_evaluators[i]->is_merge()) {
                int col_id = AggSharedState::get_slot_column_id(
                        Base::_shared_state->aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() *
                        rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    Base::_shared_state->aggregate_evaluators[i]
                            ->function()
                            ->deserialize_and_merge_vec_selected(
                                    _places.data(),
                                    Base::_parent->template cast<AggSinkOperatorX>()
                                            ._offsets_of_aggregate_states[i],
                                    _deserialize_buffer.data(),
                                    (vectorized::ColumnString*)(column.get()), _agg_arena_pool,
                                    rows);
                }
            } else {
                RETURN_IF_ERROR(
                        Base::_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                                block,
                                Base::_parent->template cast<AggSinkOperatorX>()
                                        ._offsets_of_aggregate_states[i],
                                _places.data(), _agg_arena_pool));
            }
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            if (Base::_shared_state->aggregate_evaluators[i]->is_merge() || for_spill) {
                int col_id = 0;
                if constexpr (for_spill) {
                    col_id = Base::_shared_state->probe_expr_ctxs.size() + i;
                } else {
                    col_id = AggSharedState::get_slot_column_id(
                            Base::_shared_state->aggregate_evaluators[i]);
                }
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() *
                        rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    Base::_shared_state->aggregate_evaluators[i]
                            ->function()
                            ->deserialize_and_merge_vec(
                                    _places.data(),
                                    Base::_parent->template cast<AggSinkOperatorX>()
                                            ._offsets_of_aggregate_states[i],
                                    _deserialize_buffer.data(),
                                    (vectorized::ColumnString*)(column.get()), _agg_arena_pool,
                                    rows);
                }
            } else {
                RETURN_IF_ERROR(Base::_shared_state->aggregate_evaluators[i]->execute_batch_add(
                        block,
                        Base::_parent->template cast<AggSinkOperatorX>()
                                ._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool));
            }
        }

        if (_should_limit_output) {
            _reach_limit = _get_hash_table_size() >=
                           Base::_parent->template cast<AggSinkOperatorX>()._limit;
        }
    }

    return Status::OK();
}

Status AggSinkLocalState::_merge_without_key(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_data->without_key != nullptr);
    for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
        if (Base::_shared_state->aggregate_evaluators[i]->is_merge()) {
            int col_id = AggSharedState::get_slot_column_id(
                    Base::_shared_state->aggregate_evaluators[i]);
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            SCOPED_TIMER(_deserialize_data_timer);
            Base::_shared_state->aggregate_evaluators[i]
                    ->function()
                    ->deserialize_and_merge_from_column(
                            _agg_data->without_key +
                                    Base::_parent->template cast<AggSinkOperatorX>()
                                            ._offsets_of_aggregate_states[i],
                            *column, _agg_arena_pool);
        } else {
            RETURN_IF_ERROR(Base::_shared_state->aggregate_evaluators[i]->execute_single_add(
                    block,
                    _agg_data->without_key + Base::_parent->template cast<AggSinkOperatorX>()
                                                     ._offsets_of_aggregate_states[i],
                    _agg_arena_pool));
        }
    }
    return Status::OK();
}

void AggSinkLocalState::_update_memusage_without_key() {
    auto arena_memory_usage =
            _agg_arena_pool->size() - Base::_shared_state->mem_usage_record.used_in_arena;
    Base::_mem_tracker->consume(arena_memory_usage);
    _serialize_key_arena_memory_usage->add(arena_memory_usage);
    Base::_shared_state->mem_usage_record.used_in_arena = _agg_arena_pool->size();
}

Status AggSinkLocalState::_execute_with_serialized_key(vectorized::Block* block) {
    if (_reach_limit) {
        return _execute_with_serialized_key_helper<true>(block);
    } else {
        return _execute_with_serialized_key_helper<false>(block);
    }
}

template <bool limit>
Status AggSinkLocalState::_execute_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!Base::_shared_state->probe_expr_ctxs.empty());

    size_t key_size = Base::_shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(
                    Base::_shared_state->probe_expr_ctxs[i]->execute(block, &result_column_id));
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if constexpr (limit) {
        _find_in_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(
                    Base::_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                            block,
                            Base::_parent->template cast<AggSinkOperatorX>()
                                    ._offsets_of_aggregate_states[i],
                            _places.data(), _agg_arena_pool));
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(Base::_shared_state->aggregate_evaluators[i]->execute_batch_add(
                    block,
                    Base::_parent->template cast<AggSinkOperatorX>()
                            ._offsets_of_aggregate_states[i],
                    _places.data(), _agg_arena_pool));
        }

        if (_should_limit_output && !Base::_shared_state->enable_spill) {
            _reach_limit = _get_hash_table_size() >=
                           Base::_parent->template cast<AggSinkOperatorX>()._limit;
            if (_reach_limit &&
                Base::_parent->template cast<AggSinkOperatorX>()._can_short_circuit) {
                Base::_dependency->set_ready_to_read();
                return Status::Error<ErrorCode::END_OF_FILE>("");
            }
        }
    }

    return Status::OK();
}

size_t AggSinkLocalState::_get_hash_table_size() const {
    return std::visit([&](auto&& agg_method) { return agg_method.hash_table->size(); },
                      _agg_data->method_variant);
}

void AggSinkLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                                 vectorized::ColumnRawPtrs& key_columns,
                                                 size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);

                auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                    HashMethodType::try_presis_key_and_origin(key, origin, *_agg_arena_pool);
                    auto mapped =
                            Base::_shared_state->aggregate_data_container->append_data(origin);
                    auto st = _create_agg_status(mapped);
                    if (!st) {
                        throw Exception(st.code(), st.to_string());
                    }
                    ctor(key, mapped);
                };

                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = _agg_arena_pool->aligned_alloc(
                            Base::_parent->template cast<AggSinkOperatorX>()
                                    ._total_size_of_aggregate_states,
                            Base::_parent->template cast<AggSinkOperatorX>()
                                    ._align_aggregate_states);
                    auto st = _create_agg_status(mapped);
                    if (!st) {
                        throw Exception(st.code(), st.to_string());
                    }
                };

                SCOPED_TIMER(_hash_table_emplace_timer);
                for (size_t i = 0; i < num_rows; ++i) {
                    places[i] = agg_method.lazy_emplace(state, i, creator, creator_for_null_key);
                }

                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            _agg_data->method_variant);
}

void AggSinkLocalState::_find_in_hash_table(vectorized::AggregateDataPtr* places,
                                            vectorized::ColumnRawPtrs& key_columns,
                                            size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);

                /// For all rows.
                for (size_t i = 0; i < num_rows; ++i) {
                    auto find_result = agg_method.find(state, i);

                    if (find_result.is_found()) {
                        places[i] = find_result.get_mapped();
                    } else {
                        places[i] = nullptr;
                    }
                }
            },
            _agg_data->method_variant);
}

void AggSinkLocalState::_init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs) {
    DCHECK(probe_exprs.size() >= 1);

    using Type = vectorized::AggregatedDataVariants::Type;
    Type t(Type::serialized);

    if (probe_exprs.size() == 1) {
        auto is_nullable = probe_exprs[0]->root()->is_nullable();
        PrimitiveType type = probe_exprs[0]->root()->result_type();
        switch (type) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I: {
            size_t size = get_primitive_type_size(type);
            if (size == 1) {
                t = Type::int8_key;
            } else if (size == 2) {
                t = Type::int16_key;
            } else if (size == 4) {
                t = Type::int32_key;
            } else if (size == 8) {
                t = Type::int64_key;
            } else if (size == 16) {
                t = Type::int128_key;
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "meet invalid type size, size={}, type={}", size,
                                type_to_string(type));
            }
            break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            t = Type::string_key;
            break;
        }
        default:
            t = Type::serialized;
        }

        _agg_data->init(
                get_hash_key_type_with_phase(
                        t, !Base::_parent->template cast<AggSinkOperatorX>()._is_first_phase),
                is_nullable);
    } else {
        if (!try_get_hash_map_context_fixed<PHNormalHashMap, HashCRC32,
                                            vectorized::AggregateDataPtr>(_agg_data->method_variant,
                                                                          probe_exprs)) {
            _agg_data->init(Type::serialized);
        }
    }
}

AggSinkOperatorX::AggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : DataSinkOperatorX<AggSinkLocalState>(operator_id, tnode.node_id),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _pool(pool),
          _limit(tnode.limit),
          _have_conjuncts((tnode.__isset.vconjunct && !tnode.vconjunct.nodes.empty()) ||
                          (tnode.__isset.conjuncts && !tnode.conjuncts.empty())),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {}

Status AggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AggSinkLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit =
            tnode.agg_node.aggregate_functions.empty() && state->enable_pipeline_exec();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        vectorized::AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _external_agg_bytes_threshold = state->external_agg_bytes_threshold();

    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });

    return Status::OK();
}

Status AggSinkOperatorX::prepare(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(vectorized::VExpr::prepare(
            _probe_expr_ctxs, state, DataSinkOperatorX<AggSinkLocalState>::_child_x->row_desc()));

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, DataSinkOperatorX<AggSinkLocalState>::_child_x->row_desc(),
                intermediate_slot_desc, output_slot_desc));
    }

    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;

        const auto& agg_function = _aggregate_evaluators[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _aggregate_evaluators.size()) {
            size_t alignment_of_next_state =
                    _aggregate_evaluators[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }

    return Status::OK();
}

Status AggSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    return Status::OK();
}

Status AggSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    local_state._shared_state->input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._executor->execute(&local_state, in_block));
        local_state._executor->update_memusage(&local_state);
    }
    if (eos) {
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t AggSinkOperatorX::get_revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._memory_usage();
}

Status AggSinkOperatorX::reset_hash_table(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto& ss = *local_state.Base::_shared_state;
    RETURN_IF_ERROR(ss.reset_hash_table());
    local_state._agg_arena_pool = ss.agg_arena_pool.get();
    return Status::OK();
}

Status AggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    _preagg_block.clear();
    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    Base::_mem_tracker->release(Base::_shared_state->mem_usage_record.used_in_state +
                                Base::_shared_state->mem_usage_record.used_in_arena);
    return Base::close(state, exec_status);
}

} // namespace doris::pipeline
