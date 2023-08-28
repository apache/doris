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

#include "pipeline/exec/operator.h"
#include "runtime/primitive_type.h"

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
AggSinkLocalState::AggSinkLocalState(DataSinkOperatorX* parent, RuntimeState* state)
        : PipelineXSinkLocalState(parent, state),
          _hash_table_compute_timer(nullptr),
          _hash_table_input_counter(nullptr),
          _build_timer(nullptr),
          _expr_timer(nullptr),
          _exec_timer(nullptr),
          _serialize_key_timer(nullptr),
          _merge_timer(nullptr),
          _serialize_data_timer(nullptr),
          _deserialize_data_timer(nullptr),
          _max_row_size_counter(nullptr) {}

Status AggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::init(state, info));
    _dependency = (AggDependency*)info.dependency;
    _shared_state = (AggSharedState*)_dependency->shared_state();
    _agg_data = _shared_state->agg_data.get();
    _agg_arena_pool = _shared_state->agg_arena_pool.get();
    auto& p = _parent->cast<AggSinkOperatorX>();
    _dependency->set_align_aggregate_states(p._align_aggregate_states);
    _dependency->set_total_size_of_aggregate_states(p._total_size_of_aggregate_states);
    _dependency->set_offsets_of_aggregate_states(p._offsets_of_aggregate_states);
    _dependency->set_make_nullable_keys(p._make_nullable_keys);
    _shared_state->init_spill_partition_helper(p._spill_partition_count_bits);
    for (auto& evaluator : p._aggregate_evaluators) {
        _shared_state->aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
        _shared_state->aggregate_evaluators.back()->set_timer(_exec_timer, _merge_timer,
                                                              _expr_timer);
    }
    _shared_state->probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _shared_state->probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _shared_state->probe_expr_ctxs[i]));
    }
    _memory_usage_counter = ADD_LABEL_COUNTER(profile(), "MemoryUsage");
    _hash_table_memory_usage =
            ADD_CHILD_COUNTER(profile(), "HashTable", TUnit::BYTES, "MemoryUsage");
    _serialize_key_arena_memory_usage =
            profile()->AddHighWaterMarkCounter("SerializeKeyArena", TUnit::BYTES, "MemoryUsage");

    _build_timer = ADD_TIMER(profile(), "BuildTime");
    _build_table_convert_timer = ADD_TIMER(profile(), "BuildConvertToPartitionedTime");
    _serialize_key_timer = ADD_TIMER(profile(), "SerializeKeyTime");
    _exec_timer = ADD_TIMER(profile(), "ExecTime");
    _merge_timer = ADD_TIMER(profile(), "MergeTime");
    _expr_timer = ADD_TIMER(profile(), "ExprTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");
    _deserialize_data_timer = ADD_TIMER(profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(profile(), "HashTableInputCount", TUnit::UNIT);
    _max_row_size_counter = ADD_COUNTER(profile(), "MaxRowSizeInBytes", TUnit::UNIT);
    COUNTER_SET(_max_row_size_counter, (int64_t)0);

    _shared_state->agg_profile_arena = std::make_unique<vectorized::Arena>();

    if (_shared_state->probe_expr_ctxs.empty()) {
        _agg_data->init(vectorized::AggregatedDataVariants::Type::without_key);

        _agg_data->without_key = reinterpret_cast<vectorized::AggregateDataPtr>(
                _shared_state->agg_profile_arena->alloc(p._total_size_of_aggregate_states));

        if (p._is_merge) {
            _executor.execute = std::bind<Status>(&AggSinkLocalState::_merge_without_key, this,
                                                  std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggSinkLocalState::_execute_without_key, this,
                                                  std::placeholders::_1);
        }

        _executor.update_memusage =
                std::bind<void>(&AggSinkLocalState::_update_memusage_without_key, this);
    } else {
        _init_hash_method(_shared_state->probe_expr_ctxs);

        std::visit(
                [&](auto&& agg_method) {
                    using HashTableType = std::decay_t<decltype(agg_method.data)>;
                    using KeyType = typename HashTableType::key_type;

                    /// some aggregate functions (like AVG for decimal) have align issues.
                    _shared_state->aggregate_data_container.reset(
                            new vectorized::AggregateDataContainer(
                                    sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                       p._align_aggregate_states - 1) /
                                                      p._align_aggregate_states) *
                                                             p._align_aggregate_states));
                },
                _agg_data->method_variant);
        if (p._is_merge) {
            _executor.execute = std::bind<Status>(&AggSinkLocalState::_merge_with_serialized_key,
                                                  this, std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggSinkLocalState::_execute_with_serialized_key,
                                                  this, std::placeholders::_1);
        }

        _executor.update_memusage =
                std::bind<void>(&AggSinkLocalState::_update_memusage_with_serialized_key, this);

        _should_limit_output = p._limit != -1 &&       // has limit
                               (!p._have_conjuncts) && // no having conjunct
                               p._needs_finalize;      // agg's finalize step
    }
    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (_shared_state->probe_expr_ctxs.empty()) {
        // _create_agg_status may acquire a lot of memory, may allocate failed when memory is very few
        RETURN_IF_CATCH_EXCEPTION(_dependency->create_agg_status(_agg_data->without_key));
    }
    return Status::OK();
}

Status AggSinkLocalState::_execute_without_key(vectorized::Block* block) {
    DCHECK(_agg_data->without_key != nullptr);
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_single_add(
                block,
                _agg_data->without_key +
                        _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
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
    size_t usage = 0;
    std::visit(
            [&](auto&& agg_method) {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                if constexpr (vectorized::ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      HashMethodType>::value) {
                    usage += agg_method.keys_memory_usage;
                }
                usage += agg_method.data.get_buffer_size_in_bytes();
            },
            _agg_data->method_variant);

    if (_agg_arena_pool) {
        usage += _agg_arena_pool->size();
    }

    if (_shared_state->aggregate_data_container) {
        usage += _shared_state->aggregate_data_container->memory_usage();
    }

    return usage;
}

void AggSinkLocalState::_update_memusage_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                auto arena_memory_usage = _agg_arena_pool->size() +
                                          _shared_state->aggregate_data_container->memory_usage() -
                                          _dependency->mem_usage_record().used_in_arena;
                _dependency->mem_tracker()->consume(arena_memory_usage);
                _dependency->mem_tracker()->consume(data.get_buffer_size_in_bytes() -
                                                    _dependency->mem_usage_record().used_in_state);
                _serialize_key_arena_memory_usage->add(arena_memory_usage);
                COUNTER_UPDATE(_hash_table_memory_usage,
                               data.get_buffer_size_in_bytes() -
                                       _dependency->mem_usage_record().used_in_state);
                _dependency->mem_usage_record().used_in_state = data.get_buffer_size_in_bytes();
                _dependency->mem_usage_record().used_in_arena =
                        _agg_arena_pool->size() +
                        _shared_state->aggregate_data_container->memory_usage();
            },
            _agg_data->method_variant);
}

template <bool limit, bool for_spill>
Status AggSinkLocalState::_merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);

    size_t key_size = _shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        if constexpr (for_spill) {
            key_columns[i] = block->get_by_position(i).column.get();
        } else {
            int result_column_id = -1;
            RETURN_IF_ERROR(_shared_state->probe_expr_ctxs[i]->execute(block, &result_column_id));
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

        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
            if (_shared_state->aggregate_evaluators[i]->is_merge()) {
                int col_id = _get_slot_column_id(_shared_state->aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        _shared_state->aggregate_evaluators[i]->function()->size_of_data() * rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    _shared_state->aggregate_evaluators[i]
                            ->function()
                            ->deserialize_and_merge_vec_selected(
                                    _places.data(),
                                    _parent->cast<AggSinkOperatorX>()
                                            ._offsets_of_aggregate_states[i],
                                    _deserialize_buffer.data(),
                                    (vectorized::ColumnString*)(column.get()), _agg_arena_pool,
                                    rows);
                }
            } else {
                RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                        block, _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool));
            }
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
            if (_shared_state->aggregate_evaluators[i]->is_merge() || for_spill) {
                int col_id;
                if constexpr (for_spill) {
                    col_id = _shared_state->probe_expr_ctxs.size() + i;
                } else {
                    col_id = _get_slot_column_id(_shared_state->aggregate_evaluators[i]);
                }
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        _shared_state->aggregate_evaluators[i]->function()->size_of_data() * rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    _shared_state->aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                            _places.data(),
                            _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                            _deserialize_buffer.data(), (vectorized::ColumnString*)(column.get()),
                            _agg_arena_pool, rows);
                }
            } else {
                RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_batch_add(
                        block, _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool));
            }
        }

        if (_should_limit_output) {
            _reach_limit = _get_hash_table_size() >= _parent->cast<AggSinkOperatorX>()._limit;
        }
    }

    return Status::OK();
}

// We should call this function only at 1st phase.
// 1st phase: is_merge=true, only have one SlotRef.
// 2nd phase: is_merge=false, maybe have multiple exprs.
int AggSinkLocalState::_get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
    auto ctxs = evaluator->input_exprs_ctxs();
    CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
            << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
            << ctxs[0]->root()->debug_string();
    return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
}

Status AggSinkLocalState::_merge_without_key(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_data->without_key != nullptr);
    for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
        if (_shared_state->aggregate_evaluators[i]->is_merge()) {
            int col_id = _get_slot_column_id(_shared_state->aggregate_evaluators[i]);
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            SCOPED_TIMER(_deserialize_data_timer);
            _shared_state->aggregate_evaluators[i]->function()->deserialize_and_merge_from_column(
                    _agg_data->without_key +
                            _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                    *column, _agg_arena_pool);
        } else {
            RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_single_add(
                    block,
                    _agg_data->without_key +
                            _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                    _agg_arena_pool));
        }
    }
    return Status::OK();
}

void AggSinkLocalState::_update_memusage_without_key() {
    auto arena_memory_usage =
            _agg_arena_pool->size() - _dependency->mem_usage_record().used_in_arena;
    _dependency->mem_tracker()->consume(arena_memory_usage);
    _serialize_key_arena_memory_usage->add(arena_memory_usage);
    _dependency->mem_usage_record().used_in_arena = _agg_arena_pool->size();
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
    DCHECK(!_shared_state->probe_expr_ctxs.empty());

    size_t key_size = _shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_shared_state->probe_expr_ctxs[i]->execute(block, &result_column_id));
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

        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                    block, _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                    _places.data(), _agg_arena_pool));
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_batch_add(
                    block, _parent->cast<AggSinkOperatorX>()._offsets_of_aggregate_states[i],
                    _places.data(), _agg_arena_pool));
        }

        if (_should_limit_output) {
            _reach_limit = _get_hash_table_size() >= _parent->cast<AggSinkOperatorX>()._limit;
            if (_reach_limit && _parent->cast<AggSinkOperatorX>()._can_short_circuit) {
                _dependency->set_done();
                return Status::Error<ErrorCode::END_OF_FILE>("");
            }
        }
    }

    return Status::OK();
}

size_t AggSinkLocalState::_get_hash_table_size() {
    return std::visit([&](auto&& agg_method) { return agg_method.data.size(); },
                      _agg_data->method_variant);
}

void AggSinkLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                                 vectorized::ColumnRawPtrs& key_columns,
                                                 const size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _shared_state->probe_key_sz, nullptr);

                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                auto creator = [this](const auto& ctor, const auto& key) {
                    using KeyType = std::decay_t<decltype(key)>;
                    if constexpr (HashTableTraits<HashTableType>::is_string_hash_table &&
                                  !std::is_same_v<StringRef, KeyType>) {
                        StringRef string_ref = to_string_ref(key);
                        vectorized::ArenaKeyHolder key_holder {string_ref, *_agg_arena_pool};
                        key_holder_persist_key(key_holder);
                        auto mapped = _shared_state->aggregate_data_container->append_data(
                                key_holder.key);
                        _dependency->create_agg_status(mapped);
                        ctor(key, mapped);
                    } else {
                        auto mapped = _shared_state->aggregate_data_container->append_data(key);
                        _dependency->create_agg_status(mapped);
                        ctor(key, mapped);
                    }
                };

                auto creator_for_null_key = [this](auto& mapped) {
                    mapped = _agg_arena_pool->aligned_alloc(
                            _parent->cast<AggSinkOperatorX>()._total_size_of_aggregate_states,
                            _parent->cast<AggSinkOperatorX>()._align_aggregate_states);
                    _dependency->create_agg_status(mapped);
                };

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    auto keys = state.get_keys(num_rows);
                    if (_hash_values.size() < num_rows) {
                        _hash_values.resize(num_rows);
                    }
                    for (size_t i = 0; i < num_rows; ++i) {
                        _hash_values[i] = agg_method.data.hash(keys[i]);
                    }

                    SCOPED_TIMER(_hash_table_emplace_timer);
                    if constexpr (vectorized::ColumnsHashing::IsSingleNullableColumnMethod<
                                          AggState>::value) {
                        for (size_t i = 0; i < num_rows; ++i) {
                            if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                            }

                            places[i] = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                               _hash_values[i], creator,
                                                               creator_for_null_key);
                        }
                    } else {
                        state.lazy_emplace_keys(agg_method.data, keys, _hash_values, creator,
                                                places);
                    }
                } else {
                    SCOPED_TIMER(_hash_table_emplace_timer);
                    for (size_t i = 0; i < num_rows; ++i) {
                        vectorized::AggregateDataPtr mapped = nullptr;
                        if constexpr (vectorized::ColumnsHashing::IsSingleNullableColumnMethod<
                                              AggState>::value) {
                            mapped = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                            creator, creator_for_null_key);
                        } else {
                            mapped = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                            creator);
                        }
                        places[i] = mapped;
                    }
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
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _shared_state->probe_key_sz, nullptr);

                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    if (_hash_values.size() < num_rows) _hash_values.resize(num_rows);
                    if constexpr (vectorized::ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                          AggState>::value) {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] = agg_method.data.hash(agg_method.keys[i]);
                        }
                    } else {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] =
                                    agg_method.data.hash(state.get_key_holder(i, *_agg_arena_pool));
                        }
                    }
                }

                /// For all rows.
                for (size_t i = 0; i < num_rows; ++i) {
                    auto find_result = [&]() {
                        if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                            if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                            }

                            return state.find_key_with_hash(agg_method.data, _hash_values[i], i,
                                                            *_agg_arena_pool);
                        } else {
                            return state.find_key(agg_method.data, i, *_agg_arena_pool);
                        }
                    }();

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
                get_hash_key_type_with_phase(t, !_parent->cast<AggSinkOperatorX>()._is_first_phase),
                is_nullable);
    } else {
        bool use_fixed_key = true;
        bool has_null = false;
        size_t key_byte_size = 0;
        size_t bitmap_size = vectorized::get_bitmap_size(_shared_state->probe_expr_ctxs.size());

        _shared_state->probe_key_sz.resize(_shared_state->probe_expr_ctxs.size());
        for (int i = 0; i < _shared_state->probe_expr_ctxs.size(); ++i) {
            const auto& expr = _shared_state->probe_expr_ctxs[i]->root();
            const auto& data_type = expr->data_type();

            if (!data_type->have_maximum_size_of_value()) {
                use_fixed_key = false;
                break;
            }

            auto is_null = data_type->is_nullable();
            has_null |= is_null;
            _shared_state->probe_key_sz[i] =
                    data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
            key_byte_size += _shared_state->probe_key_sz[i];
        }

        if (!has_null) {
            bitmap_size = 0;
        }

        if (bitmap_size + key_byte_size > sizeof(vectorized::UInt256)) {
            use_fixed_key = false;
        }

        if (use_fixed_key) {
            if (bitmap_size + key_byte_size <= sizeof(vectorized::UInt64)) {
                t = Type::int64_keys;
            } else if (bitmap_size + key_byte_size <= sizeof(vectorized::UInt128)) {
                t = Type::int128_keys;
            } else if (bitmap_size + key_byte_size <= sizeof(vectorized::UInt136)) {
                t = Type::int136_keys;
            } else {
                t = Type::int256_keys;
            }
            _agg_data->init(get_hash_key_type_with_phase(
                                    t, !_parent->cast<AggSinkOperatorX>()._is_first_phase),
                            has_null);
        } else {
            _agg_data->init(Type::serialized);
        }
    }
}

Status AggSinkLocalState::try_spill_disk(bool eos) {
    if (_parent->cast<AggSinkOperatorX>()._external_agg_bytes_threshold == 0) {
        return Status::OK();
    }
    return std::visit(
            [&](auto&& agg_method) -> Status {
                auto& hash_table = agg_method.data;
                if (!eos &&
                    _memory_usage() <
                            _parent->cast<AggSinkOperatorX>()._external_agg_bytes_threshold) {
                    return Status::OK();
                }

                if (_get_hash_table_size() == 0) {
                    return Status::OK();
                }

                RETURN_IF_ERROR(_spill_hash_table(agg_method, hash_table));
                return _dependency->reset_hash_table();
            },
            _agg_data->method_variant);
}

AggSinkOperatorX::AggSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : DataSinkOperatorX(tnode.node_id),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _pool(pool),
          _limit(tnode.limit),
          _have_conjuncts(tnode.__isset.vconjunct && !tnode.vconjunct.nodes.empty()) {
    _is_first_phase = tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase;
    _name = "AggSinkOperatorX";
}

Status AggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
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

    if (_external_agg_bytes_threshold > 0) {
        _spill_partition_count_bits = 4;
        if (state->query_options().__isset.external_agg_partition_bits) {
            _spill_partition_count_bits = state->query_options().external_agg_partition_bits;
        }
    }

    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });

    return Status::OK();
}

Status AggSinkOperatorX::prepare(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child_x->row_desc()));

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
                state, _child_x->row_desc(), intermediate_slot_desc, output_slot_desc));
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

    fmt::memory_buffer msg;
    fmt::format_to(msg,
                   "(_is_merge: {}, _needs_finalize: {},  agg size: "
                   "{}, limit: {})",
                   _is_merge ? "true" : "false", _needs_finalize ? "true" : "false",
                   std::to_string(_aggregate_evaluators.size()), std::to_string(_limit));
    std::string title = fmt::format("Aggregation Sink {}", fmt::to_string(msg));
    _profile = _pool->add(new RuntimeProfile(title));
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

Status AggSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                              SourceState source_state) {
    auto& local_state = state->get_sink_local_state(id())->cast<AggSinkLocalState>();
    local_state._shared_state->input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._executor.execute(in_block));
        RETURN_IF_ERROR(local_state.try_spill_disk());
        local_state._executor.update_memusage();
    }
    if (source_state == SourceState::FINISHED) {
        if (local_state._shared_state->spill_context.has_data) {
            local_state.try_spill_disk(true);
            RETURN_IF_ERROR(local_state._shared_state->spill_context.prepare_for_reading());
        }
        local_state._dependency->set_done();
    }
    return Status::OK();
}

Status AggSinkOperatorX::setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) {
    auto local_state = AggSinkLocalState::create_shared(this, state);
    state->emplace_sink_local_state(id(), local_state);
    return local_state->init(state, info);
}

Status AggSinkLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    _preagg_block.clear();
    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);

    std::vector<size_t> tmp_hash_values;
    _hash_values.swap(tmp_hash_values);
    return PipelineXSinkLocalState::close(state);
}

} // namespace doris::pipeline