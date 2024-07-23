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

#include <memory>
#include <string>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "runtime/primitive_type.h"
#include "vec/common/hash_table/hash.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

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
        : Base(parent, state) {}

Status AggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _agg_data = Base::_shared_state->agg_data.get();
    _agg_arena_pool = Base::_shared_state->agg_arena_pool.get();
    _hash_table_size_counter = ADD_COUNTER(profile(), "HashTableSize", TUnit::UNIT);
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
    _hash_table_limit_compute_timer = ADD_TIMER(Base::profile(), "DoLimitComputeTime");
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
    Base::_shared_state->probe_expr_ctxs.resize(p._probe_expr_ctxs.size());

    Base::_shared_state->limit = p._limit;
    Base::_shared_state->do_sort_limit = p._do_sort_limit;
    Base::_shared_state->null_directions = p._null_directions;
    Base::_shared_state->order_directions = p._order_directions;
    for (size_t i = 0; i < Base::_shared_state->probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                p._probe_expr_ctxs[i]->clone(state, Base::_shared_state->probe_expr_ctxs[i]));
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
        RETURN_IF_ERROR(_init_hash_method(Base::_shared_state->probe_expr_ctxs));

        std::visit(vectorized::Overload {[&](std::monostate& arg) {
                                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                                    "uninited hash table");
                                         },
                                         [&](auto& agg_method) {
                                             using HashTableType =
                                                     std::decay_t<decltype(agg_method)>;
                                             using KeyType = typename HashTableType::Key;

                                             /// some aggregate functions (like AVG for decimal) have align issues.
                                             Base::_shared_state->aggregate_data_container =
                                                     std::make_unique<AggregateDataContainer>(
                                                             sizeof(KeyType),
                                                             ((p._total_size_of_aggregate_states +
                                                               p._align_aggregate_states - 1) /
                                                              p._align_aggregate_states) *
                                                                     p._align_aggregate_states);
                                         }},
                   _agg_data->method_variant);
        if (p._is_merge) {
            _executor = std::make_unique<Executor<false, true>>();
        } else {
            _executor = std::make_unique<Executor<false, false>>();
        }

        _should_limit_output = p._limit != -1 &&       // has limit
                               (!p._have_conjuncts) && // no having conjunct
                               !Base::_shared_state->enable_spill;
    }
    for (auto& evaluator : p._aggregate_evaluators) {
        Base::_shared_state->aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    for (auto& evaluator : Base::_shared_state->aggregate_evaluators) {
        evaluator->set_timer(_merge_timer, _expr_timer);
    }
    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (Base::_shared_state->probe_expr_ctxs.empty()) {
        // _create_agg_status may acquire a lot of memory, may allocate failed when memory is very few
        RETURN_IF_ERROR(_create_agg_status(_agg_data->without_key));
        _shared_state->agg_data_created_without_key = true;
    }
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
    if (_shared_state->reach_limit) {
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

    std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                         throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                                "uninited hash table");
                                     },
                                     [&](auto& agg_method) -> void {
                                         auto data = agg_method.hash_table;
                                         usage += data->get_buffer_size_in_bytes();
                                     }},
               _agg_data->method_variant);

    return usage;
}

void AggSinkLocalState::_update_memusage_with_serialized_key() {
    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) -> void {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto& agg_method) -> void {
                           auto& data = *agg_method.hash_table;
                           auto arena_memory_usage =
                                   _agg_arena_pool->size() +
                                   Base::_shared_state->aggregate_data_container->memory_usage() -
                                   Base::_shared_state->mem_usage_record.used_in_arena;
                           Base::_mem_tracker->consume(arena_memory_usage);
                           Base::_mem_tracker->consume(
                                   data.get_buffer_size_in_bytes() -
                                   Base::_shared_state->mem_usage_record.used_in_state);
                           _serialize_key_arena_memory_usage->add(arena_memory_usage);
                           COUNTER_UPDATE(
                                   _hash_table_memory_usage,
                                   data.get_buffer_size_in_bytes() -
                                           Base::_shared_state->mem_usage_record.used_in_state);
                           Base::_shared_state->mem_usage_record.used_in_state =
                                   data.get_buffer_size_in_bytes();
                           Base::_shared_state->mem_usage_record.used_in_arena =
                                   _agg_arena_pool->size() +
                                   Base::_shared_state->aggregate_data_container->memory_usage();
                       }},
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
    std::vector<int> key_locs(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        if constexpr (for_spill) {
            key_columns[i] = block->get_by_position(i).column.get();
            key_locs[i] = i;
        } else {
            int& result_column_id = key_locs[i];
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

    if (limit && !_shared_state->do_sort_limit) {
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
                                    _deserialize_buffer.data(), column.get(), _agg_arena_pool,
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
        bool need_do_agg = true;
        if (limit) {
            need_do_agg = _emplace_into_hash_table_limit(_places.data(), block, key_locs,
                                                         key_columns, rows);
            rows = block->rows();
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);
        }

        if (need_do_agg) {
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
                        column = ((vectorized::ColumnNullable*)column.get())
                                         ->get_nested_column_ptr();
                    }

                    size_t buffer_size = Base::_shared_state->aggregate_evaluators[i]
                                                 ->function()
                                                 ->size_of_data() *
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
                                        _deserialize_buffer.data(), column.get(), _agg_arena_pool,
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
        }

        if (!limit && _should_limit_output) {
            const size_t hash_table_size = _get_hash_table_size();
            _shared_state->reach_limit =
                    hash_table_size >= Base::_parent->template cast<AggSinkOperatorX>()._limit;
            if (_shared_state->do_sort_limit && _shared_state->reach_limit) {
                _shared_state->build_limit_heap(hash_table_size);
            }
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
    if (_shared_state->reach_limit) {
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
    std::vector<int> key_locs(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int& result_column_id = key_locs[i];
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

    if (limit && !_shared_state->do_sort_limit) {
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
        auto do_aggregate_evaluators = [&] {
            for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(Base::_shared_state->aggregate_evaluators[i]->execute_batch_add(
                        block,
                        Base::_parent->template cast<AggSinkOperatorX>()
                                ._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool));
            }
            return Status::OK();
        };

        if constexpr (limit) {
            if (_emplace_into_hash_table_limit(_places.data(), block, key_locs, key_columns,
                                               rows)) {
                RETURN_IF_ERROR(do_aggregate_evaluators());
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);
            RETURN_IF_ERROR(do_aggregate_evaluators());

            if (_should_limit_output && !Base::_shared_state->enable_spill) {
                const size_t hash_table_size = _get_hash_table_size();

                _shared_state->reach_limit =
                        hash_table_size >=
                        (_shared_state->do_sort_limit
                                 ? Base::_parent->template cast<AggSinkOperatorX>()._limit *
                                           config::topn_agg_limit_multiplier
                                 : Base::_parent->template cast<AggSinkOperatorX>()._limit);
                if (_shared_state->reach_limit && _shared_state->do_sort_limit) {
                    _shared_state->build_limit_heap(hash_table_size);
                }
            }
        }
    }
    return Status::OK();
}

size_t AggSinkLocalState::_get_hash_table_size() const {
    return std::visit(
            vectorized::Overload {[&](std::monostate& arg) -> size_t { return 0; },
                                  [&](auto& agg_method) { return agg_method.hash_table->size(); }},
            _agg_data->method_variant);
}

void AggSinkLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                                 vectorized::ColumnRawPtrs& key_columns,
                                                 size_t num_rows) {
    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) -> void {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto& agg_method) -> void {
                           SCOPED_TIMER(_hash_table_compute_timer);
                           using HashMethodType = std::decay_t<decltype(agg_method)>;
                           using AggState = typename HashMethodType::State;
                           AggState state(key_columns);
                           agg_method.init_serialized_keys(key_columns, num_rows);

                           auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                               HashMethodType::try_presis_key_and_origin(key, origin,
                                                                         *_agg_arena_pool);
                               auto mapped =
                                       Base::_shared_state->aggregate_data_container->append_data(
                                               origin);
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
                               places[i] = agg_method.lazy_emplace(state, i, creator,
                                                                   creator_for_null_key);
                           }

                           COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                       }},
               _agg_data->method_variant);
}

bool AggSinkLocalState::_emplace_into_hash_table_limit(vectorized::AggregateDataPtr* places,
                                                       vectorized::Block* block,
                                                       const std::vector<int>& key_locs,
                                                       vectorized::ColumnRawPtrs& key_columns,
                                                       size_t num_rows) {
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                        return true;
                    },
                    [&](auto&& agg_method) -> bool {
                        SCOPED_TIMER(_hash_table_compute_timer);
                        using HashMethodType = std::decay_t<decltype(agg_method)>;
                        using AggState = typename HashMethodType::State;

                        bool need_filter = false;
                        {
                            SCOPED_TIMER(_hash_table_limit_compute_timer);
                            need_filter =
                                    _shared_state->do_limit_filter(block, num_rows, &key_locs);
                        }

                        auto& need_computes = _shared_state->need_computes;
                        if (auto need_agg =
                                    std::find(need_computes.begin(), need_computes.end(), 1);
                            need_agg != need_computes.end()) {
                            if (need_filter) {
                                vectorized::Block::filter_block_internal(block, need_computes);
                                for (int i = 0; i < key_locs.size(); ++i) {
                                    key_columns[i] =
                                            block->get_by_position(key_locs[i]).column.get();
                                }
                                num_rows = block->rows();
                            }

                            AggState state(key_columns);
                            agg_method.init_serialized_keys(key_columns, num_rows);
                            size_t i = 0;

                            auto refresh_top_limit = [&, this]() {
                                _shared_state->limit_heap.pop();
                                for (int j = 0; j < key_columns.size(); ++j) {
                                    _shared_state->limit_columns[j]->insert_from(*key_columns[j],
                                                                                 i);
                                }
                                _shared_state->limit_heap.emplace(
                                        _shared_state->limit_columns[0]->size() - 1,
                                        _shared_state->limit_columns,
                                        _shared_state->order_directions,
                                        _shared_state->null_directions);
                                _shared_state->limit_columns_min =
                                        _shared_state->limit_heap.top()._row_id;
                            };

                            auto creator = [this, refresh_top_limit](const auto& ctor, auto& key,
                                                                     auto& origin) {
                                try {
                                    HashMethodType::try_presis_key_and_origin(key, origin,
                                                                              *_agg_arena_pool);
                                    auto mapped =
                                            _shared_state->aggregate_data_container->append_data(
                                                    origin);
                                    auto st = _create_agg_status(mapped);
                                    if (!st) {
                                        throw Exception(st.code(), st.to_string());
                                    }
                                    ctor(key, mapped);
                                    refresh_top_limit();
                                } catch (...) {
                                    // Exception-safety - if it can not allocate memory or create status,
                                    // the destructors will not be called.
                                    ctor(key, nullptr);
                                    throw;
                                }
                            };

                            auto creator_for_null_key = [this, refresh_top_limit](auto& mapped) {
                                mapped = _agg_arena_pool->aligned_alloc(
                                        Base::_parent->template cast<AggSinkOperatorX>()
                                                ._total_size_of_aggregate_states,
                                        Base::_parent->template cast<AggSinkOperatorX>()
                                                ._align_aggregate_states);
                                auto st = _create_agg_status(mapped);
                                if (!st) {
                                    throw Exception(st.code(), st.to_string());
                                }
                                refresh_top_limit();
                            };

                            SCOPED_TIMER(_hash_table_emplace_timer);
                            for (i = 0; i < num_rows; ++i) {
                                places[i] = agg_method.lazy_emplace(state, i, creator,
                                                                    creator_for_null_key);
                            }
                            COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                            return true;
                        }
                        return false;
                    }},
            _agg_data->method_variant);
}

void AggSinkLocalState::_find_in_hash_table(vectorized::AggregateDataPtr* places,
                                            vectorized::ColumnRawPtrs& key_columns,
                                            size_t num_rows) {
    std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                         throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                                "uninited hash table");
                                     },
                                     [&](auto& agg_method) -> void {
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
                                     }},
               _agg_data->method_variant);
}

Status AggSinkLocalState::_init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(
            init_agg_hash_method(_agg_data, probe_exprs,
                                 Base::_parent->template cast<AggSinkOperatorX>()._is_first_phase));
    return Status::OK();
}

AggSinkOperatorX::AggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                   const DescriptorTbl& descs, bool require_bucket_distribution)
        : DataSinkOperatorX<AggSinkLocalState>(operator_id, tnode.node_id),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _pool(pool),
          _limit(tnode.limit),
          _have_conjuncts((tnode.__isset.vconjunct && !tnode.vconjunct.nodes.empty()) ||
                          (tnode.__isset.conjuncts && !tnode.conjuncts.empty())),
          _partition_exprs(tnode.__isset.distribute_expr_lists && require_bucket_distribution
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.agg_node.grouping_exprs),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution),
          _agg_fn_output_row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples) {}

Status AggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AggSinkLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());

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

    if (tnode.agg_node.__isset.agg_sort_info_by_group_key) {
        _do_sort_limit = true;
        const auto& agg_sort_info = tnode.agg_node.agg_sort_info_by_group_key;
        DCHECK_EQ(agg_sort_info.nulls_first.size(), agg_sort_info.is_asc_order.size());

        const int order_by_key_size = agg_sort_info.is_asc_order.size();
        _order_directions.resize(order_by_key_size);
        _null_directions.resize(order_by_key_size);
        for (int i = 0; i < order_by_key_size; ++i) {
            _order_directions[i] = agg_sort_info.is_asc_order[i] ? 1 : -1;
            _null_directions[i] =
                    agg_sort_info.nulls_first[i] ? -_order_directions[i] : _order_directions[i];
        }
    }

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
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
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
    // check output type
    if (_needs_finalize) {
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::check_agg_fn_output(
                _probe_expr_ctxs.size(), _aggregate_evaluators, _agg_fn_output_row_descriptor));
    }
    return Status::OK();
}

Status AggSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));

    for (auto& _aggregate_evaluator : _aggregate_evaluators) {
        RETURN_IF_ERROR(_aggregate_evaluator->open(state));
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
        COUNTER_SET(local_state._hash_table_size_counter,
                    (int64_t)local_state._get_hash_table_size());
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
