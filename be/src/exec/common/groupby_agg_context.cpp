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

#include "exec/common/groupby_agg_context.h"

#include <gen_cpp/Metrics_types.h>

#include "common/config.h"
#include "common/exception.h"
#include "exec/common/agg_context_utils.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exec/operator/streaming_agg_min_reduction.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

GroupByAggContext::GroupByAggContext(std::vector<AggFnEvaluator*> agg_evaluators,
                                     VExprContextSPtrs groupby_expr_ctxs, Sizes agg_state_offsets,
                                     size_t total_agg_state_size, size_t agg_state_alignment,
                                     bool is_first_phase)
        : AggContext(std::move(agg_evaluators), std::move(agg_state_offsets),
                     total_agg_state_size, agg_state_alignment),
          _hash_table_data(std::make_unique<AggregatedDataVariants>()),
          _groupby_expr_ctxs(std::move(groupby_expr_ctxs)),
          _is_first_phase(is_first_phase) {}

GroupByAggContext::~GroupByAggContext() = default;

// ==================== Profile initialization ====================

void GroupByAggContext::init_sink_profile(RuntimeProfile* profile) {
    _hash_table_compute_timer = ADD_TIMER(profile, "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(profile, "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(profile, "HashTableInputCount", TUnit::UNIT);
    _hash_table_limit_compute_timer = ADD_TIMER(profile, "DoLimitComputeTime");
    _build_timer = ADD_TIMER(profile, "BuildTime");
    _merge_timer = ADD_TIMER(profile, "MergeTime");
    _expr_timer = ADD_TIMER(profile, "ExprTime");
    _deserialize_data_timer = ADD_TIMER(profile, "DeserializeAndMergeTime");
    _hash_table_size_counter = ADD_COUNTER(profile, "HashTableSize", TUnit::UNIT);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile, "MemoryUsageHashTable", TUnit::BYTES, 1);
    _serialize_key_arena_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile, "MemoryUsageSerializeKeyArena", TUnit::BYTES, 1);
    _memory_usage_container = ADD_COUNTER(profile, "MemoryUsageContainer", TUnit::BYTES);
    _memory_usage_arena = ADD_COUNTER(profile, "MemoryUsageArena", TUnit::BYTES);
    _memory_used_counter = profile->get_counter("MemoryUsage");
}

void GroupByAggContext::init_source_profile(RuntimeProfile* profile) {
    _get_results_timer = ADD_TIMER(profile, "GetResultsTime");
    _hash_table_iterate_timer = ADD_TIMER(profile, "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile, "InsertKeysToColumnTime");
    _insert_values_to_column_timer = ADD_TIMER(profile, "InsertValuesToColumnTime");

    // Register overlapping counters on source profile (same names as sink, for
    // PartitionedAggLocalState::_update_profile to read from inner source profile).
    _source_merge_timer = ADD_TIMER(profile, "MergeTime");
    _source_deserialize_data_timer = ADD_TIMER(profile, "DeserializeAndMergeTime");
    _source_hash_table_compute_timer = ADD_TIMER(profile, "HashTableComputeTime");
    _source_hash_table_emplace_timer = ADD_TIMER(profile, "HashTableEmplaceTime");
    _source_hash_table_input_counter = ADD_COUNTER(profile, "HashTableInputCount", TUnit::UNIT);
    _source_hash_table_size_counter = ADD_COUNTER(profile, "HashTableSize", TUnit::UNIT);
    _source_hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(profile, "MemoryUsageHashTable", TUnit::BYTES, 1);
    _source_memory_usage_container = ADD_COUNTER(profile, "MemoryUsageContainer", TUnit::BYTES);
    _source_memory_usage_arena = ADD_COUNTER(profile, "MemoryUsageArena", TUnit::BYTES);
}

void GroupByAggContext::set_finalize_output(const RowDescriptor& row_desc) {
    _finalize_schema = VectorizedUtils::create_columns_with_type_and_name(row_desc);
}

// ==================== Hash table management ====================

void GroupByAggContext::init_hash_method() {
    auto st = doris::init_hash_method<AggregatedDataVariants>(
            _hash_table_data.get(), get_data_types(_groupby_expr_ctxs), _is_first_phase);
    if (!st.ok()) {
        throw Exception(st.code(), st.to_string());
    }
}

void GroupByAggContext::init_agg_data_container() {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        using HashTableType = std::decay_t<decltype(agg_method)>;
        using KeyType = typename HashTableType::Key;
        _agg_data_container = std::make_unique<AggregateDataContainer>(
                sizeof(KeyType),
                ((_total_agg_state_size + _agg_state_alignment - 1) / _agg_state_alignment) *
                        _agg_state_alignment);
    });
}

size_t GroupByAggContext::hash_table_size() const {
    return std::visit(Overload {[&](std::monostate& arg) -> size_t { return 0; },
                                [&](auto& agg_method) { return agg_method.hash_table->size(); }},
                      _hash_table_data->method_variant);
}

size_t GroupByAggContext::memory_usage() const {
    if (hash_table_size() == 0) {
        return 0;
    }
    size_t usage = 0;
    usage += _agg_arena.size();

    if (_agg_data_container) {
        usage += _agg_data_container->memory_usage();
    }

    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        usage += agg_method.hash_table->get_buffer_size_in_bytes();
    });

    return usage;
}

void GroupByAggContext::update_memusage() {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        auto& data = *agg_method.hash_table;
        int64_t memory_usage_arena = _agg_arena.size();
        int64_t memory_usage_container =
                _agg_data_container ? _agg_data_container->memory_usage() : 0;
        int64_t hash_table_memory_usage = data.get_buffer_size_in_bytes();
        auto ht_size = static_cast<int64_t>(data.size());

        using agg_context_utils::set_counter_if;
        // Update sink-side counters
        set_counter_if(_memory_usage_arena, memory_usage_arena);
        set_counter_if(_memory_usage_container, memory_usage_container);
        set_counter_if(_hash_table_memory_usage, hash_table_memory_usage);
        set_counter_if(_hash_table_size_counter, ht_size);
        set_counter_if(_serialize_key_arena_memory_usage,
                       memory_usage_arena + memory_usage_container);
        set_counter_if(_memory_used_counter,
                       memory_usage_arena + memory_usage_container + hash_table_memory_usage);

        // Update source-side counters (for PartitionedAgg source profile)
        set_counter_if(_source_memory_usage_arena, memory_usage_arena);
        set_counter_if(_source_memory_usage_container, memory_usage_container);
        set_counter_if(_source_hash_table_memory_usage, hash_table_memory_usage);
        set_counter_if(_source_hash_table_size_counter, ht_size);
    });
}

size_t GroupByAggContext::get_reserve_mem_size(RuntimeState* state) const {
    size_t size_to_reserve = std::visit(
            [&](auto&& arg) -> size_t {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<HashTableCtxType, std::monostate>) {
                    return 0;
                } else {
                    return arg.hash_table->estimate_memory(state->batch_size());
                }
            },
            _hash_table_data->method_variant);

    size_to_reserve += memory_usage_last_executing;
    return size_to_reserve;
}

size_t GroupByAggContext::estimated_memory_for_merging(size_t rows) const {
    size_t size = std::visit(
            Overload {[&](std::monostate& arg) -> size_t { return 0; },
                      [&](auto& agg_method) { return agg_method.hash_table->estimate_memory(rows); }},
            _hash_table_data->method_variant);
    size += _agg_data_container->estimate_memory(rows);
    return size;
}

bool GroupByAggContext::apply_limit_filter(Block* block) {
    if (!reach_limit) {
        return false;
    }
    if (do_sort_limit) {
        const size_t key_size = _groupby_expr_ctxs.size();
        ColumnRawPtrs key_columns(key_size);
        for (size_t i = 0; i < key_size; ++i) {
            key_columns[i] = block->get_by_position(i).column.get();
        }
        if (do_limit_filter(block->rows(), key_columns)) {
            Block::filter_block_internal(block, _need_computes);
        }
        return false; // sort-limit handles its own filtering; caller just counts rows
    }
    // Non-sort limit: caller should apply reached_limit() truncation.
    return true;
}

Status GroupByAggContext::reset_hash_table() {
    return agg_context_utils::visit_agg_method<Status>(
            *_hash_table_data, [&](auto& agg_method) -> Status {
                auto& hash_table = *agg_method.hash_table;
                using HashTableType = std::decay_t<decltype(hash_table)>;

                agg_method.arena.clear();
                agg_method.inited_iterator = false;

                hash_table.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        destroy_agg_state(mapped);
                        mapped = nullptr;
                    }
                });

                if (hash_table.has_null_key_data()) {
                    destroy_agg_state(hash_table.template get_null_key_data<AggregateDataPtr>());
                }

                _agg_data_container.reset(new AggregateDataContainer(
                        sizeof(typename HashTableType::key_type),
                        ((_total_agg_state_size + _agg_state_alignment - 1) /
                         _agg_state_alignment) *
                                _agg_state_alignment));
                agg_method.hash_table.reset(new HashTableType());
                return Status::OK();
            });
}

// ==================== Agg state management ====================

Status GroupByAggContext::create_agg_state(AggregateDataPtr data) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        try {
            _agg_evaluators[i]->create(data + _agg_state_offsets[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_evaluators[j]->destroy(data + _agg_state_offsets[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

void GroupByAggContext::destroy_agg_state(AggregateDataPtr data) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        _agg_evaluators[i]->function()->destroy(data + _agg_state_offsets[i]);
    }
}

void GroupByAggContext::close() {
    std::visit(Overload {[&](std::monostate& arg) -> void {
                             // Do nothing
                         },
                         [&](auto& agg_method) -> void {
                             auto& data = *agg_method.hash_table;
                             data.for_each_mapped([&](auto& mapped) {
                                 if (mapped) {
                                     destroy_agg_state(mapped);
                                     mapped = nullptr;
                                 }
                             });
                             if (data.has_null_key_data()) {
                                 destroy_agg_state(
                                         data.template get_null_key_data<AggregateDataPtr>());
                             }
                         }},
               _hash_table_data->method_variant);
}

// ==================== Hash table write operations ====================

void GroupByAggContext::emplace_into_hash_table(AggregateDataPtr* places,
                                                ColumnRawPtrs& key_columns, uint32_t num_rows,
                                                RuntimeProfile::Counter* hash_table_compute_timer,
                                                RuntimeProfile::Counter* hash_table_emplace_timer,
                                                RuntimeProfile::Counter* hash_table_input_counter) {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        SCOPED_TIMER(hash_table_compute_timer);
        using HashMethodType = std::decay_t<decltype(agg_method)>;
        using AggState = typename HashMethodType::State;
        AggState state(key_columns);
        agg_method.init_serialized_keys(key_columns, num_rows);

        auto creator = [this](const auto& ctor, auto& key, auto& origin) {
            HashMethodType::try_presis_key_and_origin(key, origin, _agg_arena);
            auto mapped = _agg_data_container->append_data(origin);
            auto st = create_agg_state(mapped);
            if (!st) {
                throw Exception(st.code(), st.to_string());
            }
            ctor(key, mapped);
        };

        auto creator_for_null_key = [&](auto& mapped) {
            mapped = _agg_arena.aligned_alloc(_total_agg_state_size, _agg_state_alignment);
            auto st = create_agg_state(mapped);
            if (!st) {
                throw Exception(st.code(), st.to_string());
            }
        };

        SCOPED_TIMER(hash_table_emplace_timer);
        lazy_emplace_batch(agg_method, state, num_rows, creator, creator_for_null_key,
                           [&](uint32_t row, auto& mapped) { places[row] = mapped; });

        COUNTER_UPDATE(hash_table_input_counter, num_rows);
    });
}

void GroupByAggContext::find_in_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                           uint32_t num_rows) {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        using HashMethodType = std::decay_t<decltype(agg_method)>;
        using AggState = typename HashMethodType::State;
        AggState state(key_columns);
        agg_method.init_serialized_keys(key_columns, num_rows);

        find_batch(agg_method, state, num_rows, [&](uint32_t row, auto& find_result) {
            if (find_result.is_found()) {
                places[row] = find_result.get_mapped();
            } else {
                places[row] = nullptr;
            }
        });
    });
}

bool GroupByAggContext::emplace_into_hash_table_limit(AggregateDataPtr* places, Block* block,
                                                      const std::vector<int>* key_locs,
                                                      ColumnRawPtrs& key_columns,
                                                      uint32_t num_rows) {
    return agg_context_utils::visit_agg_method<bool>(
            *_hash_table_data, [&](auto&& agg_method) -> bool {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;

                bool need_filter = false;
                {
                    SCOPED_TIMER(_hash_table_limit_compute_timer);
                    need_filter = do_limit_filter(num_rows, key_columns);
                }

                auto& need_computes = _need_computes;
                if (std::find(need_computes.begin(), need_computes.end(), 1) ==
                    need_computes.end()) {
                    return false;
                }

                if (need_filter) {
                    Block::filter_block_internal(block, need_computes);
                    if (key_locs) {
                        for (int i = 0; i < key_locs->size(); ++i) {
                            key_columns[i] =
                                    block->get_by_position((*key_locs)[i]).column.get();
                        }
                    }
                    num_rows = (uint32_t)block->rows();
                }

                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);
                size_t i = 0;

                auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                    try {
                        HashMethodType::try_presis_key_and_origin(key, origin, _agg_arena);
                        auto mapped = _agg_data_container->append_data(origin);
                        auto st = create_agg_state(mapped);
                        if (!st) {
                            throw Exception(st.code(), st.to_string());
                        }
                        ctor(key, mapped);
                        refresh_top_limit(i, key_columns);
                    } catch (...) {
                        ctor(key, nullptr);
                        throw;
                    }
                };

                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = _agg_arena.aligned_alloc(_total_agg_state_size,
                                                      _agg_state_alignment);
                    auto st = create_agg_state(mapped);
                    if (!st) {
                        throw Exception(st.code(), st.to_string());
                    }
                    refresh_top_limit(i, key_columns);
                };

                SCOPED_TIMER(_hash_table_emplace_timer);
                lazy_emplace_batch(
                        agg_method, state, num_rows, creator, creator_for_null_key,
                        [&](uint32_t row) { i = row; },
                        [&](uint32_t row, auto& mapped) { places[row] = mapped; });
                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                return true;
            });
}

// ==================== Aggregation execution ====================

Status GroupByAggContext::evaluate_groupby_keys(Block* block, ColumnRawPtrs& key_columns,
                                                std::vector<int>* key_locs) {
    SCOPED_TIMER(_expr_timer);
    const size_t key_size = _groupby_expr_ctxs.size();
    for (size_t i = 0; i < key_size; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_groupby_expr_ctxs[i]->execute(block, &result_column_id));
        block->get_by_position(result_column_id).column =
                block->get_by_position(result_column_id).column->convert_to_full_column_if_const();
        key_columns[i] = block->get_by_position(result_column_id).column.get();
        key_columns[i]->assume_mutable()->replace_float_special_values();
        if (key_locs) {
            (*key_locs)[i] = result_column_id;
        }
    }
    return Status::OK();
}

Status GroupByAggContext::update(Block* block) {
    memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&memory_usage_last_executing);

    SCOPED_TIMER(_build_timer);
    DCHECK(!_groupby_expr_ctxs.empty());

    size_t key_size = _groupby_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    std::vector<int> key_locs(key_size);
    RETURN_IF_ERROR(evaluate_groupby_keys(block, key_columns, &key_locs));

    auto rows = (uint32_t)block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if (reach_limit && !do_sort_limit) {
        find_in_hash_table(_places.data(), key_columns, rows);
        RETURN_IF_ERROR(_execute_batch_add_selected_evaluators(block, _places.data()));
    } else {
        if (reach_limit) {
            // do_sort_limit == true here
            if (emplace_into_hash_table_limit(_places.data(), block, &key_locs, key_columns,
                                              rows)) {
                RETURN_IF_ERROR(_execute_batch_add_evaluators(block, _places.data()));
            }
        } else {
            emplace_into_hash_table(_places.data(), key_columns, rows, _hash_table_compute_timer,
                                    _hash_table_emplace_timer, _hash_table_input_counter);
            RETURN_IF_ERROR(_execute_batch_add_evaluators(block, _places.data()));

            _check_limit_after_emplace();
        }
    }
    return Status::OK();
}

Status GroupByAggContext::emplace_and_forward(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                              uint32_t num_rows, Block* block,
                                              bool expand_hash_table) {
    emplace_into_hash_table(places, key_columns, num_rows, _hash_table_compute_timer,
                            _hash_table_emplace_timer, _hash_table_input_counter);
    return _execute_batch_add_evaluators(block, places, expand_hash_table);
}

Status GroupByAggContext::merge(Block* block) {
    memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&memory_usage_last_executing);

    if (reach_limit) {
        return _merge_with_serialized_key_helper<true, false>(block);
    } else {
        return _merge_with_serialized_key_helper<false, false>(block);
    }
}

Status GroupByAggContext::merge_for_spill(Block* block) {
    return _merge_with_serialized_key_helper<false, true>(block);
}

// ==================== Evaluator loop helpers ====================

Status GroupByAggContext::_execute_batch_add_evaluators(Block* block, AggregateDataPtr* places,
                                                        bool expand_hash_table) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_agg_evaluators[i]->execute_batch_add(
                block, _agg_state_offsets[i], places, _agg_arena, expand_hash_table));
    }
    return Status::OK();
}

Status GroupByAggContext::_execute_batch_add_selected_evaluators(Block* block,
                                                                 AggregateDataPtr* places) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_agg_evaluators[i]->execute_batch_add_selected(
                block, _agg_state_offsets[i], places, _agg_arena));
    }
    return Status::OK();
}

Status GroupByAggContext::_merge_evaluators_selected(Block* block, size_t rows,
                                                     RuntimeProfile::Counter* deser_timer) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        if (_agg_evaluators[i]->is_merge()) {
            int col_id = get_slot_column_id(_agg_evaluators[i]);
            auto column = block->get_by_position(col_id).column;

            size_t buffer_size = _agg_evaluators[i]->function()->size_of_data() * rows;
            if (_deserialize_buffer.size() < buffer_size) {
                _deserialize_buffer.resize(buffer_size);
            }

            {
                SCOPED_TIMER(deser_timer);
                _agg_evaluators[i]->function()->deserialize_and_merge_vec_selected(
                        _places.data(), _agg_state_offsets[i], _deserialize_buffer.data(),
                        column.get(), _agg_arena, rows);
            }
        } else {
            RETURN_IF_ERROR(_agg_evaluators[i]->execute_batch_add_selected(
                    block, _agg_state_offsets[i], _places.data(), _agg_arena));
        }
    }
    return Status::OK();
}

template <bool for_spill>
Status GroupByAggContext::_merge_evaluators(Block* block, size_t rows,
                                            RuntimeProfile::Counter* deser_timer) {
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        if (_agg_evaluators[i]->is_merge() || for_spill) {
            size_t col_id = 0;
            if constexpr (for_spill) {
                col_id = _groupby_expr_ctxs.size() + i;
            } else {
                col_id = get_slot_column_id(_agg_evaluators[i]);
            }
            auto column = block->get_by_position(col_id).column;

            size_t buffer_size = _agg_evaluators[i]->function()->size_of_data() * rows;
            if (_deserialize_buffer.size() < buffer_size) {
                _deserialize_buffer.resize(buffer_size);
            }

            {
                SCOPED_TIMER(deser_timer);
                _agg_evaluators[i]->function()->deserialize_and_merge_vec(
                        _places.data(), _agg_state_offsets[i], _deserialize_buffer.data(),
                        column.get(), _agg_arena, rows);
            }
        } else {
            RETURN_IF_ERROR(_agg_evaluators[i]->execute_batch_add(
                    block, _agg_state_offsets[i], _places.data(), _agg_arena));
        }
    }
    return Status::OK();
}

template Status GroupByAggContext::_merge_evaluators<true>(Block* block, size_t rows,
                                                           RuntimeProfile::Counter* deser_timer);
template Status GroupByAggContext::_merge_evaluators<false>(Block* block, size_t rows,
                                                            RuntimeProfile::Counter* deser_timer);

void GroupByAggContext::_serialize_agg_values(MutableColumns& value_columns,
                                              DataTypes& value_data_types, Block* block,
                                              bool mem_reuse, size_t key_size, uint32_t num_rows) {
    for (size_t i = 0; i < _agg_evaluators.size(); ++i) {
        value_data_types[i] = _agg_evaluators[i]->function()->get_serialized_type();
        if (mem_reuse) {
            value_columns[i] = std::move(*block->get_by_position(i + key_size).column).mutate();
        } else {
            value_columns[i] = _agg_evaluators[i]->function()->create_serialize_column();
        }
        _agg_evaluators[i]->function()->serialize_to_column(_values, _agg_state_offsets[i],
                                                            value_columns[i], num_rows);
    }
}

void GroupByAggContext::_insert_finalized_values(MutableColumns& value_columns,
                                                  uint32_t num_rows) {
    for (size_t i = 0; i < _agg_evaluators.size(); ++i) {
        _agg_evaluators[i]->insert_result_info_vec(_values, _agg_state_offsets[i],
                                                   value_columns[i].get(), num_rows);
    }
}

void GroupByAggContext::_insert_finalized_single(AggregateDataPtr mapped,
                                                  MutableColumns& value_columns) {
    for (size_t i = 0; i < _agg_evaluators.size(); ++i) {
        _agg_evaluators[i]->insert_result_info(mapped + _agg_state_offsets[i],
                                               value_columns[i].get());
    }
}

// ==================== Streaming preagg support ====================

bool GroupByAggContext::should_expand_preagg_hash_table(int64_t input_rows, int64_t returned_rows,
                                                         bool is_single_backend) {
    if (!_should_expand_hash_table) {
        return false;
    }

    return agg_context_utils::visit_agg_method<bool>(
            *_hash_table_data, [&](auto& agg_method) -> bool {
                auto& hash_tbl = *agg_method.hash_table;
                auto [ht_mem, ht_rows] =
                        std::pair {hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

                if (ht_rows == 0) {
                    return true;
                }

                const auto* reduction = is_single_backend ? SINGLE_BE_STREAMING_HT_MIN_REDUCTION
                                                          : STREAMING_HT_MIN_REDUCTION;

                int cache_level = 0;
                while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
                       ht_mem >= reduction[cache_level + 1].min_ht_mem) {
                    ++cache_level;
                }

                const int64_t aggregated_input_rows = input_rows - returned_rows;
                double current_reduction = static_cast<double>(aggregated_input_rows) /
                                           static_cast<double>(ht_rows);

                if (aggregated_input_rows <= 0) {
                    return true;
                }

                double min_reduction = reduction[cache_level].streaming_ht_min_reduction;
                _should_expand_hash_table = current_reduction > min_reduction;
                return _should_expand_hash_table;
            });
}

bool GroupByAggContext::should_skip_preagg(size_t rows, size_t mem_limit, int64_t input_rows,
                                           int64_t returned_rows, bool is_single_backend) {
    const bool used_too_much_memory = mem_limit > 0 && memory_usage() > mem_limit;

    return agg_context_utils::visit_agg_method<bool>(
            *_hash_table_data, [&](auto& agg_method) -> bool {
                auto& hash_tbl = *agg_method.hash_table;
                return used_too_much_memory ||
                       (hash_tbl.add_elem_size_overflow(rows) &&
                        !should_expand_preagg_hash_table(input_rows, returned_rows,
                                                         is_single_backend));
            });
}

Status GroupByAggContext::streaming_serialize_passthrough(Block* in_block, Block* out_block,
                                                          ColumnRawPtrs& key_columns,
                                                          uint32_t rows, bool mem_reuse) {
    size_t key_size = _groupby_expr_ctxs.size();
    size_t agg_size = _agg_evaluators.size();

    DataTypes data_types(agg_size);
    for (size_t i = 0; i < agg_size; ++i) {
        data_types[i] = _agg_evaluators[i]->function()->get_serialized_type();
    }

    auto value_columns = agg_context_utils::take_or_create_columns(
            out_block, mem_reuse, key_size, agg_size,
            [&](size_t i) { return _agg_evaluators[i]->function()->create_serialize_column(); });

    for (size_t i = 0; i < _agg_evaluators.size(); ++i) {
        SCOPED_TIMER(_insert_values_to_column_timer);
        RETURN_IF_ERROR(_agg_evaluators[i]->streaming_agg_serialize_to_column(
                in_block, value_columns[i], rows, _agg_arena));
    }

    if (!mem_reuse) {
        agg_context_utils::build_serialized_output_block(out_block, key_columns, rows,
                                                         _groupby_expr_ctxs, value_columns,
                                                         data_types);
    } else {
        for (size_t i = 0; i < key_size; ++i) {
            std::move(*out_block->get_by_position(i).column)
                    .mutate()
                    ->insert_range_from(*key_columns[i], 0, rows);
        }
    }

    return Status::OK();
}

Status GroupByAggContext::preagg_emplace_and_forward(ColumnRawPtrs& key_columns, uint32_t num_rows,
                                                     Block* block) {
    _places.resize(num_rows);
    return emplace_and_forward(_places.data(), key_columns, num_rows, block,
                               _should_expand_hash_table);
}

Status GroupByAggContext::emplace_and_forward_limit(Block* block, ColumnRawPtrs& key_columns,
                                                    uint32_t num_rows) {
    _places.resize(num_rows);
    bool need_agg =
            emplace_into_hash_table_limit(_places.data(), block, nullptr, key_columns, num_rows);
    if (need_agg) {
        RETURN_IF_ERROR(
                _execute_batch_add_evaluators(block, _places.data(), _should_expand_hash_table));
    }
    return Status::OK();
}

// ==================== Limit check helpers ====================

void GroupByAggContext::_check_limit_after_emplace() {
    if (should_limit_output && !enable_spill) {
        const size_t ht_size = hash_table_size();
        reach_limit =
                ht_size >= (do_sort_limit ? limit * config::topn_agg_limit_multiplier : limit);
        if (reach_limit && do_sort_limit) {
            build_limit_heap(ht_size);
        }
    }
}

void GroupByAggContext::_check_limit_after_emplace_for_merge() {
    if (should_limit_output) {
        const size_t ht_size = hash_table_size();
        reach_limit = ht_size >= limit;
        if (do_sort_limit && reach_limit) {
            build_limit_heap(ht_size);
        }
    }
}

template <bool limit, bool for_spill>
Status GroupByAggContext::_merge_with_serialized_key_helper(Block* block) {
    auto* merge_timer = for_spill ? _source_merge_timer : _merge_timer;
    auto* deser_timer = for_spill ? _source_deserialize_data_timer : _deserialize_data_timer;
    SCOPED_TIMER(merge_timer);

    size_t key_size = _groupby_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    std::vector<int> key_locs(key_size);

    if constexpr (for_spill) {
        for (int i = 0; i < key_size; ++i) {
            key_columns[i] = block->get_by_position(i).column.get();
            key_columns[i]->assume_mutable()->replace_float_special_values();
            key_locs[i] = i;
        }
    } else {
        RETURN_IF_ERROR(evaluate_groupby_keys(block, key_columns, &key_locs));
    }

    size_t rows = block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if (limit && !do_sort_limit) {
        find_in_hash_table(_places.data(), key_columns, (uint32_t)rows);
        RETURN_IF_ERROR(_merge_evaluators_selected(block, rows, deser_timer));
    } else {
        bool need_do_agg = true;
        if (limit) {
            need_do_agg = emplace_into_hash_table_limit(_places.data(), block, &key_locs,
                                                        key_columns, (uint32_t)rows);
            rows = block->rows();
        } else {
            if constexpr (for_spill) {
                emplace_into_hash_table(_places.data(), key_columns, (uint32_t)rows,
                                        _source_hash_table_compute_timer,
                                        _source_hash_table_emplace_timer,
                                        _source_hash_table_input_counter);
            } else {
                emplace_into_hash_table(_places.data(), key_columns, (uint32_t)rows,
                                        _hash_table_compute_timer, _hash_table_emplace_timer,
                                        _hash_table_input_counter);
            }
        }

        if (need_do_agg) {
            RETURN_IF_ERROR(_merge_evaluators<for_spill>(block, rows, deser_timer));
        }

        if (!limit && should_limit_output) {
            _check_limit_after_emplace_for_merge();
        }
    }

    return Status::OK();
}

// Explicit template instantiation
template Status GroupByAggContext::_merge_with_serialized_key_helper<true, false>(Block* block);
template Status GroupByAggContext::_merge_with_serialized_key_helper<false, false>(Block* block);
template Status GroupByAggContext::_merge_with_serialized_key_helper<false, true>(Block* block);

// ==================== Result output ====================

Status GroupByAggContext::serialize(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_get_results_timer);
    size_t key_size = _groupby_expr_ctxs.size();
    size_t agg_size = _agg_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    bool mem_reuse = make_nullable_keys.empty() && block->mem_reuse();

    auto key_columns = agg_context_utils::take_or_create_columns(
            block, mem_reuse, 0, key_size,
            [&](size_t i) { return _groupby_expr_ctxs[i]->root()->data_type()->create_column(); });

    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        agg_method.init_iterator();
        auto& data = *agg_method.hash_table;
        const auto size = std::min(data.size(), size_t(state->batch_size()));
        using KeyType = std::decay_t<decltype(agg_method)>::Key;
        std::vector<KeyType> keys(size);

        if (_values.size() < size + 1) {
            _values.resize(size + 1);
        }

        uint32_t num_rows = 0;
        _agg_data_container->init_once();
        auto& iter = _agg_data_container->iterator;

        {
            SCOPED_TIMER(_hash_table_iterate_timer);
            while (iter != _agg_data_container->end() && num_rows < state->batch_size()) {
                keys[num_rows] = iter.template get_key<KeyType>();
                _values[num_rows] = iter.get_aggregate_data();
                ++iter;
                ++num_rows;
            }
        }

        {
            SCOPED_TIMER(_insert_keys_to_column_timer);
            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
        }

        if (iter == _agg_data_container->end()) {
            if (agg_method.hash_table->has_null_key_data()) {
                DCHECK(key_columns.size() == 1);
                DCHECK(key_columns[0]->is_nullable());
                key_columns[0]->insert_data(nullptr, 0);
                _values[num_rows] =
                        agg_method.hash_table->template get_null_key_data<AggregateDataPtr>();
                ++num_rows;
                *eos = true;
            } else {
                *eos = true;
            }
        }

        {
            SCOPED_TIMER(_insert_values_to_column_timer);
            _serialize_agg_values(value_columns, value_data_types, block, mem_reuse, key_size,
                                  num_rows);
        }
    });

    if (!mem_reuse) {
        agg_context_utils::build_serialized_output_block(block, key_columns, _groupby_expr_ctxs,
                                                         value_columns, value_data_types);
    }

    return Status::OK();
}

Status GroupByAggContext::finalize(RuntimeState* state, Block* block, bool* eos) {
    const auto& columns_with_schema = _finalize_schema;
    bool mem_reuse = make_nullable_keys.empty() && block->mem_reuse();

    size_t key_size = _groupby_expr_ctxs.size();

    auto key_columns = agg_context_utils::take_or_create_columns(
            block, mem_reuse, 0, key_size,
            [&](size_t i) { return columns_with_schema[i].type->create_column(); });
    auto value_columns = agg_context_utils::take_or_create_columns(
            block, mem_reuse, key_size, columns_with_schema.size() - key_size,
            [&](size_t i) { return columns_with_schema[key_size + i].type->create_column(); });

    SCOPED_TIMER(_get_results_timer);
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        auto& data = *agg_method.hash_table;
        agg_method.init_iterator();
        const auto size = std::min(data.size(), size_t(state->batch_size()));
        using KeyType = std::decay_t<decltype(agg_method)>::Key;
        std::vector<KeyType> keys(size);

        if (_values.size() < size) {
            _values.resize(size);
        }

        uint32_t num_rows = 0;
        _agg_data_container->init_once();
        auto& iter = _agg_data_container->iterator;

        {
            SCOPED_TIMER(_hash_table_iterate_timer);
            while (iter != _agg_data_container->end() && num_rows < state->batch_size()) {
                keys[num_rows] = iter.template get_key<KeyType>();
                _values[num_rows] = iter.get_aggregate_data();
                ++iter;
                ++num_rows;
            }
        }

        {
            SCOPED_TIMER(_insert_keys_to_column_timer);
            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
        }

        _insert_finalized_values(value_columns, num_rows);

        if (iter == _agg_data_container->end()) {
            if (agg_method.hash_table->has_null_key_data()) {
                DCHECK(key_columns.size() == 1);
                DCHECK(key_columns[0]->is_nullable());
                if (key_columns[0]->size() < state->batch_size()) {
                    key_columns[0]->insert_data(nullptr, 0);
                    auto mapped =
                            agg_method.hash_table->template get_null_key_data<AggregateDataPtr>();
                    _insert_finalized_single(mapped, value_columns);
                    *eos = true;
                }
            } else {
                *eos = true;
            }
        }
    });

    if (!mem_reuse) {
        agg_context_utils::assemble_finalized_output(block, columns_with_schema, key_columns,
                                                     value_columns, key_size);
    }

    return Status::OK();
}

// ==================== Sort limit ====================

MutableColumns GroupByAggContext::_get_keys_hash_table() {
    return agg_context_utils::visit_agg_method<MutableColumns>(
            *_hash_table_data, [&](auto&& agg_method) -> MutableColumns {
                MutableColumns key_columns;
                for (int i = 0; i < _groupby_expr_ctxs.size(); ++i) {
                    key_columns.emplace_back(
                            _groupby_expr_ctxs[i]->root()->data_type()->create_column());
                }
                auto& data = *agg_method.hash_table;
                bool has_null_key = data.has_null_key_data();
                const auto size = data.size() - has_null_key;
                using KeyType = std::decay_t<decltype(agg_method)>::Key;
                std::vector<KeyType> keys(size);

                uint32_t num_rows = 0;
                auto iter = _agg_data_container->begin();
                {
                    while (iter != _agg_data_container->end()) {
                        keys[num_rows] = iter.get_key<KeyType>();
                        ++iter;
                        ++num_rows;
                    }
                }
                agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                if (has_null_key) {
                    key_columns[0]->insert_data(nullptr, 0);
                }
                return key_columns;
            });
}

void GroupByAggContext::build_limit_heap(size_t hash_table_size_val) {
    _limit_columns = _get_keys_hash_table();
    for (size_t i = 0; i < hash_table_size_val; ++i) {
        _limit_heap.emplace(i, _limit_columns, order_directions, null_directions);
    }
    while (hash_table_size_val > limit) {
        _limit_heap.pop();
        hash_table_size_val--;
    }
    _limit_columns_min = _limit_heap.top()._row_id;
}

bool GroupByAggContext::do_limit_filter(size_t num_rows, const ColumnRawPtrs& key_columns) {
    if (num_rows) {
        _cmp_res.resize(num_rows);
        _need_computes.resize(num_rows);
        memset(_need_computes.data(), 0, _need_computes.size());
        memset(_cmp_res.data(), 0, _cmp_res.size());

        const auto key_size = null_directions.size();
        for (int i = 0; i < key_size; i++) {
            key_columns[i]->compare_internal(_limit_columns_min, *_limit_columns[i],
                                             null_directions[i], order_directions[i], _cmp_res,
                                             _need_computes.data());
        }

        auto set_computes_arr = [](auto* __restrict res, auto* __restrict computes, size_t rows) {
            for (size_t i = 0; i < rows; ++i) {
                computes[i] = computes[i] == res[i];
            }
        };
        set_computes_arr(_cmp_res.data(), _need_computes.data(), num_rows);

        return std::find(_need_computes.begin(), _need_computes.end(), 0) != _need_computes.end();
    }

    return false;
}

void GroupByAggContext::refresh_top_limit(size_t row_id, const ColumnRawPtrs& key_columns) {
    for (int j = 0; j < key_columns.size(); ++j) {
        _limit_columns[j]->insert_from(*key_columns[j], row_id);
    }
    _limit_heap.emplace(_limit_columns[0]->size() - 1, _limit_columns, order_directions,
                        null_directions);

    _limit_heap.pop();
    _limit_columns_min = _limit_heap.top()._row_id;
}

void GroupByAggContext::add_limit_heap_top(ColumnRawPtrs& key_columns, size_t rows) {
    for (size_t i = 0; i < rows; ++i) {
        if (_cmp_res[i] == 1 && _need_computes[i]) {
            for (size_t j = 0; j < key_columns.size(); ++j) {
                _limit_columns[j]->insert_from(*key_columns[j], i);
            }
            _limit_heap.emplace(_limit_columns[0]->size() - 1, _limit_columns, order_directions,
                                null_directions);
            _limit_heap.pop();
            _limit_columns_min = _limit_heap.top()._row_id;
            break;
        }
    }
}

// ==================== Static utilities ====================

void GroupByAggContext::make_nullable_output_key(Block* block,
                                                 const std::vector<size_t>& make_nullable_keys) {
    if (block->rows() != 0) {
        for (auto cid : make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

int GroupByAggContext::get_slot_column_id(const AggFnEvaluator* evaluator) {
    return agg_context_utils::get_slot_column_id(evaluator);
}

} // namespace doris
