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

#include "exec/operator/streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "core/value/hll.h"
#include "exec/operator/operator.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vslot_ref.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
} // namespace doris

namespace doris {

StreamingAggLocalState::StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _agg_data(std::make_unique<AggregatedDataVariants>()),
          _hll(std::make_unique<HyperLogLog>()),
          _child_block(Block::create_unique()) {}

Status StreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _serialize_key_arena_memory_usage = Base::custom_profile()->AddHighWaterMarkCounter(
            "MemoryUsageSerializeKeyArena", TUnit::BYTES, "", 1);

    _build_timer = ADD_TIMER(Base::custom_profile(), "BuildTime");
    _merge_timer = ADD_TIMER(Base::custom_profile(), "MergeTime");
    _expr_timer = ADD_TIMER(Base::custom_profile(), "ExprTime");
    _insert_values_to_column_timer = ADD_TIMER(Base::custom_profile(), "InsertValuesToColumnTime");
    _deserialize_data_timer = ADD_TIMER(Base::custom_profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _hash_table_limit_compute_timer =
            ADD_TIMER(Base::custom_profile(), "HashTableLimitComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
    _hash_table_size_counter = ADD_COUNTER(custom_profile(), "HashTableSize", TUnit::UNIT);
    _streaming_agg_timer = ADD_TIMER(custom_profile(), "StreamingAggTime");
    _get_results_timer = ADD_TIMER(custom_profile(), "GetResultsTime");
    _hash_table_iterate_timer = ADD_TIMER(custom_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");
    _flush_timer = ADD_TIMER(custom_profile(), "FlushTime");
    _flush_count = ADD_COUNTER(custom_profile(), "FlushCount", TUnit::UNIT);

    return Status::OK();
}

Status StreamingAggLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
    for (auto& evaluator : p._aggregate_evaluators) {
        _aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }

    for (auto& evaluator : _aggregate_evaluators) {
        evaluator->set_timer(_merge_timer, _expr_timer);
    }

    DCHECK(!_probe_expr_ctxs.empty());

    RETURN_IF_ERROR(_init_hash_method(_probe_expr_ctxs));

    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) {
                             using HashTableType = std::decay_t<decltype(agg_method)>;
                             using KeyType = typename HashTableType::Key;

                             /// some aggregate functions (like AVG for decimal) have align issues.
                             _aggregate_data_container = std::make_unique<AggregateDataContainer>(
                                     sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                        p._align_aggregate_states - 1) /
                                                       p._align_aggregate_states) *
                                                              p._align_aggregate_states);

                             /// Pre-reserve the micro hash table to MICRO_HT_CAPACITY slots.
                             agg_method.hash_table->reserve(MICRO_HT_CAPACITY);
                         }},
               _agg_data->method_variant);

    limit = p._sort_limit;
    do_sort_limit = p._do_sort_limit;
    null_directions = p._null_directions;
    order_directions = p._order_directions;

    _output_batch_size = state->batch_size();

    return Status::OK();
}

size_t StreamingAggLocalState::_get_hash_table_size() {
    return std::visit(Overload {[&](std::monostate& arg) -> size_t {
                                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                           "uninited hash table");
                                    return 0;
                                },
                                [&](auto& agg_method) { return agg_method.hash_table->size(); }},
                      _agg_data->method_variant);
}

void StreamingAggLocalState::_update_memusage_with_serialized_key() {
    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) -> void {
                             auto& data = *agg_method.hash_table;
                             int64_t arena_memory_usage = _agg_arena_pool.size() +
                                                          _aggregate_data_container->memory_usage();
                             int64_t hash_table_memory_usage = data.get_buffer_size_in_bytes();

                             COUNTER_SET(_memory_used_counter,
                                         arena_memory_usage + hash_table_memory_usage);

                             COUNTER_SET(_serialize_key_arena_memory_usage, arena_memory_usage);
                             COUNTER_SET(_hash_table_memory_usage, hash_table_memory_usage);
                         }},
               _agg_data->method_variant);
}

Status StreamingAggLocalState::_init_hash_method(const VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(init_hash_method<AggregatedDataVariants>(
            _agg_data.get(), get_data_types(probe_exprs),
            Base::_parent->template cast<StreamingAggOperatorX>()._is_first_phase));
    return Status::OK();
}

Status StreamingAggLocalState::do_pre_agg(RuntimeState* state, Block* input_block) {
    RETURN_IF_ERROR(_pre_agg_with_serialized_key(input_block));
    _update_memusage_with_serialized_key();
    return Status::OK();
}

/// Flush the hash table contents into a new Block and push it to _output_blocks.
/// Then clear all aggregation state: destroy agg states, clear HT, arena, container.
Status StreamingAggLocalState::_flush_and_reset_hash_table() {
    SCOPED_TIMER(_flush_timer);
    COUNTER_UPDATE(_flush_count, 1);
    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
    const auto key_size = _probe_expr_ctxs.size();
    const auto agg_size = _aggregate_evaluators.size();

    return std::visit(
            Overload {
                    [&](std::monostate& arg) -> Status {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                        return Status::OK();
                    },
                    [&](auto& agg_method) -> Status {
                        auto& data = *agg_method.hash_table;
                        const auto ht_size = data.size();
                        if (ht_size == 0) {
                            return Status::OK();
                        }

                        _abandoned_count += ht_size;

                        using KeyType = std::decay_t<decltype(agg_method)>::Key;

                        _ensure_pending_columns();

                        // Iterate through the AggregateDataContainer to extract keys and values.
                        std::vector<KeyType> keys(ht_size);
                        std::vector<AggregateDataPtr> values(ht_size);

                        uint32_t num_rows = 0;
                        auto iter = _aggregate_data_container->begin();
                        while (iter != _aggregate_data_container->end() && num_rows < ht_size) {
                            keys[num_rows] = iter.template get_key<KeyType>();
                            values[num_rows] = iter.get_aggregate_data();
                            ++iter;
                            ++num_rows;
                        }

                        // Write keys into fresh temporary columns, then append
                        // into pending output columns.  Some hash methods (e.g.
                        // MethodKeysFixed) use resize(num_rows) inside
                        // insert_keys_into_columns, which overwrites rather than
                        // appends.  Using temp columns avoids destroying any
                        // previously accumulated rows in the pending columns.
                        {
                            MutableColumns tmp_key_columns;
                            for (size_t i = 0; i < key_size; ++i) {
                                tmp_key_columns.push_back(
                                        _probe_expr_ctxs[i]->root()->data_type()->create_column());
                            }
                            agg_method.insert_keys_into_columns(keys, tmp_key_columns, num_rows);

                            // Handle null key if present — append null marker to
                            // the temporary key column.
                            if (data.has_null_key_data()) {
                                DCHECK(key_size == 1);
                                DCHECK(tmp_key_columns[0]->is_nullable());
                                tmp_key_columns[0]->insert_data(nullptr, 0);
                                // num_rows == Base::size() here; values[num_rows]
                                // is the pre-allocated slot at index Base::size()
                                // (within the ht_size-sized vector).
                                values[num_rows] =
                                        data.template get_null_key_data<AggregateDataPtr>();
                                ++num_rows;
                            }

                            for (size_t i = 0; i < key_size; ++i) {
                                _pending_output_columns[i]->insert_range_from(*tmp_key_columns[i],
                                                                              0, num_rows);
                            }
                        }

                        // Serialize aggregate states into temporary columns, then
                        // append into pending output columns.  serialize_to_column
                        // uses resize() (not append), so we cannot write directly
                        // into columns that may already contain rows.
                        for (size_t i = 0; i < agg_size; ++i) {
                            auto tmp_col =
                                    _aggregate_evaluators[i]->function()->create_serialize_column();
                            _aggregate_evaluators[i]->function()->serialize_to_column(
                                    values, p._offsets_of_aggregate_states[i], tmp_col, num_rows);
                            _pending_output_columns[key_size + i]->insert_range_from(*tmp_col, 0,
                                                                                     num_rows);
                        }

                        _pending_output_rows += num_rows;
                        _finalize_pending_block(false);

                        // Destroy all aggregate states
                        data.for_each_mapped([&](auto& mapped) {
                            if (mapped) {
                                _destroy_agg_status(mapped);
                                mapped = nullptr;
                            }
                        });
                        if (data.has_null_key_data()) {
                            _destroy_agg_status(
                                    data.template get_null_key_data<AggregateDataPtr>());
                        }

                        // Clear the hash table (keeps capacity/buffer allocated)
                        data.clear();

                        // Reset arena and aggregate data container
                        _agg_arena_pool.clear();
                        _reset_aggregate_data_container();

                        return Status::OK();
                    }},
            _agg_data->method_variant);
}

void StreamingAggLocalState::_reset_aggregate_data_container() {
    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) {
                             using HashTableType = std::decay_t<decltype(agg_method)>;
                             using KeyType = typename HashTableType::Key;
                             _aggregate_data_container = std::make_unique<AggregateDataContainer>(
                                     sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                        p._align_aggregate_states - 1) /
                                                       p._align_aggregate_states) *
                                                              p._align_aggregate_states);
                         }},
               _agg_data->method_variant);
}

Status StreamingAggLocalState::_check_adaptive_decision() {
    DCHECK(_sink_count >= ADAPTIVITY_THRESHOLD);
    DCHECK(_hll != nullptr);

    const int64_t hll_count = _hll->estimate_cardinality();
    const double cardinality_ratio =
            static_cast<double>(hll_count) / static_cast<double>(ADAPTIVITY_THRESHOLD);

    // Rule A: extremely high cardinality → pass-through
    if (cardinality_ratio > HIGH_CARDINALITY_RATIO) {
        _adaptive_phase = AdaptivePhase::PASS_THROUGH;
        _hll.reset();
        return Status::OK();
    }

    // Rule B: hash table capacity bottleneck → resize
    if (hll_count > 0 && static_cast<double>(_abandoned_count) / static_cast<double>(hll_count) >
                                 ABANDON_RATIO_THRESHOLD) {
        _adaptive_phase = AdaptivePhase::RESIZED;
        _hll.reset();

        // Flush existing hash table data before reserving, because
        // StringHashTable::reserve() calls init_buf_size() which frees
        // the internal buffers — destroying all existing entries.
        RETURN_IF_ERROR(_flush_and_reset_hash_table());

        // Calculate target capacity: hll_count * 1.5, capped by MAX_HT_MEMORY_BYTES.
        const auto target_elements = static_cast<size_t>(double(hll_count) * 1.5);
        std::visit(Overload {[&](std::monostate& arg) -> void {
                                 throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                        "uninited hash table");
                             },
                             [&](auto& agg_method) -> void {
                                 using HashMapType =
                                         typename std::decay_t<decltype(agg_method)>::HashMapType;
                                 // Estimate memory for the target capacity.
                                 // PHHashMap slot_type size gives us a rough per-slot memory cost.
                                 const size_t per_slot_bytes =
                                         sizeof(typename HashMapType::value_type) +
                                         1 /* overhead */;
                                 // PHHashMap reserves capacity rounded up by phmap internally.
                                 size_t capped_elements = target_elements;
                                 if (capped_elements * per_slot_bytes > MAX_HT_MEMORY_BYTES) {
                                     capped_elements = MAX_HT_MEMORY_BYTES / per_slot_bytes;
                                 }
                                 agg_method.hash_table->reserve(capped_elements);
                             }},
                   _agg_data->method_variant);
        return Status::OK();
    }

    // Rule C: stable state, keep current micro HT
    _adaptive_phase = AdaptivePhase::STABLE;
    _hll.reset();
    return Status::OK();
}

Status StreamingAggLocalState::_output_pass_through(Block* in_block, ColumnRawPtrs& key_columns,
                                                    uint32_t rows) {
    _ensure_pending_columns();

    const size_t key_size = _probe_expr_ctxs.size();
    const size_t agg_size = _aggregate_evaluators.size();

    // Append key columns into pending columns (indices 0..key_size-1).
    for (size_t i = 0; i < key_size; ++i) {
        _pending_output_columns[i]->insert_range_from(*key_columns[i], 0, rows);
    }

    // Serialize agg values into temporary columns, then append into pending
    // columns.  streaming_agg_serialize_to_column uses resize() (not append),
    // so we cannot write directly into columns that may already contain rows.
    for (size_t i = 0; i < agg_size; ++i) {
        SCOPED_TIMER(_insert_values_to_column_timer);
        auto tmp_col = _aggregate_evaluators[i]->function()->create_serialize_column();
        RETURN_IF_ERROR(_aggregate_evaluators[i]->streaming_agg_serialize_to_column(
                in_block, tmp_col, rows, _agg_arena_pool));
        CHECK_EQ(tmp_col->size(), rows) << "pass-through tmp_col should have " << rows
                                        << " rows after streaming_agg_serialize_to_column, but has "
                                        << tmp_col->size() << " for agg[" << i << "]";
        const size_t before = _pending_output_columns[key_size + i]->size();
        _pending_output_columns[key_size + i]->insert_range_from(*tmp_col, 0, rows);
        CHECK_EQ(_pending_output_columns[key_size + i]->size(), before + rows)
                << "pass-through pending agg col[" << i << "] should have " << (before + rows)
                << " rows after insert_range_from, but has "
                << _pending_output_columns[key_size + i]->size();
    }

    _pending_output_rows += rows;
    _finalize_pending_block(false);

    // Clear the arena to release memory allocated by streaming_agg_serialize_to_column.
    // In pass-through mode the arena would otherwise grow monotonically for the
    // entire query lifetime.
    _agg_arena_pool.clear();

    return Status::OK();
}

void StreamingAggLocalState::_ensure_pending_columns() {
    if (!_pending_output_columns.empty()) {
        // Validate consistency: all columns should have same size == _pending_output_rows
        for (size_t i = 0; i < _pending_output_columns.size(); ++i) {
            CHECK_EQ(_pending_output_columns[i]->size(), _pending_output_rows)
                    << "_ensure_pending_columns: existing col[" << i << "] has "
                    << _pending_output_columns[i]->size()
                    << " rows but _pending_output_rows=" << _pending_output_rows;
        }
        return;
    }
    const size_t key_size = _probe_expr_ctxs.size();
    const size_t agg_size = _aggregate_evaluators.size();

    _pending_output_columns.resize(key_size + agg_size);
    for (size_t i = 0; i < key_size; ++i) {
        _pending_output_columns[i] = _probe_expr_ctxs[i]->root()->data_type()->create_column();
    }
    for (size_t i = 0; i < agg_size; ++i) {
        _pending_output_columns[key_size + i] =
                _aggregate_evaluators[i]->function()->create_serialize_column();
    }
    _pending_output_rows = 0;
}

void StreamingAggLocalState::_finalize_pending_block(bool force) {
    if (_pending_output_rows == 0) {
        return;
    }

    const size_t batch_size = static_cast<size_t>(_output_batch_size);
    const size_t key_size = _probe_expr_ctxs.size();
    const size_t agg_size = _aggregate_evaluators.size();
    const size_t total_cols = key_size + agg_size;

    // Validate that all pending columns have _pending_output_rows entries
    for (size_t i = 0; i < total_cols; ++i) {
        CHECK_EQ(_pending_output_columns[i]->size(), _pending_output_rows)
                << "pending column[" << i << "] has " << _pending_output_columns[i]->size()
                << " rows but _pending_output_rows=" << _pending_output_rows
                << " (key_size=" << key_size << ", agg_size=" << agg_size
                << ", is_agg=" << (i >= key_size) << ")";
    }

    // Emit full batch_size blocks from the pending columns.
    while (_pending_output_rows >= batch_size) {
        ColumnsWithTypeAndName columns_with_schema;
        columns_with_schema.reserve(total_cols);
        for (size_t i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(_pending_output_columns[i]->cut(0, batch_size),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        for (size_t i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(
                    _pending_output_columns[key_size + i]->cut(0, batch_size),
                    _aggregate_evaluators[i]->function()->get_serialized_type(), "");
        }
        Block out_block(columns_with_schema);
        make_nullable_output_key(&out_block);
        _output_blocks.push_back(std::move(out_block));

        // Trim the consumed prefix from pending columns.
        const size_t remaining = _pending_output_rows - batch_size;
        if (remaining > 0) {
            for (size_t i = 0; i < total_cols; ++i) {
                auto tail = _pending_output_columns[i]->cut(batch_size, remaining);
                _pending_output_columns[i] = tail->assume_mutable();
            }
        } else {
            // All consumed — clear pending columns for lazy re-creation.
            _pending_output_columns.clear();
        }
        _pending_output_rows = remaining;
    }

    // If force is set and there are leftover rows, emit them as a partial block.
    if (force && _pending_output_rows > 0) {
        ColumnsWithTypeAndName columns_with_schema;
        columns_with_schema.reserve(total_cols);
        for (size_t i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(_pending_output_columns[i]),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        for (size_t i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(
                    std::move(_pending_output_columns[key_size + i]),
                    _aggregate_evaluators[i]->function()->get_serialized_type(), "");
        }
        Block out_block(columns_with_schema);
        make_nullable_output_key(&out_block);
        _output_blocks.push_back(std::move(out_block));

        _pending_output_columns.clear();
        _pending_output_rows = 0;
    }
}

Status StreamingAggLocalState::_pre_agg_with_serialized_key(doris::Block* in_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
            key_columns[i]->assume_mutable()->replace_float_special_values();
        }
    }

    uint32_t rows = (uint32_t)in_block->rows();

    // --- Pass-through mode: skip hash table entirely ---
    if (_adaptive_phase == AdaptivePhase::PASS_THROUGH) {
        SCOPED_TIMER(_streaming_agg_timer);

        // Handle sort_limit in pass-through mode
        if (limit > 0 && do_sort_limit) {
            if (need_do_sort_limit == -1) {
                const size_t hash_table_size = _get_hash_table_size();
                need_do_sort_limit = hash_table_size >= limit ? 1 : 0;
                if (need_do_sort_limit == 1) {
                    build_limit_heap(hash_table_size);
                }
            }
            if (need_do_sort_limit == 1) {
                if (_do_limit_filter(rows, key_columns)) {
                    bool need_filter = std::find(need_computes.begin(), need_computes.end(), 1) !=
                                       need_computes.end();
                    if (need_filter) {
                        _add_limit_heap_top(key_columns, rows);
                        Block::filter_block_internal(in_block, need_computes);
                        rows = (uint32_t)in_block->rows();
                    } else {
                        return Status::OK();
                    }
                }
            }
        }

        return _output_pass_through(in_block, key_columns, rows);
    }

    // --- Normal aggregation path (PROBING / RESIZED / STABLE) ---
    _places.resize(rows);

    // Check if the adaptive decision needs to be made.
    // We check before processing the current batch so that the decision takes effect
    // starting from this batch.
    const size_t sink_count_before = _sink_count;
    _sink_count += rows;

    if (_adaptive_phase == AdaptivePhase::PROBING && sink_count_before < ADAPTIVITY_THRESHOLD &&
        _sink_count >= ADAPTIVITY_THRESHOLD) {
        RETURN_IF_ERROR(_check_adaptive_decision());

        // If decision was pass-through, flush HT then handle this batch as pass-through
        if (_adaptive_phase == AdaptivePhase::PASS_THROUGH) {
            RETURN_IF_ERROR(_flush_and_reset_hash_table());
            return _output_pass_through(in_block, key_columns, rows);
        }
    }

    // Process the batch: row-by-row emplace with abandon-on-full logic.
    return std::visit(
            Overload {[&](std::monostate& arg) -> Status {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                          return Status::OK();
                      },
                      [&](auto& agg_method) -> Status {
                          SCOPED_TIMER(_hash_table_compute_timer);
                          using HashMethodType = std::decay_t<decltype(agg_method)>;
                          using AggState = typename HashMethodType::State;
                          AggState state(key_columns);
                          agg_method.init_serialized_keys(key_columns, rows);

                          // Update HLL with hash values during PROBING phase
                          if (_adaptive_phase == AdaptivePhase::PROBING && _hll) {
                              for (uint32_t i = 0; i < rows; ++i) {
                                  _hll->update(agg_method.hash_values[i]);
                              }
                          }

                          auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                              HashMethodType::try_presis_key_and_origin(key, origin,
                                                                        _agg_arena_pool);
                              auto mapped = _aggregate_data_container->append_data(origin);
                              auto st = _create_agg_status(mapped);
                              if (!st) {
                                  throw Exception(st.code(), st.to_string());
                              }
                              ctor(key, mapped);
                          };

                          auto creator_for_null_key = [&](auto& mapped) {
                              mapped = _agg_arena_pool.aligned_alloc(
                                      p._total_size_of_aggregate_states, p._align_aggregate_states);
                              auto st = _create_agg_status(mapped);
                              if (!st) {
                                  throw Exception(st.code(), st.to_string());
                              }
                          };

                          {
                              SCOPED_TIMER(_hash_table_emplace_timer);
                              auto& hash_tbl = *agg_method.hash_table;

                              // Batch-level overflow check: if inserting the entire
                              // batch might overflow, flush the HT first then proceed.
                              if (hash_tbl.add_elem_size_overflow(rows)) {
                                  RETURN_IF_ERROR(_flush_and_reset_hash_table());
                              }

                              for (uint32_t i = 0; i < rows; ++i) {
                                  _places[i] = *agg_method.lazy_emplace(state, i, creator,
                                                                        creator_for_null_key);
                              }
                          }

                          COUNTER_UPDATE(_hash_table_input_counter, rows);

                          // Execute aggregate functions for all rows in the batch.
                          for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                              RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add_selected(
                                      in_block, p._offsets_of_aggregate_states[i], _places.data(),
                                      _agg_arena_pool));
                          }

                          // Handle sort limit: init heap if we've reached the limit
                          if (limit > 0 && need_do_sort_limit == -1 &&
                              _get_hash_table_size() >= limit) {
                              need_do_sort_limit = 1;
                              build_limit_heap(_get_hash_table_size());
                          }

                          return Status::OK();
                      }},
            _agg_data->method_variant);
}

Status StreamingAggLocalState::_create_agg_status(AggregateDataPtr data) {
    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        try {
            _aggregate_evaluators[i]->create(data + p._offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _aggregate_evaluators[j]->destroy(data + p._offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status StreamingAggLocalState::_get_results_with_serialized_key(RuntimeState* state, Block* block,
                                                                bool* eos) {
    SCOPED_TIMER(_get_results_timer);
    auto& p = _parent->cast<StreamingAggOperatorX>();
    const auto key_size = _probe_expr_ctxs.size();
    const auto agg_size = _aggregate_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = p._make_nullable_keys.empty() && block->mem_reuse();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    std::visit(
            Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        agg_method.init_iterator();
                        auto& data = *agg_method.hash_table;
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method)>::Key;
                        std::vector<KeyType> keys(size);
                        if (_values.size() < size + 1) {
                            _values.resize(size + 1);
                        }

                        uint32_t num_rows = 0;
                        _aggregate_data_container->init_once();
                        auto& iter = _aggregate_data_container->iterator;

                        {
                            SCOPED_TIMER(_hash_table_iterate_timer);
                            while (iter != _aggregate_data_container->end() &&
                                   num_rows < state->batch_size()) {
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

                        if (iter == _aggregate_data_container->end()) {
                            if (agg_method.hash_table->has_null_key_data()) {
                                // only one key of group by support wrap null key
                                // here need additional processing logic on the null key / value
                                DCHECK(key_columns.size() == 1);
                                DCHECK(key_columns[0]->is_nullable());
                                if (agg_method.hash_table->has_null_key_data()) {
                                    key_columns[0]->insert_data(nullptr, 0);
                                    _values[num_rows] =
                                            agg_method.hash_table->template get_null_key_data<
                                                    AggregateDataPtr>();
                                    ++num_rows;
                                    *eos = true;
                                }
                            } else {
                                *eos = true;
                            }
                        }

                        {
                            SCOPED_TIMER(_insert_values_to_column_timer);
                            for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                                value_data_types[i] =
                                        _aggregate_evaluators[i]->function()->get_serialized_type();
                                if (mem_reuse) {
                                    value_columns[i] =
                                            std::move(*block->get_by_position(i + key_size).column)
                                                    .mutate();
                                } else {
                                    value_columns[i] = _aggregate_evaluators[i]
                                                               ->function()
                                                               ->create_serialize_column();
                                }
                                _aggregate_evaluators[i]->function()->serialize_to_column(
                                        _values, p._offsets_of_aggregate_states[i],
                                        value_columns[i], num_rows);
                            }
                        }
                    }},
            _agg_data->method_variant);

    if (!mem_reuse) {
        ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        *block = Block(columns_with_schema);
    }

    return Status::OK();
}

void StreamingAggLocalState::make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _parent->cast<StreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void StreamingAggLocalState::_destroy_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(
                data + _parent->cast<StreamingAggOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

MutableColumns StreamingAggLocalState::_get_keys_hash_table() {
    return std::visit(
            Overload {[&](std::monostate& arg) {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                          return MutableColumns();
                      },
                      [&](auto&& agg_method) -> MutableColumns {
                          MutableColumns key_columns;
                          for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
                              key_columns.emplace_back(
                                      _probe_expr_ctxs[i]->root()->data_type()->create_column());
                          }
                          auto& data = *agg_method.hash_table;
                          bool has_null_key = data.has_null_key_data();
                          const auto size = data.size() - has_null_key;
                          using KeyType = std::decay_t<decltype(agg_method)>::Key;
                          std::vector<KeyType> keys(size);

                          uint32_t num_rows = 0;
                          auto iter = _aggregate_data_container->begin();
                          {
                              while (iter != _aggregate_data_container->end()) {
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
                      }},
            _agg_data->method_variant);
}

void StreamingAggLocalState::build_limit_heap(size_t hash_table_size) {
    limit_columns = _get_keys_hash_table();
    for (size_t i = 0; i < hash_table_size; ++i) {
        limit_heap.emplace(i, limit_columns, order_directions, null_directions);
    }
    while (hash_table_size > limit) {
        limit_heap.pop();
        hash_table_size--;
    }
    limit_columns_min = limit_heap.top()._row_id;
}

void StreamingAggLocalState::_add_limit_heap_top(ColumnRawPtrs& key_columns, size_t rows) {
    for (int i = 0; i < rows; ++i) {
        if (cmp_res[i] == 1 && need_computes[i]) {
            for (int j = 0; j < key_columns.size(); ++j) {
                limit_columns[j]->insert_from(*key_columns[j], i);
            }
            limit_heap.emplace(limit_columns[0]->size() - 1, limit_columns, order_directions,
                               null_directions);
            limit_heap.pop();
            limit_columns_min = limit_heap.top()._row_id;
            break;
        }
    }
}

bool StreamingAggLocalState::_do_limit_filter(size_t num_rows, ColumnRawPtrs& key_columns) {
    SCOPED_TIMER(_hash_table_limit_compute_timer);
    if (num_rows) {
        cmp_res.resize(num_rows);
        need_computes.resize(num_rows);
        memset(need_computes.data(), 0, need_computes.size());
        memset(cmp_res.data(), 0, cmp_res.size());

        const auto key_size = null_directions.size();
        for (int i = 0; i < key_size; i++) {
            key_columns[i]->compare_internal(limit_columns_min, *limit_columns[i],
                                             null_directions[i], order_directions[i], cmp_res,
                                             need_computes.data());
        }

        auto set_computes_arr = [](auto* __restrict res, auto* __restrict computes, size_t rows) {
            for (size_t i = 0; i < rows; ++i) {
                computes[i] = computes[i] == res[i];
            }
        };
        set_computes_arr(cmp_res.data(), need_computes.data(), num_rows);

        return std::find(need_computes.begin(), need_computes.end(), 0) != need_computes.end();
    }

    return false;
}

StreamingAggOperatorX::StreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs)
        : StatefulOperatorX<StreamingAggLocalState>(pool, tnode, operator_id, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _agg_fn_output_row_descriptor(descs, tnode.row_tuples) {}

void StreamingAggOperatorX::update_operator(const TPlanNode& tnode,
                                            bool followed_by_shuffled_operator,
                                            bool require_bucket_distribution) {
    _followed_by_shuffled_operator = followed_by_shuffled_operator;
    _require_bucket_distribution = require_bucket_distribution;
    _partition_exprs =
            tnode.__isset.distribute_expr_lists &&
                            (StatefulOperatorX<
                                     StreamingAggLocalState>::_followed_by_shuffled_operator ||
                             std::any_of(
                                     tnode.agg_node.aggregate_functions.begin(),
                                     tnode.agg_node.aggregate_functions.end(),
                                     [](const TExpr& texpr) -> bool {
                                         return texpr.nodes[0].fn.name.function_name.starts_with(
                                                 DISTINCT_FUNCTION_PREFIX);
                                     }))
                    ? tnode.distribute_expr_lists[0]
                    : tnode.agg_node.grouping_exprs;
}

Status StreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit = tnode.agg_node.aggregate_functions.empty();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                tnode.agg_node.grouping_exprs.empty(), false, &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    auto is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                                [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    if (is_merge || _needs_finalize) {
        return Status::InvalidArgument(
                "StreamingAggLocalState only support no merge and no finalize, "
                "but got is_merge={}, needs_finalize={}",
                is_merge, _needs_finalize);
    }

    // Handle sort limit
    if (tnode.agg_node.__isset.agg_sort_info_by_group_key) {
        _sort_limit = _limit;
        _limit = -1;
        _do_sort_limit = true;
        const auto& agg_sort_info = tnode.agg_node.agg_sort_info_by_group_key;
        DCHECK_EQ(agg_sort_info.nulls_first.size(), agg_sort_info.is_asc_order.size());

        const size_t order_by_key_size = agg_sort_info.is_asc_order.size();
        _order_directions.resize(order_by_key_size);
        _null_directions.resize(order_by_key_size);
        for (int i = 0; i < order_by_key_size; ++i) {
            _order_directions[i] = agg_sort_info.is_asc_order[i] ? 1 : -1;
            _null_directions[i] =
                    agg_sort_info.nulls_first[i] ? -_order_directions[i] : _order_directions[i];
        }
    }

    _op_name = "STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status StreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::prepare(state));

    RETURN_IF_ERROR(_init_probe_expr_ctx(state));

    RETURN_IF_ERROR(_init_aggregate_evaluators(state));

    RETURN_IF_ERROR(_calc_aggregate_evaluators());

    return Status::OK();
}

Status StreamingAggOperatorX::_init_probe_expr_ctx(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    return Status::OK();
}

Status StreamingAggOperatorX::_init_aggregate_evaluators(RuntimeState* state) {
    size_t j = _probe_expr_ctxs.size();
    for (size_t i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, _child->row_desc(), intermediate_slot_desc, output_slot_desc));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }
    return Status::OK();
}

Status StreamingAggOperatorX::_calc_aggregate_evaluators() {
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

Status StreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    _output_blocks.clear();
    PODArray<AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(Overload {[&](std::monostate& arg) -> void {
                                 // Do nothing
                             },
                             [&](auto& agg_method) {
                                 COUNTER_SET(_hash_table_size_counter,
                                             int64_t(agg_method.hash_table->size()));
                             }},
                   _agg_data->method_variant);
    }
    _close_with_serialized_key();
    _output_blocks.clear();
    _pending_output_columns.clear();
    _pending_output_rows = 0;
    _agg_arena_pool.clear(true);
    return Base::close(state);
}

Status StreamingAggOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    if (!local_state._output_blocks.empty()) {
        // Pop a block from the output queue produced during the last push.
        *block = std::move(local_state._output_blocks.front());
        local_state._output_blocks.pop_front();
    } else if (local_state._child_eos) {
        // Child is done — drain whatever remains in the hash table.
        RETURN_IF_ERROR(local_state._get_results_with_serialized_key(state, block, eos));
        local_state.make_nullable_output_key(block);
    }
    // dispose the having clause, should not be executed in prestreaming agg
    RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    local_state._cur_num_rows_returned += block->rows();
    local_state.reached_limit(block, eos);

    return Status::OK();
}

Status StreamingAggOperatorX::push(RuntimeState* state, Block* in_block, bool eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state.do_pre_agg(state, in_block));
    }
    // Flush any remaining pending output rows into the output queue so that
    // pull() can return them.  This ensures the pending buffer does not hold
    // data across push/pull boundaries.
    local_state._finalize_pending_block(true);
    local_state._child_eos = eos;
    in_block->clear_column_data(_child->row_desc().num_materialized_slots());
    return Status::OK();
}

bool StreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._output_blocks.empty() && !local_state._child_eos;
}

#include "common/compile_check_end.h"
} // namespace doris
