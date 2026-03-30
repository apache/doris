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

#include "exec/common/inline_count_agg_context.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/column/column_fixed_length_object.h"
#include "exec/common/agg_context_utils.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/aggregate/aggregate_function_count.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {

// ==================== Hash table write ====================

void InlineCountAggContext::emplace_into_hash_table(AggregateDataPtr* /*places*/,
                                                    ColumnRawPtrs& key_columns,
                                                    uint32_t num_rows,
                                                    RuntimeProfile::Counter* hash_table_compute_timer,
                                                    RuntimeProfile::Counter* hash_table_emplace_timer,
                                                    RuntimeProfile::Counter* hash_table_input_counter) {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        SCOPED_TIMER(hash_table_compute_timer);
        using HashMethodType = std::decay_t<decltype(agg_method)>;
        using AggState = typename HashMethodType::State;
        AggState state(key_columns);
        agg_method.init_serialized_keys(key_columns, num_rows);

        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            HashMethodType::try_presis_key_and_origin(key, origin, _agg_arena);
            AggregateDataPtr mapped = nullptr;
            ctor(key, mapped);
        };

        auto creator_for_null_key = [&](auto& mapped) { mapped = nullptr; };

        SCOPED_TIMER(hash_table_emplace_timer);
        lazy_emplace_batch(agg_method, state, num_rows, creator,
                           creator_for_null_key, [&](uint32_t, auto& mapped) {
                               ++reinterpret_cast<UInt64&>(mapped);
                           });

        COUNTER_UPDATE(hash_table_input_counter, num_rows);
    });
}

// ==================== Aggregation execution ====================

Status InlineCountAggContext::update(Block* block) {
    memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&memory_usage_last_executing);
    SCOPED_TIMER(_build_timer);
    DCHECK(!block->empty());

    ColumnRawPtrs key_columns(_groupby_expr_ctxs.size());
    RETURN_IF_ERROR(evaluate_groupby_keys(block, key_columns));

    // InlineCount: emplace all keys, count is incremented inside emplace.
    // No evaluator execution needed.
    emplace_into_hash_table(nullptr, key_columns, cast_set<uint32_t>(block->rows()),
                            _hash_table_compute_timer, _hash_table_emplace_timer,
                            _hash_table_input_counter);

    return Status::OK();
}

Status InlineCountAggContext::emplace_and_forward(AggregateDataPtr* places,
                                                  ColumnRawPtrs& key_columns, uint32_t num_rows,
                                                  Block* block, bool expand_hash_table) {
    // InlineCount: emplace increments UInt64 count directly, no execute_batch_add needed.
    emplace_into_hash_table(places, key_columns, num_rows, _hash_table_compute_timer,
                            _hash_table_emplace_timer, _hash_table_input_counter);
    return Status::OK();
}

Status InlineCountAggContext::merge(Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(!block->empty());

    size_t key_size = _groupby_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    RETURN_IF_ERROR(evaluate_groupby_keys(block, key_columns));

    const auto rows = block->rows();

    // Get the serialized count column (ColumnFixedLengthObject containing AggregateFunctionCountData)
    DCHECK_EQ(_agg_evaluators.size(), 1);
    auto col_id = get_slot_column_id(_agg_evaluators[0]);
    auto column = block->get_by_position(col_id).column;

    _merge_inline_count(key_columns, column.get(), cast_set<uint32_t>(rows));

    return Status::OK();
}

void InlineCountAggContext::_merge_inline_count(ColumnRawPtrs& key_columns,
                                                const IColumn* merge_column,
                                                uint32_t num_rows) {
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        SCOPED_TIMER(_hash_table_compute_timer);
        using HashMethodType = std::decay_t<decltype(agg_method)>;
        using AggState = typename HashMethodType::State;
        AggState state(key_columns);
        agg_method.init_serialized_keys(key_columns, num_rows);

        const auto& col =
                assert_cast<const ColumnFixedLengthObject&>(*merge_column);
        const auto* col_data =
                reinterpret_cast<const AggregateFunctionCountData*>(
                        col.get_data().data());

        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            HashMethodType::try_presis_key_and_origin(key, origin, _agg_arena);
            AggregateDataPtr mapped = nullptr;
            ctor(key, mapped);
        };

        auto creator_for_null_key = [&](auto& mapped) { mapped = nullptr; };

        SCOPED_TIMER(_hash_table_emplace_timer);
        lazy_emplace_batch(agg_method, state, num_rows, creator,
                           creator_for_null_key, [&](uint32_t i, auto& mapped) {
                               reinterpret_cast<UInt64&>(mapped) +=
                                       col_data[i].count;
                           });

        COUNTER_UPDATE(_hash_table_input_counter, num_rows);
    });
}

// ==================== Result output ====================

Status InlineCountAggContext::serialize(RuntimeState* state, Block* block,
                                                     bool* eos) {
    SCOPED_TIMER(_get_results_timer);
    size_t key_size = _groupby_expr_ctxs.size();
    DCHECK_EQ(_agg_evaluators.size(), 1);

    bool mem_reuse = make_nullable_keys.empty() && block->mem_reuse();

    auto key_columns = agg_context_utils::take_or_create_columns(
            block, mem_reuse, 0, key_size,
            [&](size_t i) { return _groupby_expr_ctxs[i]->root()->data_type()->create_column(); });

    MutableColumnPtr value_column;
    DataTypePtr value_data_type = _agg_evaluators[0]->function()->get_serialized_type();
    if (mem_reuse) {
        value_column = std::move(*block->get_by_position(key_size).column).mutate();
    } else {
        value_column = _agg_evaluators[0]->function()->create_serialize_column();
    }

    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        agg_method.init_iterator();
        auto& data = *agg_method.hash_table;
        const auto size = std::min(data.size(), size_t(state->batch_size()));
        using KeyType = std::decay_t<decltype(agg_method)>::Key;
        std::vector<KeyType> keys(size);

        auto& count_col =
                assert_cast<ColumnFixedLengthObject&>(*value_column);
        uint32_t num_rows = 0;
        {
            SCOPED_TIMER(_hash_table_iterate_timer);
            auto& it = agg_method.begin;
            while (it != agg_method.end && num_rows < state->batch_size()) {
                keys[num_rows] = it.get_first();
                auto inline_count =
                        std::bit_cast<UInt64>(it.get_second());
                count_col.insert_data(
                        reinterpret_cast<const char*>(&inline_count),
                        sizeof(UInt64));
                ++it;
                ++num_rows;
            }
        }

        {
            SCOPED_TIMER(_insert_keys_to_column_timer);
            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
        }

        // Handle null key if present
        if (agg_method.begin == agg_method.end) {
            if (agg_method.hash_table->has_null_key_data()) {
                DCHECK(key_columns.size() == 1);
                DCHECK(key_columns[0]->is_nullable());
                if (num_rows < state->batch_size()) {
                    key_columns[0]->insert_data(nullptr, 0);
                    auto mapped =
                            agg_method.hash_table->template get_null_key_data<
                                    AggregateDataPtr>();
                    auto inline_count =
                            std::bit_cast<UInt64>(mapped);
                    count_col.insert_data(
                            reinterpret_cast<const char*>(&inline_count),
                            sizeof(UInt64));
                    *eos = true;
                }
            } else {
                *eos = true;
            }
        }
    });

    if (!mem_reuse) {
        MutableColumns value_columns;
        value_columns.emplace_back(std::move(value_column));
        DataTypes value_types {value_data_type};
        agg_context_utils::build_serialized_output_block(block, key_columns, _groupby_expr_ctxs,
                                                        value_columns, value_types);
    }

    return Status::OK();
}

Status InlineCountAggContext::finalize(
        RuntimeState* state, Block* block, bool* eos) {
    bool mem_reuse = make_nullable_keys.empty() && block->mem_reuse();

    size_t key_size = _groupby_expr_ctxs.size();

    auto key_columns = agg_context_utils::take_or_create_columns(
            block, mem_reuse, 0, key_size,
            [&](size_t i) { return _finalize_schema[i].type->create_column(); });
    MutableColumnPtr value_column;
    if (!mem_reuse) {
        value_column = _finalize_schema[key_size].type->create_column();
    } else {
        value_column = std::move(*block->get_by_position(key_size).column).mutate();
    }

    SCOPED_TIMER(_get_results_timer);
    agg_context_utils::visit_agg_method(*_hash_table_data, [&](auto& agg_method) {
        auto& data = *agg_method.hash_table;
        agg_method.init_iterator();
        const auto size = std::min(data.size(), size_t(state->batch_size()));
        using KeyType = std::decay_t<decltype(agg_method)>::Key;
        std::vector<KeyType> keys(size);

        DCHECK_EQ(_agg_evaluators.size(), 1);
        auto& count_column = assert_cast<ColumnInt64&>(*value_column);
        uint32_t num_rows = 0;
        {
            SCOPED_TIMER(_hash_table_iterate_timer);
            auto& it = agg_method.begin;
            while (it != agg_method.end && num_rows < state->batch_size()) {
                keys[num_rows] = it.get_first();
                auto& mapped = it.get_second();
                count_column.insert_value(static_cast<Int64>(
                        std::bit_cast<UInt64>(mapped)));
                ++it;
                ++num_rows;
            }
        }
        {
            SCOPED_TIMER(_insert_keys_to_column_timer);
            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
        }

        // Handle null key if present
        if (agg_method.begin == agg_method.end) {
            if (agg_method.hash_table->has_null_key_data()) {
                DCHECK(key_columns.size() == 1);
                DCHECK(key_columns[0]->is_nullable());
                if (key_columns[0]->size() < state->batch_size()) {
                    key_columns[0]->insert_data(nullptr, 0);
                    auto mapped =
                            agg_method.hash_table->template get_null_key_data<
                                    AggregateDataPtr>();
                    count_column.insert_value(
                            static_cast<Int64>(std::bit_cast<UInt64>(mapped)));
                    *eos = true;
                }
            } else {
                *eos = true;
            }
        }
    });

    if (!mem_reuse) {
        MutableColumns value_columns;
        value_columns.emplace_back(std::move(value_column));
        agg_context_utils::assemble_finalized_output(block, _finalize_schema, key_columns,
                                                    value_columns, key_size);
    }

    return Status::OK();
}

// ==================== Agg state management ====================

Status InlineCountAggContext::create_agg_state(AggregateDataPtr /*data*/) {
    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                           "InlineCountAggContext should never create agg state");
}

void InlineCountAggContext::destroy_agg_state(AggregateDataPtr /*data*/) {
    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                           "InlineCountAggContext should never destroy agg state");
}

void InlineCountAggContext::close() {
    // InlineCount stores UInt64 directly in mapped slots, not real agg state pointers.
    // Skip agg state destruction — the hash table memory is managed by AggregatedDataVariants.
}

Status InlineCountAggContext::reset_hash_table() {
    return agg_context_utils::visit_agg_method<Status>(
            *_hash_table_data, [&](auto& agg_method) -> Status {
                auto& hash_table = *agg_method.hash_table;
                using HashTableType = std::decay_t<decltype(hash_table)>;

                agg_method.arena.clear();
                agg_method.inited_iterator = false;

                // No agg state to destroy — mapped slots hold UInt64 counts.
                // No AggregateDataContainer to reset either.
                agg_method.hash_table.reset(new HashTableType());
                return Status::OK();
            });
}

} // namespace doris
