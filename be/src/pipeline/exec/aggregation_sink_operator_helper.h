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

#pragma once

#include <cstdint>
#include <type_traits>

#include "operator.h"
#include "pipeline/exec/aggregation_operator_helper.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {

namespace pipeline {
template <typename Derived, typename OperatorX>
class AggSinkLocalStateHelper : public AggLocalStateHelper<Derived, OperatorX> {
public:
    AggSinkLocalStateHelper(Derived* derived) : AggLocalStateHelper<Derived, OperatorX>(derived) {}
    ~AggSinkLocalStateHelper() = default;
    using AggLocalStateHelper<Derived, OperatorX>::_get_hash_table_size;
    using AggLocalStateHelper<Derived, OperatorX>::_find_in_hash_table;
    using AggLocalStateHelper<Derived, OperatorX>::_emplace_into_hash_table;
    using AggLocalStateHelper<Derived, OperatorX>::_shared_state;
    using AggLocalStateHelper<Derived, OperatorX>::_derived;
    using AggLocalStateHelper<Derived, OperatorX>::_operator;
    using AggLocalStateHelper<Derived, OperatorX>::_create_agg_status;
    using AggLocalStateHelper<Derived, OperatorX>::_destroy_agg_status;
    using AggLocalStateHelper<Derived, OperatorX>::_init_hash_method;

    Status _execute_without_key(vectorized::Block* block) {
        DCHECK(_shared_state()->agg_data->without_key != nullptr);
        SCOPED_TIMER(_derived()->_build_timer);
        for (int i = 0; i < _shared_state()->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_shared_state()->aggregate_evaluators[i]->execute_single_add(
                    block,
                    _shared_state()->agg_data->without_key +
                            _operator().offsets_of_aggregate_states[i],
                    _shared_state()->agg_arena_pool.get()));
        }
        return Status::OK();
    }
    Status _execute_with_serialized_key(vectorized::Block* block) {
        if (_derived()->_reach_limit) {
            return _execute_with_serialized_key_helper<true>(block);
        } else {
            return _execute_with_serialized_key_helper<false>(block);
        }
    }
    template <bool limit>
    Status _execute_with_serialized_key_helper(vectorized::Block* block) {
        SCOPED_TIMER(_derived()->_build_timer);
        auto& _probe_expr_ctxs = _shared_state()->probe_expr_ctxs;
        auto& _places = _derived()->_places;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        auto& agg_arena_pool = _shared_state()->agg_arena_pool;
        DCHECK(!_probe_expr_ctxs.empty());

        size_t key_size = _probe_expr_ctxs.size();
        vectorized::ColumnRawPtrs key_columns(key_size);
        {
            SCOPED_TIMER(_derived()->_expr_timer);
            for (size_t i = 0; i < key_size; ++i) {
                int result_column_id = -1;
                RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
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

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add_selected(
                        block, _operator().offsets_of_aggregate_states[i], _places.data(),
                        agg_arena_pool.get()));
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add(
                        block, _operator().offsets_of_aggregate_states[i], _places.data(),
                        agg_arena_pool.get()));
            }

            if (_derived()->_should_limit_output) {
                _derived()->_reach_limit = _get_hash_table_size() >= _operator()._limit;
                if (_derived()->_reach_limit && _operator()._can_short_circuit) {
                    _derived()->_dependency->set_ready_to_read();
                    return Status::Error<ErrorCode::END_OF_FILE>("");
                }
            }
        }

        return Status::OK();
    }
    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    int _get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
        auto ctxs = evaluator->input_exprs_ctxs();
        CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
                << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
                << ctxs[0]->root()->debug_string();
        return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
    }

    Status _merge_without_key(vectorized::Block* block) {
        SCOPED_TIMER(_derived()->_merge_timer);
        auto& agg_data = _shared_state()->agg_data;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        auto& agg_arena_pool = _shared_state()->agg_arena_pool;
        DCHECK(agg_data->without_key != nullptr);
        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            if (aggregate_evaluators[i]->is_merge()) {
                int col_id = _get_slot_column_id(aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                SCOPED_TIMER(_derived()->_deserialize_data_timer);
                aggregate_evaluators[i]->function()->deserialize_and_merge_from_column(
                        agg_data->without_key + _operator().offsets_of_aggregate_states[i], *column,
                        agg_arena_pool.get());
            } else {
                RETURN_IF_ERROR(aggregate_evaluators[i]->execute_single_add(
                        block, agg_data->without_key + _operator().offsets_of_aggregate_states[i],
                        agg_arena_pool.get()));
            }
        }
        return Status::OK();
    }

    Status _merge_with_serialized_key(vectorized::Block* block) {
        if (_derived()->_reach_limit) {
            return _merge_with_serialized_key_helper<true, false>(block);
        } else {
            return _merge_with_serialized_key_helper<false, false>(block);
        }
    }

    template <bool limit, bool for_spill>
    Status _merge_with_serialized_key_helper(vectorized::Block* block) {
        SCOPED_TIMER(_derived()->_merge_timer);
        auto& probe_expr_ctxs = _shared_state()->probe_expr_ctxs;
        auto& _places = _derived()->_places;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        auto& agg_arena_pool = _shared_state()->agg_arena_pool;
        auto _deserialize_data_timer = _derived()->_deserialize_data_timer;
        auto& _deserialize_buffer = _derived()->_deserialize_buffer;
        size_t key_size = probe_expr_ctxs.size();
        vectorized::ColumnRawPtrs key_columns(key_size);

        for (size_t i = 0; i < key_size; ++i) {
            if constexpr (for_spill) {
                key_columns[i] = block->get_by_position(i).column.get();
            } else {
                int result_column_id = -1;
                RETURN_IF_ERROR(probe_expr_ctxs[i]->execute(block, &result_column_id));
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

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                if (aggregate_evaluators[i]->is_merge()) {
                    int col_id = _get_slot_column_id(aggregate_evaluators[i]);
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((vectorized::ColumnNullable*)column.get())
                                         ->get_nested_column_ptr();
                    }

                    size_t buffer_size = aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        SCOPED_TIMER(_deserialize_data_timer);
                        aggregate_evaluators[i]->function()->deserialize_and_merge_vec_selected(
                                _places.data(), _operator().offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(),
                                (vectorized::ColumnString*)(column.get()), agg_arena_pool.get(),
                                rows);
                    }
                } else {
                    RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add_selected(
                            block, _operator().offsets_of_aggregate_states[i], _places.data(),
                            agg_arena_pool.get()));
                }
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                if (aggregate_evaluators[i]->is_merge() || for_spill) {
                    int col_id = 0;
                    if constexpr (for_spill) {
                        col_id = probe_expr_ctxs.size() + i;
                    } else {
                        col_id = _get_slot_column_id(aggregate_evaluators[i]);
                    }
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((vectorized::ColumnNullable*)column.get())
                                         ->get_nested_column_ptr();
                    }

                    size_t buffer_size = aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        SCOPED_TIMER(_deserialize_data_timer);
                        aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                                _places.data(), _operator().offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(),
                                (vectorized::ColumnString*)(column.get()), agg_arena_pool.get(),
                                rows);
                    }
                } else {
                    RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add(
                            block, _operator().offsets_of_aggregate_states[i], _places.data(),
                            agg_arena_pool.get()));
                }
            }

            if (_derived()->_should_limit_output) {
                _derived()->_reach_limit = _get_hash_table_size() >= _operator()._limit;
            }
        }

        return Status::OK();
    }

    void _update_memusage_without_key() {
        auto& agg_arena_pool = _shared_state()->agg_arena_pool;
        auto& _mem_usage_record = _derived()->_mem_usage_record;
        auto arena_memory_usage =
                agg_arena_pool->size() - _derived()->_mem_usage_record.used_in_arena;
        _derived()->mem_tracker()->consume(arena_memory_usage);
        _derived()->_serialize_key_arena_memory_usage->add(arena_memory_usage);
        _mem_usage_record.used_in_arena = agg_arena_pool->size();
    }
    void _update_memusage_with_serialized_key() {
        auto& agg_arena_pool = _shared_state()->agg_arena_pool;
        auto& _mem_usage_record = _derived()->_mem_usage_record;
        auto& aggregate_data_container = _shared_state()->aggregate_data_container;
        auto& agg_data = _shared_state()->agg_data;
        std::visit(
                [&](auto&& agg_method) -> void {
                    auto& data = *agg_method.hash_table;
                    auto arena_memory_usage = agg_arena_pool->size() +
                                              aggregate_data_container->memory_usage() -
                                              _mem_usage_record.used_in_arena;
                    _derived()->mem_tracker()->consume(arena_memory_usage);
                    _derived()->mem_tracker()->consume(data.get_buffer_size_in_bytes() -
                                                       _mem_usage_record.used_in_state);
                    _derived()->_serialize_key_arena_memory_usage->add(arena_memory_usage);
                    COUNTER_UPDATE(
                            _derived()->_hash_table_memory_usage,
                            data.get_buffer_size_in_bytes() - _mem_usage_record.used_in_state);
                    _mem_usage_record.used_in_state = data.get_buffer_size_in_bytes();
                    _mem_usage_record.used_in_arena =
                            agg_arena_pool->size() + aggregate_data_container->memory_usage();
                },
                agg_data->method_variant);
    }
};
}; // namespace pipeline

}; // namespace doris
