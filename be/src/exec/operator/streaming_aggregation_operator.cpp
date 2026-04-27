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
#include "core/column/column_fixed_length_object.h"
#include "exec/operator/operator.h"
#include "exec/operator/streaming_agg_min_reduction.h"
#include "exprs/aggregate/aggregate_function_count.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vslot_ref.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris {

StreamingAggLocalState::StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _agg_data(std::make_unique<AggregatedDataVariants>()),
          _child_block(Block::create_unique()),
          _pre_aggregated_block(Block::create_unique()),
          _is_single_backend(state->get_query_ctx()->is_single_backend_query()) {}

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
    _build_timer = ADD_TIMER(custom_profile(), "BuildTime");
    _expr_timer = ADD_TIMER(Base::custom_profile(), "ExprTime");
    _get_results_timer = ADD_TIMER(custom_profile(), "GetResultsTime");
    _hash_table_iterate_timer = ADD_TIMER(custom_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");

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

    // Determine whether to use simple count aggregation.
    // StreamingAgg only operates in update + serialize mode: input is raw data, output is serialized intermediate state.
    // The serialization format of count is UInt64 itself, so it can be inlined into the hash table mapped slot.
    if (_aggregate_evaluators.size() == 1 &&
        _aggregate_evaluators[0]->function()->is_simple_count() && p._sort_limit == -1) {
        _use_simple_count = true;
#ifndef NDEBUG
        // Randomly enable/disable in debug mode to verify correctness of multi-phase agg promotion/demotion.
        _use_simple_count = rand() % 2 == 0;
#endif
    }

    std::visit(
            Overload {[&](std::monostate& arg) -> void {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& agg_method) {
                          using HashTableType = std::decay_t<decltype(agg_method)>;
                          using KeyType = typename HashTableType::Key;

                          if (!_use_simple_count) {
                              /// some aggregate functions (like AVG for decimal) have align issues.
                              _aggregate_data_container = std::make_unique<AggregateDataContainer>(
                                      sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                         p._align_aggregate_states - 1) /
                                                        p._align_aggregate_states) *
                                                               p._align_aggregate_states);
                          }
                      }},
            _agg_data->method_variant);

    limit = p._sort_limit;
    do_sort_limit = p._do_sort_limit;
    null_directions = p._null_directions;
    order_directions = p._order_directions;

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
                             int64_t arena_memory_usage =
                                     _agg_arena_pool.size() +
                                     (_aggregate_data_container
                                              ? _aggregate_data_container->memory_usage()
                                              : 0);
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

Status StreamingAggLocalState::do_pre_agg(RuntimeState* state, Block* input_block,
                                          Block* output_block) {
    if (low_memory_mode()) {
        auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
        p.set_low_memory_mode(state);
    }
    RETURN_IF_ERROR(_pre_agg_with_serialized_key(input_block, output_block));

    // pre stream agg need use _num_row_return to decide whether to do pre stream agg
    _cur_num_rows_returned += output_block->rows();
    make_nullable_output_key(output_block);
    _update_memusage_with_serialized_key();
    return Status::OK();
}

bool StreamingAggLocalState::_should_expand_preagg_hash_tables() {
    if (!_should_expand_hash_table) {
        return false;
    }

    return std::visit(
            Overload {
                    [&](std::monostate& arg) -> bool {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                        return false;
                    },
                    [&](auto& agg_method) -> bool {
                        auto& hash_tbl = *agg_method.hash_table;
                        auto [ht_mem, ht_rows] =
                                std::pair {hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

                        // Need some rows in tables to have valid statistics.
                        if (ht_rows == 0) {
                            return true;
                        }

                        const auto* reduction = _is_single_backend
                                                        ? SINGLE_BE_STREAMING_HT_MIN_REDUCTION
                                                        : STREAMING_HT_MIN_REDUCTION;

                        // Find the appropriate reduction factor in our table for the current hash table sizes.
                        int cache_level = 0;
                        while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
                               ht_mem >= reduction[cache_level + 1].min_ht_mem) {
                            ++cache_level;
                        }

                        // Compare the number of rows in the hash table with the number of input rows that
                        // were aggregated into it. Exclude passed through rows from this calculation since
                        // they were not in hash tables.
                        const int64_t input_rows = _input_num_rows;
                        const int64_t aggregated_input_rows = input_rows - _cur_num_rows_returned;
                        // TODO chenhao
                        //  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
                        double current_reduction = static_cast<double>(aggregated_input_rows) /
                                                   static_cast<double>(ht_rows);

                        // TODO: workaround for IMPALA-2490: subplan node rows_returned counter may be
                        // inaccurate, which could lead to a divide by zero below.
                        if (aggregated_input_rows <= 0) {
                            return true;
                        }

                        // Extrapolate the current reduction factor (r) using the formula
                        // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
                        // set, N is the number of input rows, excluding passed-through rows, and n is the
                        // number of rows inserted or merged into the hash tables. This is a very rough
                        // approximation but is good enough to be useful.
                        // TODO: consider collecting more statistics to better estimate reduction.
                        //  double estimated_reduction = aggregated_input_rows >= expected_input_rows
                        //      ? current_reduction
                        //      : 1 + (expected_input_rows / aggregated_input_rows) * (current_reduction - 1);
                        double min_reduction = reduction[cache_level].streaming_ht_min_reduction;

                        //  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
                        //    COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
                        //  return estimated_reduction > min_reduction;
                        _should_expand_hash_table = current_reduction > min_reduction;
                        return _should_expand_hash_table;
                    }},
            _agg_data->method_variant);
}

size_t StreamingAggLocalState::_memory_usage() const {
    size_t usage = 0;
    usage += _agg_arena_pool.size();

    if (_aggregate_data_container) {
        usage += _aggregate_data_container->memory_usage();
    }

    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) {
                             usage += agg_method.hash_table->get_buffer_size_in_bytes();
                         }},
               _agg_data->method_variant);

    return usage;
}

bool StreamingAggLocalState::_should_not_do_pre_agg(size_t rows) {
    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In either case we should always use the remaining space in the hash table
    // to avoid wasting memory.
    // But for fixed hash map, it never need to expand
    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
    bool ret_flag = false;
    const auto spill_streaming_agg_mem_limit = p._spill_streaming_agg_mem_limit;
    const bool used_too_much_memory =
            spill_streaming_agg_mem_limit > 0 && _memory_usage() > spill_streaming_agg_mem_limit;
    std::visit(
            Overload {
                    [&](std::monostate& arg) {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) {
                        auto& hash_tbl = *agg_method.hash_table;
                        /// If too much memory is used during the pre-aggregation stage,
                        /// it is better to output the data directly without performing further aggregation.
                        // do not try to do agg, just init and serialize directly return the out_block
                        if (used_too_much_memory || (hash_tbl.add_elem_size_overflow(rows) &&
                                                     !_should_expand_preagg_hash_tables())) {
                            SCOPED_TIMER(_streaming_agg_timer);
                            ret_flag = true;
                        }
                    }},
            _agg_data->method_variant);

    return ret_flag;
}

Status StreamingAggLocalState::_pre_agg_with_serialized_key(doris::Block* in_block,
                                                            doris::Block* out_block) {
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
    _places.resize(rows);

    if (_should_not_do_pre_agg(rows)) {
        if (limit > 0) {
            DCHECK(do_sort_limit);
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
        bool mem_reuse = p._make_nullable_keys.empty() && out_block->mem_reuse();

        std::vector<DataTypePtr> data_types;
        MutableColumns value_columns;
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            auto data_type = _aggregate_evaluators[i]->function()->get_serialized_type();
            if (mem_reuse) {
                value_columns.emplace_back(
                        std::move(*out_block->get_by_position(i + key_size).column).mutate());
            } else {
                value_columns.emplace_back(
                        _aggregate_evaluators[i]->function()->create_serialize_column());
            }
            data_types.emplace_back(data_type);
        }

        for (int i = 0; i != _aggregate_evaluators.size(); ++i) {
            SCOPED_TIMER(_insert_values_to_column_timer);
            RETURN_IF_ERROR(_aggregate_evaluators[i]->streaming_agg_serialize_to_column(
                    in_block, value_columns[i], rows, _agg_arena_pool));
        }

        if (!mem_reuse) {
            ColumnsWithTypeAndName columns_with_schema;
            for (int i = 0; i < key_size; ++i) {
                columns_with_schema.emplace_back(key_columns[i]->clone_resized(rows),
                                                 _probe_expr_ctxs[i]->root()->data_type(),
                                                 _probe_expr_ctxs[i]->root()->expr_name());
            }
            for (int i = 0; i < value_columns.size(); ++i) {
                columns_with_schema.emplace_back(std::move(value_columns[i]), data_types[i], "");
            }
            out_block->swap(Block(columns_with_schema));
        } else {
            for (int i = 0; i < key_size; ++i) {
                std::move(*out_block->get_by_position(i).column)
                        .mutate()
                        ->insert_range_from(*key_columns[i], 0, rows);
            }
        }
    } else {
        bool need_agg = true;
        if (need_do_sort_limit != 1) {
            if (_use_simple_count) {
                _emplace_into_hash_table_inline_count(key_columns, rows);
                need_agg = false;
            } else {
                _emplace_into_hash_table(_places.data(), key_columns, rows);
            }
        } else {
            need_agg = _emplace_into_hash_table_limit(_places.data(), in_block, key_columns, rows);
        }

        if (need_agg) {
            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                        in_block, p._offsets_of_aggregate_states[i], _places.data(),
                        _agg_arena_pool, _should_expand_hash_table));
            }
            if (limit > 0 && need_do_sort_limit == -1 && _get_hash_table_size() >= limit) {
                need_do_sort_limit = 1;
                build_limit_heap(_get_hash_table_size());
            }
        }
    }

    return Status::OK();
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

                        if (_use_simple_count) {
                            DCHECK_EQ(_aggregate_evaluators.size(), 1);

                            value_data_types[0] =
                                    _aggregate_evaluators[0]->function()->get_serialized_type();
                            if (mem_reuse) {
                                value_columns[0] =
                                        std::move(*block->get_by_position(key_size).column)
                                                .mutate();
                            } else {
                                value_columns[0] = _aggregate_evaluators[0]
                                                           ->function()
                                                           ->create_serialize_column();
                            }

                            auto& count_col =
                                    assert_cast<ColumnFixedLengthObject&>(*value_columns[0]);
                            uint32_t num_rows = 0;
                            {
                                SCOPED_TIMER(_hash_table_iterate_timer);
                                auto& it = agg_method.begin;
                                while (it != agg_method.end && num_rows < state->batch_size()) {
                                    keys[num_rows] = it.get_first();
                                    auto inline_count =
                                            reinterpret_cast<const UInt64&>(it.get_second());
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
                                        count_col.resize(num_rows + 1);
                                        *reinterpret_cast<UInt64*>(count_col.get_data().data() +
                                                                   num_rows * sizeof(UInt64)) =
                                                std::bit_cast<UInt64>(mapped);
                                        *eos = true;
                                    }
                                } else {
                                    *eos = true;
                                }
                            }
                            return;
                        }

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

void StreamingAggLocalState::_refresh_limit_heap(size_t i, ColumnRawPtrs& key_columns) {
    for (int j = 0; j < key_columns.size(); ++j) {
        limit_columns[j]->insert_from(*key_columns[j], i);
    }
    limit_heap.emplace(limit_columns[0]->size() - 1, limit_columns, order_directions,
                       null_directions);
    limit_heap.pop();
    limit_columns_min = limit_heap.top()._row_id;
}

bool StreamingAggLocalState::_emplace_into_hash_table_limit(AggregateDataPtr* places, Block* block,
                                                            ColumnRawPtrs& key_columns,
                                                            uint32_t num_rows) {
    return std::visit(
            Overload {[&](std::monostate& arg) {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                          return true;
                      },
                      [&](auto&& agg_method) -> bool {
                          SCOPED_TIMER(_hash_table_compute_timer);
                          using HashMethodType = std::decay_t<decltype(agg_method)>;
                          using AggState = typename HashMethodType::State;

                          bool need_filter = _do_limit_filter(num_rows, key_columns);
                          if (auto need_agg =
                                      std::find(need_computes.begin(), need_computes.end(), 1);
                              need_agg != need_computes.end()) {
                              if (need_filter) {
                                  Block::filter_block_internal(block, need_computes);
                                  num_rows = (uint32_t)block->rows();
                              }

                              AggState state(key_columns);
                              agg_method.init_serialized_keys(key_columns, num_rows);
                              size_t i = 0;

                              auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                  try {
                                      HashMethodType::try_presis_key_and_origin(key, origin,
                                                                                _agg_arena_pool);
                                      auto mapped = _aggregate_data_container->append_data(origin);
                                      auto st = _create_agg_status(mapped);
                                      if (!st) {
                                          throw Exception(st.code(), st.to_string());
                                      }
                                      ctor(key, mapped);
                                      _refresh_limit_heap(i, key_columns);
                                  } catch (...) {
                                      // Exception-safety - if it can not allocate memory or create status,
                                      // the destructors will not be called.
                                      ctor(key, nullptr);
                                      throw;
                                  }
                              };

                              auto creator_for_null_key = [&](auto& mapped) {
                                  mapped = _agg_arena_pool.aligned_alloc(
                                          Base::_parent->template cast<StreamingAggOperatorX>()
                                                  ._total_size_of_aggregate_states,
                                          Base::_parent->template cast<StreamingAggOperatorX>()
                                                  ._align_aggregate_states);
                                  auto st = _create_agg_status(mapped);
                                  if (!st) {
                                      throw Exception(st.code(), st.to_string());
                                  }
                                  _refresh_limit_heap(i, key_columns);
                              };

                              SCOPED_TIMER(_hash_table_emplace_timer);
                              lazy_emplace_batch(
                                      agg_method, state, num_rows, creator, creator_for_null_key,
                                      [&](uint32_t row) { i = row; },
                                      [&](uint32_t row, auto& mapped) { places[row] = mapped; });
                              COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                              return true;
                          }
                          return false;
                      }},
            _agg_data->method_variant);
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

void StreamingAggLocalState::_emplace_into_hash_table(AggregateDataPtr* places,
                                                      ColumnRawPtrs& key_columns,
                                                      const uint32_t num_rows) {
    if (_use_simple_count) {
        _emplace_into_hash_table_inline_count(key_columns, num_rows);
        return;
    }

    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) -> void {
                             SCOPED_TIMER(_hash_table_compute_timer);
                             using HashMethodType = std::decay_t<decltype(agg_method)>;
                             using AggState = typename HashMethodType::State;
                             AggState state(key_columns);
                             agg_method.init_serialized_keys(key_columns, num_rows);

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
                                         Base::_parent->template cast<StreamingAggOperatorX>()
                                                 ._total_size_of_aggregate_states,
                                         Base::_parent->template cast<StreamingAggOperatorX>()
                                                 ._align_aggregate_states);
                                 auto st = _create_agg_status(mapped);
                                 if (!st) {
                                     throw Exception(st.code(), st.to_string());
                                 }
                             };

                             SCOPED_TIMER(_hash_table_emplace_timer);
                             lazy_emplace_batch(
                                     agg_method, state, num_rows, creator, creator_for_null_key,
                                     [&](uint32_t row, auto& mapped) { places[row] = mapped; });

                             COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                         }},
               _agg_data->method_variant);
}

void StreamingAggLocalState::_emplace_into_hash_table_inline_count(ColumnRawPtrs& key_columns,
                                                                   uint32_t num_rows) {
    std::visit(Overload {[&](std::monostate& arg) -> void {
                             throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                    "uninited hash table");
                         },
                         [&](auto& agg_method) -> void {
                             SCOPED_TIMER(_hash_table_compute_timer);
                             using HashMethodType = std::decay_t<decltype(agg_method)>;
                             using AggState = typename HashMethodType::State;
                             AggState state(key_columns);
                             agg_method.init_serialized_keys(key_columns, num_rows);

                             auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                 HashMethodType::try_presis_key_and_origin(key, origin,
                                                                           _agg_arena_pool);
                                 AggregateDataPtr mapped = nullptr;
                                 ctor(key, mapped);
                             };

                             auto creator_for_null_key = [&](auto& mapped) { mapped = nullptr; };

                             SCOPED_TIMER(_hash_table_emplace_timer);
                             lazy_emplace_batch(agg_method, state, num_rows, creator,
                                                creator_for_null_key, [&](uint32_t, auto& mapped) {
                                                    ++reinterpret_cast<UInt64&>(mapped);
                                                });

                             COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                         }},
               _agg_data->method_variant);
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

    if (state->enable_spill()) {
        // If spill enabled, the streaming agg should not occupy too much memory.
        _spill_streaming_agg_mem_limit =
                state->query_options().__isset.spill_streaming_agg_mem_limit
                        ? state->query_options().spill_streaming_agg_mem_limit
                        : 0;
    } else {
        _spill_streaming_agg_mem_limit = 0;
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
    if (Base::_closed) {
        return Status::OK();
    }
    _pre_aggregated_block->clear();
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
    _agg_arena_pool.clear(true);
    return Base::close(state);
}

Status StreamingAggOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    if (!local_state._pre_aggregated_block->empty()) {
        local_state._pre_aggregated_block->swap(*block);
    } else {
        RETURN_IF_ERROR(local_state._get_results_with_serialized_key(state, block, eos));
        local_state.make_nullable_output_key(block);
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    }
    local_state.reached_limit(block, eos);

    return Status::OK();
}

Status StreamingAggOperatorX::push(RuntimeState* state, Block* in_block, bool eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(
                local_state.do_pre_agg(state, in_block, local_state._pre_aggregated_block.get()));
    }
    in_block->clear_column_data(_child->row_desc().num_materialized_slots());
    return Status::OK();
}

bool StreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._pre_aggregated_block->empty() && !local_state._child_eos;
}

} // namespace doris
