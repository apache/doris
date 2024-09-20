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

#include "streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class RuntimeState;
} // namespace doris

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
struct StreamingHtMinReductionEntry {
    // Use 'streaming_ht_min_reduction' if the total size of hash table bucket directories in
    // bytes is greater than this threshold.
    int min_ht_mem;
    // The minimum reduction factor to expand the hash tables.
    double streaming_ht_min_reduction;
};

// TODO: experimentally tune these values and also programmatically get the cache size
// of the machine that we're running on.
static constexpr StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
        // Expand up to L2 cache always.
        {0, 0.0},
        // Expand into L3 cache if we look like we're getting some reduction.
        // At present, The L2 cache is generally 1024k or more
        {1024 * 1024, 1.1},
        // Expand into main memory if we're getting a significant reduction.
        // The L3 cache is generally 16MB or more
        {16 * 1024 * 1024, 2.0},
};

static constexpr int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

StreamingAggLocalState::StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _agg_arena_pool(std::make_unique<vectorized::Arena>()),
          _agg_data(std::make_unique<AggregatedDataVariants>()),
          _agg_profile_arena(std::make_unique<vectorized::Arena>()),
          _child_block(vectorized::Block::create_unique()),
          _pre_aggregated_block(vectorized::Block::create_unique()) {}

Status StreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _hash_table_memory_usage = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "HashTable",
                                                            TUnit::BYTES, "MemoryUsage", 1);
    _serialize_key_arena_memory_usage = Base::profile()->AddHighWaterMarkCounter(
            "SerializeKeyArena", TUnit::BYTES, "MemoryUsage", 1);

    _build_timer = ADD_TIMER(Base::profile(), "BuildTime");
    _build_table_convert_timer = ADD_TIMER(Base::profile(), "BuildConvertToPartitionedTime");
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
    _hash_table_size_counter = ADD_COUNTER(profile(), "HashTableSize", TUnit::UNIT);
    _queue_byte_size_counter = ADD_COUNTER(profile(), "MaxSizeInBlockQueue", TUnit::BYTES);
    _queue_size_counter = ADD_COUNTER(profile(), "MaxSizeOfBlockQueue", TUnit::UNIT);
    _streaming_agg_timer = ADD_TIMER(profile(), "StreamingAggTime");
    _build_timer = ADD_TIMER(profile(), "BuildTime");
    _expr_timer = ADD_TIMER(Base::profile(), "ExprTime");
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");

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

    if (_probe_expr_ctxs.empty()) {
        _agg_data->without_key = reinterpret_cast<vectorized::AggregateDataPtr>(
                _agg_profile_arena->alloc(p._total_size_of_aggregate_states));

        if (p._is_merge) {
            if (p._needs_finalize) {
                _executor = std::make_unique<Executor<true, true, true>>();
            } else {
                _executor = std::make_unique<Executor<true, true, false>>();
            }
        } else {
            if (p._needs_finalize) {
                _executor = std::make_unique<Executor<true, false, true>>();
            } else {
                _executor = std::make_unique<Executor<true, false, false>>();
            }
        }
    } else {
        RETURN_IF_ERROR(_init_hash_method(_probe_expr_ctxs));

        std::visit(vectorized::Overload {
                           [&](std::monostate& arg) -> void {
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
                           }},
                   _agg_data->method_variant);
        if (p._is_merge) {
            if (p._needs_finalize) {
                _executor = std::make_unique<Executor<false, true, true>>();
            } else {
                _executor = std::make_unique<Executor<false, true, false>>();
            }
        } else {
            if (p._needs_finalize) {
                _executor = std::make_unique<Executor<false, false, true>>();
            } else {
                _executor = std::make_unique<Executor<false, false, false>>();
            }
        }

        _should_limit_output = p._limit != -1 &&       // has limit
                               (!p._have_conjuncts) && // no having conjunct
                               p._needs_finalize;      // agg's finalize step
    }
    return Status::OK();
}

void StreamingAggLocalState::_make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : Base::_parent->cast<StreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status StreamingAggLocalState::_execute_without_key(vectorized::Block* block) {
    DCHECK(_agg_data->without_key != nullptr);
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_single_add(
                block,
                _agg_data->without_key + Base::_parent->template cast<StreamingAggOperatorX>()
                                                 ._offsets_of_aggregate_states[i],
                _agg_arena_pool.get()));
    }
    return Status::OK();
}

template <bool limit, bool for_spill>
Status StreamingAggLocalState::_merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);

    size_t key_size = _probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        if constexpr (for_spill) {
            key_columns[i] = block->get_by_position(i).column.get();
        } else {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
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

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            if (_aggregate_evaluators[i]->is_merge()) {
                int col_id = _get_slot_column_id(_aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size = _aggregate_evaluators[i]->function()->size_of_data() * rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    _aggregate_evaluators[i]->function()->deserialize_and_merge_vec_selected(
                            _places.data(),
                            Base::_parent->template cast<StreamingAggOperatorX>()
                                    ._offsets_of_aggregate_states[i],
                            _deserialize_buffer.data(), column.get(), _agg_arena_pool.get(), rows);
                }
            } else {
                RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add_selected(
                        block,
                        Base::_parent->template cast<StreamingAggOperatorX>()
                                ._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool.get()));
            }
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            if (_aggregate_evaluators[i]->is_merge() || for_spill) {
                int col_id = 0;
                if constexpr (for_spill) {
                    col_id = _probe_expr_ctxs.size() + i;
                } else {
                    col_id = _get_slot_column_id(_aggregate_evaluators[i]);
                }
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size = _aggregate_evaluators[i]->function()->size_of_data() * rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    _aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                            _places.data(),
                            Base::_parent->template cast<StreamingAggOperatorX>()
                                    ._offsets_of_aggregate_states[i],
                            _deserialize_buffer.data(), column.get(), _agg_arena_pool.get(), rows);
                }
            } else {
                RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                        block,
                        Base::_parent->template cast<StreamingAggOperatorX>()
                                ._offsets_of_aggregate_states[i],
                        _places.data(), _agg_arena_pool.get()));
            }
        }

        if (_should_limit_output) {
            _reach_limit = _get_hash_table_size() >=
                           Base::_parent->template cast<StreamingAggOperatorX>()._limit;
        }
    }

    return Status::OK();
}

size_t StreamingAggLocalState::_get_hash_table_size() {
    return std::visit(
            vectorized::Overload {[&](std::monostate& arg) -> size_t {
                                      throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                             "uninited hash table");
                                      return 0;
                                  },
                                  [&](auto& agg_method) { return agg_method.hash_table->size(); }},
            _agg_data->method_variant);
}

Status StreamingAggLocalState::_merge_without_key(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_data->without_key != nullptr);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (_aggregate_evaluators[i]->is_merge()) {
            int col_id = _get_slot_column_id(_aggregate_evaluators[i]);
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            SCOPED_TIMER(_deserialize_data_timer);
            _aggregate_evaluators[i]->function()->deserialize_and_merge_from_column(
                    _agg_data->without_key + Base::_parent->template cast<StreamingAggOperatorX>()
                                                     ._offsets_of_aggregate_states[i],
                    *column, _agg_arena_pool.get());
        } else {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_single_add(
                    block,
                    _agg_data->without_key + Base::_parent->template cast<StreamingAggOperatorX>()
                                                     ._offsets_of_aggregate_states[i],
                    _agg_arena_pool.get()));
        }
    }
    return Status::OK();
}

void StreamingAggLocalState::_update_memusage_without_key() {
    auto arena_memory_usage = _agg_arena_pool->size() - _mem_usage_record.used_in_arena;
    Base::_mem_tracker->consume(arena_memory_usage);
    _serialize_key_arena_memory_usage->add(arena_memory_usage);
    _mem_usage_record.used_in_arena = _agg_arena_pool->size();
}

Status StreamingAggLocalState::_execute_with_serialized_key(vectorized::Block* block) {
    if (_reach_limit) {
        return _execute_with_serialized_key_helper<true>(block);
    } else {
        return _execute_with_serialized_key_helper<false>(block);
    }
}

void StreamingAggLocalState::_update_memusage_with_serialized_key() {
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        auto& data = *agg_method.hash_table;
                        auto arena_memory_usage = _agg_arena_pool->size() +
                                                  _aggregate_data_container->memory_usage() -
                                                  _mem_usage_record.used_in_arena;
                        Base::_mem_tracker->consume(arena_memory_usage);
                        Base::_mem_tracker->consume(data.get_buffer_size_in_bytes() -
                                                    _mem_usage_record.used_in_state);
                        _serialize_key_arena_memory_usage->add(arena_memory_usage);
                        COUNTER_UPDATE(
                                _hash_table_memory_usage,
                                data.get_buffer_size_in_bytes() - _mem_usage_record.used_in_state);
                        _mem_usage_record.used_in_state = data.get_buffer_size_in_bytes();
                        _mem_usage_record.used_in_arena =
                                _agg_arena_pool->size() + _aggregate_data_container->memory_usage();
                    }},
            _agg_data->method_variant);
}

template <bool limit>
Status StreamingAggLocalState::_execute_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
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

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add_selected(
                    block,
                    Base::_parent->template cast<StreamingAggOperatorX>()
                            ._offsets_of_aggregate_states[i],
                    _places.data(), _agg_arena_pool.get()));
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                    block,
                    Base::_parent->template cast<StreamingAggOperatorX>()
                            ._offsets_of_aggregate_states[i],
                    _places.data(), _agg_arena_pool.get()));
        }

        if (_should_limit_output) {
            _reach_limit = _get_hash_table_size() >=
                           Base::_parent->template cast<StreamingAggOperatorX>()._limit;
            if (_reach_limit &&
                Base::_parent->template cast<StreamingAggOperatorX>()._can_short_circuit) {
                Base::_dependency->set_ready_to_read();
                return Status::Error<ErrorCode::END_OF_FILE>("");
            }
        }
    }

    return Status::OK();
}

// We should call this function only at 1st phase.
// 1st phase: is_merge=true, only have one SlotRef.
// 2nd phase: is_merge=false, maybe have multiple exprs.
int StreamingAggLocalState::_get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
    auto ctxs = evaluator->input_exprs_ctxs();
    CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
            << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
            << ctxs[0]->root()->debug_string();
    return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
}

void StreamingAggLocalState::_find_in_hash_table(vectorized::AggregateDataPtr* places,
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

Status StreamingAggLocalState::_merge_with_serialized_key(vectorized::Block* block) {
    if (_reach_limit) {
        return _merge_with_serialized_key_helper<true, false>(block);
    } else {
        return _merge_with_serialized_key_helper<false, false>(block);
    }
}

Status StreamingAggLocalState::_init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(init_agg_hash_method(
            _agg_data.get(), probe_exprs,
            Base::_parent->template cast<StreamingAggOperatorX>()._is_first_phase));
    return Status::OK();
}

Status StreamingAggLocalState::do_pre_agg(vectorized::Block* input_block,
                                          vectorized::Block* output_block) {
    RETURN_IF_ERROR(_pre_agg_with_serialized_key(input_block, output_block));

    // pre stream agg need use _num_row_return to decide whether to do pre stream agg
    _cur_num_rows_returned += output_block->rows();
    _make_nullable_output_key(output_block);
    //    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    _executor->update_memusage(this);
    return Status::OK();
}

bool StreamingAggLocalState::_should_expand_preagg_hash_tables() {
    if (!_should_expand_hash_table) {
        return false;
    }

    return std::visit(
            vectorized::Overload {
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

                        // Find the appropriate reduction factor in our table for the current hash table sizes.
                        int cache_level = 0;
                        while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
                               ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
                            ++cache_level;
                        }

                        // Compare the number of rows in the hash table with the number of input rows that
                        // were aggregated into it. Exclude passed through rows from this calculation since
                        // they were not in hash tables.
                        const int64_t input_rows = _input_num_rows;
                        const int64_t aggregated_input_rows = input_rows - _cur_num_rows_returned;
                        // TODO chenhao
                        //  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
                        double current_reduction =
                                static_cast<double>(aggregated_input_rows) / ht_rows;

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
                        double min_reduction =
                                STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

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
    if (_agg_arena_pool) {
        usage += _agg_arena_pool->size();
    }

    if (_aggregate_data_container) {
        usage += _aggregate_data_container->memory_usage();
    }

    std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                         throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                                "uninited hash table");
                                     },
                                     [&](auto& agg_method) {
                                         usage += agg_method.hash_table->get_buffer_size_in_bytes();
                                     }},
               _agg_data->method_variant);

    return usage;
}

Status StreamingAggLocalState::_pre_agg_with_serialized_key(doris::vectorized::Block* in_block,
                                                            doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();

    size_t key_size = _probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = in_block->rows();
    _places.resize(rows);

    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In either case we should always use the remaining space in the hash table
    // to avoid wasting memory.
    // But for fixed hash map, it never need to expand
    bool ret_flag = false;
    const auto spill_streaming_agg_mem_limit =
            _parent->cast<StreamingAggOperatorX>()._spill_streaming_agg_mem_limit;
    const bool used_too_much_memory =
            spill_streaming_agg_mem_limit > 0 && _memory_usage() > spill_streaming_agg_mem_limit;
    RETURN_IF_ERROR(std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> Status {
                        return Status::InternalError("Uninited hash table");
                    },
                    [&](auto& agg_method) -> Status {
                        auto& hash_tbl = *agg_method.hash_table;
                        /// If too much memory is used during the pre-aggregation stage,
                        /// it is better to output the data directly without performing further aggregation.
                        // do not try to do agg, just init and serialize directly return the out_block
                        if (used_too_much_memory || (hash_tbl.add_elem_size_overflow(rows) &&
                                                     !_should_expand_preagg_hash_tables())) {
                            SCOPED_TIMER(_streaming_agg_timer);
                            ret_flag = true;

                            // will serialize value data to string column.
                            // non-nullable column(id in `_make_nullable_keys`)
                            // will be converted to nullable.
                            bool mem_reuse =
                                    p._make_nullable_keys.empty() && out_block->mem_reuse();

                            std::vector<vectorized::DataTypePtr> data_types;
                            vectorized::MutableColumns value_columns;
                            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                                auto data_type =
                                        _aggregate_evaluators[i]->function()->get_serialized_type();
                                if (mem_reuse) {
                                    value_columns.emplace_back(
                                            std::move(*out_block->get_by_position(i + key_size)
                                                               .column)
                                                    .mutate());
                                } else {
                                    // slot type of value it should always be string type
                                    value_columns.emplace_back(_aggregate_evaluators[i]
                                                                       ->function()
                                                                       ->create_serialize_column());
                                }
                                data_types.emplace_back(data_type);
                            }

                            for (int i = 0; i != _aggregate_evaluators.size(); ++i) {
                                SCOPED_TIMER(_serialize_data_timer);
                                RETURN_IF_ERROR(
                                        _aggregate_evaluators[i]->streaming_agg_serialize_to_column(
                                                in_block, value_columns[i], rows,
                                                _agg_arena_pool.get()));
                            }

                            if (!mem_reuse) {
                                vectorized::ColumnsWithTypeAndName columns_with_schema;
                                for (int i = 0; i < key_size; ++i) {
                                    columns_with_schema.emplace_back(
                                            key_columns[i]->clone_resized(rows),
                                            _probe_expr_ctxs[i]->root()->data_type(),
                                            _probe_expr_ctxs[i]->root()->expr_name());
                                }
                                for (int i = 0; i < value_columns.size(); ++i) {
                                    columns_with_schema.emplace_back(std::move(value_columns[i]),
                                                                     data_types[i], "");
                                }
                                out_block->swap(vectorized::Block(columns_with_schema));
                            } else {
                                for (int i = 0; i < key_size; ++i) {
                                    std::move(*out_block->get_by_position(i).column)
                                            .mutate()
                                            ->insert_range_from(*key_columns[i], 0, rows);
                                }
                            }
                        }
                        return Status::OK();
                    }},
            _agg_data->method_variant));

    if (!ret_flag) {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                    in_block, p._offsets_of_aggregate_states[i], _places.data(),
                    _agg_arena_pool.get(), _should_expand_hash_table));
        }
    }

    return Status::OK();
}

Status StreamingAggLocalState::_create_agg_status(vectorized::AggregateDataPtr data) {
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

Status StreamingAggLocalState::_get_with_serialized_key_result(RuntimeState* state,
                                                               vectorized::Block* block,
                                                               bool* eos) {
    auto& p = _parent->cast<StreamingAggOperatorX>();
    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = p._make_nullable_keys.empty() && block->mem_reuse();

    auto columns_with_schema =
            vectorized::VectorizedUtils::create_columns_with_type_and_name(p._row_descriptor);
    int key_size = _probe_expr_ctxs.size();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    vectorized::MutableColumns value_columns;
    for (int i = key_size; i < columns_with_schema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) -> void {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto& agg_method) -> void {
                           auto& data = *agg_method.hash_table;
                           agg_method.init_iterator();
                           const auto size = std::min(data.size(), size_t(state->batch_size()));
                           using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                           std::vector<KeyType> keys(size);
                           if (_values.size() < size) {
                               _values.resize(size);
                           }

                           size_t num_rows = 0;
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

                           for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                               _aggregate_evaluators[i]->insert_result_info_vec(
                                       _values, p._offsets_of_aggregate_states[i],
                                       value_columns[i].get(), num_rows);
                           }

                           if (iter == _aggregate_data_container->end()) {
                               if (agg_method.hash_table->has_null_key_data()) {
                                   // only one key of group by support wrap null key
                                   // here need additional processing logic on the null key / value
                                   DCHECK(key_columns.size() == 1);
                                   DCHECK(key_columns[0]->is_nullable());
                                   if (key_columns[0]->size() < state->batch_size()) {
                                       key_columns[0]->insert_data(nullptr, 0);
                                       auto mapped =
                                               agg_method.hash_table->template get_null_key_data<
                                                       vectorized::AggregateDataPtr>();
                                       for (size_t i = 0; i < _aggregate_evaluators.size(); ++i)
                                           _aggregate_evaluators[i]->insert_result_info(
                                                   mapped + p._offsets_of_aggregate_states[i],
                                                   value_columns[i].get());
                                       *eos = true;
                                   }
                               } else {
                                   *eos = true;
                               }
                           }
                       }},
               _agg_data->method_variant);

    if (!mem_reuse) {
        *block = columns_with_schema;
        vectorized::MutableColumns columns(block->columns());
        for (int i = 0; i < block->columns(); ++i) {
            if (i < key_size) {
                columns[i] = std::move(key_columns[i]);
            } else {
                columns[i] = std::move(value_columns[i - key_size]);
            }
        }
        block->set_columns(std::move(columns));
    }

    return Status::OK();
}

Status StreamingAggLocalState::_serialize_without_key(RuntimeState* state, vectorized::Block* block,
                                                      bool* eos) {
    // 1. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return NULL result
    //    level one aggregation node set `eos = true` return directly
    SCOPED_TIMER(_serialize_result_timer);
    if (UNLIKELY(_input_num_rows == 0)) {
        *eos = true;
        return Status::OK();
    }
    block->clear();

    DCHECK(_agg_data->without_key != nullptr);
    int agg_size = _aggregate_evaluators.size();

    vectorized::MutableColumns value_columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = _aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] = _aggregate_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                _agg_data->without_key +
                        _parent->cast<StreamingAggOperatorX>()._offsets_of_aggregate_states[i],
                *value_columns[i]);
    }

    {
        vectorized::ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            vectorized::ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = vectorized::Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

Status StreamingAggLocalState::_serialize_with_serialized_key_result(RuntimeState* state,
                                                                     vectorized::Block* block,
                                                                     bool* eos) {
    SCOPED_TIMER(_serialize_result_timer);
    auto& p = _parent->cast<StreamingAggOperatorX>();
    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();
    vectorized::MutableColumns value_columns(agg_size);
    vectorized::DataTypes value_data_types(agg_size);

    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = p._make_nullable_keys.empty() && block->mem_reuse();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        agg_method.init_iterator();
                        auto& data = *agg_method.hash_table;
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                        std::vector<KeyType> keys(size);
                        if (_values.size() < size + 1) {
                            _values.resize(size + 1);
                        }

                        size_t num_rows = 0;
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
                                                    vectorized::AggregateDataPtr>();
                                    ++num_rows;
                                    *eos = true;
                                }
                            } else {
                                *eos = true;
                            }
                        }

                        {
                            SCOPED_TIMER(_serialize_data_timer);
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
        vectorized::ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        *block = vectorized::Block(columns_with_schema);
    }

    return Status::OK();
}

void StreamingAggLocalState::make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _parent->cast<StreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status StreamingAggLocalState::_get_without_key_result(RuntimeState* state,
                                                       vectorized::Block* block, bool* eos) {
    DCHECK(_agg_data->without_key != nullptr);
    block->clear();

    auto& p = _parent->cast<StreamingAggOperatorX>();
    *block = vectorized::VectorizedUtils::create_empty_columnswithtypename(p._row_descriptor);
    int agg_size = _aggregate_evaluators.size();

    vectorized::MutableColumns columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = _aggregate_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        auto* column = columns[i].get();
        _aggregate_evaluators[i]->insert_result_info(
                _agg_data->without_key + p._offsets_of_aggregate_states[i], column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (!vectorized::is_array(remove_nullable(column_type))) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "node id = {}, column_type not match data_types, column_type={}, "
                            "data_types={}",
                            _parent->node_id(), column_type->get_name(), data_types[i]->get_name());
                }
            }

            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                vectorized::ColumnPtr ptr = std::move(columns[i]);
                // unless `count`, other aggregate function dispose empty set should be null
                // so here check the children row return
                ptr = make_nullable(ptr, _input_num_rows == 0);
                columns[i] = ptr->assume_mutable();
            }
        }
    }

    block->set_columns(std::move(columns));
    *eos = true;
    return Status::OK();
}

void StreamingAggLocalState::_destroy_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(
                data + _parent->cast<StreamingAggOperatorX>()._offsets_of_aggregate_states[i]);
    }
}

void StreamingAggLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                                      vectorized::ColumnRawPtrs& key_columns,
                                                      const size_t num_rows) {
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
                               auto mapped = _aggregate_data_container->append_data(origin);
                               auto st = _create_agg_status(mapped);
                               if (!st) {
                                   throw Exception(st.code(), st.to_string());
                               }
                               ctor(key, mapped);
                           };

                           auto creator_for_null_key = [&](auto& mapped) {
                               mapped = _agg_arena_pool->aligned_alloc(
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
                           for (size_t i = 0; i < num_rows; ++i) {
                               places[i] = agg_method.lazy_emplace(state, i, creator,
                                                                   creator_for_null_key);
                           }

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
          _is_merge(false),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _have_conjuncts(tnode.__isset.vconjunct && !tnode.vconjunct.nodes.empty()),
          _agg_fn_output_row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples) {}

Status StreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit = tnode.agg_node.aggregate_functions.empty();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        vectorized::AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    if (state->enable_agg_spill()) {
        // If spill enabled, the streaming agg should not occupy too much memory.
        _spill_streaming_agg_mem_limit =
                state->query_options().__isset.spill_streaming_agg_mem_limit
                        ? state->query_options().spill_streaming_agg_mem_limit
                        : 0;
    } else {
        _spill_streaming_agg_mem_limit = 0;
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    _op_name = "STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status StreamingAggOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::open(state));
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));

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
                state, _child->row_desc(), intermediate_slot_desc, output_slot_desc));
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
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
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
    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    Base::_mem_tracker->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                             // Do nothing
                                         },
                                         [&](auto& agg_method) {
                                             COUNTER_SET(_hash_table_size_counter,
                                                         int64_t(agg_method.hash_table->size()));
                                         }},
                   _agg_data->method_variant);
    }
    _close_with_serialized_key();
    return Base::close(state);
}

Status StreamingAggOperatorX::pull(RuntimeState* state, vectorized::Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    if (!local_state._pre_aggregated_block->empty()) {
        local_state._pre_aggregated_block->swap(*block);
    } else {
        RETURN_IF_ERROR(local_state._executor->get_result(&local_state, state, block, eos));
        local_state.make_nullable_output_key(block);
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                               block->columns()));
    }
    local_state.reached_limit(block, eos);

    return Status::OK();
}

Status StreamingAggOperatorX::push(RuntimeState* state, vectorized::Block* in_block,
                                   bool eos) const {
    auto& local_state = get_local_state(state);
    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state.do_pre_agg(in_block, local_state._pre_aggregated_block.get()));
    }
    in_block->clear_column_data(_child->row_desc().num_materialized_slots());
    return Status::OK();
}

bool StreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._pre_aggregated_block->empty() && !local_state._child_eos;
}

} // namespace doris::pipeline
