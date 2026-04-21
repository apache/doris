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

#include "exec/operator/distinct_streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "exec/operator/streaming_agg_min_reduction.h"
#include "exprs/vectorized_agg_fn.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris {

DistinctStreamingAggLocalState::DistinctStreamingAggLocalState(RuntimeState* state,
                                                               OperatorXBase* parent)
        : PipelineXLocalState<FakeSharedState>(state, parent),
          batch_size(state->batch_size()),
          _agg_data(std::make_unique<DistinctDataVariants>()),
          _child_block(Block::create_unique()),
          _aggregated_block(Block::create_unique()),
          _is_single_backend(state->get_query_ctx()->is_single_backend_query()) {}

Status DistinctStreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _build_timer = ADD_TIMER(Base::custom_profile(), "BuildTime");
    _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
    _hash_table_size_counter = ADD_COUNTER(custom_profile(), "HashTableSize", TUnit::UNIT);
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");

    return Status::OK();
}

Status DistinctStreamingAggLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<DistinctStreamingAggOperatorX>();
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    RETURN_IF_ERROR(_init_hash_method(_probe_expr_ctxs));
    return Status::OK();
}

bool DistinctStreamingAggLocalState::_should_expand_preagg_hash_tables() {
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
                        const int64_t aggregated_input_rows = input_rows - _num_rows_returned;
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

Status DistinctStreamingAggLocalState::_init_hash_method(const VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(init_hash_method<DistinctDataVariants>(
            _agg_data.get(), get_data_types(probe_exprs),
            Base::_parent->template cast<DistinctStreamingAggOperatorX>()._is_first_phase));
    return Status::OK();
}

Status DistinctStreamingAggLocalState::_distinct_pre_agg_with_serialized_key(
        doris::Block* in_block, doris::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    std::vector<int> result_idxs(key_size);
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
            result_idxs[i] = result_column_id;
        }
    }

    const uint32_t rows = (uint32_t)in_block->rows();
    _distinct_row.clear();

    if (_parent->cast<DistinctStreamingAggOperatorX>()._is_streaming_preagg && low_memory_mode()) {
        _stop_emplace_flag = true;
    }

    if (!_stop_emplace_flag) {
        // _distinct_row is used to calculate non-duplicate data in key_columns
        // _emplace_into_hash_table_to_distinct will determine whether to continue inserting data into the hashmap
        // If it decides not to insert data, it will set _stop_emplace_flag = true and _distinct_row will be empty
        _distinct_row.reserve(rows);
        _emplace_into_hash_table_to_distinct(_distinct_row, key_columns, rows);
        DCHECK_LE(_distinct_row.size(), rows)
                << "_distinct_row size should be less than or equal to rows";
    }

    bool mem_reuse = _parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys.empty() &&
                     out_block->mem_reuse();
    SCOPED_TIMER(_insert_keys_to_column_timer);
    if (mem_reuse) {
        if (_stop_emplace_flag && !out_block->empty()) {
            // when out_block row >= batch_size, push it to data_queue, so when _stop_emplace_flag = true, maybe have some data in block
            // need output those data firstly
            DCHECK(_distinct_row.empty());
            _distinct_row.resize(rows);
            std::iota(_distinct_row.begin(), _distinct_row.end(), 0);
        }
        DCHECK_EQ(out_block->columns(), key_size);
        if (_stop_emplace_flag && _distinct_row.empty()) {
            // If _stop_emplace_flag is true and _distinct_row is also empty, it means it is in streaming mode, outputting what is input
            // swap the column directly, to solve Check failed: d.column->use_count() == 1 (2 vs. 1)
            for (int i = 0; i < key_size; ++i) {
                auto output_column = out_block->get_by_position(i).column;
                out_block->replace_by_position(i, key_columns[i]->assume_mutable());
                in_block->replace_by_position(result_idxs[i], output_column);
            }
        } else {
            DCHECK_EQ(_cache_block.rows(), 0);
            // is output row > batch_size, split some to cache_block
            if (out_block->rows() + _distinct_row.size() > batch_size) {
                size_t split_size = batch_size - out_block->rows();
                for (int i = 0; i < key_size; ++i) {
                    auto output_dst = out_block->get_by_position(i).column->assume_mutable();
                    key_columns[i]->append_data_by_selector(output_dst, _distinct_row, 0,
                                                            split_size);
                    auto cache_dst = _cache_block.get_by_position(i).column->assume_mutable();
                    key_columns[i]->append_data_by_selector(cache_dst, _distinct_row, split_size,
                                                            _distinct_row.size());
                }
            } else {
                for (int i = 0; i < key_size; ++i) {
                    auto output_column = out_block->get_by_position(i).column;
                    auto dst = output_column->assume_mutable();
                    key_columns[i]->append_data_by_selector(dst, _distinct_row);
                }
            }
        }
    } else {
        DCHECK(out_block->empty()) << "out_block must be empty , but rows is " << out_block->rows();
        ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            if (_stop_emplace_flag) {
                columns_with_schema.emplace_back(key_columns[i]->assume_mutable(),
                                                 _probe_expr_ctxs[i]->root()->data_type(),
                                                 _probe_expr_ctxs[i]->root()->expr_name());
            } else {
                auto distinct_column = key_columns[i]->clone_empty();
                key_columns[i]->append_data_by_selector(distinct_column, _distinct_row);
                columns_with_schema.emplace_back(std::move(distinct_column),
                                                 _probe_expr_ctxs[i]->root()->data_type(),
                                                 _probe_expr_ctxs[i]->root()->expr_name());
            }
        }
        out_block->swap(Block(columns_with_schema));
        _cache_block = out_block->clone_empty();
        if (_stop_emplace_flag) {
            in_block->clear(); // clear the column ref with stop_emplace_flag = true
        }
    }
    return Status::OK();
}

void DistinctStreamingAggLocalState::_make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : Base::_parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void DistinctStreamingAggLocalState::_emplace_into_hash_table_to_distinct(
        IColumn::Selector& distinct_row, ColumnRawPtrs& key_columns, const uint32_t num_rows) {
    std::visit(
            Overload {[&](std::monostate& arg) -> void {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& agg_method) -> void {
                          SCOPED_TIMER(_hash_table_compute_timer);
                          using HashMethodType = std::decay_t<decltype(agg_method)>;
                          using AggState = typename HashMethodType::State;
                          auto& hash_tbl = *agg_method.hash_table;
                          if (_parent->cast<DistinctStreamingAggOperatorX>()._is_streaming_preagg &&
                              hash_tbl.add_elem_size_overflow(num_rows)) {
                              if (!_should_expand_preagg_hash_tables()) {
                                  _stop_emplace_flag = true;
                                  return;
                              }
                          }
                          AggState state(key_columns);
                          agg_method.init_serialized_keys(key_columns, num_rows);
                          size_t row = 0;
                          auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                              HashMethodType::try_presis_key(key, origin, _arena);
                              ctor(key);
                              distinct_row.push_back(row);
                          };
                          auto creator_for_null_key = [&]() { distinct_row.push_back(row); };

                          SCOPED_TIMER(_hash_table_emplace_timer);
                          lazy_emplace_batch_void(agg_method, state, num_rows, creator,
                                                  creator_for_null_key,
                                                  [&](uint32_t r) { row = r; });

                          COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                      }},
            _agg_data->method_variant);
}

DistinctStreamingAggOperatorX::DistinctStreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                                             const TPlanNode& tnode,
                                                             const DescriptorTbl& descs)
        : StatefulOperatorX<DistinctStreamingAggLocalState>(pool, tnode, operator_id, descs),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
        }
    } else {
        _is_streaming_preagg = false;
    }
}

Status DistinctStreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    _op_name = "DISTINCT_STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    init_make_nullable(state);
    return Status::OK();
}

void DistinctStreamingAggOperatorX::init_make_nullable(RuntimeState* state) {
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);

    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
}

Status DistinctStreamingAggOperatorX::push(RuntimeState* state, Block* in_block, bool eos) const {
    auto& local_state = get_local_state(state);
    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(local_state._distinct_pre_agg_with_serialized_key(
            in_block, local_state._aggregated_block.get()));
    // Prevents exceeding the row limit when the aggregated block reaches or equals the threshold.
    if (_limit != -1 &&
        (local_state._num_rows_returned + local_state._aggregated_block->rows()) >= _limit) {
        auto limit_rows = _limit - local_state._num_rows_returned;
        local_state._aggregated_block->set_num_rows(limit_rows);
        local_state._reach_limit = true;
    }
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    if (!local_state._aggregated_block->empty()) {
        block->swap(*local_state._aggregated_block);
        local_state._aggregated_block->clear_column_data(block->columns());
        // The cache block may have additional data due to exceeding the batch size.
        if (!local_state._cache_block.empty()) {
            local_state._swap_cache_block(local_state._aggregated_block.get());
        }
    }

    local_state._make_nullable_output_key(block);
    if (!_is_streaming_preagg) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    }
    local_state.add_num_rows_returned(block->rows());
    // If the limit is not reached, it is important to ensure that _aggregated_block is empty
    // because it may still contain data.
    // However, if the limit is reached, there is no need to output data even if some exists.
    *eos = (local_state._child_eos && local_state._aggregated_block->empty()) ||
           (local_state._reach_limit);
    return Status::OK();
}

bool DistinctStreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    const bool need_batch = local_state._stop_emplace_flag
                                    ? local_state._aggregated_block->empty()
                                    : local_state._aggregated_block->rows() < state->batch_size();
    return need_batch && !(local_state._child_eos || local_state._reach_limit);
}

Status DistinctStreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter && !_probe_expr_ctxs.empty()) {
        std::visit(Overload {[&](std::monostate& arg) {
                                 // Do nothing
                             },
                             [&](auto& agg_method) {
                                 COUNTER_SET(_hash_table_size_counter,
                                             int64_t(agg_method.hash_table->size()));
                             }},
                   _agg_data->method_variant);
    }
    if (Base::_closed) {
        return Status::OK();
    }
    _aggregated_block->clear();
    // If the limit is reached, there may still be remaining data in the cache block.
    // If the limit is not reached, the cache block must be empty.
    // If the query is canceled, it might not satisfy the above conditions.
    if (!state->is_cancelled()) {
        if (!_reach_limit && !_cache_block.empty()) {
            LOG_WARNING("If the limit is not reached, the cache block must be empty.");
        }
    }
    _cache_block.clear();

    _arena.clear();
    return Base::close(state);
}

} // namespace doris
