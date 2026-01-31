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

#include "distinct_streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {
#include "common/compile_check_begin.h"
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
        {.min_ht_mem = 0, .streaming_ht_min_reduction = 0.0},
        // Expand into L3 cache if we look like we're getting some reduction.
        // At present, The L2 cache is generally 1024k or more
        {.min_ht_mem = 256 * 1024, .streaming_ht_min_reduction = 1.1},
        // Expand into main memory if we're getting a significant reduction.
        // The L3 cache is generally 16MB or more
        {.min_ht_mem = 16 * 1024 * 1024, .streaming_ht_min_reduction = 2.0},
};

static constexpr int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

DistinctStreamingAggLocalState::DistinctStreamingAggLocalState(RuntimeState* state,
                                                               OperatorXBase* parent)
        : PipelineXLocalState<FakeSharedState>(state, parent),
          batch_size(state->batch_size()),
          _agg_data(std::make_unique<DistinctDataVariants>()),
          _child_block(vectorized::Block::create_unique()),
          _block_acc(state->batch_size()) {}

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

Status DistinctStreamingAggLocalState::_init_hash_method(
        const vectorized::VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(init_hash_method<DistinctDataVariants>(
            _agg_data.get(), get_data_types(probe_exprs),
            Base::_parent->template cast<DistinctStreamingAggOperatorX>()._is_first_phase));
    return Status::OK();
}

Status DistinctStreamingAggLocalState::_distinct_pre_agg_with_serialized_key(
        doris::vectorized::Block* in_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());
    if (in_block->rows() == 0) {
        return Status::OK();
    }

    size_t key_size = _probe_expr_ctxs.size();
    vectorized::ColumnsWithTypeAndName key_columns(key_size);
    vectorized::ColumnRawPtrs key_raw_columns(key_size);
    std::vector<int> result_idxs(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i].column = in_block->get_by_position(result_column_id).column;
            key_columns[i].column->assume_mutable()->replace_float_special_values();
            key_columns[i].type = _probe_expr_ctxs[i]->root()->data_type();
            key_columns[i].name = _probe_expr_ctxs[i]->root()->expr_name();
            key_raw_columns[i] = key_columns[i].column.get();
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
        _emplace_into_hash_table_to_distinct(_distinct_row, key_raw_columns, rows);
        DCHECK_LE(_distinct_row.size(), rows)
                << "_distinct_row size should be less than or equal to rows";
    }
    SCOPED_TIMER(_insert_keys_to_column_timer);

    vectorized::Block key_block(key_columns);

    in_block->clear();

    if (_stop_emplace_flag) {
        RETURN_IF_ERROR(_block_acc.push(std::move(key_block)));

    } else {
        RETURN_IF_ERROR(_block_acc.push_with_selector(std::move(key_block), _distinct_row));
    }
    return Status::OK();
}

void DistinctStreamingAggLocalState::_make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : Base::_parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void DistinctStreamingAggLocalState::_emplace_into_hash_table_to_distinct(
        vectorized::IColumn::Selector& distinct_row, vectorized::ColumnRawPtrs& key_columns,
        const uint32_t num_rows) {
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
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
                        for (; row < num_rows; ++row) {
                            agg_method.lazy_emplace(state, row, creator, creator_for_null_key);
                        }

                        COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                    }},
            _agg_data->method_variant);
}

DistinctStreamingAggOperatorX::DistinctStreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                                             const TPlanNode& tnode,
                                                             const DescriptorTbl& descs,
                                                             bool require_bucket_distribution)
        : StatefulOperatorX<DistinctStreamingAggLocalState>(pool, tnode, operator_id, descs),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _partition_exprs(tnode.__isset.distribute_expr_lists && require_bucket_distribution
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.agg_node.grouping_exprs),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution) {
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
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    _op_name = "DISTINCT_STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));
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

Status DistinctStreamingAggOperatorX::push(RuntimeState* state, vectorized::Block* in_block,
                                           bool eos) const {
    auto& local_state = get_local_state(state);
    local_state._input_num_rows += in_block->rows();
    RETURN_IF_ERROR(local_state._distinct_pre_agg_with_serialized_key(in_block));
    if (eos) {
        local_state._block_acc.set_finish();
    }
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::pull(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) const {
    auto& local_state = get_local_state(state);

    if (auto block_ptr = local_state._block_acc.pull()) {
        block->swap(*block_ptr);
        // set limit if needed
        ///TODO: This can be optimized by setting num_rows before push, which may reduce some unnecessary calculations, but this would make the code more complex
        if (_limit != -1 && (local_state._num_rows_returned + block->rows()) > _limit) {
            auto limit_rows = _limit - local_state._num_rows_returned;
            block->set_num_rows(limit_rows);
            local_state._reach_limit = true;
            local_state._block_acc.set_finish();
        }
    }

    local_state._make_nullable_output_key(block);
    if (!_is_streaming_preagg) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block, block->columns()));
    }
    local_state.add_num_rows_returned(block->rows());

    // We have two stop flags
    // One is that we got eos
    // One is that we reached the limit
    // Only when both flags are met, will it be a true eos (and both need to meet the _block_acc is_finished condition)
    *eos = (local_state._child_eos && local_state._block_acc.is_finished()) ||
           (local_state._reach_limit && local_state._block_acc.is_finished());
    return Status::OK();
}

bool DistinctStreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    // Conditions for needing more input data are:
    // 1. block_acc needs input data
    // 2. Have not reached eos or limit
    return local_state._block_acc.need_input() &&
           !(local_state._child_eos || local_state._reach_limit);
}

Status DistinctStreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter && !_probe_expr_ctxs.empty()) {
        std::visit(vectorized::Overload {[&](std::monostate& arg) {
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
    _arena.clear();
    _block_acc.close();
    return Base::close(state);
}

} // namespace doris::pipeline
