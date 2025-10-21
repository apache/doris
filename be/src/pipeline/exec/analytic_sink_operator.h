
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

#include <stdint.h>

#include "operator.h"
#include "pipeline/dependency.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace pipeline {
class AnalyticSinkOperatorX;

struct BoundaryPose {
    int64_t start = 0;
    int64_t end = 0;
    bool is_ended = false;
    void remove_unused_rows(int64_t cnt) {
        start -= cnt;
        end -= cnt;
    }
};

class PartitionStatistics {
public:
    void update(int64_t size) {
        _count++;
        _cumulative_size += size;
        _average_size = _cumulative_size / _count;
    }

    void reset() {
        _count = 0;
        _cumulative_size = 0;
        _average_size = 0;
    }

    bool is_high_cardinality() const { return _count > 16 && _average_size < 8; }

    int64_t _count = 0;
    int64_t _cumulative_size = 0;
    int64_t _average_size = 0;
};

// those function cacluate need partition info, so can't be used in streaming mode
static const std::set<std::string> PARTITION_FUNCTION_SET {"ntile", "cume_dist", "percent_rank"};

class AnalyticSinkLocalState : public PipelineXSinkLocalState<AnalyticSharedState> {
    ENABLE_FACTORY_CREATOR(AnalyticSinkLocalState);

public:
    AnalyticSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<AnalyticSharedState>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class AnalyticSinkOperatorX;
    Status _execute_impl();
    // over(partition by k1 order by k2 range|rows unbounded preceding and unbounded following)
    bool _get_next_for_partition(int64_t current_block_rows, int64_t current_block_base_pos);
    // over(partition by k1 order by k2 range between unbounded preceding and current row)
    bool _get_next_for_unbounded_range(int64_t current_block_rows, int64_t current_block_base_pos);
    // over(partition by k1 order by k2 range between M preceding and N following)
    bool _get_next_for_range_between(int64_t current_block_rows, int64_t current_block_base_pos);
    // over(partition by k1 order by k2 rows between unbounded preceding and current row)
    bool _get_next_for_unbounded_rows(int64_t current_block_rows, int64_t current_block_base_pos);
    // over(partition by k1 order by k2 rows between M preceding and N following)
    bool _get_next_for_sliding_rows(int64_t current_block_rows, int64_t current_block_base_pos);

    void _init_result_columns();
    template <bool incremental = false>
    void _execute_for_function(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                               int64_t frame_end);
    void _insert_result_info(int64_t start, int64_t end);
    int64_t current_pos_in_block() {
        return _current_row_position + _have_removed_rows -
               _input_block_first_row_positions[_output_block_index];
    }
    void _output_current_block(vectorized::Block* block);
    void _reset_state_for_next_partition();
    void _refresh_buffer_and_dependency_state(vectorized::Block* block);

    void _create_agg_status();
    void _reset_agg_status();
    void _destroy_agg_status();
    void _remove_unused_rows();

    void _get_partition_by_end();
    void _find_next_partition_ends();
    void _update_order_by_range();
    void _find_next_order_by_ends();
    int64_t find_first_not_equal(vectorized::IColumn* reference_column,
                                 vectorized::IColumn* compared_column, int64_t target,
                                 int64_t start, int64_t end);

    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
    vectorized::VExprContextSPtrs _partition_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _order_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _range_between_expr_ctxs;
    std::vector<std::vector<vectorized::MutableColumnPtr>> _agg_input_columns;
    std::vector<vectorized::MutableColumnPtr> _partition_by_columns;
    std::vector<vectorized::MutableColumnPtr> _order_by_columns;
    std::vector<vectorized::MutableColumnPtr> _range_result_columns;
    size_t _partition_exprs_size = 0;
    size_t _order_by_exprs_size = 0;
    BoundaryPose _partition_by_pose;
    BoundaryPose _order_by_pose;
    PartitionStatistics _partition_column_statistics;
    PartitionStatistics _order_by_column_statistics;
    std::queue<int64_t> _next_partition_ends;
    std::queue<int64_t> _next_order_by_ends;

    size_t _agg_functions_size = 0;
    bool _agg_functions_created = false;
    vectorized::AggregateDataPtr _fn_place_ptr = nullptr;
    vectorized::Arena _agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    std::vector<bool> _result_column_nullable_flags;
    std::vector<bool> _result_column_could_resize;

    using vectorized_get_next = bool (AnalyticSinkLocalState::*)(int64_t, int64_t);
    struct executor {
        vectorized_get_next get_next_impl;
    };
    executor _executor;

    std::vector<uint8_t> _use_null_result;
    std::vector<uint8_t> _could_use_previous_result;
    bool _streaming_mode = false;
    bool _support_incremental_calculate = true;
    bool _need_more_data = false;
    int64_t _current_row_position = 0;
    int64_t _output_block_index = 0;
    std::vector<vectorized::MutableColumnPtr> _result_window_columns;

    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;
    int64_t _input_total_rows = 0;
    bool _input_eos = false;
    std::vector<vectorized::Block> _input_blocks;
    std::vector<int64_t> _input_block_first_row_positions;
    int64_t _removed_block_index = 0;
    int64_t _have_removed_rows = 0;

    RuntimeProfile::Counter* _evaluation_timer = nullptr;
    RuntimeProfile::Counter* _compute_agg_data_timer = nullptr;
    RuntimeProfile::Counter* _compute_partition_by_timer = nullptr;
    RuntimeProfile::Counter* _compute_order_by_timer = nullptr;
    RuntimeProfile::Counter* _compute_range_between_function_timer = nullptr;
    RuntimeProfile::Counter* _partition_search_timer = nullptr;
    RuntimeProfile::Counter* _order_search_timer = nullptr;
    RuntimeProfile::Counter* _remove_rows_timer = nullptr;
    RuntimeProfile::Counter* _remove_count = nullptr;
    RuntimeProfile::Counter* _remove_rows = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;

    int64_t _reserve_mem_size = 0;
};

class AnalyticSinkOperatorX final : public DataSinkOperatorX<AnalyticSinkLocalState> {
public:
    AnalyticSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs, bool require_bucket_distribution);

#ifdef BE_TEST
    AnalyticSinkOperatorX(ObjectPool* pool)
            : _pool(pool),
              _buffered_tuple_id(0),
              _is_colocate(false),
              _require_bucket_distribution(false) {}
#endif

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<AnalyticSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        if (_partition_by_eq_expr_ctxs.empty()) {
            return {ExchangeType::PASSTHROUGH};
        } else {
            return _is_colocate && _require_bucket_distribution && !_followed_by_shuffled_operator
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        }
    }

    bool require_data_distribution() const override { return true; }

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

private:
    friend class AnalyticSinkLocalState;
    Status _insert_range_column(vectorized::Block* block, const vectorized::VExprContextSPtr& expr,
                                vectorized::IColumn* dst_column, size_t length);
    Status _add_input_block(doris::RuntimeState* state, vectorized::Block* input_block);

    ObjectPool* _pool = nullptr;
    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
    vectorized::VExprContextSPtrs _partition_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _order_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _range_between_expr_ctxs;

    size_t _agg_functions_size = 0;
    std::vector<size_t> _num_agg_input;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    TupleId _intermediate_tuple_id;
    TupleId _output_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;
    TupleDescriptor* _output_tuple_desc = nullptr;
    const TTupleId _buffered_tuple_id;

    const bool _is_colocate;
    const bool _require_bucket_distribution;
    const std::vector<TExpr> _partition_exprs;

    TAnalyticWindow _window;
    bool _has_window;
    bool _has_range_window;
    bool _has_window_start;
    bool _has_window_end;

    /// The offset of the n-th functions.
    std::vector<size_t> _offsets_of_aggregate_states;
    /// The total size of the row from the functions.
    size_t _total_size_of_aggregate_states = 0;
    /// The max align size for functions
    size_t _align_aggregate_states = 1;
    std::vector<bool> _change_to_nullable_flags;
};

} // namespace pipeline
} // namespace doris
#include "common/compile_check_end.h"