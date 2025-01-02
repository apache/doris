
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
enum AnalyticFnScope { PARTITION, RANGE, ROWS };

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
    // For window frame `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    Status _get_next_for_partition();
    // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    Status _get_next_for_range();
    // 1. `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // 2. `ROWS BETWEEN UNBOUNDED PRECEDING AND N PRECEDING`
    // 3. `ROWS BETWEEN UNBOUNDED PRECEDING AND N FOLLOWING`
    Status _get_next_for_rows();

    // 1. `ROWS BETWEEN N PRECEDING AND M PRECEDING` or
    // 2. `ROWS BETWEEN N FOLLOWING AND M FOLLOWING` or
    // 3. `ROWS BETWEEN N PRECEDING AND M FOLLOWING` or
    // 4. `ROWS BETWEEN N PRECEDING AND CURRENT ROW` or
    // 5. `ROWS BETWEEN CURRENT ROW AND M FOLLOWING`
    Status _get_next_for_sliding_rows();

    void _execute_for_win_func(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                               int64_t frame_end);
    void _insert_result_info(int64_t real_deal_with_width);
    // void _insert_result_info(int64_t start, int64_t end);
    Status output_current_block(vectorized::Block* block);
    void _init_result_columns();

    void _reset_agg_status();
    void _create_agg_status();
    void _destroy_agg_status();

    void _update_order_by_range();
    void _get_partition_by_end();
    BlockRowPos _compare_row_to_find_end(int64_t idx, BlockRowPos start, BlockRowPos end,
                                         bool need_check_first = false);
    bool _has_input_data() { return _output_block_index < _input_blocks.size(); }
    bool _check_need_block_task();
    void _refresh_buffer_and_dependency_state(vectorized::Block* block);
    void _reset_state_for_next_partition();

    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
    vectorized::VExprContextSPtrs _partition_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _order_by_eq_expr_ctxs;
    size_t _partition_exprs_size = 0;
    size_t _order_by_exprs_size = 0;

    size_t _agg_functions_size = 0;
    vectorized::AggregateDataPtr _fn_place_ptr = nullptr;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool = nullptr;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    using vectorized_get_next = Status (AnalyticSinkLocalState::*)();
    struct executor {
        vectorized_get_next get_next_impl;
    };
    executor _executor;

    bool _agg_functions_created = false;
    bool _current_window_empty = false;
    bool _next_partition = false;
    int64_t _output_block_index = 0;
    int64_t _window_end_position = 0;
    std::vector<vectorized::MutableColumnPtr> _result_window_columns;

    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;
    std::vector<int64_t> _partition_by_column_idxs;
    std::vector<int64_t> _order_by_column_idxs;

    // BlockRowPos _order_by_start;
    // BlockRowPos _order_by_end;
    // BlockRowPos _partition_by_start;
    // BlockRowPos _partition_by_end;

    BoundaryPose _partition_by_pose;
    BoundaryPose _order_by_pose;
    int64_t _current_row_position = 0;
    // BlockRowPos partition_by_end;
    int64_t _input_total_rows = 0;
    BlockRowPos _all_block_end;
    std::vector<vectorized::Block> _input_blocks;
    bool _input_eos = false;
    // BlockRowPos found_partition_end;
    std::vector<int64_t> _input_col_ids;
    std::vector<int64_t> _input_block_first_row_positions;
    std::vector<std::vector<vectorized::MutableColumnPtr>> _agg_input_columns;

    RuntimeProfile::Counter* _evaluation_timer = nullptr;
    RuntimeProfile::Counter* _compute_agg_data_timer = nullptr;
    RuntimeProfile::Counter* _compute_partition_by_timer = nullptr;
    RuntimeProfile::Counter* _compute_order_by_timer = nullptr;
    RuntimeProfile::Counter* _execute_timer = nullptr;
    RuntimeProfile::Counter* _get_next_timer = nullptr;
    RuntimeProfile::Counter* _get_result_timer = nullptr;
    // RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;
};

class AnalyticSinkOperatorX final : public DataSinkOperatorX<AnalyticSinkLocalState> {
public:
    AnalyticSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs, bool require_bucket_distribution);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<AnalyticSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(RuntimeState* state) override;

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

private:
    friend class AnalyticSinkLocalState;
    Status _insert_range_column(vectorized::Block* block, const vectorized::VExprContextSPtr& expr,
                                vectorized::IColumn* dst_column, size_t length);
    Status _add_input_block(doris::RuntimeState* state, vectorized::Block* input_block);

    ObjectPool* _pool = nullptr;
    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
    vectorized::VExprContextSPtrs _partition_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _order_by_eq_expr_ctxs;

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

    AnalyticFnScope _fn_scope;
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