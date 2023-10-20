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

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vanalytic_eval_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class AnalyticSourceOperatorBuilder final : public OperatorBuilder<vectorized::VAnalyticEvalNode> {
public:
    AnalyticSourceOperatorBuilder(int32_t, ExecNode*);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class AnalyticSourceOperator final : public SourceOperator<AnalyticSourceOperatorBuilder> {
public:
    AnalyticSourceOperator(OperatorBuilderBase*, ExecNode*);

    Status open(RuntimeState*) override { return Status::OK(); }
};

class AnalyticSourceOperatorX;
class AnalyticLocalState final : public PipelineXLocalState<AnalyticDependency> {
public:
    ENABLE_FACTORY_CREATOR(AnalyticLocalState);
    AnalyticLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

    Status init_result_columns();

    Status output_current_block(vectorized::Block* block);

    bool init_next_partition(vectorized::BlockRowPos found_partition_end);

private:
    Status _get_next_for_rows(size_t rows);
    Status _get_next_for_range(size_t rows);
    Status _get_next_for_partition(size_t rows);

    void _execute_for_win_func(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                               int64_t frame_end);
    void _insert_result_info(int64_t current_block_rows);

    void _update_order_by_range();

    Status _reset_agg_status();
    Status _create_agg_status();
    Status _destroy_agg_status();

    friend class AnalyticSourceOperatorX;

    int64_t _output_block_index;
    int64_t _window_end_position;
    bool _next_partition;
    std::vector<vectorized::MutableColumnPtr> _result_window_columns;

    int64_t _rows_start_offset;
    int64_t _rows_end_offset;
    vectorized::AggregateDataPtr _fn_place_ptr;
    size_t _agg_functions_size;
    bool _agg_functions_created;
    vectorized::BlockRowPos _order_by_start;
    vectorized::BlockRowPos _order_by_end;
    vectorized::BlockRowPos _partition_by_start;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _evaluation_timer;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage;

    using vectorized_execute = std::function<void(int64_t peer_group_start, int64_t peer_group_end,
                                                  int64_t frame_start, int64_t frame_end)>;
    using vectorized_get_next = std::function<Status(size_t rows)>;
    using vectorized_get_result = std::function<void(int64_t current_block_rows)>;

    struct executor {
        vectorized_execute execute;
        vectorized_get_next get_next;
        vectorized_get_result insert_result;
    };

    executor _executor;
};

class AnalyticSourceOperatorX final : public OperatorX<AnalyticLocalState> {
public:
    AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);
    Dependency* wait_for_dependency(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

private:
    friend class AnalyticLocalState;

    TAnalyticWindow _window;

    TupleId _intermediate_tuple_id;
    TupleId _output_tuple_id;

    bool _has_window;
    bool _has_range_window;
    bool _has_window_start;
    bool _has_window_end;

    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    vectorized::AnalyticFnScope _fn_scope;

    TupleDescriptor* _intermediate_tuple_desc;
    TupleDescriptor* _output_tuple_desc;

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
