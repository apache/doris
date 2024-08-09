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

namespace doris {
class RuntimeState;

namespace pipeline {

enum AnalyticFnScope { PARTITION, RANGE, ROWS };

class AnalyticSourceOperatorX;
class AnalyticLocalState final : public PipelineXLocalState<AnalyticSharedState> {
public:
    ENABLE_FACTORY_CREATOR(AnalyticLocalState);
    AnalyticLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    void init_result_columns();

    Status output_current_block(vectorized::Block* block);

    bool init_next_partition(BlockRowPos found_partition_end);

private:
    Status _get_next_for_rows(size_t rows);
    Status _get_next_for_range(size_t rows);
    Status _get_next_for_partition(size_t rows);

    void _execute_for_win_func(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                               int64_t frame_end);
    void _insert_result_info(int64_t current_block_rows);

    void _update_order_by_range();
    bool _refresh_need_more_input() {
        auto need_more_input = _whether_need_next_partition(_shared_state->found_partition_end);
        if (need_more_input) {
            _dependency->block();
            _dependency->set_ready_to_write();
        } else {
            _dependency->set_block_to_write();
            _dependency->set_ready();
        }
        return need_more_input;
    }
    BlockRowPos _get_partition_by_end();
    BlockRowPos _compare_row_to_find_end(int idx, BlockRowPos start, BlockRowPos end,
                                         bool need_check_first = false);
    bool _whether_need_next_partition(BlockRowPos& found_partition_end);

    void _reset_agg_status();
    void _create_agg_status();
    void _destroy_agg_status();

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
    bool _current_window_empty = false;

    BlockRowPos _order_by_start;
    BlockRowPos _order_by_end;
    BlockRowPos _partition_by_start;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    RuntimeProfile::Counter* _evaluation_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;

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

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

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

    AnalyticFnScope _fn_scope;

    TupleDescriptor* _intermediate_tuple_desc = nullptr;
    TupleDescriptor* _output_tuple_desc = nullptr;

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
