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

#include "partition_sort_source_operator.h"

#include "pipeline/exec/operator.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

OperatorPtr PartitionSortSourceOperatorBuilder::build_operator() {
    return std::make_shared<PartitionSortSourceOperator>(this, _node);
}

Status PartitionSortSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<PartitionSortDependency>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _get_next_timer = ADD_TIMER(profile(), "GetResultTime");
    _get_sorted_timer = ADD_TIMER(profile(), "GetSortedTime");
    _shared_state->previous_row = std::make_unique<vectorized::SortCursorCmp>();
    return Status::OK();
}

Status PartitionSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* output_block,
                                               SourceState& source_state) {
    RETURN_IF_CANCELLED(state);
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    SCOPED_TIMER(local_state._get_next_timer);
    output_block->clear_column_data();
    {
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
        if (local_state._shared_state->blocks_buffer.empty() == false) {
            local_state._shared_state->blocks_buffer.front().swap(*output_block);
            local_state._shared_state->blocks_buffer.pop();
            //if buffer have no data, block reading and wait for signal again
            RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                                   output_block->columns()));
            if (local_state._shared_state->blocks_buffer.empty()) {
                local_state._dependency->block_reading();
            }
            return Status::OK();
        }
    }

    // is_ready_for_read: this is set by sink node using: local_state._dependency->set_ready_for_read()
    // notice: must output block from _blocks_buffer firstly, and then get_sorted_block.
    // as when the child is eos, then set _can_read = true, and _partition_sorts have push_back sorter.
    // if we move the _blocks_buffer output at last(behind 286 line),
    // it's maybe eos but not output all data: when _blocks_buffer.empty() and _can_read = false (this: _sort_idx && _partition_sorts.size() are 0)
    RETURN_IF_ERROR(get_sorted_block(state, output_block, local_state));
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                           output_block->columns()));
    {
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
        if (local_state._shared_state->blocks_buffer.empty() &&
            local_state._shared_state->sort_idx >=
                    local_state._shared_state->partition_sorts.size()) {
            source_state = SourceState::FINISHED;
        }
    }
    return Status::OK();
}

Dependency* PartitionSortSourceOperatorX::wait_for_dependency(RuntimeState* state) {
    CREATE_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
    return local_state._dependency->read_blocked_by();
}

Status PartitionSortSourceOperatorX::get_sorted_block(RuntimeState* state,
                                                      vectorized::Block* output_block,
                                                      PartitionSortSourceLocalState& local_state) {
    SCOPED_TIMER(local_state._get_sorted_timer);
    //sorter output data one by one
    bool current_eos = false;
    if (local_state._shared_state->sort_idx < local_state._shared_state->partition_sorts.size()) {
        RETURN_IF_ERROR(
                local_state._shared_state->partition_sorts[local_state._shared_state->sort_idx]
                        ->get_next(state, output_block, &current_eos));
    }
    if (current_eos) {
        //current sort have eos, so get next idx
        local_state._shared_state->previous_row->reset();
        auto rows = local_state._shared_state->partition_sorts[local_state._shared_state->sort_idx]
                            ->get_output_rows();
        local_state._num_rows_returned += rows;
        local_state._shared_state->partition_sorts[local_state._shared_state->sort_idx].reset(
                nullptr);
        local_state._shared_state->sort_idx++;
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris