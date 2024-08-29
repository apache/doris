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
class RuntimeState;

namespace pipeline {

Status PartitionSortSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<PartitionSortNodeSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_sorted_timer = ADD_TIMER(profile(), "GetSortedTime");
    _sorted_partition_output_rows_counter =
            ADD_COUNTER(profile(), "SortedPartitionOutputRows", TUnit::UNIT);
    return Status::OK();
}

Status PartitionSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* output_block,
                                               bool* eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    output_block->clear_column_data();
    {
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
        if (local_state._shared_state->blocks_buffer.empty() == false) {
            local_state._shared_state->blocks_buffer.front().swap(*output_block);
            local_state._shared_state->blocks_buffer.pop();
            //if buffer have no data and sink not eos, block reading and wait for signal again
            RETURN_IF_ERROR(vectorized::VExprContext::filter_block(
                    local_state._conjuncts, output_block, output_block->columns()));
            if (local_state._shared_state->blocks_buffer.empty() &&
                local_state._shared_state->sink_eos == false) {
                // add this mutex to check, as in some case maybe is doing block(), and the sink is doing set eos.
                // so have to hold mutex to set block(), avoid to sink have set eos and set ready, but here set block() by mistake
                std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
                if (local_state._shared_state->sink_eos == false) {
                    local_state._dependency->block();
                }
            }
            if (!output_block->empty()) {
                COUNTER_UPDATE(local_state.blocks_returned_counter(), 1);
                local_state._num_rows_returned += output_block->rows();
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
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, output_block,
                                                           output_block->columns()));
    {
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);

        *eos = local_state._shared_state->blocks_buffer.empty() &&
               local_state._sort_idx >= local_state._shared_state->partition_sorts.size();
    }
    if (!output_block->empty()) {
        COUNTER_UPDATE(local_state.blocks_returned_counter(), 1);
        local_state._num_rows_returned += output_block->rows();
    }
    return Status::OK();
}

Status PartitionSortSourceOperatorX::get_sorted_block(RuntimeState* state,
                                                      vectorized::Block* output_block,
                                                      PartitionSortSourceLocalState& local_state) {
    SCOPED_TIMER(local_state._get_sorted_timer);
    //sorter output data one by one
    bool current_eos = false;
    if (local_state._sort_idx < local_state._shared_state->partition_sorts.size()) {
        RETURN_IF_ERROR(local_state._shared_state->partition_sorts[local_state._sort_idx]->get_next(
                state, output_block, &current_eos));
    }
    if (current_eos) {
        //current sort have eos, so get next idx
        auto rows = local_state._shared_state->partition_sorts[local_state._sort_idx]
                            ->get_output_rows();
        COUNTER_UPDATE(local_state._sorted_partition_output_rows_counter, rows);
        local_state._shared_state->partition_sorts[local_state._sort_idx].reset(nullptr);
        local_state._sort_idx++;
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris