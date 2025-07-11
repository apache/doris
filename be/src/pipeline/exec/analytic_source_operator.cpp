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

#include "analytic_source_operator.h"

#include <cstddef>
#include <string>

#include "pipeline/exec/operator.h"
#include "vec/columns/column_nullable.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

AnalyticLocalState::AnalyticLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<AnalyticSharedState>(state, parent) {}

Status AnalyticLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<AnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_next_timer = ADD_TIMER(custom_profile(), "GetNextTime");
    _filtered_rows_counter = ADD_COUNTER(custom_profile(), "FilteredRows", TUnit::UNIT);
    return Status::OK();
}

AnalyticSourceOperatorX::AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs)
        : OperatorX<AnalyticLocalState>(pool, tnode, operator_id, descs) {}

Status AnalyticSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* output_block,
                                          bool* eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    SCOPED_TIMER(local_state._get_next_timer);
    local_state._estimate_memory_usage = 0;
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    output_block->clear_column_data();
    size_t output_rows = 0;
    {
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
        if (!local_state._shared_state->blocks_buffer.empty()) {
            local_state._shared_state->blocks_buffer.front().swap(*output_block);
            local_state._shared_state->blocks_buffer.pop();
            output_rows = output_block->rows();
            //if buffer have no data and sink not eos, block reading and wait for signal again
            RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, output_block,
                                                     output_block->columns()));
            if (local_state._shared_state->blocks_buffer.empty() &&
                !local_state._shared_state->sink_eos) {
                // add this mutex to check, as in some case maybe is doing block(), and the sink is doing set eos.
                // so have to hold mutex to set block(), avoid to sink have set eos and set ready, but here set block() by mistake
                std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
                if (!local_state._shared_state->sink_eos) {
                    local_state._dependency->block();              // block self source
                    local_state._dependency->set_ready_to_write(); // ready for sink write
                }
            }
        } else {
            //iff buffer have no data and sink eos, set eos
            std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
            *eos = local_state._shared_state->sink_eos;
        }
    }
    local_state.reached_limit(output_block, eos);
    if (!output_block->empty()) {
        auto return_rows = output_block->rows();
        COUNTER_UPDATE(local_state._filtered_rows_counter, output_rows - return_rows);
    }
    return Status::OK();
}

Status AnalyticSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<AnalyticLocalState>::prepare(state));
    DCHECK(_child->row_desc().is_prefix_of(_row_descriptor));
    return Status::OK();
}

} // namespace doris::pipeline
