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

#include "multi_cast_data_stream_source.h"

#include "common/status.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"
#include "vec/core/materialize_block.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
MultiCastDataStreamSourceLocalState::MultiCastDataStreamSourceLocalState(RuntimeState* state,
                                                                         OperatorXBase* parent)
        : Base(state, parent), _helper(parent->runtime_filter_descs()) {}

Status MultiCastDataStreamSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<Parent>();
    // Pass operator profile to multi cast data streamer
    // so that it could get common_profile and custom_profile as same time.
    _shared_state->multi_cast_data_streamer->set_source_profile(p._consumer_id, operator_profile());
    _shared_state->multi_cast_data_streamer->set_dep_by_sender_idx(p._consumer_id, _dependency);
    _wait_for_rf_timer = ADD_TIMER(common_profile(), "WaitForRuntimeFilter");
    _filter_timer = ADD_TIMER(custom_profile(), "FilterTime");
    _get_data_timer = ADD_TIMER(custom_profile(), "GetDataTime");
    _materialize_data_timer = ADD_TIMER(custom_profile(), "MaterializeDataTime");

    // TODO: Not sure if runtime profile info shuold be added to common_profile or custom_profile
    // init profile for runtime filter
    RETURN_IF_ERROR(_helper.init(state, false, p.dest_id_from_sink(), p.operator_id(),
                                 _filter_dependencies, p.get_name() + "_FILTER_DEPENDENCY"));
    return Status::OK();
}

std::vector<Dependency*> MultiCastDataStreamSourceLocalState::dependencies() const {
    auto dependencies = Base::dependencies();
    auto& p = _parent->cast<Parent>();
    dependencies.emplace_back(
            _shared_state->multi_cast_data_streamer->get_spill_read_dependency(p._consumer_id));
    return dependencies;
}

Status MultiCastDataStreamSourceLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<Parent>();
    RETURN_IF_ERROR(
            _helper.acquire_runtime_filter(state, _conjuncts, p._multi_cast_output_row_descriptor));
    _output_expr_contexts.resize(p._output_expr_contexts.size());
    for (size_t i = 0; i < p._output_expr_contexts.size(); i++) {
        RETURN_IF_ERROR(p._output_expr_contexts[i]->clone(state, _output_expr_contexts[i]));
    }
    return Status::OK();
}

Status MultiCastDataStreamSourceLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }

    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(exec_time_counter());
    int64_t rf_time = 0;
    for (auto& dep : _filter_dependencies) {
        rf_time += dep->watcher_elapse_time();
    }
    COUNTER_SET(_wait_for_rf_timer, rf_time);
    _helper.collect_realtime_profile(custom_profile());
    return Base::close(state);
}

Status MultiCastDataStreamerSourceOperatorX::get_block(RuntimeState* state,
                                                       vectorized::Block* block, bool* eos) {
    //auto& local_state = get_local_state(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    vectorized::Block tmp_block;
    vectorized::Block* output_block = block;
    if (!local_state._output_expr_contexts.empty()) {
        output_block = &tmp_block;
    }
    {
        SCOPED_TIMER(local_state._get_data_timer);
        RETURN_IF_ERROR(local_state._shared_state->multi_cast_data_streamer->pull(
                state, _consumer_id, output_block, eos));
    }

    int arrived_rf_num = 0;
    RETURN_IF_ERROR(local_state._helper.try_append_late_arrival_runtime_filter(
            state, &arrived_rf_num, local_state._conjuncts, _multi_cast_output_row_descriptor));

    if (!local_state._conjuncts.empty() && !output_block->empty()) {
        SCOPED_TIMER(local_state._filter_timer);
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, output_block,
                                                               output_block->columns()));
    }

    if (!local_state._output_expr_contexts.empty() && output_block->rows() > 0) {
        SCOPED_TIMER(local_state._materialize_data_timer);
        RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
                local_state._output_expr_contexts, *output_block, block, true));
        vectorized::materialize_block_inplace(*block);
    }
    return Status::OK();
}

} // namespace doris::pipeline
