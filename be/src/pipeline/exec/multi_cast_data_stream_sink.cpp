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

#include "multi_cast_data_stream_sink.h"

#include "pipeline/dependency.h"
#include "pipeline/exec/multi_cast_data_streamer.h"

namespace doris::pipeline {

std::string MultiCastDataStreamSinkLocalState::name_suffix() {
    auto& sinks = static_cast<MultiCastDataStreamSinkOperatorX*>(_parent)->sink_node().sinks;
    std::string id_name = " (dst id : ";
    for (auto& sink : sinks) {
        id_name += std::to_string(sink.dest_node_id) + ",";
    }
    id_name += ")";
    return id_name;
}

std::shared_ptr<BasicSharedState> MultiCastDataStreamSinkOperatorX::create_shared_state() const {
    std::shared_ptr<BasicSharedState> ss =
            std::make_shared<MultiCastSharedState>(_row_desc, _pool, _cast_sender_count);
    ss->id = operator_id();
    for (auto& dest : dests_id()) {
        ss->related_op_ids.insert(dest);
    }
    return ss;
}

Status MultiCastDataStreamSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                              bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0 || eos) {
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        auto st = local_state._shared_state->multi_cast_data_streamer->push(state, in_block, eos);
        // TODO: improvement: if sink returned END_OF_FILE, pipeline task can be finished
        if (st.template is<ErrorCode::END_OF_FILE>()) {
            return Status::OK();
        }
        return st;
    }
    return Status::OK();
}

} // namespace doris::pipeline
