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

#include "exec/operator/multi_cast_data_stream_sink.h"

#include "exec/operator/multi_cast_data_streamer.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/dependency.h"

namespace doris {

std::string MultiCastDataStreamSinkLocalState::name_suffix() {
    auto* parent = static_cast<MultiCastDataStreamSinkOperatorX*>(_parent);
    auto& dest_ids = parent->dests_id();
    std::string dest_list;
    for (size_t i = 0; i < dest_ids.size(); ++i) {
        if (i > 0) {
            dest_list += ",";
        }
        dest_list += std::to_string(dest_ids[i]);
    }
    if (_parent->nereids_id() == -1) {
        return fmt::format("(id={}, dest_ids=[{}])", parent->operator_id(), dest_list);
    } else {
        return fmt::format("(nereids_id={}, id={}, dest_ids=[{}])", _parent->nereids_id(),
                           parent->operator_id(), dest_list);
    }
}

std::shared_ptr<BasicSharedState> MultiCastDataStreamSinkOperatorX::create_shared_state() const {
    std::shared_ptr<BasicSharedState> ss =
            std::make_shared<MultiCastSharedState>(_pool, _cast_sender_count, _node_id);

    ss->id = operator_id();
    for (const auto& dest : dests_id()) {
        ss->related_op_ids.insert(dest);
    }
    return ss;
}

Status MultiCastDataStreamSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    _shared_state->multi_cast_data_streamer->set_sink_profile(operator_profile());
    _shared_state->multi_cast_data_streamer->set_write_dependency(_dependency);
    return Status::OK();
}

std::string MultiCastDataStreamSinkLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, {}", Base::debug_string(indentation_level),
                   _shared_state->multi_cast_data_streamer->debug_string());
    return fmt::to_string(debug_string_buffer);
}

Status MultiCastDataStreamSinkOperatorX::sink(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (in_block->rows() > 0 || eos) {
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        // push block to multi cast data streamer , it will not return the EOF status.
        RETURN_IF_ERROR(
                local_state._shared_state->multi_cast_data_streamer->push(state, in_block, eos));
    }
    return Status::OK();
}

} // namespace doris
