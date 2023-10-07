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

namespace doris::pipeline {

OperatorPtr MultiCastDataStreamSinkOperatorBuilder::build_operator() {
    return std::make_shared<MultiCastDataStreamSinkOperator>(this, _sink);
}

Status MultiCastDataStreamSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    auto& sinks = static_cast<MultiCastDataStreamSinkOperatorX*>(_parent)->sink_node().sinks;
    std::string id_name = " (dst id : ";
    for (auto& sink : sinks) {
        id_name += std::to_string(sink.dest_node_id) + ",";
    }
    id_name += ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(_parent->get_name() + id_name));
    _dependency = (MultiCastDependency*)info.dependency;
    if (_dependency) {
        _shared_state = (typename MultiCastDependency::SharedState*)_dependency->shared_state();
        _wait_for_dependency_timer =
                ADD_TIMER(_profile, "WaitForDependency[" + _dependency->name() + "]Time");
    }
    _rows_input_counter = ADD_COUNTER(_profile, "InputRows", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseTime");
    info.parent_profile->add_child(_profile, true, nullptr);
    _mem_tracker = std::make_unique<MemTracker>(_parent->get_name());
    return Status::OK();
}

} // namespace doris::pipeline
