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
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = _parent->cast<MultiCastDataStreamSinkOperatorX>();
    _shared_state->multi_cast_data_streamer = std::make_shared<pipeline::MultiCastDataStreamer>(
            p._row_desc, p._pool, p._cast_sender_count);
    return Status::OK();
}

} // namespace doris::pipeline
