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

#include "result_sink_operator.h"

#include "runtime/buffer_control_block.h"
#include "vec/sink/vresult_sink.h"

namespace doris::pipeline {
ResultSinkOperator::ResultSinkOperator(OperatorBuilder* operator_builder,
                                       vectorized::VResultSink* sink)
        : Operator(operator_builder), _sink(sink) {}

Status ResultSinkOperator::init(const TDataSink& tsink) {
    return Status::OK();
}

Status ResultSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _sink->prepare(state);
}

Status ResultSinkOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return _sink->open(state);
}

bool ResultSinkOperator::can_write() {
    return _sink->_sender->can_sink();
}

Status ResultSinkOperator::sink(RuntimeState* state, vectorized::Block* block,
                                SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (!block) {
        DCHECK(source_state == SourceState::FINISHED)
                << "block is null, eos should invoke in finalize.";
        return Status::OK();
    }
    return _sink->send(state, block);
}

Status ResultSinkOperator::finalize(RuntimeState* state) {
    _finalized = true;
    return _sink->close(state, Status::OK());
}

// TODO: Support fresh exec time for sink
Status ResultSinkOperator::close(RuntimeState* state) {
    if (!_finalized) {
        RETURN_IF_ERROR(_sink->close(state, Status::InternalError("Not finalized")));
    }
    return Status::OK();
}
} // namespace doris::pipeline