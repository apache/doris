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

#include "exchange_sink_operator.h"

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/brpc_client_cache.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vpartition_info.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

ExchangeSinkOperator::ExchangeSinkOperator(OperatorBuilder* operator_builder,
                                           vectorized::VDataStreamSender* sink,
                                           PipelineFragmentContext* context)
        : Operator(operator_builder), _sink(sink), _context(context) {}

ExchangeSinkOperator::~ExchangeSinkOperator() = default;

Status ExchangeSinkOperator::init(ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    _state = state;
    return Status::OK();
}

Status ExchangeSinkOperator::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(_sink->init(tsink));

    PUniqueId query_id;
    query_id.set_hi(_state->query_id().hi);
    query_id.set_lo(_state->query_id().lo);
    _sink_buffer =
            std::make_unique<ExchangeSinkBuffer>(query_id, tsink.stream_sink.dest_node_id,
                                                 _sink->_sender_id, _state->be_number(), _context);
    return Status::OK();
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_sink->prepare(state));
    _sink->profile()->add_child(_runtime_profile.get(), true, nullptr);

    _sink->registe_channels(_sink_buffer.get());
    return Status::OK();
}

Status ExchangeSinkOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_sink->open(state));
    return Status::OK();
}

bool ExchangeSinkOperator::can_write() {
    return _sink_buffer->can_write() && _sink->channel_all_can_write();
}

Status ExchangeSinkOperator::finalize(RuntimeState* state) {
    Status result = Status::OK();
    RETURN_IF_ERROR(_sink->close(state, result));
    return result;
}

Status ExchangeSinkOperator::sink(RuntimeState* state, vectorized::Block* block,
                                  SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_sink->send(state, block, source_state == SourceState::FINISHED));
    return Status::OK();
}

bool ExchangeSinkOperator::is_pending_finish() const {
    return _sink_buffer->is_pending_finish();
}

Status ExchangeSinkOperator::close(RuntimeState* state) {
    _sink_buffer->close();
    RETURN_IF_ERROR(Operator::close(state));
    return Status::OK();
}

} // namespace doris::pipeline
