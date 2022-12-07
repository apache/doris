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

ExchangeSinkOperatorBuilder::ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink,
                                                         PipelineFragmentContext* context)
        : DataSinkOperatorBuilder(id, "ExchangeSinkOperator", sink), _context(context) {}

OperatorPtr ExchangeSinkOperatorBuilder::build_operator() {
    return std::make_shared<ExchangeSinkOperator>(this, _sink, _context);
}

ExchangeSinkOperator::ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                                           PipelineFragmentContext* context)
        : DataSinkOperator(operator_builder, sink), _context(context) {}

Status ExchangeSinkOperator::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(_sink->init(tsink));
    _dest_node_id = tsink.stream_sink.dest_node_id;
    return Status::OK();
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    _state = state;
    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer>(id, _dest_node_id, _sink->_sender_id,
                                                        _state->be_number(), _context);

    RETURN_IF_ERROR(DataSinkOperator::prepare(state));
    _sink->registe_channels(_sink_buffer.get());
    return Status::OK();
}

bool ExchangeSinkOperator::can_write() {
    return _sink_buffer->can_write() && _sink->channel_all_can_write();
}

bool ExchangeSinkOperator::is_pending_finish() const {
    return _sink_buffer->is_pending_finish();
}

Status ExchangeSinkOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperator::close(state));
    _sink_buffer->close();
    return Status::OK();
}

} // namespace doris::pipeline
