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

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "pipeline/exec/operator.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

ExchangeSinkOperatorBuilder::ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink,
                                                         PipelineFragmentContext* context,
                                                         int mult_cast_id)
        : DataSinkOperatorBuilder(id, "ExchangeSinkOperator", sink),
          _context(context),
          _mult_cast_id(mult_cast_id) {}

OperatorPtr ExchangeSinkOperatorBuilder::build_operator() {
    return std::make_shared<ExchangeSinkOperator>(this, _sink, _context, _mult_cast_id);
}

ExchangeSinkOperator::ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                                           PipelineFragmentContext* context, int mult_cast_id)
        : DataSinkOperator(operator_builder, sink),
          _context(context),
          _mult_cast_id(mult_cast_id) {}

Status ExchangeSinkOperator::init(const TDataSink& tsink) {
    // -1 means not the mult cast stream sender
    if (_mult_cast_id == -1) {
        _dest_node_id = tsink.stream_sink.dest_node_id;
    } else {
        _dest_node_id = tsink.multi_cast_stream_sink.sinks[_mult_cast_id].dest_node_id;
    }
    return Status::OK();
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    _state = state;
    PUniqueId id;
    id.set_hi(_state->query_id().hi);
    id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<ExchangeSinkBuffer>(id, _dest_node_id, _sink->_sender_id,
                                                        _state->be_number(), _context);

    _sink_buffer->set_query_statistics(_sink->query_statistics());
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
    _sink_buffer->update_profile(_sink->profile());
    _sink_buffer->close();
    return Status::OK();
}

} // namespace doris::pipeline
