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

#pragma once

#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "operator.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class DataSink;
class RuntimeState;
class TDataSink;

namespace pipeline {
class PipelineFragmentContext;

class ExchangeSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::VDataStreamSender> {
public:
    ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink, PipelineFragmentContext* context,
                                int mult_cast_id = -1);

    OperatorPtr build_operator() override;

private:
    PipelineFragmentContext* _context;
    int _mult_cast_id = -1;
};

// Now local exchange is not supported since VDataStreamRecvr is considered as a pipeline broker.
class ExchangeSinkOperator final : public DataSinkOperator<ExchangeSinkOperatorBuilder> {
public:
    ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                         PipelineFragmentContext* context, int mult_cast_id);
    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override;
    bool can_write() override;
    bool is_pending_finish() const override;

    Status close(RuntimeState* state) override;

private:
    std::unique_ptr<ExchangeSinkBuffer> _sink_buffer;
    int _dest_node_id = -1;
    RuntimeState* _state = nullptr;
    PipelineFragmentContext* _context;
    int _mult_cast_id = -1;
};

} // namespace pipeline
} // namespace doris