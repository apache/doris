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

#include "exchange_sink_buffer.h"
#include "operator.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {

namespace pipeline {
class PipelineFragmentContext;

// Now local exchange is not supported since VDataStreamRecvr is considered as a pipeline broker.
class ExchangeSinkOperator : public Operator {
public:
    ExchangeSinkOperator(OperatorBuilder* operator_builder, vectorized::VDataStreamSender* sink,
                         PipelineFragmentContext* context);
    ~ExchangeSinkOperator() override;
    Status init(ExecNode* exec_node, RuntimeState* state = nullptr) override;
    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    bool can_write() override;
    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;
    bool is_pending_finish() const override;
    Status finalize(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    RuntimeState* state() { return _state; }

private:
    std::unique_ptr<ExchangeSinkBuffer> _sink_buffer;
    vectorized::VDataStreamSender* _sink;
    RuntimeState* _state = nullptr;
    PipelineFragmentContext* _context;
};

class ExchangeSinkOperatorBuilder : public OperatorBuilder {
public:
    ExchangeSinkOperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node,
                                vectorized::VDataStreamSender* sink,
                                PipelineFragmentContext* context)
            : OperatorBuilder(id, name, exec_node), _sink(sink), _context(context) {}

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<ExchangeSinkOperator>(this, _sink, _context);
    }

private:
    vectorized::VDataStreamSender* _sink;
    PipelineFragmentContext* _context;
};

} // namespace pipeline
} // namespace doris