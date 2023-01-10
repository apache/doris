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

#include "operator.h"
#include "pipeline/exec/data_queue.h"

namespace doris {
namespace vectorized {
class AggregationNode;
class VExprContext;
class Block;
} // namespace vectorized

namespace pipeline {

class StreamingAggSinkOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    StreamingAggSinkOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; };
    bool is_source() const override { return false; };

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class StreamingAggSinkOperator final : public StreamingOperator<StreamingAggSinkOperatorBuilder> {
public:
    StreamingAggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode*,
                             std::shared_ptr<DataQueue>);

    Status prepare(RuntimeState*) override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    bool can_write() override;

    Status close(RuntimeState* state) override;

private:
    vectorized::Block _preagg_block = vectorized::Block();

    RuntimeProfile::Counter* _queue_byte_size_counter;
    RuntimeProfile::Counter* _queue_size_counter;

    std::shared_ptr<DataQueue> _data_queue;
};

} // namespace pipeline
} // namespace doris