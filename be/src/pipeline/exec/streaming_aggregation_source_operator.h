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
#include "operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
namespace pipeline {
class DataQueue;

class StreamingAggSourceOperatorBuilder final
        : public OperatorBuilder<vectorized::AggregationNode> {
public:
    StreamingAggSourceOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class StreamingAggSourceOperator final : public SourceOperator<vectorized::AggregationNode> {
public:
    StreamingAggSourceOperator(OperatorBuilderBase*, ExecNode*, std::shared_ptr<DataQueue>);
    bool can_read() override;
    Status get_block(RuntimeState*, vectorized::Block*, SourceState& source_state) override;
    Status open(RuntimeState*) override { return Status::OK(); }

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class StreamingAggSourceOperatorX final : public AggSourceOperatorX {
public:
    using Base = AggSourceOperatorX;
    StreamingAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                const DescriptorTbl& descs);
    ~StreamingAggSourceOperatorX() = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;
};

} // namespace pipeline
} // namespace doris
