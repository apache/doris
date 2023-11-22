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

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
namespace pipeline {
class DataQueue;

class DistinctStreamingAggSourceOperatorBuilder final
        : public OperatorBuilder<vectorized::DistinctAggregationNode> {
public:
    DistinctStreamingAggSourceOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class DistinctStreamingAggSourceOperator final
        : public SourceOperator<DistinctStreamingAggSourceOperatorBuilder> {
public:
    DistinctStreamingAggSourceOperator(OperatorBuilderBase*, ExecNode*, std::shared_ptr<DataQueue>);
    bool can_read() override;
    Status get_block(RuntimeState*, vectorized::Block*, SourceState& source_state) override;
    Status open(RuntimeState*) override { return Status::OK(); }
    Status pull_data(RuntimeState* state, vectorized::Block* output_block, bool* eos);

private:
    int64_t rows_have_returned = 0;
    std::shared_ptr<DataQueue> _data_queue;
};

class DistinctStreamingAggSourceOperatorX final : public AggSourceOperatorX {
public:
    using Base = AggSourceOperatorX;
    DistinctStreamingAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                        const DescriptorTbl& descs);
    ~DistinctStreamingAggSourceOperatorX() = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;
    bool _is_streaming_preagg = false;
};

} // namespace pipeline
} // namespace doris
