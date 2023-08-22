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
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
class DataQueue;

class DistinctStreamingAggSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::DistinctAggregationNode> {
public:
    DistinctStreamingAggSinkOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; }
    bool is_source() const override { return false; }

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class DistinctStreamingAggSinkOperator final
        : public StreamingOperator<DistinctStreamingAggSinkOperatorBuilder> {
public:
    DistinctStreamingAggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode*,
                                     std::shared_ptr<DataQueue>);

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    bool can_write() override;

    Status close(RuntimeState* state) override;

    bool reached_limited_rows() {
        return _node->limit() != -1 && _output_distinct_rows >= _node->limit();
    }

private:
    int64_t _output_distinct_rows = 0;
    std::shared_ptr<DataQueue> _data_queue;
    std::unique_ptr<vectorized::Block> _output_block = vectorized::Block::create_unique();
};

} // namespace pipeline
} // namespace doris