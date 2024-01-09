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

#include "operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/join/vnested_loop_join_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class NestLoopJoinBuildOperatorBuilder final
        : public OperatorBuilder<vectorized::VNestedLoopJoinNode> {
public:
    NestLoopJoinBuildOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
    bool is_sink() const override { return true; }
};

class NestLoopJoinBuildOperator final : public StreamingOperator<vectorized::VNestedLoopJoinNode> {
public:
    NestLoopJoinBuildOperator(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return true; }
};

class NestedLoopJoinBuildSinkDependency final : public Dependency {
public:
    using SharedState = NestedLoopJoinSharedState;
    NestedLoopJoinBuildSinkDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "NestedLoopJoinBuildSinkDependency", true, query_ctx) {}
    ~NestedLoopJoinBuildSinkDependency() override = default;
};

class NestedLoopJoinBuildSinkOperatorX;

class NestedLoopJoinBuildSinkLocalState final
        : public JoinBuildSinkLocalState<NestedLoopJoinBuildSinkDependency,
                                         NestedLoopJoinBuildSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(NestedLoopJoinBuildSinkLocalState);
    using Parent = NestedLoopJoinBuildSinkOperatorX;
    NestedLoopJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~NestedLoopJoinBuildSinkLocalState() = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs();
    vectorized::VExprContextSPtrs& filter_src_expr_ctxs() { return _filter_src_expr_ctxs; }
    RuntimeProfile::Counter* runtime_filter_compute_timer() {
        return _runtime_filter_compute_timer;
    }
    vectorized::Blocks& build_blocks() { return _shared_state->build_blocks; }
    RuntimeProfile::Counter* publish_runtime_filter_timer() {
        return _publish_runtime_filter_timer;
    }

private:
    friend class NestedLoopJoinBuildSinkOperatorX;
    uint64_t _build_rows = 0;
    uint64_t _total_mem_usage = 0;

    vectorized::VExprContextSPtrs _filter_src_expr_ctxs;
};

class NestedLoopJoinBuildSinkOperatorX final
        : public JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState> {
public:
    NestedLoopJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError(
                "{} should not init with TDataSink",
                JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return _child_x->ignore_data_distribution() ? DataDistribution(ExchangeType::BROADCAST)
                                                    : DataDistribution(ExchangeType::NOOP);
    }

private:
    friend class NestedLoopJoinBuildSinkLocalState;

    vectorized::VExprContextSPtrs _filter_src_expr_ctxs;

    const std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    const bool _is_output_left_side_only;
    RowDescriptor _row_descriptor;
};

} // namespace pipeline
} // namespace doris
