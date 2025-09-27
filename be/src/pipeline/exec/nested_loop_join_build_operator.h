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
#include "runtime_filter/runtime_filter_producer_helper_cross.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class NestedLoopJoinBuildSinkOperatorX;

class NestedLoopJoinBuildSinkLocalState final
        : public JoinBuildSinkLocalState<NestedLoopJoinSharedState,
                                         NestedLoopJoinBuildSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(NestedLoopJoinBuildSinkLocalState);
    using Parent = NestedLoopJoinBuildSinkOperatorX;
    NestedLoopJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~NestedLoopJoinBuildSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    vectorized::Blocks& build_blocks() { return _shared_state->build_blocks; }

private:
    friend class NestedLoopJoinBuildSinkOperatorX;

    vectorized::VExprContextSPtrs _filter_src_expr_ctxs;
    std::shared_ptr<RuntimeFilterProducerHelperCross> _runtime_filter_producer_helper;
};

class NestedLoopJoinBuildSinkOperatorX final
        : public JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState> {
public:
    NestedLoopJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                     const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError(
                "{} should not init with TDataSink",
                JoinBuildSinkOperatorX<NestedLoopJoinBuildSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return _child->is_serial_operator() ? DataDistribution(ExchangeType::BROADCAST)
                                            : DataDistribution(ExchangeType::NOOP);
    }

private:
    friend class NestedLoopJoinBuildSinkLocalState;

    vectorized::VExprContextSPtrs _filter_src_expr_ctxs;

    const bool _is_output_left_side_only;
    RowDescriptor _row_descriptor;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
