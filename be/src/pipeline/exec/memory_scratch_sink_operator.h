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
#include "runtime/result_queue_mgr.h"

namespace doris::pipeline {

class MemoryScratchSinkOperatorX;
class MemoryScratchSinkLocalState final : public PipelineXSinkLocalState<FakeSharedState> {
    ENABLE_FACTORY_CREATOR(MemoryScratchSinkLocalState);

public:
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    MemoryScratchSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status close(RuntimeState* state, Status exec_status) override;
    std::vector<Dependency*> dependencies() const override { return {_queue_dependency.get()}; }

private:
    friend class MemoryScratchSinkOperatorX;
    BlockQueueSharedPtr _queue;

    // Owned by the RuntimeState.
    VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<Dependency> _queue_dependency = nullptr;
};

class MemoryScratchSinkOperatorX final : public DataSinkOperatorX<MemoryScratchSinkLocalState> {
public:
    MemoryScratchSinkOperatorX(const RowDescriptor& row_desc, int operator_id,
                               const std::vector<TExpr>& t_output_expr);
    Status init(const TDataSink& thrift_sink) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class MemoryScratchSinkLocalState;
    const RowDescriptor& _row_desc;
    cctz::time_zone _timezone_obj;
    const std::vector<TExpr>& _t_output_expr;
    VExprContextSPtrs _output_vexpr_ctxs;
};

} // namespace doris::pipeline
