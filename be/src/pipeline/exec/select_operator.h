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
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vselect_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class SelectOperatorBuilder final : public OperatorBuilder<vectorized::VSelectNode> {
public:
    SelectOperatorBuilder(int32_t id, ExecNode* select_node);

    OperatorPtr build_operator() override;
};

class SelectOperator final : public StreamingOperator<SelectOperatorBuilder> {
public:
    SelectOperator(OperatorBuilderBase* operator_builder, ExecNode* select_node);
};

class SelectOperatorX;
class SelectLocalState final : public PipelineXLocalState<FakeDependency> {
public:
    ENABLE_FACTORY_CREATOR(SelectLocalState);

    SelectLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeDependency>(state, parent) {}
    ~SelectLocalState() = default;

private:
    friend class SelectOperatorX;
};

class SelectOperatorX final : public StreamingOperatorX<SelectLocalState> {
public:
    SelectOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                    const DescriptorTbl& descs)
            : StreamingOperatorX<SelectLocalState>(pool, tnode, operator_id, descs) {}

    Status pull(RuntimeState* state, vectorized::Block* block, SourceState& source_state) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                               block->columns()));
        local_state.reached_limit(block, source_state);
        return Status::OK();
    }

    [[nodiscard]] bool is_source() const override { return false; }
};

} // namespace pipeline
} // namespace doris
