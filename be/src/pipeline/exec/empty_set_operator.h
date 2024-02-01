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
#include "vec/exec/vempty_set_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class EmptySetSourceOperatorBuilder final : public OperatorBuilder<vectorized::VEmptySetNode> {
public:
    EmptySetSourceOperatorBuilder(int32_t id, ExecNode* empty_set_node);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class EmptySetSourceOperator final : public SourceOperator<vectorized::VEmptySetNode> {
public:
    EmptySetSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* empty_set_node);
    bool can_read() override { return true; }
};

class EmptySetLocalState final : public PipelineXLocalState<FakeDependency> {
public:
    ENABLE_FACTORY_CREATOR(EmptySetLocalState);

    EmptySetLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeDependency>(state, parent) {}
    ~EmptySetLocalState() = default;
};

class EmptySetSourceOperatorX final : public OperatorX<EmptySetLocalState> {
public:
    EmptySetSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs)
            : OperatorX<EmptySetLocalState>(pool, tnode, operator_id, descs) {}

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    [[nodiscard]] bool is_source() const override { return true; }
};

} // namespace pipeline
} // namespace doris
