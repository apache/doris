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
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vassert_num_rows_node.h"

namespace doris {

namespace pipeline {

class AssertNumRowsOperatorBuilder final : public OperatorBuilder<vectorized::VAssertNumRowsNode> {
public:
    AssertNumRowsOperatorBuilder(int32_t id, ExecNode* node)
            : OperatorBuilder(id, "AssertNumRowsOperator", node) {}

    OperatorPtr build_operator() override;
};

class AssertNumRowsOperator final : public StreamingOperator<AssertNumRowsOperatorBuilder> {
public:
    AssertNumRowsOperator(OperatorBuilderBase* operator_builder, ExecNode* node)
            : StreamingOperator(operator_builder, node) {}
};

class AssertNumRowsLocalState final : public PipelineXLocalState<FakeDependency> {
public:
    ENABLE_FACTORY_CREATOR(AssertNumRowsLocalState);

    AssertNumRowsLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeDependency>(state, parent) {}
    ~AssertNumRowsLocalState() = default;
};

class AssertNumRowsOperatorX final : public StreamingOperatorX<AssertNumRowsLocalState> {
public:
    AssertNumRowsOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);

    Status pull(RuntimeState* state, vectorized::Block* block, SourceState& source_state) override;

    [[nodiscard]] bool is_source() const override { return false; }

private:
    friend class AssertNumRowsLocalState;

    int64_t _desired_num_rows;
    const std::string _subquery_string;
    TAssertion::type _assertion;
};

} // namespace pipeline
} // namespace doris
