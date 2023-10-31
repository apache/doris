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

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vsort_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class SortSourceOperatorBuilder final : public OperatorBuilder<vectorized::VSortNode> {
public:
    SortSourceOperatorBuilder(int32_t id, ExecNode* sort_node);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class SortSourceOperator final : public SourceOperator<SortSourceOperatorBuilder> {
public:
    SortSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node);
    Status open(RuntimeState*) override { return Status::OK(); }
};

class SortSourceOperatorX;
class SortLocalState final : public PipelineXLocalState<SortDependency> {
public:
    ENABLE_FACTORY_CREATOR(SortLocalState);
    SortLocalState(RuntimeState* state, OperatorXBase* parent);
    ~SortLocalState() override = default;

private:
    friend class SortSourceOperatorX;
};

class SortSourceOperatorX final : public OperatorX<SortLocalState> {
public:
    SortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                        const DescriptorTbl& descs);
    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class SortLocalState;
};

} // namespace pipeline
} // namespace doris
