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
#include "vec/exec/vpartition_sort_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class PartitionSortSourceOperatorBuilder final
        : public OperatorBuilder<vectorized::VPartitionSortNode> {
public:
    PartitionSortSourceOperatorBuilder(int32_t id, ExecNode* sort_node)
            : OperatorBuilder(id, "PartitionSortSourceOperator", sort_node) {}

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class PartitionSortSourceOperator final
        : public SourceOperator<PartitionSortSourceOperatorBuilder> {
public:
    PartitionSortSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node)
            : SourceOperator(operator_builder, sort_node) {}
    Status open(RuntimeState*) override { return Status::OK(); }
};

class PartitionSortSourceDependency final : public Dependency {
public:
    using SharedState = PartitionSortNodeSharedState;
    PartitionSortSourceDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "PartitionSortSourceDependency", query_ctx) {}
    ~PartitionSortSourceDependency() override = default;
};

class PartitionSortSourceOperatorX;
class PartitionSortSourceLocalState final
        : public PipelineXLocalState<PartitionSortSourceDependency> {
public:
    ENABLE_FACTORY_CREATOR(PartitionSortSourceLocalState);
    using Base = PipelineXLocalState<PartitionSortSourceDependency>;
    PartitionSortSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<PartitionSortSourceDependency>(state, parent),
              _get_sorted_timer(nullptr) {}

    Status init(RuntimeState* state, LocalStateInfo& info) override;

private:
    friend class PartitionSortSourceOperatorX;
    RuntimeProfile::Counter* _get_sorted_timer;
    std::atomic<int> _sort_idx = 0;
};

class PartitionSortSourceOperatorX final : public OperatorX<PartitionSortSourceLocalState> {
public:
    using Base = OperatorX<PartitionSortSourceLocalState>;
    PartitionSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs)
            : OperatorX<PartitionSortSourceLocalState>(pool, tnode, operator_id, descs) {}

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class PartitionSortSourceLocalState;
    Status get_sorted_block(RuntimeState* state, vectorized::Block* output_block,
                            PartitionSortSourceLocalState& local_state);
};

} // namespace pipeline
} // namespace doris
