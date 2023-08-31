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
#include "vec/core/field.h"
#include "vec/exec/vsort_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class SortSinkOperatorBuilder final : public OperatorBuilder<vectorized::VSortNode> {
public:
    SortSinkOperatorBuilder(int32_t id, ExecNode* sort_node);

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;
};

class SortSinkOperator final : public StreamingOperator<SortSinkOperatorBuilder> {
public:
    SortSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node);

    bool can_write() override { return true; }
};

enum class SortAlgorithm { HEAP_SORT, TOPN_SORT, FULL_SORT };

class SortSinkOperatorX;

class SortSinkLocalState : public PipelineXSinkLocalState {
    ENABLE_FACTORY_CREATOR(SortSinkLocalState);

public:
    SortSinkLocalState(DataSinkOperatorX* parent, RuntimeState* state)
            : PipelineXSinkLocalState(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class SortSinkOperatorX;
    SortDependency* _dependency;
    SortSharedState* _shared_state;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _sort_blocks_memory_usage;
    RuntimeProfile::Counter* _child_get_next_timer = nullptr;
    RuntimeProfile::Counter* _sink_timer = nullptr;

    // topn top value
    vectorized::Field old_top {vectorized::Field::Types::Null};
};

class SortSinkOperatorX final : public DataSinkOperatorX {
public:
    SortSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode", _name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    bool can_write(RuntimeState* state) override { return true; }

    void get_dependency(DependencySPtr& dependency) override {
        dependency.reset(new SortDependency(id()));
    }

private:
    friend class SortSinkLocalState;

    // Number of rows to skip.
    const int64_t _offset;
    ObjectPool* _pool;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    bool _reuse_mem;
    const int64_t _limit;
    const bool _use_topn_opt;
    SortAlgorithm _algorithm;

    const RowDescriptor _row_descriptor;
    const bool _use_two_phase_read;
};

} // namespace pipeline
} // namespace doris
