
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
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vanalytic_eval_node.h"

namespace doris {
class ExecNode;

namespace pipeline {
class AnalyticSinkOperatorBuilder final : public OperatorBuilder<vectorized::VAnalyticEvalNode> {
public:
    AnalyticSinkOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; }
};

class AnalyticSinkOperator final : public StreamingOperator<AnalyticSinkOperatorBuilder> {
public:
    AnalyticSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* node);

    bool can_write() override { return _node->can_write(); }
};

class AnalyticSinkOperatorX;

class AnalyticSinkLocalState : public PipelineXSinkLocalState<AnalyticDependency> {
    ENABLE_FACTORY_CREATOR(AnalyticSinkLocalState);

public:
    AnalyticSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<AnalyticDependency>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class AnalyticSinkOperatorX;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _evaluation_timer;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage;

    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
};

class AnalyticSinkOperatorX final : public DataSinkOperatorX<AnalyticSinkLocalState> {
public:
    AnalyticSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<AnalyticSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

private:
    Status _insert_range_column(vectorized::Block* block, const vectorized::VExprContextSPtr& expr,
                                vectorized::IColumn* dst_column, size_t length);

    friend class AnalyticSinkLocalState;

    std::vector<vectorized::VExprContextSPtrs> _agg_expr_ctxs;
    vectorized::VExprContextSPtrs _partition_by_eq_expr_ctxs;
    vectorized::VExprContextSPtrs _order_by_eq_expr_ctxs;

    size_t _agg_functions_size = 0;

    const TTupleId _buffered_tuple_id;

    std::vector<size_t> _num_agg_input;
};

} // namespace pipeline
} // namespace doris
