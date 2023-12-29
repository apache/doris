
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

class AnalyticSinkOperator final : public StreamingOperator<vectorized::VAnalyticEvalNode> {
public:
    AnalyticSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* node);

    bool can_write() override { return _node->can_write(); }
};

class AnalyticSinkDependency final : public Dependency {
public:
    using SharedState = AnalyticSharedState;
    AnalyticSinkDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "AnalyticSinkDependency", true, query_ctx) {}
    ~AnalyticSinkDependency() override = default;
};

class AnalyticSinkOperatorX;

class AnalyticSinkLocalState : public PipelineXSinkLocalState<AnalyticSinkDependency> {
    ENABLE_FACTORY_CREATOR(AnalyticSinkLocalState);

public:
    AnalyticSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<AnalyticSinkDependency>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class AnalyticSinkOperatorX;

    bool _refresh_need_more_input() {
        auto need_more_input = _whether_need_next_partition(_shared_state->found_partition_end);
        if (need_more_input) {
            _shared_state->source_dep->block();
            _dependency->set_ready();
        } else {
            _dependency->block();
            _shared_state->source_dep->set_ready();
        }
        return need_more_input;
    }
    vectorized::BlockRowPos _get_partition_by_end();
    vectorized::BlockRowPos _compare_row_to_find_end(int idx, vectorized::BlockRowPos start,
                                                     vectorized::BlockRowPos end,
                                                     bool need_check_first = false);
    bool _whether_need_next_partition(vectorized::BlockRowPos& found_partition_end);

    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _evaluation_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;

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
    DataDistribution required_data_distribution() const override {
        if (_partition_by_eq_expr_ctxs.empty()) {
            return {ExchangeType::PASSTHROUGH};
        } else if (_order_by_eq_expr_ctxs.empty()) {
            return _is_colocate
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        }
        return DataSinkOperatorX<AnalyticSinkLocalState>::required_data_distribution();
    }

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
    const bool _is_colocate;
    const std::vector<TExpr> _partition_exprs;
};

} // namespace pipeline
} // namespace doris
