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

#include <gen_cpp/PlanNodes_types.h>

#include <memory>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "exec/common/groupjoin_utils.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/dependency.h"
#include "exec/runtime_filter/runtime_filter_producer_helper_groupjoin.h"

namespace doris {

class AggFnEvaluator;
class GroupJoinBuildSinkOperatorX;

class GroupJoinBuildSinkLocalState final : public PipelineXSinkLocalState<GroupJoinSharedState> {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinBuildSinkLocalState);
    using Base = PipelineXSinkLocalState<GroupJoinSharedState>;
    using Parent = GroupJoinBuildSinkOperatorX;

    GroupJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~GroupJoinBuildSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status terminate(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    Dependency* finishdependency() override { return _finish_dependency.get(); }

private:
    friend class GroupJoinBuildSinkOperatorX;

    Status _append_runtime_filter_columns(Block* block);

    VExprContextSPtrs _build_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    ColumnRawPtrs _build_key_not_nullable_columns;
    std::vector<ColumnPtr> _key_columns_holder;
    std::vector<AggregateDataPtr> _places;
    ColumnUInt8::MutablePtr _null_map_column;
    std::shared_ptr<RuntimeFilterProducerHelperGroupJoin> _runtime_filter_producer_helper;
    std::shared_ptr<CountedFinishDependency> _finish_dependency;
    bool _runtime_filter_size_sent = false;
};

class GroupJoinBuildSinkOperatorX final : public DataSinkOperatorX<GroupJoinBuildSinkLocalState> {
public:
    using Base = DataSinkOperatorX<GroupJoinBuildSinkLocalState>;

    GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink", _name);
    }
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status sink_impl(RuntimeState* state, Block* in_block, bool eos) override;

    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_shuffled_operator() const override;
    bool is_colocated_operator() const override;
    bool followed_by_shuffled_operator() const override;

private:
    friend class GroupJoinBuildSinkLocalState;

    const TJoinDistributionType::type _join_distribution;
    ObjectPool* _pool = nullptr;
    std::vector<TExpr> _partition_exprs;
    VExprContextSPtrs _build_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::vector<int> _aggregate_indices;
    std::vector<TGroupJoinAggSide::type> _aggregate_sides;
    const std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    Sizes _sizes_of_aggregate_states;
    Sizes _aligns_of_aggregate_states;
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
};

} // namespace doris
