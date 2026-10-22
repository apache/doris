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

#include "core/arena.h"
#include "core/custom_allocator.h"
#include "exec/operator/groupjoin_shared_state.h"
#include "exec/operator/operator.h"

namespace doris {
class GroupJoinBuildSinkOperatorX;
class RuntimeFilterProducerHelper;

class GroupJoinBuildSinkLocalState final : public PipelineXSinkLocalState<GroupJoinSharedState> {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinBuildSinkLocalState);
    using Base = PipelineXSinkLocalState<GroupJoinSharedState>;
    using Parent = GroupJoinBuildSinkOperatorX;
    GroupJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {
        _finish_dependency = std::make_shared<CountedFinishDependency>(
                parent->operator_id(), parent->node_id(),
                parent->get_name() + "_FINISH_DEPENDENCY");
    }
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status terminate(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Dependency* finishdependency() override { return _finish_dependency.get(); }

private:
    friend class GroupJoinBuildSinkOperatorX;
    MutableBlock _build_rows;
    VExprContextSPtrs _key_exprs;
    std::shared_ptr<RuntimeFilterProducerHelper> _filter_helper;
    std::shared_ptr<CountedFinishDependency> _finish_dependency;
    bool _filter_size_sent = false;
    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
};

class GroupJoinBuildSinkOperatorX final : public DataSinkOperatorX<GroupJoinBuildSinkLocalState> {
public:
    using Base = DataSinkOperatorX<GroupJoinBuildSinkLocalState>;
    GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TDataSink&) override {
        return Status::InternalError("GroupJoin requires a plan node");
    }
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status sink_impl(RuntimeState* state, Block* block, bool eos) override;
    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_shuffled_operator() const override;
    bool is_colocated_operator() const override;
    bool followed_by_shuffled_operator() const override;
    size_t get_reserve_mem_size(RuntimeState* state, bool eos, const Block* block) override;

private:
    friend class GroupJoinBuildSinkLocalState;
    Status _build_hash_table(RuntimeState* state, GroupJoinBuildSinkLocalState& local) const;
    VExprContextSPtrs _key_exprs;
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    TJoinDistributionType::type _distribution;
    std::vector<TExpr> _partition_exprs;
};

extern template class DataSinkOperatorX<GroupJoinBuildSinkLocalState>;
extern template class PipelineXSinkLocalState<GroupJoinSharedState>;

} // namespace doris
