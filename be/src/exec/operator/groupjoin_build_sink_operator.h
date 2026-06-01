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
#include "exec/operator/hashjoin_build_sink.h"
#include "exprs/vectorized_agg_fn.h"

namespace doris {

class GroupJoinBuildSinkLocalState;

class GroupJoinBuildSinkOperatorX MOCK_REMOVE(final) : public HashJoinBuildSinkOperatorX {
public:
    GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;

protected:
    Status _on_build_complete(RuntimeState* state,
                              HashJoinBuildSinkLocalState& local_state) override;

    friend class GroupJoinBuildSinkLocalState;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states = 0;
};

class GroupJoinBuildSinkLocalState : public HashJoinBuildSinkLocalState {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinBuildSinkLocalState);
    GroupJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : HashJoinBuildSinkLocalState(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status close(RuntimeState* state, Status exec_status) override;

    friend class GroupJoinBuildSinkOperatorX;

private:
    Status _compute_group_join_aggregates(RuntimeState* state);
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::unique_ptr<Arena> _agg_arena;
    std::vector<AggregateDataPtr> _group_states;
};

} // namespace doris
