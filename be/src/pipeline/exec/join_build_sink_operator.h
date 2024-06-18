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

namespace doris::pipeline {
template <typename LocalStateType>
class JoinBuildSinkOperatorX;

template <typename SharedStateType, typename Derived>
class JoinBuildSinkLocalState : public PipelineXSinkLocalState<SharedStateType> {
public:
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    const std::vector<IRuntimeFilter*>& runtime_filters() const { return _runtime_filters; }

protected:
    JoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<SharedStateType>(parent, state) {}
    ~JoinBuildSinkLocalState() override = default;
    template <typename LocalStateType>
    friend class JoinBuildSinkOperatorX;

    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _publish_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _runtime_filter_compute_timer = nullptr;
    RuntimeProfile::Counter* _runtime_filter_init_timer = nullptr;
    std::vector<IRuntimeFilter*> _runtime_filters;
};

template <typename LocalStateType>
class JoinBuildSinkOperatorX : public DataSinkOperatorX<LocalStateType> {
public:
    JoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                           const DescriptorTbl& descs);
    ~JoinBuildSinkOperatorX() override = default;

protected:
    void _init_join_op();
    template <typename DependencyType, typename Derived>
    friend class JoinBuildSinkLocalState;

    const TJoinOp::type _join_op;
    JoinOpVariants _join_op_variants;

    const bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    bool _build_unique;          // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_left_semi_anti;
    const bool _is_outer_join;
    const bool _is_mark_join;

    // For null aware left anti join, we apply a short circuit strategy.
    // 1. Set _short_circuit_for_null_in_build_side to true if join operator is null aware left anti join.
    // 2. In build phase, we stop materialize build side when we meet the first null value and set _has_null_in_build_side to true.
    // 3. In probe phase, if _has_null_in_build_side is true, join node returns empty block directly. Otherwise, probing will continue as the same as generic left anti join.
    const bool _short_circuit_for_null_in_build_side;

    const std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
};

} // namespace doris::pipeline
