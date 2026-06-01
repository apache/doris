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

#include "exec/operator/hashjoin_probe_operator.h"
#include "exprs/vectorized_agg_fn.h"

namespace doris {

class GroupJoinProbeLocalState;

class GroupJoinProbeOperatorX MOCK_REMOVE(final) : public HashJoinProbeOperatorX {
public:
    GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::vector<DataTypePtr> _aggregate_data_types;
    std::vector<std::string> _aggregate_column_names;
};

class GroupJoinProbeLocalState : public HashJoinProbeLocalState {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinProbeLocalState);
    GroupJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : HashJoinProbeLocalState(state, parent) {}

    Status open(RuntimeState* state) override;
};

} // namespace doris
