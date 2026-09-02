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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "exec/pipeline/dependency.h"

namespace doris {

class AggFnEvaluator;
class Arena;
struct GroupJoinDataVariants;

struct GroupJoinSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(GroupJoinSharedState)

    GroupJoinSharedState();
    ~GroupJoinSharedState() override;

    bool probe_eos = false;
    bool result_emitted = false;
    std::unique_ptr<GroupJoinDataVariants> data_variants;
    std::shared_ptr<Arena> arena;
    int64_t total_match_count = 0;
    bool drain_inited = false;
    std::vector<TGroupJoinAggSide::type> aggregate_sides;
    std::vector<AggFnEvaluator*> aggregate_evaluators;
    std::vector<size_t> offsets_of_aggregate_states;
    std::vector<size_t> sizes_of_aggregate_states;
    std::vector<size_t> aligns_of_aggregate_states;
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    bool agg_layout_ready = false;
};

} // namespace doris
