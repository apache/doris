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

#include "exec/pipeline/dependency.h"

namespace doris {

struct DistinctDataVariants;

struct RecCTESharedState : public BasicSharedState {
    // Defined in rec_cte_shared_state.cpp: DistinctDataVariants carries the
    // full hash-table variant machinery.
    RecCTESharedState();
    ~RecCTESharedState() override;

    std::vector<TRecCTETarget> targets;
    std::vector<Block> blocks;
    IColumn::Selector distinct_row;
    Dependency* source_dep = nullptr;
    Dependency* anchor_dep = nullptr;
    Arena arena;
    RuntimeProfile::Counter* hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* hash_table_input_counter = nullptr;

    // No `= nullptr` initializer: a default member initializer makes GCC
    // instantiate ~unique_ptr<DistinctDataVariants> in every TU that merely sees
    // this class, which needs the complete type. The defaulted constructor in
    // rec_cte_shared_state.cpp already leaves this null.
    std::unique_ptr<DistinctDataVariants> agg_data;

    int current_round = 0;
    int last_round_offset = 0;
    int max_recursion_depth = 0;
    bool ready_to_return = false;

    void update_ready_to_return() {
        if (last_round_offset == blocks.size()) {
            ready_to_return = true;
        }
    }

    // Bodies live in rec_cte_shared_state.cpp: they touch the distinct hash
    // table variant machinery and the brpc client stack.
    Status emplace_block(RuntimeState* state, Block&& block);

    PTransmitRecCTEBlockParams build_basic_param(RuntimeState* state,
                                                 const TRecCTETarget& target) const;

    Status send_data_to_targets(RuntimeState* state, size_t round_offset) const;
};

} // namespace doris
