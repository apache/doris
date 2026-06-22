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

#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "exec/common/groupjoin_utils.h"
#include "exprs/vexpr_fwd.h"

namespace doris {

class Arena;
class AggFnEvaluator;
class GroupJoinProbeLocalState;
struct GroupJoinSharedState;
namespace groupjoin {

Status validate_group_join_node(const TPlanNode& tnode);

Status do_evaluate(const Block& block, VExprContextSPtrs& exprs,
                   std::vector<ColumnPtr>& key_columns_holder);

// Hash map batch APIs take ColumnRawPtrs as key input. This helper converts evaluated
// key expression columns to raw column pointers and builds the combined null map.
// GroupJoin currently supports only normal equal join keys. Therefore nullable keys are
// split into nested columns and the combined null map is used to skip rows with NULL keys:
// for a normal inner equal join, any row containing a NULL join key can never match.
// Null-safe equal needs a different path that keeps nullable keys encoded in the hash key.
Status extract_key_columns(size_t rows, const std::vector<ColumnPtr>& key_columns_holder,
                           ColumnRawPtrs& key_not_nullable_columns,
                           ColumnUInt8::MutablePtr& null_map_column);

Status register_agg_state_layout(GroupJoinSharedState* shared_state,
                                 const std::vector<TGroupJoinAggSide::type>& aggregate_sides,
                                 const Sizes& sizes_of_aggregate_states,
                                 const Sizes& aligns_of_aggregate_states,
                                 const std::vector<int>& aggregate_indices,
                                 const std::vector<AggFnEvaluator*>& aggregate_evaluators);

Status add_build_counts_by_key(GroupJoinSharedState* shared_state, Arena& arena,
                               ColumnRawPtrs& key_not_nullable_columns, uint32_t num_rows,
                               const uint8_t* null_map, const std::vector<int>& aggregate_indices,
                               AggregateDataPtr* places);

Status update_probe_counts(GroupJoinSharedState* shared_state, Arena& arena,
                           ColumnRawPtrs& key_not_nullable_columns, uint32_t num_rows,
                           const uint8_t* null_map, const std::vector<int>& aggregate_indices,
                           AggregateDataPtr* places, int64_t& matched_rows,
                           uint32_t& matched_probe_rows);

void create_all_agg_states(GroupJoinSharedState* shared_state, AggregateDataPtr data);

void destroy_all_agg_states(GroupJoinSharedState* shared_state, AggregateDataPtr data);

Status drain_groupjoin_result(GroupJoinSharedState* shared_state, size_t batch_size,
                              GroupJoinProbeLocalState& local_state, MutableColumns& key_columns,
                              MutableColumns& value_columns, bool& output_eos);

void destroy_agg_states(GroupJoinSharedState* shared_state);

} // namespace groupjoin
} // namespace doris
