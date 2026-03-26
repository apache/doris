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

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "runtime/runtime_profile.h"

namespace doris {

class AggFnEvaluator;
class RuntimeState;
class RowDescriptor;
using AggregateDataPtr = char*;
using Sizes = std::vector<size_t>;

/// UngroupByAggContext encapsulates aggregation logic for queries WITHOUT GROUP BY.
/// There is no hash table — only a single AggregateDataPtr pointing to one agg state row.
///
/// This context is used by AggSinkLocalState (sink side: execute/merge) and
/// AggLocalState (source side: get_serialized_result/get_finalized_result).
class UngroupByAggContext {
public:
    UngroupByAggContext(std::vector<AggFnEvaluator*> agg_evaluators, Sizes agg_state_offsets,
                        size_t total_agg_state_size, size_t agg_state_alignment);

    ~UngroupByAggContext();

    // ==================== Aggregation execution (Sink side) ====================

    /// Update mode: execute_single_add for each evaluator.
    Status execute(Block* block);

    /// Merge mode: deserialize_and_merge or execute_single_add depending on evaluator.
    Status merge(Block* block);

    // ==================== Result output (Source side) ====================

    /// Serialize mode: serialize agg state to output block (for non-finalize path).
    Status get_serialized_result(RuntimeState* state, Block* block, bool* eos);

    /// Finalize mode: insert final result info to output block.
    Status get_finalized_result(RuntimeState* state, Block* block, bool* eos,
                                const RowDescriptor& row_desc);

    // ==================== Utilities ====================

    /// Update memory usage counters.
    void update_memusage();

    /// Create profile counters under the given profile.
    void init_profile(RuntimeProfile* profile);

    /// Destroy agg state if created. Safe to call multiple times.
    void close();

    AggregateDataPtr agg_state_data() const { return _agg_state_data; }
    Arena& agg_arena() { return _agg_arena; }
    std::vector<AggFnEvaluator*>& agg_evaluators() { return _agg_evaluators; }

    /// Track total input rows (used by source to detect empty input).
    size_t input_num_rows = 0;

    /// Memory tracking for reserve estimation (Sink side).
    int64_t memory_usage_last_executing = 0;

    // Profile timer accessors (for SCOPED_TIMER in operator code)
    RuntimeProfile::Counter* build_timer() const { return _build_timer; }
    RuntimeProfile::Counter* merge_timer() const { return _merge_timer; }
    RuntimeProfile::Counter* deserialize_data_timer() const { return _deserialize_data_timer; }
    RuntimeProfile::Counter* get_results_timer() const { return _get_results_timer; }

private:
    /// Allocate and initialize aggregate states for all evaluators.
    Status _create_agg_state();

    /// Destroy aggregate states for all evaluators.
    void _destroy_agg_state();

    /// Get the input column id for a merge-mode evaluator (same logic as GroupByAggContext).
    static int _get_slot_column_id(const AggFnEvaluator* evaluator);

    AggregateDataPtr _agg_state_data = nullptr;
    Arena _agg_arena;
    Arena _alloc_arena; // used to allocate the agg state memory block

    std::vector<AggFnEvaluator*> _agg_evaluators;
    Sizes _agg_state_offsets;
    size_t _total_agg_state_size;
    size_t _agg_state_alignment;
    bool _agg_state_created = false;

    // ---- Profile counters (created by init_profile) ----
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _memory_usage_arena = nullptr;
};

} // namespace doris
