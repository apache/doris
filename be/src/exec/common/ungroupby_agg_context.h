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
#include "exec/common/agg_context.h"
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
/// AggLocalState (source side: serialize/finalize).
class UngroupByAggContext : public AggContext {
public:
    UngroupByAggContext(std::vector<AggFnEvaluator*> agg_evaluators, Sizes agg_state_offsets,
                        size_t total_agg_state_size, size_t agg_state_alignment);

    ~UngroupByAggContext();

    // ==================== Aggregation execution (Sink side) ====================

    /// Update mode: execute_single_add for each evaluator.
    Status update(Block* block) override;

    /// Merge mode: deserialize_and_merge or execute_single_add depending on evaluator.
    Status merge(Block* block) override;

    // ==================== Result output (Source side) ====================

    /// Serialize mode: serialize agg state to output block (for non-finalize path).
    Status serialize(RuntimeState* state, Block* block, bool* eos) override;

    /// Finalize mode: insert final result info to output block.
    /// Caller must call set_finalize_output() before the first call.
    Status finalize(RuntimeState* state, Block* block, bool* eos) override;

    /// Store the RowDescriptor reference for finalize().
    void set_finalize_output(const RowDescriptor& row_desc) override;

    // ==================== Utilities ====================

    /// Update memory usage counters.
    void update_memusage() override;

    /// Return total memory usage (arena only for ungroupby).
    size_t memory_usage() const override;

    /// Create profile counters under the given profile.
    void init_profile(RuntimeProfile* profile);

    /// Destroy agg state if created. Safe to call multiple times.
    void close() override;

    AggregateDataPtr agg_state_data() const { return _agg_state_data; }

    /// Track total input rows (used by source to detect empty input).
    size_t input_num_rows = 0;

private:
    /// Allocate and initialize aggregate states for all evaluators.
    Status _create_agg_state();

    /// Destroy aggregate states for all evaluators.
    void _destroy_agg_state();

    /// Get the input column id for a merge-mode evaluator (same logic as GroupByAggContext).
    static int _get_slot_column_id(const AggFnEvaluator* evaluator);

    AggregateDataPtr _agg_state_data = nullptr;
    Arena _alloc_arena; // used to allocate the agg state memory block
    bool _agg_state_created = false;

    ColumnsWithTypeAndName _finalize_schema;
};

} // namespace doris
