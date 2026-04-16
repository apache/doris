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

/// AggContext is the abstract base class for all aggregation context implementations.
/// It unifies GroupByAggContext (with hash table) and UngroupByAggContext (without GROUP BY)
/// behind a common interface aligned with AggMode semantics:
///   - update:    original data -> aggregate state  (INPUT_TO_*)
///   - merge:     intermediate state -> aggregate state  (BUFFER_TO_*)
///   - serialize: aggregate state -> intermediate state  (*_TO_BUFFER)
///   - finalize:  aggregate state -> final result  (*_TO_RESULT)
class AggContext {
public:
    /// Constructor: state layout only, evaluators set later via set_evaluators().
    AggContext(Sizes agg_state_offsets, size_t total_agg_state_size, size_t agg_state_alignment)
            : _agg_state_offsets(std::move(agg_state_offsets)),
              _total_agg_state_size(total_agg_state_size),
              _agg_state_alignment(agg_state_alignment) {}

    virtual ~AggContext() = default;

    void set_evaluators(std::vector<AggFnEvaluator*> evaluators) {
        _agg_evaluators = std::move(evaluators);
    }

    // ==================== Sink-side operations ====================

    /// Update mode: feed original data into aggregate state.
    virtual Status update(Block* block) = 0;

    /// Merge mode: merge intermediate state into aggregate state.
    virtual Status merge(Block* block) = 0;

    // ==================== Source-side operations ====================

    /// Serialize: output intermediate aggregate state for downstream merge.
    virtual Status serialize(RuntimeState* state, Block* block, bool* eos) = 0;

    /// Finalize: output final aggregate results.
    /// Caller must call set_finalize_output() before the first finalize() call.
    virtual Status finalize(RuntimeState* state, Block* block, bool* eos) = 0;

    /// Set output schema for finalize(). Called once during source-side init.
    /// GroupByAggContext converts RowDescriptor to ColumnsWithTypeAndName.
    /// UngroupByAggContext stores the RowDescriptor reference.
    virtual void set_finalize_output(const RowDescriptor& row_desc) {}

    // ==================== Lifecycle ====================

    virtual void close() = 0;
    virtual void update_memusage() = 0;

    // ==================== Profile ====================

    /// Initialize source-side profile counters. Only GroupByAggContext has source-side counters.
    virtual void init_source_profile(RuntimeProfile* /*profile*/) {}

    // ==================== Memory / hash table queries ====================

    /// Return total memory usage of this aggregation context.
    virtual size_t memory_usage() const { return 0; }

    /// Return the number of rows in the hash table (0 for UngroupByAggContext).
    virtual size_t hash_table_size() const { return 0; }

    /// Reset the hash table (destroy agg states, clear buckets).
    virtual Status reset_hash_table() { return Status::OK(); }

    /// Estimate memory needed for reserving `rows` more rows (for spill decisions).
    virtual size_t get_reserve_mem_size(RuntimeState* /*state*/) const { return 0; }

    /// Estimate memory needed to merge `rows` rows into the hash table (for spill decisions).
    virtual size_t estimated_memory_for_merging(size_t /*rows*/) const { return 0; }

    // ==================== Spill support ====================

    /// Merge for spill restore (keys already materialized as first N columns of block).
    /// Only GroupByAggContext implements this; UngroupByAggContext never spills.
    virtual Status merge_for_spill(Block* /*block*/) {
        return Status::InternalError("merge_for_spill not supported for this context");
    }

    // ==================== Limit support ====================

    /// Apply limit/sort-limit filtering on the output block.
    /// Returns true if the caller should apply reached_limit() truncation.
    /// Returns false if the block is already filtered (or no limit applies) — caller just counts.
    virtual bool apply_limit_filter(Block* /*block*/) { return false; }

    // ==================== Common accessors ====================

    std::vector<AggFnEvaluator*>& agg_evaluators() { return _agg_evaluators; }
    const Sizes& agg_state_offsets() const { return _agg_state_offsets; }
    size_t total_agg_state_size() const { return _total_agg_state_size; }
    size_t agg_state_alignment() const { return _agg_state_alignment; }
    Arena& agg_arena() { return _agg_arena; }

    // ==================== Common profile timer accessors ====================

    RuntimeProfile::Counter* build_timer() const { return _build_timer; }
    RuntimeProfile::Counter* merge_timer() const { return _merge_timer; }
    RuntimeProfile::Counter* deserialize_data_timer() const { return _deserialize_data_timer; }
    RuntimeProfile::Counter* get_results_timer() const { return _get_results_timer; }

    /// Memory tracking for reserve estimation.
    int64_t memory_usage_last_executing = 0;

protected:
    std::vector<AggFnEvaluator*> _agg_evaluators;
    Sizes _agg_state_offsets;
    size_t _total_agg_state_size;
    size_t _agg_state_alignment;
    Arena _agg_arena;

    // Common profile counters (initialized by subclass init_profile / init_sink_profile)
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _memory_usage_arena = nullptr;
};

} // namespace doris
