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

#include <queue>

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "exec/common/agg_utils.h"
#include "runtime/runtime_profile.h"

namespace doris {

class AggFnEvaluator;
class RuntimeState;
class VExprContext;
using VExprContextSPtr = std::shared_ptr<VExprContext>;
using VExprContextSPtrs = std::vector<VExprContextSPtr>;

/// GroupByAggContext encapsulates all hash-table-based aggregation logic for GROUP BY queries.
/// It is shared between AggSinkLocalState (write path) and AggLocalState (read path) in
/// 2-phase aggregation, or owned locally by StreamingAggLocalState in 1-phase streaming agg.
///
/// InlineCountAggContext (subclass) overrides virtual methods to implement the
/// inline-count optimization (storing UInt64 count directly in the hash table mapped slot
/// instead of a full aggregate state).
class GroupByAggContext {
public:
    GroupByAggContext(std::vector<AggFnEvaluator*> agg_evaluators,
                     VExprContextSPtrs groupby_expr_ctxs, Sizes agg_state_offsets,
                     size_t total_agg_state_size, size_t agg_state_alignment,
                     bool is_first_phase);

    virtual ~GroupByAggContext();

    // ==================== Aggregation execution (Sink side) ====================

    /// Update mode: evaluate groupby exprs → emplace → execute_batch_add
    virtual Status execute_with_serialized_key(Block* block);

    /// Emplace + execute_batch_add with pre-evaluated key columns.
    /// InlineCountAggContext overrides to only emplace (count++ is done internally).
    /// Used by StreamingAgg which evaluates key expressions separately.
    virtual Status emplace_and_forward(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                       uint32_t num_rows, Block* block, bool expand_hash_table);

    /// Merge mode: evaluate groupby exprs → emplace → deserialize_and_merge
    virtual Status merge_with_serialized_key(Block* block);

    /// Merge for spill restore (keys already materialized as first N columns of block)
    Status merge_with_serialized_key_for_spill(Block* block);

    // ==================== Result output (Source side) ====================

    /// Serialize mode output (for non-finalize path and StreamingAgg)
    virtual Status get_serialized_results(RuntimeState* state, Block* block, bool* eos);

    /// Finalize mode output (for AggSource finalize path)
    virtual Status get_finalized_results(RuntimeState* state, Block* block, bool* eos,
                                         const ColumnsWithTypeAndName& columns_with_schema);

    // ==================== Agg state management ====================

    virtual Status create_agg_state(AggregateDataPtr data);
    virtual void close();

    // ==================== Utilities ====================

    size_t hash_table_size() const;
    size_t memory_usage() const;
    void update_memusage();
    void init_hash_method();
    /// Initialize the AggregateDataContainer after hash method is set up.
    /// Must be called after init_hash_method().
    virtual void init_agg_data_container();
    virtual Status reset_hash_table();

    /// Sink operator calls this to register sink-side profile counters.
    void init_sink_profile(RuntimeProfile* profile);
    /// Source operator calls this to register source-side profile counters.
    void init_source_profile(RuntimeProfile* profile);

    /// Evaluate groupby expressions on block, filling key_columns and optionally key_locs.
    /// Handles convert_to_full_column_if_const and replace_float_special_values.
    Status evaluate_groupby_keys(Block* block, ColumnRawPtrs& key_columns,
                                 std::vector<int>* key_locs = nullptr);

    // ==================== Sort limit ====================

    void build_limit_heap(size_t hash_table_size);
    bool do_limit_filter(size_t num_rows, const ColumnRawPtrs& key_columns);
    void refresh_top_limit(size_t row_id, const ColumnRawPtrs& key_columns);
    /// Update limit heap with new top-N candidates from passthrough path.
    /// Finds the first row where cmp_res==1 && need_computes[i], inserts into heap, then breaks.
    void add_limit_heap_top(ColumnRawPtrs& key_columns, size_t rows);

    /// Emplace with sort-limit filtering. Returns true if aggregation should proceed.
    /// When key_locs is provided, re-fetches key_columns from block after filtering.
    bool emplace_into_hash_table_limit(AggregateDataPtr* places, Block* block,
                                       const std::vector<int>* key_locs,
                                       ColumnRawPtrs& key_columns, uint32_t num_rows);

    // ==================== Data accessors ====================

    AggregatedDataVariants* hash_table_data() { return _hash_table_data.get(); }
    Arena& agg_arena() { return _agg_arena; }
    AggregateDataContainer* agg_data_container() { return _agg_data_container.get(); }
    std::vector<AggFnEvaluator*>& agg_evaluators() { return _agg_evaluators; }
    const VExprContextSPtrs& groupby_expr_ctxs() const { return _groupby_expr_ctxs; }
    const Sizes& agg_state_offsets() const { return _agg_state_offsets; }
    size_t total_agg_state_size() const { return _total_agg_state_size; }
    size_t agg_state_alignment() const { return _agg_state_alignment; }
    PaddedPODArray<uint8_t>& need_computes() { return _need_computes; }

    // Sort limit public state
    int64_t limit = -1;
    bool do_sort_limit = false;
    bool reach_limit = false;
    std::vector<int> order_directions;
    std::vector<int> null_directions;

    // Limit check configuration (set by operator during open)
    bool should_limit_output = false;
    bool enable_spill = false;

    // Key columns that need nullable wrapping in output (left/full join).
    // When non-empty, mem_reuse must be disabled in get_*_results to avoid
    // column type mismatch after make_nullable_output_key transforms the block.
    std::vector<size_t> make_nullable_keys;

    // Memory tracking for reserve estimation
    int64_t memory_usage_last_executing = 0;

    // Sink-side profile counters (public for operator-level SCOPED_TIMER access)
    RuntimeProfile::Counter* build_timer() const { return _build_timer; }
    RuntimeProfile::Counter* merge_timer() const { return _merge_timer; }
    RuntimeProfile::Counter* expr_timer() const { return _expr_timer; }
    RuntimeProfile::Counter* deserialize_data_timer() const { return _deserialize_data_timer; }
    RuntimeProfile::Counter* hash_table_compute_timer() const { return _hash_table_compute_timer; }
    RuntimeProfile::Counter* hash_table_emplace_timer() const { return _hash_table_emplace_timer; }
    RuntimeProfile::Counter* hash_table_input_counter() const { return _hash_table_input_counter; }

    // Source-side profile counters
    RuntimeProfile::Counter* get_results_timer() const { return _get_results_timer; }
    RuntimeProfile::Counter* hash_table_iterate_timer() const { return _hash_table_iterate_timer; }
    RuntimeProfile::Counter* insert_keys_to_column_timer() const {
        return _insert_keys_to_column_timer;
    }
    RuntimeProfile::Counter* insert_values_to_column_timer() const {
        return _insert_values_to_column_timer;
    }
    RuntimeProfile::Counter* hash_table_limit_compute_timer() const {
        return _hash_table_limit_compute_timer;
    }

    // For spill: estimate memory needed
    size_t get_reserve_mem_size(RuntimeState* state) const;

protected:
    // ==================== Internal hash table operations ====================

    /// Insert keys into the hash table, fill places array. New keys get agg state created.
    /// Counter parameters allow callers to direct timing to sink or source profile counters.
    virtual void emplace_into_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                         uint32_t num_rows,
                                         RuntimeProfile::Counter* hash_table_compute_timer,
                                         RuntimeProfile::Counter* hash_table_emplace_timer,
                                         RuntimeProfile::Counter* hash_table_input_counter);

    /// Find existing keys in hash table (used when reach_limit && !do_sort_limit).
    void find_in_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                            uint32_t num_rows);

    virtual void destroy_agg_state(AggregateDataPtr data);

    /// Convert columns at specified positions to nullable.
    static void make_nullable_output_key(Block* block,
                                         const std::vector<size_t>& make_nullable_keys);

    /// Get the column id from an evaluator's input expression (used in merge path).
    /// Only valid for 1st phase evaluators with a single SlotRef input.
    static int get_slot_column_id(const AggFnEvaluator* evaluator);

    // Core hash table data
    AggregatedDataVariantsUPtr _hash_table_data;
    Arena _agg_arena;
    std::unique_ptr<AggregateDataContainer> _agg_data_container;

    // Aggregation metadata
    std::vector<AggFnEvaluator*> _agg_evaluators;
    VExprContextSPtrs _groupby_expr_ctxs;
    Sizes _agg_state_offsets;
    size_t _total_agg_state_size;
    size_t _agg_state_alignment;
    bool _is_first_phase;

    // Working buffers
    PODArray<AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;
    std::vector<AggregateDataPtr> _values;

    // Sort limit state
    MutableColumns _limit_columns;
    int _limit_columns_min = -1;
    PaddedPODArray<uint8_t> _need_computes;
    std::vector<uint8_t> _cmp_res;

    struct HeapLimitCursor {
        HeapLimitCursor(int row_id, MutableColumns& limit_columns,
                        std::vector<int>& order_directions, std::vector<int>& null_directions)
                : _row_id(row_id),
                  _limit_columns(limit_columns),
                  _order_directions(order_directions),
                  _null_directions(null_directions) {}

        HeapLimitCursor(const HeapLimitCursor& other) = default;

        HeapLimitCursor(HeapLimitCursor&& other) noexcept
                : _row_id(other._row_id),
                  _limit_columns(other._limit_columns),
                  _order_directions(other._order_directions),
                  _null_directions(other._null_directions) {}

        HeapLimitCursor& operator=(const HeapLimitCursor& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        HeapLimitCursor& operator=(HeapLimitCursor&& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        bool operator<(const HeapLimitCursor& rhs) const {
            for (int i = 0; i < _limit_columns.size(); ++i) {
                const auto& col = _limit_columns[i];
                auto res = col->compare_at(_row_id, rhs._row_id, *col, _null_directions[i]) *
                           _order_directions[i];
                if (res < 0) {
                    return true;
                } else if (res > 0) {
                    return false;
                }
            }
            return false;
        }

        int _row_id;
        MutableColumns& _limit_columns;
        std::vector<int>& _order_directions;
        std::vector<int>& _null_directions;
    };

    std::priority_queue<HeapLimitCursor> _limit_heap;
    MutableColumns _get_keys_hash_table();

    template <bool limit, bool for_spill = false>
    Status _merge_with_serialized_key_helper(Block* block);

    /// Check and update reach_limit after emplace (execute path)
    void _check_limit_after_emplace();
    /// Check and update reach_limit after emplace (merge path, simpler: no topn multiplier)
    void _check_limit_after_emplace_for_merge();

    // ---- Sink-side profile counters (created by init_sink_profile) ----
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_limit_compute_timer = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _serialize_key_arena_memory_usage = nullptr;
    RuntimeProfile::Counter* _memory_usage_container = nullptr;
    RuntimeProfile::Counter* _memory_usage_arena = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

    // ---- Source-side profile counters (created by init_source_profile) ----
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _insert_values_to_column_timer = nullptr;

    // Source-side counters for overlapping metrics (same names as sink, different profile).
    // Used during spill recovery merge path (for_spill=true) so that
    // PartitionedAggLocalState::_update_profile can read them from the inner source profile.
    RuntimeProfile::Counter* _source_merge_timer = nullptr;
    RuntimeProfile::Counter* _source_deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _source_hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _source_hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _source_hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _source_hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _source_hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _source_memory_usage_container = nullptr;
    RuntimeProfile::Counter* _source_memory_usage_arena = nullptr;
};

} // namespace doris
