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

#include <stdint.h>

#include <deque>
#include <memory>

#include "common/status.h"
#include "core/block/block.h"
#include "core/value/hll.h"
#include "exec/operator/operator.h"
#include "runtime/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

class StreamingAggOperatorX;

/// Adaptive phase of the streaming aggregation operator.
enum class AdaptivePhase : uint8_t {
    /// Initial phase: fixed-size micro hash table with HLL cardinality probing.
    PROBING,
    /// Rule A triggered: extremely high cardinality, skip hash table entirely.
    PASS_THROUGH,
    /// Rule B triggered: hash table resized to a larger fixed capacity.
    RESIZED,
    /// Rule C (default after probing): keep the initial micro hash table.
    STABLE,
};

class StreamingAggLocalState MOCK_REMOVE(final) : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = StreamingAggOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    ENABLE_FACTORY_CREATOR(StreamingAggLocalState);
    StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~StreamingAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_pre_agg(RuntimeState* state, Block* input_block);
    void make_nullable_output_key(Block* block);
    void build_limit_heap(size_t hash_table_size);

private:
    friend class StreamingAggOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    // --- Constants for adaptive streaming aggregation ---
    /// Micro hash table slot capacity (2^17).
    static constexpr size_t MICRO_HT_CAPACITY = 131072;
    /// Row threshold for adaptive decision (2^20 = 1,048,576).
    static constexpr size_t ADAPTIVITY_THRESHOLD = 1048576;
    /// Rule A: cardinality ratio above which we go pass-through.
    static constexpr double HIGH_CARDINALITY_RATIO = 0.95;
    /// Rule B: abandoned_count / hll_count ratio above which we resize.
    static constexpr double ABANDON_RATIO_THRESHOLD = 2.0;
    /// Hard memory ceiling for Rule B resize (256MB).
    static constexpr size_t MAX_HT_MEMORY_BYTES = 256ULL * 1024 * 1024;

    void _add_limit_heap_top(ColumnRawPtrs& key_columns, size_t rows);
    bool _do_limit_filter(size_t num_rows, ColumnRawPtrs& key_columns);
    Status _pre_agg_with_serialized_key(doris::Block* in_block);

    void _update_memusage_with_serialized_key();
    Status _init_hash_method(const VExprContextSPtrs& probe_exprs);
    Status _get_results_with_serialized_key(RuntimeState* state, Block* block, bool* eos);
    Status _create_agg_status(AggregateDataPtr data);
    size_t _get_hash_table_size();

    /// Flush all aggregated data from hash table into _output_blocks queue,
    /// destroy aggregate states, then clear hash table / arena / container.
    Status _flush_and_reset_hash_table();

    /// Recreate the AggregateDataContainer (called after clearing hash table).
    void _reset_aggregate_data_container();

    /// Run the adaptive decision logic when _sink_count reaches ADAPTIVITY_THRESHOLD.
    Status _check_adaptive_decision();

    /// Output a single input block as pass-through: each row gets a fresh agg state
    /// serialized independently (streaming_agg_serialize_to_column).
    Status _output_pass_through(Block* in_block, ColumnRawPtrs& key_columns, uint32_t rows);

    RuntimeProfile::Counter* _streaming_agg_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_limit_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _insert_values_to_column_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _flush_timer = nullptr;
    RuntimeProfile::Counter* _flush_count = nullptr;

    int64_t _cur_num_rows_returned = 0;
    Arena _agg_arena_pool;
    AggregatedDataVariantsUPtr _agg_data = nullptr;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    // group by k1,k2
    VExprContextSPtrs _probe_expr_ctxs;
    std::unique_ptr<AggregateDataContainer> _aggregate_data_container = nullptr;
    bool _reach_limit = false;
    size_t _input_num_rows = 0;

    // --- Adaptive streaming aggregation state ---
    AdaptivePhase _adaptive_phase = AdaptivePhase::PROBING;
    size_t _sink_count = 0;
    size_t _abandoned_count = 0;
    std::unique_ptr<HyperLogLog> _hll;
    /// Queue of output blocks ready for pull().
    std::deque<Block> _output_blocks;

    /// Persistent MutableColumns that accumulate output rows.  Both
    /// _flush_and_reset_hash_table() and _output_pass_through() write directly
    /// into these columns, avoiding any intermediate Block construction or
    /// merge-copy overhead.  When the row count reaches _output_batch_size the
    /// columns are finalized into a Block and pushed to _output_blocks.
    MutableColumns _pending_output_columns;
    size_t _pending_output_rows = 0;
    /// Batch size threshold for finalizing pending columns into output blocks.
    int _output_batch_size = 4096;

    /// Lazily create _pending_output_columns if they are empty.
    void _ensure_pending_columns();
    /// Finalize _pending_output_columns into a Block and push to _output_blocks.
    /// Called when pending rows >= batch_size, or with force=true at end of push.
    void _finalize_pending_block(bool force);

    int64_t limit = -1;
    int need_do_sort_limit = -1;
    bool do_sort_limit = false;
    MutableColumns limit_columns;
    int limit_columns_min = -1;
    PaddedPODArray<uint8_t> need_computes;
    std::vector<uint8_t> cmp_res;
    std::vector<int> order_directions;
    std::vector<int> null_directions;

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
                const auto& _limit_column = _limit_columns[i];
                auto res = _limit_column->compare_at(_row_id, rhs._row_id, *_limit_column,
                                                     _null_directions[i]) *
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

    std::priority_queue<HeapLimitCursor> limit_heap;

    MutableColumns _get_keys_hash_table();

    PODArray<AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;

    std::unique_ptr<Block> _child_block = nullptr;
    bool _child_eos = false;
    std::vector<AggregateDataPtr> _values;
    bool _opened = false;

    void _destroy_agg_status(AggregateDataPtr data);

    void _close_with_serialized_key() {
        std::visit(Overload {[&](std::monostate& arg) -> void {
                                 // Do nothing
                             },
                             [&](auto& agg_method) -> void {
                                 auto& data = *agg_method.hash_table;
                                 data.for_each_mapped([&](auto& mapped) {
                                     if (mapped) {
                                         _destroy_agg_status(mapped);
                                         mapped = nullptr;
                                     }
                                 });
                                 if (data.has_null_key_data()) {
                                     _destroy_agg_status(
                                             data.template get_null_key_data<AggregateDataPtr>());
                                 }
                             }},
                   _agg_data->method_variant);
    }
};

class StreamingAggOperatorX MOCK_REMOVE(final) : public StatefulOperatorX<StreamingAggLocalState> {
public:
    StreamingAggOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs);
#ifdef BE_TEST
    StreamingAggOperatorX() : _is_first_phase {false} {}
#endif

    ~StreamingAggOperatorX() override = default;
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override;
    Status prepare(RuntimeState* state) override;
    Status pull(RuntimeState* state, Block* block, bool* eos) const override;
    Status push(RuntimeState* state, Block* input_block, bool eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;
    DataDistribution required_data_distribution(RuntimeState* state) const override {
        if (_child && _child->is_hash_join_probe() &&
            state->enable_streaming_agg_hash_join_force_passthrough()) {
            return DataDistribution(ExchangeType::PASSTHROUGH);
        }
        if (!state->get_query_ctx()->should_be_shuffled_agg(
                    StatefulOperatorX<StreamingAggLocalState>::node_id())) {
            return StatefulOperatorX<StreamingAggLocalState>::required_data_distribution(state);
        }
        if (_partition_exprs.empty()) {
            return _needs_finalize
                           ? DataDistribution(ExchangeType::NOOP)
                           : StatefulOperatorX<StreamingAggLocalState>::required_data_distribution(
                                     state);
        }
        return DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

private:
    friend class StreamingAggLocalState;

    MOCK_FUNCTION Status _init_probe_expr_ctx(RuntimeState* state);

    MOCK_FUNCTION Status _init_aggregate_evaluators(RuntimeState* state);

    MOCK_FUNCTION Status _calc_aggregate_evaluators();

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    bool _needs_finalize;
    const bool _is_first_phase;
    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    // group by k1,k2
    VExprContextSPtrs _probe_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;
    std::vector<size_t> _make_nullable_keys;
    RowDescriptor _agg_fn_output_row_descriptor;

    // For sort limit
    bool _do_sort_limit = false;
    int64_t _sort_limit = -1;
    std::vector<int> _order_directions;
    std::vector<int> _null_directions;

    std::vector<TExpr> _partition_exprs;
};

#include "common/compile_check_end.h"
} // namespace doris
