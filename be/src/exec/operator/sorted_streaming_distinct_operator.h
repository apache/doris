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

#include <cstdint>
#include <memory>
#include <vector>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/operator.h"
#include "runtime/runtime_profile.h"

namespace doris {
class ExecNode;
class RuntimeState;

#include "common/compile_check_begin.h"

class SortedStreamingDistinctOperatorX;

/**
 * Local state for SortedStreamingDistinctOperatorX.
 *
 * This operator performs DISTINCT by comparing adjacent rows, leveraging the fact
 * that input data is already sorted by all DISTINCT columns (full key coverage).
 * No hash table is needed — memory usage is O(1) per distinct column.
 */
class SortedStreamingDistinctLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = SortedStreamingDistinctOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    ENABLE_FACTORY_CREATOR(SortedStreamingDistinctLocalState);
    SortedStreamingDistinctLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

private:
    friend class SortedStreamingDistinctOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    void _make_nullable_output_key(Block* block);

    /// Evaluate group-by expressions on the input block, returning key column pointers.
    Status _evaluate_key_columns(const Block* block, std::vector<ColumnPtr>& key_columns);

    /// Build a filter by comparing adjacent rows on key columns.
    /// Uses linear probe + binary search to skip over ranges of duplicate keys.
    /// Returns the number of output (distinct) rows.
    size_t _build_distinct_filter(const std::vector<ColumnPtr>& key_columns, size_t num_rows,
                                  IColumn::Filter& filter);

    /// Apply the filter to key columns and produce the output block.
    void _build_output_block(const std::vector<ColumnPtr>& key_columns,
                             const IColumn::Filter& filter, size_t output_rows);

    /// Check if two rows within the current chunk have the same key.
    bool _is_key(const std::vector<ColumnPtr>& key_columns, size_t key_pos, size_t row_pos) const;

    /// Check if a row matches the saved key from the previous chunk.
    bool _is_prev_chunk_key(const std::vector<ColumnPtr>& key_columns, size_t row_pos) const;

    /// Handle continuation from the previous chunk: skip rows matching the prev chunk's last key.
    /// Returns the range_end for the consumed prefix (rows [0, range_end) are skipped duplicates).
    size_t _continue_with_prev_range(const std::vector<ColumnPtr>& key_columns, size_t num_rows,
                                     IColumn::Filter& filter);

    // Previous chunk's last key values for cross-chunk deduplication
    Columns _prev_key_columns;
    bool _has_prev_key = false;

    VExprContextSPtrs _group_by_expr_ctxs;

    std::unique_ptr<Block> _child_block = nullptr;
    bool _child_eos = false;
    bool _reach_limit = false;
    std::unique_ptr<Block> _output_block = nullptr;

    RuntimeProfile::Counter* _comparison_timer = nullptr;
    RuntimeProfile::Counter* _filter_timer = nullptr;
    RuntimeProfile::Counter* _input_rows_counter = nullptr;
};

/**
 * Sorted streaming distinct operator.
 *
 * Leverages sorted input to perform DISTINCT without a hash table.
 * For each input block, builds a filter by comparing adjacent rows on all key columns.
 * A row passes the filter only if it differs from the previous row on at least one key column.
 *
 * Cross-chunk continuity: saves the last row's key from each chunk to compare with
 * the first row of the next chunk.
 */
class SortedStreamingDistinctOperatorX MOCK_REMOVE(final)
        : public StatefulOperatorX<SortedStreamingDistinctLocalState> {
public:
    SortedStreamingDistinctOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs);
#ifdef BE_TEST
    SortedStreamingDistinctOperatorX() : _needs_finalize(true), _is_colocate(false) {}
#endif

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status pull(RuntimeState* state, Block* block, bool* eos) const override;
    Status push(RuntimeState* state, Block* input_block, bool eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;

    DataDistribution required_data_distribution(RuntimeState* state) const override {
        if (_needs_finalize && _group_by_expr_ctxs.empty()) {
            return {ExchangeType::NOOP};
        }
        if (_needs_finalize || (!_group_by_expr_ctxs.empty() && !_is_streaming_preagg)) {
            return _is_colocate && _require_bucket_distribution
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        }
        return StatefulOperatorX<SortedStreamingDistinctLocalState>::required_data_distribution(
                state);
    }

    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override {
        _followed_by_shuffled_operator = followed_by_shuffled_operator;
        _require_bucket_distribution = require_bucket_distribution;
        _partition_exprs = tnode.__isset.distribute_expr_lists && _followed_by_shuffled_operator
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.agg_node.grouping_exprs;
    }

    bool is_colocated_operator() const override { return _is_colocate; }
    bool is_shuffled_operator() const override {
        return !_partition_exprs.empty() && _needs_finalize;
    }

private:
    friend class SortedStreamingDistinctLocalState;

    void init_make_nullable(RuntimeState* state);

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    const bool _needs_finalize;
    std::vector<TExpr> _partition_exprs;
    const bool _is_colocate;

    VExprContextSPtrs _group_by_expr_ctxs;
    std::vector<size_t> _make_nullable_keys;

    bool _is_streaming_preagg = false;
};

} // namespace doris

#include "common/compile_check_end.h"
