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

#include "exec/common/groupby_agg_context.h"

namespace doris {

/// InlineCountAggContext: specialized subclass of GroupByAggContext for single count(*)
/// optimization. Instead of allocating full aggregate states, it stores a UInt64 counter
/// directly in the hash table's mapped slot (reinterpret_cast<UInt64&>(mapped)).
///
/// This avoids:
///   - AggregateDataContainer allocation (no separate agg state storage)
///   - AggFnEvaluator dispatch (direct UInt64 increment)
///   - Agg state create/destroy overhead
///
/// Usage condition: exactly one evaluator, count(*), first-phase (not merge), is_simple_count.
class InlineCountAggContext final : public GroupByAggContext {
public:
    using GroupByAggContext::GroupByAggContext;

    // ==================== Hash table write (override) ====================

    /// Emplace keys, increment UInt64 count for each row (new key starts at 1).
    void emplace_into_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                 uint32_t num_rows,
                                 RuntimeProfile::Counter* hash_table_compute_timer,
                                 RuntimeProfile::Counter* hash_table_emplace_timer,
                                 RuntimeProfile::Counter* hash_table_input_counter) override;

    // ==================== Aggregation execution (override) ====================

    /// Execute path: emplace keys (count already incremented), skip evaluator execution.
    Status update(Block* block) override;

    /// Emplace only (count++ done internally), skip execute_batch_add.
    Status emplace_and_forward(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                               uint32_t num_rows, Block* block,
                               bool expand_hash_table) override;

    /// Merge path: read count from ColumnFixedLengthObject (AggregateFunctionCountData)
    /// and add to mapped UInt64.
    Status merge(Block* block) override;

    // ==================== Result output (override) ====================

    /// Serialize output: iterate hash table directly, output ColumnFixedLengthObject with UInt64.
    Status serialize(RuntimeState* state, Block* block, bool* eos) override;

    /// Finalize output: iterate hash table directly, output ColumnInt64.
    Status finalize(RuntimeState* state, Block* block, bool* eos) override;

    // ==================== Agg state management (override) ====================

    /// InlineCount does NOT use aggregate states. Calling these is a logic bug.
    Status create_agg_state(AggregateDataPtr data) override;
    void destroy_agg_state(AggregateDataPtr data) override;

    /// Skip agg state destruction (mapped slots hold UInt64, not agg state pointers).
    void close() override;

    /// Skip agg state destruction, just clear hash table. No AggregateDataContainer needed.
    Status reset_hash_table() override;

    /// No-op: InlineCount does not use AggregateDataContainer.
    void init_agg_data_container() override {}

private:
    /// Merge helper: emplace keys and add count values from a serialized count column.
    void _merge_inline_count(ColumnRawPtrs& key_columns, const IColumn* merge_column,
                             uint32_t num_rows);
};

} // namespace doris
