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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/custom_allocator.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/schema.h"

namespace doris {

// Folds the rows of each key of key-ordered AGG_KEYS Blocks, newest row first, into one row.
// The last key stays open, as the next Block may go on with it; AggKeyBlockIterator decides
// when to take the finished keys.
class AggKeyBlockAggregator {
public:
    // The key columns lead `read_schema`. `merged_rows` counts the rows folded into an earlier
    // row of their key.
    AggKeyBlockAggregator(ReadSchemaSPtr read_schema, uint64_t* merged_rows);
    ~AggKeyBlockAggregator();

    AggKeyBlockAggregator(const AggKeyBlockAggregator&) = delete;
    AggKeyBlockAggregator& operator=(const AggKeyBlockAggregator&) = delete;

    // Resolve the aggregate functions of the value columns.
    Status init(int be_exec_version);

    // Aggregate every row of `source`, which must have rows.
    void aggregate(const Block& source);

    // The rows of the batch, one per key, its open last key included.
    size_t batch_rows() const { return _batch_rows; }
    // Output every key of the batch but the open last one to `block`, which must be empty; the
    // last key starts the next batch.
    void output_finished_keys(Block* block);
    // At the end of the input: output every key of the batch, the last one included, to `block`,
    // which must be empty.
    void output_all_keys(Block* block);

private:
    // The aggregation of a value column: its ordinal, its function and the state the function folds
    // the rows of a key into.
    struct ValueAggregation {
        uint32_t ordinal = 0;
        AggregateFunctionPtr function;
        // Allocated in _state_arena, created by `function`, destroyed by the destructor.
        AggregateDataPtr place = nullptr;
    };

    // Mark the rows of `source` whose key equals the key of the row before, the first row against
    // the batch's last key.
    void _mark_same_keys(const Block& source);
    // Add rows [begin, end) of `source`, rows of one key, to the state of `aggregation`. A nullable
    // function adds them as one batch when `has_null` is false.
    void _aggregate_range(ValueAggregation& aggregation, const IColumn& source, size_t begin,
                          size_t end, bool has_null);
    // Output the value folded in the state of `aggregation` to the batch, and reset the state.
    void _output_value_to_batch(ValueAggregation& aggregation);
    // Hand the batch over to `columns`, the empty columns of the caller's Block, which the next
    // batch fills.
    void _swap_batch(MutableColumns& columns);

    const ReadSchemaSPtr _read_schema;
    const size_t _num_key_columns;
    uint64_t* _merged_rows;

    // One for every column after the keys.
    std::vector<ValueAggregation> _aggregations;
    // The states themselves, for the whole read.
    Arena _state_arena;
    // What the states allocate while folding, released once their keys are appended.
    Arena _arena;

    // The batch: its columns and its rows, one per key.
    MutableColumns _batch_columns;
    size_t _batch_rows = 0;

    // For the source being aggregated. With 2 as the batch's last key and source keys
    // 2 2 3 4 4 4 5:
    //   _same_key        = {1, 1, 0, 0, 1, 1, 0}: whether a row has the key of the row before,
    //                      row 0 that of the batch's last key;
    //   _selective_index = {2, 3, 6}: the first row of each new key, 3, 4 and 5;
    //   _aggregate_loops = {2, 1, 3, 1}: the row count of each key, 2 first, which goes on from
    //                      the batch and has no first row here.
    EqualFlags _same_key;
    DorisVector<uint32_t> _selective_index;
    DorisVector<uint32_t> _aggregate_loops;
};

} // namespace doris
