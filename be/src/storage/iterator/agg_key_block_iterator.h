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

#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/iterator/tablet_block_iterator.h"
#include "storage/schema.h"

namespace doris {

class VCollectIterator;

// Folds the value columns of consecutive same-key rows with the storage aggregate function
// of each column (the `_reader` variant of the column's aggregation, e.g. sum_reader).
//
// The rows arrive one at a time as references into the merge iterator's Blocks
// (IteratorRowRef). They are buffered as references and only copied into a per-column
// buffer right before they would become invalid or the buffer is full (`flush`); the copy
// is one replace_column_data per row for fixed-length columns, so a key group of 1000 rows
// costs one add_batch_range instead of 1000 virtual add() calls.
//
// Groups are delimited by close_group(). A group may span several flushes: the partial
// state stays in the aggregate function's place until the group is closed and flushed, which
// is when its result row is appended to the output columns.
class AggValueAggregator {
public:
    AggValueAggregator() = default;
    ~AggValueAggregator();

    AggValueAggregator(const AggValueAggregator&) = delete;
    AggValueAggregator& operator=(const AggValueAggregator&) = delete;

    // `agg_ordinals` are the read-schema ordinals of the value columns, ascending.
    // `max_buffered_rows` bounds the references kept before a flush (the output row ceiling).
    Status init(const ReadSchema& read_schema, std::vector<uint32_t> agg_ordinals,
                int be_exec_version, size_t max_buffered_rows);

    const std::vector<uint32_t>& agg_ordinals() const { return _agg_ordinals; }

    // Buffer one more row of the open group. Flushes when `row` is the last row of its Block,
    // because the merge iterator recycles that Block on its next advance, or when the buffer
    // is full. `out` receives the result rows of the groups closed before this one.
    void add_row(const IteratorRowRef& row, MutableColumns& out);

    // End the open group; the next add_row() starts a new one.
    void close_group();

    // Aggregate every buffered row; the result row of each closed group is appended to `out`,
    // the open group keeps its partial state.
    void flush(MutableColumns& out);

private:
    struct AggColumn {
        uint32_t ordinal = 0;
        AggregateFunctionPtr function;
        // Partial state of the open group.
        AggregateDataPtr place = nullptr;
        // The buffered rows' values, indexed by buffered row position.
        MutableColumnPtr buffer;
        // Variable-length columns cannot replace_column_data; the buffer is rebuilt in order.
        bool variable_length = false;
        // Whether `buffer` holds a NULL among the rows of the last materialization.
        bool buffer_has_null = false;
    };

    // Copy the buffered references into the column buffers; returns the number of rows.
    size_t _materialize_buffered_rows();
    // Add the `rows` buffered rows starting at `begin` of every column to its place, then
    // append the result row to `out` and reset the place when `close` is set.
    void _aggregate_rows(size_t begin, size_t rows, bool close, MutableColumns& out);

    std::vector<uint32_t> _agg_ordinals;
    std::vector<AggColumn> _columns;
    size_t _max_buffered_rows = 0;

    // Buffered rows of the groups not aggregated yet, in arrival order.
    std::vector<IteratorRowRef> _buffered_rows;
    // Row counts of the closed groups inside `_buffered_rows`, in order, followed by the open
    // group's rows (`_open_group_rows`). Example with buffered rows a a b b b c:
    // closed groups {2, 3}, open group 1.
    std::vector<size_t> _closed_group_rows;
    size_t _open_group_rows = 0;

    // Scratch map from source Block to its (source row, buffered row) pairs.
    phmap::flat_hash_map<const Block*, std::vector<std::pair<int, int>>> _rows_by_block;
    Arena _arena;
};

// AGG_KEYS tables: queries that did not push their aggregation down, compactions and the
// checksum task.
//
// VCollectIterator yields the rows in key order and flags every row that repeats the key of
// the previous one with is_same. Each key group becomes one output row: the key columns are
// copied from the group's first row and the value columns are folded by AggValueAggregator.
// An output Block only ends in front of a new key, so it never splits a key group.
class AggKeyBlockIterator final : public TabletBlockIterator {
public:
    AggKeyBlockIterator(VCollectIterator* collect_iter, OutputBlockLimits limits,
                        ReadSchemaSPtr read_schema, int be_exec_version, uint64_t* merged_rows);

    Status init() override;

    Status next_block(Block* block, bool* eof) override;

private:
    // Copy the key columns of the cursor's row, the first row of a new key group.
    Status _append_key_columns(MutableColumns& columns);

    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    uint64_t* _merged_rows;

    // Read-schema ordinals of the key columns, i.e. everything the aggregator does not fold.
    std::vector<uint32_t> _key_ordinals;
    AggValueAggregator _aggregator;
};

} // namespace doris
