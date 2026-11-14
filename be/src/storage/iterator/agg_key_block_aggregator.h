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
#include <span>
#include <vector>

#include "common/status.h"
#include "core/arena.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/schema.h"

namespace doris {

// Folds the key-ordered rows of AGG_KEYS input Blocks, the rows of a key newest first, into one
// row per key, like StarRocks' ChunkAggregator. The caller hands it the input Blocks one by one
// (update_source), lets it aggregate (aggregate), and takes a batch of aggregated rows once it is
// full (is_finish) or once the input ends (has_aggregate_data), see AggKeyBlockIterator.
//
// aggregate() works on the rows of the source from where it stopped, in three steps:
//   1. the groups of rows that share a key: build_equal_flags_for_column() marked the rows whose
//      key equals the key of the row before;
//   2. the key columns and the REPLACE columns of every group, copied from its first row, the
//      newest one;
//   3. the other value columns, folded by ValueAggregator column by column. A key that has one
//      row is copied as it is stored, as the reads that need no merge return it.
// Example with the source k1 k1 k2 k3 k4 k4 (flags 0 1 0 0 0 1): k1 is folded from rows 0-1, k2
// and k3 are copied from rows 2-3, and k4 is folded from rows 4-5.
//
// The rows of a key may go on in the next source, so the last key of a source stays open: its key
// and REPLACE values are in the batch already, its other values in the aggregate states. A batch
// is full once it holds its row limit of keys and the next row has a new key, so every key comes
// out once. The row limit is `max_rows`, or with a byte budget, the budget divided by the average
// row size of the source a batch starts in, like the row limit of the merge.
class AggKeyBlockAggregator {
public:
    // Folds the value columns of key groups with the storage aggregate function of each column,
    // the `_reader` variant of its aggregation (e.g. sum_reader). The rows of a key come newest
    // first, so REPLACE_IF_NOT_NULL takes the first value that is not NULL; its function keeps a
    // copy of the value, so a key can go on in the next source.
    class ValueAggregator {
    public:
        ValueAggregator() = default;
        ~ValueAggregator();

        ValueAggregator(const ValueAggregator&) = delete;
        ValueAggregator& operator=(const ValueAggregator&) = delete;

        // `agg_ordinals` are the read-schema ordinals of the folded columns, ascending.
        Status init(const ReadSchema& read_schema, std::vector<uint32_t> agg_ordinals,
                    int be_exec_version);

        const std::vector<uint32_t>& agg_ordinals() const { return _agg_ordinals; }

        // Fold the groups that follow each other in `block` from row `begin` on, `group_rows` rows
        // each, into one row of `out` each. A group of one row is copied as it is stored.
        void fold_groups(const Block& block, size_t begin, std::span<const uint32_t> group_rows,
                         MutableColumns& out);
        // Add rows [begin, end) of `block` to the open group.
        void add_to_open_group(const Block& block, size_t begin, size_t end);
        // Append the folded values of the open group to `out`, and start the next group.
        void close_open_group(MutableColumns& out);

    private:
        struct AggColumn {
            uint32_t ordinal = 0;
            AggregateFunctionPtr function;
            // Allocated in _state_arena, created by `function`, destroyed by ~ValueAggregator.
            AggregateDataPtr place = nullptr;
        };

        // Fold rows [begin, end) of `source`, the rows of one key, into one row of `result`.
        void _fold(AggColumn& column, const IColumn& source, size_t begin, size_t end,
                   IColumn& result);

        std::vector<uint32_t> _agg_ordinals;
        std::vector<AggColumn> _columns;
        // The memory of the places. Not `_arena`, which is cleared after every group.
        Arena _state_arena;
        // Allocations of the states of the group being folded, released once its row is appended.
        Arena _arena;
    };

    // `key_ordinals` are the read-schema ordinals of the key columns. A batch holds `max_rows`
    // rows at most, and about `preferred_block_size_bytes` bytes unless that is 0.
    // `merged_rows` counts the rows folded into an earlier row of their key.
    AggKeyBlockAggregator(ReadSchemaSPtr read_schema, std::vector<uint32_t> key_ordinals,
                          size_t max_rows, size_t preferred_block_size_bytes,
                          uint64_t* merged_rows);

    // Resolve the aggregate functions of the value columns.
    Status init(int be_exec_version);

    // Aggregate `source` next, from its first row. It stays the caller's Block, which must not
    // change before the source is exhausted.
    void update_source(const Block& source);
    // Whether every row of the source is aggregated.
    bool source_exhausted() const { return _source_row == _source_rows; }

    // Aggregate the rows of the source from where it stopped, until the batch is full in front of
    // a new key or the source ends.
    void aggregate();
    // Whether the batch holds its row limit of keys.
    bool is_finish() const { return _aggregate_rows >= _max_aggregate_rows; }
    // Whether the batch holds a key.
    bool has_aggregate_data() const { return _aggregate_rows > 0; }
    // Close the open key and hand the batch over to `block`, which must be empty; start the next
    // batch.
    void aggregate_result(Block* block);

private:
    // The row limit of a batch that starts in the source, see the class comment.
    size_t _row_limit() const;
    // Add rows [begin, end) of the source, which have the key of the open group, to it.
    void _add_to_open_group(size_t begin, size_t end);
    // Append the values of the open group, which is complete, to the batch.
    void _close_open_group();

    const ReadSchemaSPtr _read_schema;
    const std::vector<uint32_t> _key_ordinals;
    const size_t _max_rows;
    const size_t _preferred_block_size_bytes;
    uint64_t* _merged_rows;

    // Read-schema ordinals of the columns copied from a group's first row: the keys and the
    // REPLACE columns.
    std::vector<uint32_t> _copied_ordinals;
    ValueAggregator _values;

    // The source, the same-key flag of each of its rows (the first row against the last key of the
    // batch), its row count and the row to go on from.
    const Block* _source = nullptr;
    EqualFlags _same_key;
    size_t _source_rows = 0;
    size_t _source_row = 0;

    // The batch: its columns, its keys and its row limit.
    MutableColumns _aggregate_columns;
    size_t _aggregate_rows = 0;
    size_t _max_aggregate_rows = 0;

    // The rows of the open group so far, 0 when there is none. While it has one row, that row is
    // kept as it is stored in `_open_group_single_row`, since a key of one row is copied as it is.
    size_t _open_group_row_count = 0;
    Block _open_group_single_row;

    // The first row and the row count of every new group of the source.
    std::vector<uint32_t> _first_rows;
    std::vector<uint32_t> _group_rows;
};

} // namespace doris
