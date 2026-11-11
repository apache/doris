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
#include "exprs/aggregate/aggregate_function.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

// The reads of AGG_KEYS tables that fold the rows of a key: queries that did not push their
// aggregation down, compactions and the checksum task. Like StarRocks' AggregateIterator and its
// ChunkAggregator, it takes the rows of the read in key order as merged Blocks and, for each
// merged Block,
//   1. marks the rows whose key equals the key of the row before (build_equal_flags_for_column()
//      for every key column; the first row is compared with the last row of the output, the key
//      of the group still open);
//   2. turns every key group into one output row:
//      - the key columns and the REPLACE columns are copied from the group's first row, the
//        newest one, which the REPLACE reader function keeps (replace_if_not_null skips NULLs and
//        is folded);
//      - the other value columns are folded by ValueAggregator;
//      - a stretch of one-row groups is copied to the output as is when every folded function
//        returns its single input unchanged.
// Example with the merged Block k1 k1 k2 k3 k4 k4 (flags 0 1 0 0 0 1): k1 is folded from rows
// 0-1, k2 and k3 are copied from rows 2-3, and k4 starts a group folded from rows 4-5 that
// stays open into the next merged Block.
// An output Block only ends in front of a new key, so it never splits a key group.
class AggKeyBlockIterator {
public:
    // Folds the value columns of key groups with the storage aggregate function of each column
    // (the `_reader` variant of the column's aggregation, e.g. sum_reader). The rows of a group
    // are row ranges of the merged Blocks, added with one add_batch_range per range and column. A
    // group may span several merged Blocks: its partial state stays in the function's place until
    // close_group() appends the result row.
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

        // Whether the function of every folded column returns the single input of a one-row
        // group unchanged (SUM, MIN, MAX, REPLACE, REPLACE_IF_NOT_NULL, HLL_UNION, BITMAP_UNION),
        // so that a one-row group can be copied to the output instead of folded.
        bool returns_single_row_unchanged() const { return _returns_single_row_unchanged; }

        // The rows of `block` are added next: note which folded columns of it hold a NULL, so
        // that the nullable functions take their batch path for the columns that hold none.
        void start_block(const Block& block);

        // Add rows [begin, end) of the Block passed to start_block() to the open group.
        void add_rows(const Block& block, size_t begin, size_t end);

        // Append the open group's result row to `out` and start an empty group.
        void close_group(MutableColumns& out);

    private:
        struct AggColumn {
            uint32_t ordinal = 0;
            AggregateFunctionPtr function;
            // Allocated in _state_arena, created by `function`, destroyed by ~ValueAggregator.
            AggregateDataPtr place = nullptr;
            bool block_has_null = false;
        };

        std::vector<uint32_t> _agg_ordinals;
        std::vector<AggColumn> _columns;
        bool _returns_single_row_unchanged = true;
        // The memory of the places. Not `_arena`, which close_group() clears.
        Arena _state_arena;
        // Allocations of the open group's states, released when the group closes.
        Arena _arena;
    };

    // `rows` hands out the rows of the read in key order. `merged` says whether it merges several
    // inputs, so that the rows of one key may follow each other; when it does not (rowsets read
    // one after another), no two of its rows share a key and every row is a group of its own. An
    // output Block holds `max_rows` rows at most and ends once it holds
    // `preferred_block_size_bytes` with the adaptive batch size. `merged_rows` counts the rows
    // folded into an earlier row of their group.
    AggKeyBlockIterator(RowwiseIteratorUPtr rows, bool merged, size_t max_rows,
                        size_t preferred_block_size_bytes, ReadSchemaSPtr read_schema,
                        int be_exec_version, uint64_t* merged_rows);

    Status init();

    // Append the result rows of the next key groups to `block`; `*eof` once there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _rows->update_profile(profile); }

private:
    // The output byte budget is measured every this many result rows, because measuring the
    // columns costs a walk over them.
    static constexpr size_t BYTE_BUDGET_CHECK_INTERVAL_ROWS = 64;

    // What one next_block() call has appended so far.
    struct OutputProgress {
        // Result rows; the open group is not one yet.
        size_t rows = 0;
        // `rows` when the byte budget was measured last.
        size_t rows_at_last_check = 0;
        // Rows folded into an earlier row of their group.
        uint64_t merged_rows = 0;
    };

    // Copy the collected heads and read the next merged Block; past the last row, close the
    // open group and finish.
    Status _advance_merged_block(MutableColumns& columns, OutputProgress* progress);
    // Fold the rows of `_merged` from `_pos` on until it is consumed or the output Block is
    // full (`*full`); a full output Block ends in front of the first row of a group.
    Status _fold_merged_rows(MutableColumns& columns, OutputProgress* progress, bool* full);
    // Whether the output Block is full: `_max_rows` result rows, or the byte budget measured
    // every BYTE_BUDGET_CHECK_INTERVAL_ROWS rows.
    Status _output_full(MutableColumns& columns, OutputProgress* progress, bool* full);
    // Whether output Blocks end at a byte budget: the adaptive batch size is on and a budget is
    // set, as in BlockReader::_reached_byte_budget().
    bool _has_byte_budget() const;
    // The end (exclusive) of the stretch of one-row groups starting at `_pos`, `room` at most.
    size_t _one_row_groups_end(size_t room) const;
    // Read the next merged Block into `_merged` and mark its rows of one key in `_same_key`;
    // `_rows_eof` once there is none. `columns` is the output, whose last row holds the key of the
    // open group.
    Status _read_merged_block(const MutableColumns& columns);
    // Set `_same_key` for the rows of `_merged`, see the class comment.
    void _mark_same_keys(const MutableColumns& columns);
    // The end (exclusive) of the group starting at or continued at row `begin` of `_merged`.
    size_t _group_end(size_t begin) const;
    // Copy the head columns of the group heads collected from `_merged` into `columns`.
    Status _copy_heads(MutableColumns& columns);
    // Copy the folded columns of rows [begin, end) of `_merged`, one-row groups, into `columns`.
    Status _copy_single_rows(MutableColumns& columns, size_t begin, size_t end);

    const RowwiseIteratorUPtr _rows;
    const bool _merged_input;
    const size_t _max_rows;
    const size_t _preferred_block_size_bytes;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    uint64_t* _merged_rows;

    // Read-schema ordinals of the key columns, and of the columns copied from a group's first
    // row: the keys and the REPLACE columns.
    std::vector<uint32_t> _key_ordinals;
    std::vector<uint32_t> _head_ordinals;
    ValueAggregator _aggregator;
    bool _copy_one_row_groups = false;

    // The merged rows being folded, their same-key flags and the first row not consumed yet.
    Block _merged;
    EqualFlags _same_key;
    size_t _pos = 0;
    bool _rows_eof = false;
    // The last group started has no result row in the output yet.
    bool _group_open = false;
    // Rows of `_merged` that start a group whose head columns are not copied yet.
    std::vector<uint32_t> _heads;
    // Every row has been read and every group emitted.
    bool _done = false;
};

} // namespace doris
