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
// ChunkAggregator, it takes the rows of the read in key order as input Blocks, the rows of one
// key in the order they were written and never split between two input Blocks (see
// VHeapMergeIterator), and, for each input Block,
//   1. marks the rows whose key equals the key of the row before (build_equal_flags_for_column()
//      for every key column); the first row starts a group;
//   2. turns every key group into one output row:
//      - the key columns and the REPLACE columns are copied from the group's last row, the
//        newest one;
//      - the other value columns are folded by ValueAggregator;
//      - a stretch of one-row groups is copied to the output as is when every folded function
//        returns its single input unchanged.
// Example with the input Block k1 k1 k2 k3 k4 k4 (flags 0 1 0 0 0 1): k1 is folded from rows
// 0-1, k2 and k3 are copied from rows 2-3, and k4 is folded from rows 4-5.
// An output Block only ends in front of a new key, so it never splits a key group.
class AggKeyBlockIterator {
public:
    // Folds the value columns of key groups with the storage aggregate function of each column:
    // the `_reader` variant of the column's aggregation (e.g. sum_reader), or the `_load` one of
    // REPLACE_IF_NOT_NULL, which takes the last non-NULL value as the rows come in the order they
    // were written. A group lies within one input Block, so fold_group() adds its rows with one
    // add_batch_range per column and appends the result row while the Block still holds them.
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

        // The rows of `block` are folded next: note which folded columns of it hold a NULL, so
        // that the nullable functions take their batch path for the columns that hold none.
        void start_block(const Block& block);

        // Fold rows [begin, end) of the Block passed to start_block(), the rows of one key, and
        // append the result row to `out`.
        void fold_group(const Block& block, size_t begin, size_t end, MutableColumns& out);

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
        // The memory of the places. Not `_arena`, which fold_group() clears.
        Arena _state_arena;
        // Allocations of the states of the group being folded, released once it is folded.
        Arena _arena;
    };

    // `input` hands out the rows of the read in key order. `merged` says whether it merges several
    // inputs, so that the rows of one key may follow each other; when it does not (rowsets read
    // one after another), no two of its rows share a key and every row is a group of its own. An
    // output Block holds `max_rows` rows at most and ends once it holds
    // `preferred_block_size_bytes`, unless that is 0. `merged_rows` counts the rows folded into an
    // earlier row of their group.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, bool merged, size_t max_rows,
                        size_t preferred_block_size_bytes, ReadSchemaSPtr read_schema,
                        int be_exec_version, uint64_t* merged_rows);

    Status init();

    // Append the result rows of the next key groups to `block`; `*eof` once there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    // The output byte budget is measured every this many result rows, because measuring the
    // columns costs a walk over them.
    static constexpr size_t BYTE_BUDGET_CHECK_INTERVAL_ROWS = 64;

    // What one next_block() call has appended so far.
    struct OutputProgress {
        // Result rows.
        size_t rows = 0;
        // `rows` when the byte budget was measured last.
        size_t rows_at_last_check = 0;
        // Rows folded into an earlier row of their group.
        uint64_t merged_rows = 0;
    };

    // Copy the collected last rows into `columns`, the output, and read the next input Block.
    Status _advance_input_block(MutableColumns& columns);
    // Fold the rows of `_input_block` from `_pos` on until it is consumed or the output Block is
    // full (`*full`); a full output Block ends in front of the first row of a group.
    Status _fold_input_rows(MutableColumns& columns, OutputProgress* progress, bool* full);
    // Whether the output Block is full: `_max_rows` result rows, or the byte budget measured
    // every BYTE_BUDGET_CHECK_INTERVAL_ROWS rows.
    Status _output_full(MutableColumns& columns, OutputProgress* progress, bool* full);
    // Whether output Blocks end at a byte budget; BlockReader passes 0 when the adaptive batch size
    // is off.
    bool _has_byte_budget() const;
    // The end (exclusive) of the stretch of one-row groups starting at `_pos`, `room` at most.
    size_t _one_row_groups_end(size_t room) const;
    // Read the next input Block into `_input_block` and mark its rows of one key in `_same_key`;
    // `_done` once there is none.
    Status _read_input_block();
    // Set `_same_key` for the rows of `_input_block`, see the class comment.
    void _mark_same_keys();
    // Whether the first row of `_input_block` has the key of the last row of `columns`.
    bool _continues_last_output_key(const MutableColumns& columns) const;
    // The end (exclusive) of the group starting at row `begin` of `_input_block`.
    size_t _group_end(size_t begin) const;
    // Copy the copied columns of the group last rows collected from `_input_block` into `columns`.
    Status _copy_last_rows(MutableColumns& columns);
    // Copy the folded columns of rows [begin, end) of `_input_block`, one-row groups, into `columns`.
    Status _copy_single_rows(MutableColumns& columns, size_t begin, size_t end);

    const RowwiseIteratorUPtr _input;
    const bool _merged_input;
    const size_t _max_rows;
    const size_t _preferred_block_size_bytes;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    uint64_t* _merged_rows;

    // Read-schema ordinals of the key columns, and of the columns copied from a group's last
    // row: the keys and the REPLACE columns.
    std::vector<uint32_t> _key_ordinals;
    std::vector<uint32_t> _copied_ordinals;
    ValueAggregator _aggregator;
    bool _copy_one_row_groups = false;

    // The input rows being folded, their same-key flags and the first row not consumed yet.
    Block _input_block;
    EqualFlags _same_key;
    size_t _pos = 0;
    // Rows of `_input_block` that end a group whose copied columns are not copied yet.
    std::vector<uint32_t> _last_rows;
    // Every row has been read and every group emitted.
    bool _done = false;
};

} // namespace doris
