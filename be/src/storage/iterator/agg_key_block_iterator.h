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
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

// The reads of AGG_KEYS tables that fold the rows of a key into one row: queries that did not push
// their aggregation down, compactions and the checksum task, over rowsets that may hold a key more
// than once. Like StarRocks' AggregateIterator and its ChunkAggregator, it reads the rows in key
// order from a VHeapMergeIterator, the rows of a key newest first, and folds every input Block in
// three steps:
//   1. it finds the groups of rows that share a key: build_equal_flags_for_column() marks the rows
//      whose key equals the key of the row before;
//   2. it copies the key columns and the REPLACE columns of every group from its first row, the
//      newest one;
//   3. it folds the other value columns with ValueAggregator, column by column. A key that has one
//      row is copied as it is stored, as the reads that need no merge return it.
// Example with the input Block k1 k1 k2 k3 k4 k4 (flags 0 1 0 0 0 1): k1 is folded from rows
// 0-1, k2 and k3 are copied from rows 2-3, and k4 is folded from rows 4-5.
//
// The merge may split the rows of a key between its Blocks, so the last group of an input Block
// stays open: its key and REPLACE values are in the output already, its other values in the
// aggregate states, until the next input Block shows whether it goes on with that key. An output
// Block ends only in front of a new key or at the end of the input, so every key comes out once.
// It holds `max_rows` keys, or with a byte budget, the budget divided by the average row size of
// the input Block, as many as the merge puts into one of its Blocks.
class AggKeyBlockIterator {
public:
    // Folds the value columns of key groups with the storage aggregate function of each column,
    // the `_reader` variant of its aggregation (e.g. sum_reader). The rows of a key come newest
    // first, so REPLACE_IF_NOT_NULL takes the first value that is not NULL; its function keeps a
    // copy of the value, so a group can go on in the next input Block.
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

    // `input` hands out the rows of the read in key order, the rows of a key newest first, as a
    // VHeapMergeIterator does. `key_ordinals` are the read-schema ordinals of the key columns,
    // the ones the merge orders by. An output Block holds `max_rows` rows at most, and about
    // `preferred_block_size_bytes` bytes unless that is 0. `merged_rows` counts the rows folded
    // into an earlier row of their key.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, std::vector<uint32_t> key_ordinals,
                        size_t max_rows, size_t preferred_block_size_bytes,
                        ReadSchemaSPtr read_schema, int be_exec_version, uint64_t* merged_rows);

    // Set up the folding and read the first input Block.
    Status init();

    // Append the folded rows of the next keys to `block`; `*eof` once there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    // Read the next input Block into `_input_block` and mark its rows that have the key of the row
    // before in `_same_key`; `_done` once there is none.
    Status _read_input_block();
    // The row limit of the next output Block, see the class comment.
    size_t _output_max_rows() const;
    // Fold the rows of `_input_block` from `_input_row` on into `columns`, which hold `*rows` keys:
    // the rows that go on with the open group, then new groups while there is room. Return true
    // when it stopped in front of a new key because `columns` hold `max_rows` keys.
    bool _fold_input_block(MutableColumns& columns, size_t max_rows, size_t* rows);
    // Add rows [begin, end) of `_input_block`, which have the key of the open group, to it.
    void _add_to_open_group(size_t begin, size_t end);
    // Append the values of the open group, which is complete, to `columns`.
    void _close_open_group(MutableColumns& columns);
    // Whether the first row of `_input_block` has the key of the last row of `columns`.
    bool _continues_last_output_key(const MutableColumns& columns) const;

    const RowwiseIteratorUPtr _input;
    const std::vector<uint32_t> _key_ordinals;
    const size_t _max_rows;
    const size_t _preferred_block_size_bytes;
    const ReadSchemaSPtr _read_schema;
    const int _be_exec_version;
    uint64_t* _merged_rows;

    // Read-schema ordinals of the columns copied from a group's first row: the keys and the
    // REPLACE columns.
    std::vector<uint32_t> _copied_ordinals;
    ValueAggregator _aggregator;

    // The input Block being folded, read ahead by init(), the same-key flag of each of its rows,
    // and the row to go on from.
    Block _input_block;
    EqualFlags _same_key;
    size_t _input_row = 0;
    // The input has returned EndOfFile.
    bool _done = false;

    // The rows of the open group so far, 0 when there is none. While it has one row, that row is
    // kept as it is stored in `_open_row`, since a key of one row is copied as it is.
    size_t _open_rows = 0;
    Block _open_row;

    // The first row and the row count of every new group of the input Block being folded.
    std::vector<uint32_t> _first_rows;
    std::vector<uint32_t> _group_rows;
};

} // namespace doris
