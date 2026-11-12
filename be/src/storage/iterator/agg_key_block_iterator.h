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

// The reads of AGG_KEYS tables that fold the rows of a key into one row: queries that did not push
// their aggregation down, compactions and the checksum task, over rowsets that may hold a key more
// than once. Like StarRocks' AggregateIterator and its ChunkAggregator, it reads the rows in key
// order from a VHeapMergeIterator, as input Blocks that never split the rows of one key, the rows
// of a key in the order they were written, and folds every input Block as a whole:
//   1. it marks the rows whose key equals the key of the row before (build_equal_flags_for_column()
//      for every key column); the first row starts a group;
//   2. it turns every key group into one output row:
//      - the key columns and the REPLACE columns are copied from the group's last row, the
//        newest one;
//      - the other value columns are folded by ValueAggregator, but a key that has one row is
//        copied as it is stored, as the reads that need no merge return it.
// Example with the input Block k1 k1 k2 k3 k4 k4 (flags 0 1 0 0 0 1): k1 is folded from rows
// 0-1, k2 and k3 are copied from rows 2-3, and k4 is folded from rows 4-5.
//
// An output Block takes whole input Blocks while the rows they fold into fit into its row limit and
// its byte budget. An input Block holds at most the row limit of keys, so it always fits into an
// empty output Block.
class AggKeyBlockIterator {
public:
    // Folds the value columns of key groups with the storage aggregate function of each column:
    // the `_reader` variant of the column's aggregation (e.g. sum_reader), or the `_load` one of
    // REPLACE_IF_NOT_NULL, which takes the last non-NULL value as the rows come in the order they
    // were written.
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

        // Fold rows [begin, end) of `block`, the rows of one key, and append the result row to
        // `out`.
        void fold_group(const Block& block, size_t begin, size_t end, MutableColumns& out);

    private:
        struct AggColumn {
            uint32_t ordinal = 0;
            AggregateFunctionPtr function;
            // Allocated in _state_arena, created by `function`, destroyed by ~ValueAggregator.
            AggregateDataPtr place = nullptr;
        };

        std::vector<uint32_t> _agg_ordinals;
        std::vector<AggColumn> _columns;
        // The memory of the places. Not `_arena`, which fold_group() clears.
        Arena _state_arena;
        // Allocations of the states of the group being folded, released once it is folded.
        Arena _arena;
    };

    // `input` hands out the rows of the read in key order, in Blocks that never split the rows of
    // one key and hold `max_rows` keys at most, as a VHeapMergeIterator with that row limit does.
    // An output Block holds `max_rows` rows at most, and about `preferred_block_size_bytes` bytes
    // at most unless that is 0. `merged_rows` counts the rows folded into an earlier row of their
    // key.
    AggKeyBlockIterator(RowwiseIteratorUPtr input, size_t max_rows,
                        size_t preferred_block_size_bytes, ReadSchemaSPtr read_schema,
                        int be_exec_version, uint64_t* merged_rows);

    // Set up the folding and read the first input Block.
    Status init();

    // Append the folded rows of the next input Blocks to `block`; `*eof` once there are none.
    Status next_block(Block* block, bool* eof);

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    // Read the next input Block into `_input_block`, mark its rows of one key in `_same_key` and
    // count its groups; `_done` once there is none.
    Status _read_input_block();
    // Whether the groups of `_input_block` fit into the output `columns`, which hold `rows` rows.
    bool _input_fits(const MutableColumns& columns, size_t rows) const;
    // Fold every group of `_input_block` into one row of `columns`.
    Status _fold_input_block(MutableColumns& columns);
    // Whether the first row of `_input_block` has the key of the last row of `columns`.
    bool _continues_last_output_key(const MutableColumns& columns) const;

    const RowwiseIteratorUPtr _input;
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

    // The next input Block to fold, read ahead by init() and after every fold, the same-key flag
    // of each of its rows, and its number of groups, the rows it folds into.
    Block _input_block;
    EqualFlags _same_key;
    size_t _num_groups = 0;
    // The input has returned EndOfFile: there is no input Block left to fold.
    bool _done = false;
};

} // namespace doris
