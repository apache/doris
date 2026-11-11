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
#include <optional>
#include <queue>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "exec/sort/sort_description.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

// How a VHeapMergeIterator orders the rows of its children and cuts its output Blocks.
struct MergeDecision {
    // The key columns two rows are compared by, in order, each with its direction (1 ascending, -1
    // descending) and where its NULLs go (-1 first), like the SortDescription of a sort and
    // StarRocks' SortDescs: the whole key, or a prefix of it for a read sorted by an ORDER BY
    // prefix (descending for ORDER BY ... DESC).
    SortDescription keys;
    // The column that orders the rows of one key, when there is one: the sequence column or the
    // changelog TSO, ascending, so that the row with the largest value comes last.
    std::optional<SortColumnDescription> sequence;
    // Whether an output Block ends only in front of a new key, so that the rows of one key are
    // never split between two Blocks and the reader above takes every key from one Block. For
    // children that hold every key once (AGG_KEYS and UNIQUE_KEYS segments and rowsets), a Block
    // then goes past the row limit by fewer rows than there are children.
    bool whole_key_groups = false;
};

// Sorts the key-ordered rows of several iterators into one key-ordered stream, the way a
// MergeDecision orders them. It only sorts: every row of every child comes out once, and what
// the rows of one key turn into (a sum, the newest value) is up to the reader above it, see
// AggKeyBlockIterator.
//
// The children come in (rowset version, segment) order, the oldest first: the segments of one
// rowset, or the inputs of a whole tablet read (see BlockReader::_init_agg_key_iter). Like
// StarRocks, the rows of one key come in the order they were written: by the sequence column when
// there is one, then by child order, the older child first. The newest row of a key is its last
// one. Example with key 1 in child 0 (v = 10) and in child 1 (v = 11): the merge hands out
// v = 10, then v = 11.
//
// The children are read as they are, without init(): a LazyInitSegmentIterator opens its
// segment with the read options of its rowset on its first read.
//
// It merges like StarRocks' HeapMergeIterator. Every child that has rows sits in a priority queue
// ordered by its current row. Each round takes the child on top and the run of its rows that
// still go before the current row of the next child, one comparison per row, appends that run to
// the output Block and puts the child back. A run also ends at the end of the child's Block,
// which is then refilled, and at the row limit of the output Block (past it, only the rows of
// the last key go on with `whole_key_groups`).
//
// Like StarRocks, which sizes the chunks of a compaction by its memory limit, the merge counts
// rows only: the row limit of an output Block is block_row_max, or, with a byte budget, the
// budget divided by the average row size of the children's current Blocks.
class VHeapMergeIterator final : public RowwiseIterator {
public:
    VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children, ReadSchemaSPtr read_schema,
                       MergeDecision decision);

    ~VHeapMergeIterator() override = default;

    // Reads the first Block of every child. `opts` supplies the row limit and the byte budget
    // of an output Block, and the runtime state.
    Status init(const StorageReadOptions& opts) override;

    // Return OK with at least one row added to `block`, or EndOfFile without adding one once
    // every row has been read.
    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override;

private:
    // One input of the merge and the Block it is positioned in.
    struct Child {
        RowwiseIteratorUPtr iter;
        // The current Block, refilled in place by _fill(); the rows are copied out of it.
        Block block;
        // The current row of `block`, its row count and its byte size; both 0 once the child is
        // done.
        size_t pos = 0;
        size_t rows = 0;
        size_t bytes = 0;
    };

    Status _validate_compare_contract(const Block& block) const;
    // Read the next Block of `child` that has rows; `*exhausted` when it has no more rows.
    Status _fill(Child& child, bool* exhausted);

    // Whether row `a_pos` of child `a` goes before the current row of child `b`. Two rows of
    // different children never tie: the child order decides last.
    bool _less(uint32_t a, size_t a_pos, uint32_t b) const;
    // Row `a_pos` of child `a` against the current row of child `b` on `column`, in the direction
    // of `column`: negative when the row of `a` goes first.
    int _compare(const SortColumnDescription& column, uint32_t a, size_t a_pos, uint32_t b) const;
    // Whether the current row of child `index` has the key of the last row of `columns`.
    bool _has_last_key(const MutableColumns& columns, uint32_t index) const;

    // The order of `_heap`. std::priority_queue keeps its largest element on top, so `a` is
    // "less" than `b` when the current row of `b` goes first.
    struct HeapOrder {
        const VHeapMergeIterator* merge;
        bool operator()(uint32_t a, uint32_t b) const {
            return merge->_less(b, merge->_children[b].pos, a);
        }
    };

    // The row limit of the next output Block, see the class comment.
    size_t _max_rows() const;
    // Append rows [child.pos, child.pos + rows) of `child` to `columns`.
    static void _append(MutableColumns& columns, const Child& child, size_t rows);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const MergeDecision _decision;

    // From init():
    size_t _block_row_max = 0;
    size_t _preferred_block_size_bytes = 0;
    RuntimeState* _runtime_state = nullptr;
    int64_t _tablet_id = 0;

    // The children that still have rows; the one whose current row goes first is on top.
    std::priority_queue<uint32_t, std::vector<uint32_t>, HeapOrder> _heap {HeapOrder {this}};
    bool _eof = false;
};

RowwiseIteratorUPtr new_heap_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                            ReadSchemaSPtr read_schema,
                                            const MergeDecision& decision);

} // namespace doris
