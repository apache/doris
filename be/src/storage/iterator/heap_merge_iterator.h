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

// How a VHeapMergeIterator orders the rows of its children.
struct MergeDecision {
    // The key columns two rows are compared by, in order, each with its direction (1 ascending, -1
    // descending) and where its NULLs go (-1 first), like the SortDescription of a sort and
    // StarRocks' SortDescs.
    SortDescription keys;
    // The column that orders the rows of one key, when there is one, in its direction: the
    // sequence column or the changelog TSO.
    std::optional<SortColumnDescription> sequence;
};

// Sorts the key-ordered rows of several iterators into one key-ordered stream, the way a
// MergeDecision orders them. It only sorts: every row of every child comes out once, and what
// the rows of one key turn into (a sum, the newest value) is up to the reader above it, see
// AggKeyBlockIterator.
//
// The children come in (rowset version, segment) order, the oldest first: the segments of one
// rowset, or the inputs of a whole tablet read (see BlockReader::_init_agg_key_iter). Like the
// other merges of the storage layer (VCollectIterator, vertical compaction), the rows of one key
// come newest first: by the sequence column when there is one, then by child order, the newer
// child first. The newest row of a key is its first one. Example with key 1 in child 0 (v = 10)
// and in child 1 (v = 11): the merge hands out v = 11, then v = 10.
//
// An output Block ends at its row limit, also inside the rows of one key: the reader above
// carries a key from one Block to the next, like StarRocks' ChunkAggregator.
//
// The children are read as they are, without init(): a LazyInitSegmentIterator opens its
// segment with the read options of its rowset on its first read.
//
// It merges like StarRocks' HeapMergeIterator. Every child that has rows sits in a priority queue
// ordered by its current row. Each round takes the child on top and the run of its rows that
// still go before the current row of the next child, one comparison per row, appends that run to
// the output Block and puts the child back. A run also ends at the end of the child's Block,
// which is then refilled, and at the row limit of the output Block.
//
// Like StarRocks, a child's Block that no row has been taken from and whose rows all go before
// the next child goes into an output Block whole. An empty output Block takes its columns instead
// of copying its rows, and an output Block that it does not fit into ends in front of it, so that
// the next one takes it. Where the children barely overlap, as a large rowset under a few small
// loads, most Blocks thus pass without a copy.
//
// Like StarRocks, which sizes the chunks of a compaction by its memory limit, the merge counts
// rows only: the row limit of an output Block is block_row_max, or, with a byte budget
// (preferred_block_size_bytes, 0 for none), the budget divided by the average row size of the
// children's current Blocks.
class VHeapMergeIterator final : public RowwiseIterator {
public:
    VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children, ReadSchemaSPtr read_schema,
                       MergeDecision decision);

    ~VHeapMergeIterator() override = default;

    // Reads the first Block of every child. `opts` supplies the row limit and the byte budget
    // of an output Block, and the runtime state.
    Status init(const StorageReadOptions& opts) override;

    // Fill `block`, which must be empty, with at least one row and return OK, or return EndOfFile
    // once every row has been read. `block` may get the columns of a child's Block.
    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override;

private:
    // One input of the merge and the Block it is positioned in.
    struct Child {
        RowwiseIteratorUPtr iter;
        // The current Block, refilled in place by _fill_child(); the rows are copied out of it.
        Block block;
        // The current row of `block`.
        size_t pos = 0;
    };

    // Read the next Block of `child` that has rows; `*eof` once the child has no more rows.
    Status _fill_child(Child& child, bool* eof);

    // Put child `child` back into the heap, after refilling its Block when it is used up, or
    // release it once it has no more rows.
    Status _put_back(uint32_t child);

    // Whether row `row` of child `child` goes before the current row of child `other`; both are
    // indexes into `_children`. Rows that tie on the keys and the sequence column go in child
    // order, the newer child (the higher index) first.
    bool _goes_before(uint32_t child, size_t row, uint32_t other) const;
    // The run of child `child`, which is out of the heap: its rows from the current one on that
    // still go before the current row of the next child, `limit` at most.
    size_t _run(uint32_t child, size_t limit) const;

    // The order of `_heap`. std::priority_queue keeps its largest element on top, so `lhs` is
    // "less" than `rhs` when the current row of `rhs` goes first.
    struct HeapOrder {
        const VHeapMergeIterator* merge;
        bool operator()(uint32_t lhs, uint32_t rhs) const {
            return merge->_goes_before(rhs, merge->_children[rhs].pos, lhs);
        }
    };

    // The row limit of the next output Block, see the class comment.
    size_t _max_rows() const;
    // Append rows [child.pos, child.pos + rows) of `child` to `columns`.
    static void _append(MutableColumns& columns, const Child& child, size_t rows);
    // Move the rows of `child`'s Block into the empty `columns` without copying them: the two swap
    // columns, and the child refills the empty ones.
    static void _swap_columns(MutableColumns& columns, Child& child);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const MergeDecision _decision;

    // From init():
    size_t _block_row_max = 0;
    size_t _preferred_block_size_bytes = 0;
    RuntimeState* _runtime_state = nullptr;

    // The children that still have rows; the one whose current row goes first is on top.
    std::priority_queue<uint32_t, std::vector<uint32_t>, HeapOrder> _heap {HeapOrder {this}};
};

} // namespace doris
