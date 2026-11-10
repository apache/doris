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
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

// Sorts the key-ordered rows of several iterators into one key-ordered stream, the way a
// MergeDecision orders them. It only sorts: every row of every child comes out once, and what
// the rows of one key turn into (the newest one, a sum, a change) is up to the reader above it,
// see UniqueKeyIterator and MergedRowCursor.
//
// The children come in (rowset version, segment) order, the oldest first: the segments of one
// rowset, or the inputs of a whole tablet read (see BlockReader::_init_rows_iter). Two rows that
// compare equal are ordered by the sequence column when there is one (the larger value first,
// or the smaller one with `small_sequence_first`, which changelog TSO merges use), then by child
// order: a later child holds the newer row and goes first, unless `older_first` asks for the
// older one first. Example with key 1 in child 0 (v = 10) and in child 1 (v = 11): the merge
// hands out v = 11, then v = 10.
//
// The children are read as they are, without init(): a LazyInitSegmentIterator opens its
// segment with the read options of its rowset on its first read.
//
// It merges like StarRocks' HeapMergeIterator. Every child that has rows sits in a heap ordered by
// its current row (std::push_heap/std::pop_heap). Each round takes the child on top and the run
// of its rows that still go before the current row of the next child, one comparison per row,
// appends that run to the output Block and puts the child back. A run also ends at the end of
// the child's Block, which is then refilled, and at the row limit of the output Block.
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
        // The current Block, refilled in place by _fill().
        std::shared_ptr<Block> block;
        // The current row of `block`, its row count and its byte size.
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
    // The heap order of std::push_heap/std::pop_heap, which keep the largest element on top:
    // `a` is "less" than `b` when the current row of `b` goes first.
    auto _heap_order() const {
        return [this](uint32_t a, uint32_t b) { return _less(b, _children[b].pos, a); };
    }

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

    // The children that still have rows, a heap by _heap_order(): front() goes next.
    std::vector<uint32_t> _heap;
    bool _eof = false;
};

RowwiseIteratorUPtr new_heap_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                            ReadSchemaSPtr read_schema,
                                            const MergeDecision& decision);

} // namespace doris
