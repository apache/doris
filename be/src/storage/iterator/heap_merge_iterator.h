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
#include <optional>
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
// The child whose row goes next, the lead, is kept out of the heap, so the heap's top is the
// best row of every other child. After every row the lead's next row is compared with that top
// once: while it still goes first, the heap is not touched, so a rowset whose keys are far from
// the others costs one comparison per row. Otherwise the lead goes back into the heap and its
// top becomes the lead (std::push_heap/std::pop_heap).
//
// The rows are copied column by column: the stretches of consecutive rows taken from one child
// are recorded, then every stretch of the first column is copied, then of the second one, and so
// on. Copying row by row with all its columns walks children x columns memory streams at once
// when keys interleave row by row, which costs up to twice as much on wide rows. The recorded
// stretches point into the children's Blocks, so they are copied before the lead's Block is
// refilled, before the byte budget is measured and at the end of the batch.
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
    // The byte budget of an output Block is measured every this many rows: measuring the
    // columns costs a walk over all of them.
    static constexpr size_t BYTE_BUDGET_CHECK_INTERVAL_ROWS = 64;

    // Rows [pos, pos + rows) of a child's Block, taken by next_batch() but not copied yet.
    struct TakenRows {
        uint32_t child = 0;
        size_t pos = 0;
        size_t rows = 0;
    };

    // One input of the merge and the Block it is positioned in.
    struct Child {
        RowwiseIteratorUPtr iter;
        // The current Block, refilled in place by _fill().
        std::shared_ptr<Block> block;
        // The current row of `block`, and its row count.
        size_t pos = 0;
        size_t rows = 0;
    };

    Status _validate_compare_contract(const Block& block) const;
    // Read the next Block of `child` that has rows; `*exhausted` when it has no more rows.
    Status _fill(Child& child, bool* exhausted);

    // Whether the current row of child `a` goes before the current row of child `b`. Two rows
    // of different children never tie: the child order decides last.
    bool _less(uint32_t a, uint32_t b) const;
    // The heap order of std::push_heap/std::pop_heap, which keep the largest element on top:
    // `a` is "less" than `b` when it goes after `b`.
    auto _heap_order() const {
        return [this](uint32_t a, uint32_t b) { return _less(b, a); };
    }

    // Record the lead's current row as taken, then move the lead past it: refill its Block
    // when it ran out (copying the taken rows first, which may point into it), and hand the
    // lead over to the heap's top when that row goes first.
    Status _take_lead_row(MutableColumns& columns);
    Status _advance_lead(MutableColumns& columns);
    // Copy `_taken` into `columns` column by column and forget it.
    void _copy_taken(MutableColumns& columns);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const MergeDecision _decision;

    // From init():
    size_t _block_row_max = 0;
    size_t _preferred_block_size_bytes = 0;
    RuntimeState* _runtime_state = nullptr;
    int64_t _tablet_id = 0;

    // The child whose current row goes next; none once every row has been taken.
    std::optional<uint32_t> _lead;
    // The other children that still have rows, a heap by _heap_order(): front() goes next
    // after the lead.
    std::vector<uint32_t> _heap;
    // The rows taken since the last copy, in output order.
    std::vector<TakenRows> _taken;
    bool _eof = false;
};

RowwiseIteratorUPtr new_heap_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                            ReadSchemaSPtr read_schema,
                                            const MergeDecision& decision);

} // namespace doris
