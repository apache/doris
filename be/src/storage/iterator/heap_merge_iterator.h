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
#include "storage/iterator/output_block_limits.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

// Merges the key-ordered rows of several iterators into one key-ordered stream, the way the
// read's MergeDecision says.
//
// The children are the segment iterators of a read in (rowset version, segment) order, the
// oldest first: every segment of a rowset whose segments overlap on keys, and one
// VUnionIterator for a rowset whose segments do not. Two rows that compare equal are ordered
// by the sequence column when there is one (the larger value first, or the smaller one with
// `small_sequence_first`, which changelog TSO merges use), then by child order: a later child
// holds the newer row and goes first, unless `older_first` asks for the older one first.
// Example with key 1 in child 0 (v = 10) and in child 1 (v = 11): the merge hands out v = 11
// and then v = 10 with is_same set, and `drop_older_versions` drops v = 10.
//
// There are four ways to consume the merge; a consumer uses one of them:
//   next_batch(Block*)        copies runs of rows into the caller's Block (DUP_KEYS tables,
//                             schema changes, TopN); with `drop_older_versions` only the
//                             first row of every key is copied.
//   next_batch(Block*, same_key)
//                             the same, plus a same-key flag per copied row (AGG_KEYS folds).
//   next_batch(Block*, runs)  the same as next_batch(Block*), plus the child of every row
//                             taken, a dropped row included (the key group of a vertical
//                             compaction records where its rows came from).
//   next_row(IteratorRowRef*) hands out one row at a time with is_same set when its key
//                             equals the key of the row handed out before it; with
//                             `drop_older_versions` those rows are not handed out at all. The
//                             reference stays valid until the next next_row() call.
//
// A run is a stretch of rows of the top child that all sort before the best row of every
// other child, which is one of the two children of the heap's root. Only the end of a run
// touches the heap, so N children interleaving their keys every row cost log N per row, and
// a rowset whose keys are far from the others costs one comparison per row.
//
// next_batch() only records the runs it takes and copies them column by column: every run of
// the first column, then every run of the second one, and so on, a one-row run with
// insert_from. Copying run by run with all its columns walks children x columns memory
// streams at once when keys interleave row by row, which costs up to twice as much on wide
// rows. The recorded runs point into the children's Blocks, so they are copied before the top
// child's Block is refilled, before the byte budget is measured and at the end of the batch.
class VHeapMergeIterator final : public RowwiseIterator {
public:
    // Rows next_batch(Block*, runs) took from one child in a row, in merge order: `rows` rows
    // of child `child`, the first of them `first_same` when its key equals the key of the row
    // taken before it. With drop_older_versions that first row is dropped: it counts in `rows`
    // but is not copied.
    struct MergedRun {
        uint32_t child = 0;
        uint32_t rows = 0;
        bool first_same = false;
    };

    VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children, ReadSchemaSPtr read_schema,
                       MergeDecision decision);

    ~VHeapMergeIterator() override = default;

    // Reads the first Block of every child. `opts` supplies the output Block limits and the
    // runtime state.
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    // Not with drop_older_versions: the folds that need the flags keep every row.
    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    // `runs` describes the rows taken, also when it returns EndOfFile: the last rows taken may
    // all be dropped.
    Status next_batch(Block* block, std::vector<MergedRun>* runs);

    Status next_row(IteratorRowRef* ref) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override;

    // Rows dropped by drop_older_versions.
    uint64_t merged_rows() const override { return _merged_rows; }

private:
    // Rows [pos, pos + rows) of a child's Block, taken by next_batch() but not copied yet.
    struct TakenRun {
        uint32_t child = 0;
        size_t pos = 0;
        size_t rows = 0;
    };

    // One input of the merge and the Block it is positioned in.
    struct Child {
        RowwiseIteratorUPtr iter;
        // The current Block, refilled in place by _fill(); the rows handed out by next_row()
        // reference it, which is why a reference lives until the child reads on.
        std::shared_ptr<Block> block;
        // The current row of `block`, and its row count.
        size_t pos = 0;
        size_t rows = 0;
    };

    Status _validate_compare_contract(const Block& block) const;
    // Read the next Block of `child`; `*exhausted` when it has no more rows.
    Status _fill(Child& child, bool* exhausted);

    int _compare_keys(const Block& a, size_t a_pos, const Block& b, size_t b_pos) const;
    // Whether row `a_pos` of child `a` goes before row `b_pos` of child `b`.
    bool _less(uint32_t a, size_t a_pos, uint32_t b, size_t b_pos) const;
    bool _less_current(uint32_t a, uint32_t b) const {
        return _less(a, _children[a].pos, b, _children[b].pos);
    }

    // `_heap` holds the indexes of the children that still have rows as a binary min-heap by
    // _less_current(); `_heap[0]` is the top child.
    void _sift_down(size_t index);
    void _pop_top();
    // Recompute `_second`, the child with the best row among all but the top child.
    void _update_second();

    // Keep the key of row `pos` of `block` in `_last_key`, which outlives a refill of `block`.
    void _remember_key(const Block& block, size_t pos);
    bool _key_equals_remembered(const Block& block, size_t pos) const;

    // Move the top child past its current row and restore the heap; `*same` tells whether the
    // new top row has the key of the row left behind. The heap is empty afterwards when the
    // last row has been consumed.
    Status _advance_top(bool* same);
    // The top child has moved past the row it was at: refill its Block when it ran out, drop
    // the child when it has no more rows, and restore the heap for its new row.
    Status _top_moved();

    // The end (exclusive) of the run of the top child: its rows from its current row on that
    // sort before the best row of every other child, `cap` at most.
    size_t _run_end(uint32_t top_index, size_t cap) const;
    // The three next_batch() overloads; `same_key` and `runs` may be null.
    Status _next_batch(Block* block, std::vector<uint8_t>* same_key, std::vector<MergedRun>* runs);
    // Take the run of the top child, at most `max_rows` rows, and move the heap on; `*rows`
    // grows by the rows taken. With drop_older_versions the first row of the run is dropped
    // when it repeats the key taken last; with `same_key` a flag per taken row is appended,
    // with `runs` the run.
    Status _take_run(MutableColumns& columns, size_t max_rows, size_t* rows,
                     std::vector<uint8_t>* same_key, std::vector<MergedRun>* runs);
    // Copy `_taken_runs` into `columns` column by column and forget them.
    void _copy_taken_runs(MutableColumns& columns);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const MergeDecision _decision;

    // From init():
    OutputBlockLimits _limits;
    RuntimeState* _runtime_state = nullptr;
    int64_t _tablet_id = 0;

    std::vector<uint32_t> _heap;
    uint32_t _second = 0;
    // One row: the key columns of the last row handed out or taken. Empty until the first run
    // ends.
    MutableColumns _last_key;
    // next_batch(): the runs taken since the last copy, in output order.
    std::vector<TakenRun> _taken_runs;
    // next_row(): whether a row has been handed out, so the next call advances first.
    bool _row_handed_out = false;
    bool _eof = false;
    uint64_t _merged_rows = 0;
};

RowwiseIteratorUPtr new_heap_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                            ReadSchemaSPtr read_schema,
                                            const MergeDecision& decision);

} // namespace doris
