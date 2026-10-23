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

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/output_block_limits.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace segment_v2 {
class Segment;
class ColumnIterator;
} // namespace segment_v2

class VStatisticsIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VStatisticsIterator(std::shared_ptr<Segment> segment, const ReadSchema& schema)
            : _segment(std::move(segment)), _schema(schema) {}

    ~VStatisticsIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return _schema; }

private:
    std::shared_ptr<Segment> _segment;
    const ReadSchema& _schema;
    size_t _target_rows = 0;
    size_t _output_rows = 0;
    bool _init = false;
    TPushAggOp::type _push_down_agg_type_opt;
    std::vector<std::unique_ptr<ColumnIterator>> _column_iterators;

    static constexpr size_t MAX_ROW_SIZE_IN_COUNT = 65535;
};

// Merges the key-ordered rows of several iterators into one key-ordered stream.
//
// The children are the segment iterators of a read in (rowset version, segment) order, the
// oldest first: every segment of a rowset whose segments overlap on keys, and one
// VUnionIterator for a rowset whose segments do not. Two rows of the same key are ordered by
// the sequence column when there is one (the larger value first, or the smaller one with
// `small_sequence_first`, which changelog TSO merges use), then by child order: a later child
// holds the newer row and goes first, unless `use_insert_order_when_same` asks for the older
// one first. Example with key 1 in child 0 (v = 10) and in child 1 (v = 11): the merge hands
// out v = 11 and then v = 10 with is_same set, and skip_same drops v = 10.
//
// There are two ways to consume the merge; a consumer uses one of them:
//   next_batch(Block*)        copies runs of rows into the caller's Block (DUP_KEYS tables,
//                             schema changes, TopN); with `skip_same` only the first row of
//                             every key is copied.
//   next_row(IteratorRowRef*) hands out one row at a time with is_same set when its key
//                             equals the key of the row handed out before it; with
//                             `skip_same` those rows are not handed out at all. The reference
//                             stays valid until the next next_row() call.
//
// A run is a stretch of rows of the top child that all sort before the best row of every
// other child, which is one of the two children of the heap's root. Only the end of a run
// touches the heap, so N children interleaving their keys every row cost log N per row, and
// a rowset whose keys are far from the others costs one comparison per row.
class VMergeIterator final : public RowwiseIterator {
public:
    // `sequence_ordinal` is the read-schema ordinal of the sequence column, -1 without one.
    // `is_reverse` orders the keys descending. `skip_same` drops every row whose key equals
    // the key of the row before it, i.e. every version of a key but the newest.
    VMergeIterator(std::vector<RowwiseIteratorUPtr> children, ReadSchemaSPtr read_schema,
                   int sequence_ordinal, bool small_sequence_first, bool is_reverse,
                   bool skip_same);

    ~VMergeIterator() override = default;

    // Reads the first Block of every child. `opts` supplies the output Block limits, the
    // tie-break direction (use_insert_order_when_same), the columns to compare by
    // (read_orderby_key_columns, the key columns otherwise) and the runtime state.
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    Status next_row(IteratorRowRef* ref) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override;

    // Rows dropped by skip_same.
    uint64_t merged_rows() const override { return _merged_rows; }

private:
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
    // Copy the run of the top child into `columns`, at most `max_rows` rows, and move the
    // heap on; `*rows` grows by the rows copied. With skip_same the first row of the run is
    // dropped when it repeats the key copied out last.
    Status _copy_run(MutableColumns& columns, size_t max_rows, size_t* rows);

    std::vector<Child> _children;
    const ReadSchemaSPtr _read_schema;
    const int _sequence_ordinal;
    const bool _small_sequence_first;
    const bool _is_reverse;
    const bool _skip_same;

    // From init():
    // Read-schema ordinals two rows are compared by.
    std::vector<uint32_t> _key_ordinals;
    // Whether `_key_ordinals` is the key prefix of the read schema (validated to be keys)
    // rather than caller-given compare columns.
    bool _compare_by_key_prefix = true;
    bool _use_insert_order_when_same = false;
    OutputBlockLimits _limits;
    RuntimeState* _runtime_state = nullptr;
    int64_t _tablet_id = 0;

    std::vector<uint32_t> _heap;
    uint32_t _second = 0;
    // One row: the key columns of the last row handed out or copied out. Empty until the
    // first run ends.
    MutableColumns _last_key;
    // next_row(): whether a row has been handed out, so the next call advances first.
    bool _row_handed_out = false;
    bool _eof = false;
    uint64_t _merged_rows = 0;
};

RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                       ReadSchemaSPtr read_schema, int sequence_ordinal,
                                       bool small_sequence_first, bool is_reverse, bool skip_same);

// The rows of `inputs` one input after another. Every input is initialized when it is reached.
RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       ReadSchemaSPtr read_schema);

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment,
                                          const ReadSchema& schema);

} // namespace doris
