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

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/changelog_types.h"
#include "storage/iterator/tablet_block_iterator.h"
#include "storage/schema.h"

namespace doris {

class RowwiseIterator;

// Parks one output row across an output Block boundary.
//
// Both changelog iterators turn one UPDATE into a BEFORE row followed by an AFTER row. When
// the BEFORE row is the last row that fits into the Block, the AFTER row is parked here and
// is the first row the next next_block() call emits, so the pair always stays adjacent.
class ChangelogPendingRow {
public:
    bool has_row() const { return _has_row; }

    // Start parking a row: returns the empty columns, shaped like `target_columns`, for the
    // caller to append exactly one row to.
    MutableColumns& park(const MutableColumns& target_columns);

    // Append the parked row to `target_columns`; false when nothing is parked.
    bool emit(MutableColumns& target_columns);

private:
    MutableColumns _columns;
    bool _has_row = false;
};

// Changelog MIN_DELTA scans: the net change of every primary key within the scanned window.
//
// The scan is forced to be globally key-ordered (ReaderParams::force_key_ordered_read), with
// same-key rows ordered by TSO, so all changes of one key are consecutive. The iterator keeps
// the first and the most recent row of the current key group and, once the key changes, folds
// the group with the first/last op transition matrix of changelog_block_reader_utils.h into
// nothing (SKIP), one INSERT, one DELETE, or an UPDATE_BEFORE + UPDATE_AFTER pair:
//   APPEND(v=1) UPDATE(v=2)  -> INSERT v=2
//   APPEND(v=1) DELETE       -> nothing
//   UPDATE(1->2) UPDATE(2->1) -> nothing: the before image equals the final image
//   UPDATE(1->2) DELETE      -> DELETE with the before image v=1
class ChangelogMinDeltaBlockIterator final : public TabletBlockIterator {
public:
    ChangelogMinDeltaBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits,
                                   ReadSchemaSPtr read_schema);

    Status init() override { return _cursor.init(); }

    Status next_block(Block* block, bool* eof) override;

private:
    // Append the cursor's row to the group: the first row stays at index 0, the most recent
    // row replaces index 1.
    void _append_row_to_group();
    // Whether the cursor's row has the user key of the current group.
    bool _row_belongs_to_group() const;
    // Whether the group's before image equals its final image, i.e. the UPDATE is a no-op.
    bool _group_values_unchanged();

    // Fold the group into its net change and append it to `target_columns`; parks the AFTER
    // row of an UPDATE pair in `_pending_row` when `*output_rows` reached the row ceiling.
    Status _emit_group(MutableColumns& target_columns, size_t* output_rows);
    Status _emit_update_pair(MutableColumns& target_columns, size_t* output_rows);

    // The output rows built from the group, one per net change:
    //   INSERT        the most recent value of every column;
    //   DELETE        the before image of the first op, with the TSO/LSN of the last op;
    //   UPDATE_BEFORE the before image of the first op;
    //   UPDATE_AFTER  the most recent value of every column.
    Status _append_insert_row(MutableColumns& columns) const;
    Status _append_delete_row(MutableColumns& columns) const;
    Status _append_update_before_row(MutableColumns& columns) const;
    Status _append_update_after_row(MutableColumns& columns) const;

    size_t _last_group_row() const { return _group_columns[0]->size() - 1; }

    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
    const ReadSchemaSPtr _read_schema;
    // Read-schema ordinal of the changelog op column; every changelog read schema has one.
    const size_t _op_ordinal;

    // Row 0: the first row of the current key group; row 1, when present: its most recent row.
    // Shaped like the merged Blocks on first use.
    MutableColumns _group_columns;
    ChangelogPendingRow _pending_row;
    // An unsupported compare_at means equality cannot be proven, so no-op detection stays
    // off for the rest of the read. Column types are stable within a read; the capability
    // result is cached so BITMAP-like columns pay the exception cost at most once.
    bool _value_compare_unsupported = false;
};

// Changelog DETAIL scans: every recorded change verbatim, in key then TSO order.
// APPEND becomes one INSERT row, DELETE one DELETE row carrying the before image, and UPDATE
// an UPDATE_BEFORE + UPDATE_AFTER pair.
class ChangelogDetailBlockIterator final : public TabletBlockIterator {
public:
    ChangelogDetailBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits,
                                 ReadSchemaSPtr read_schema);

    Status init() override { return _cursor.init(); }

    Status next_block(Block* block, bool* eof) override;

private:
    // Append the output rows of the cursor's row, a change with op `op`, to `target_columns`;
    // parks the AFTER row of an UPDATE in `_pending_row` when `*output_rows` reached the row
    // ceiling.
    Status _emit_change(ChangelogOp op, MutableColumns& target_columns, size_t* output_rows);
    // Append the cursor's row to `target_columns` with `output_op` in the op column, reading
    // every value column from its before image when `use_before` is set.
    Status _append_change_row(MutableColumns& target_columns, changelog::StreamChangeOp output_op,
                              bool use_before) const;

    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
    const ReadSchemaSPtr _read_schema;
    // Read-schema ordinal of the changelog op column; every changelog read schema has one.
    const size_t _op_ordinal;
    ChangelogPendingRow _pending_row;
};

} // namespace doris
