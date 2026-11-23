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

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/changelog_types.h"
#include "storage/iterator/block_iterator.h"
#include "storage/schema.h"

namespace doris {
class RuntimeProfile;

// Parks the UPDATE_AFTER half of an UPDATE pair when UPDATE_BEFORE fills an output Block.
class ChangelogPendingRow {
public:
    bool has_row() const { return _has_row; }

    MutableColumns& park(const MutableColumns& target_columns);
    bool emit(MutableColumns& target_columns);

private:
    MutableColumns _columns;
    bool _has_row = false;
};

// Steps row by row over a BlockIterator, reading its first Block lazily on first use.
class ChangelogInputCursor {
public:
    ChangelogInputCursor(std::unique_ptr<BlockIterator> input, ReadSchemaSPtr read_schema);

    Status ensure_started();
    Status advance();

    bool eof() const { return _eof; }
    const Block& block() const { return _block; }
    size_t row_pos() const { return _pos; }

    void update_profile(RuntimeProfile* profile) { _input->update_profile(profile); }

private:
    Status _refill();

    const std::unique_ptr<BlockIterator> _input;
    const ReadSchemaSPtr _read_schema;
    Block _block;
    size_t _pos = 0;
    bool _started = false;
    bool _eof = false;
};

// Folds consecutive (key, TSO)-ordered changelog rows of each primary key into its minimum net
// change (SKIP, INSERT, DELETE, or UPDATE_BEFORE + UPDATE_AFTER) for MIN_DELTA scans.
class ChangelogMinDeltaBlockIterator final : public BlockIterator {
public:
    ChangelogMinDeltaBlockIterator(std::unique_ptr<BlockIterator> input, size_t block_row_max,
                                   ReadSchemaSPtr read_schema);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _cursor.update_profile(profile); }

private:
    Status _fill_block(MutableColumns& target_columns, size_t* output_rows);
    void _append_row_to_group();
    bool _row_belongs_to_group() const;
    bool _group_values_unchanged();
    Status _emit_group(MutableColumns& target_columns, size_t* output_rows);
    Status _emit_update_pair(MutableColumns& target_columns, size_t* output_rows);
    Status _append_insert_row(MutableColumns& columns) const;
    Status _append_delete_row(MutableColumns& columns) const;
    Status _append_update_before_row(MutableColumns& columns) const;
    Status _append_update_after_row(MutableColumns& columns) const;
    size_t _last_group_row() const { return _group_columns[0]->size() - 1; }

    ChangelogInputCursor _cursor;
    const size_t _block_row_max;
    const ReadSchemaSPtr _read_schema;
    const size_t _num_key_columns;
    const int32_t _op_ordinal;
    const int32_t _tso_ordinal;
    const int32_t _lsn_ordinal;

    MutableColumns _group_columns;
    ChangelogPendingRow _pending_row;
    bool _value_compare_unsupported = false;
};

// Emits every recorded changelog event in (key, TSO) order for DETAIL scans: APPEND -> INSERT,
// DELETE -> DELETE (with before image), UPDATE -> UPDATE_BEFORE + UPDATE_AFTER.
class ChangelogDetailBlockIterator final : public BlockIterator {
public:
    ChangelogDetailBlockIterator(std::unique_ptr<BlockIterator> input, size_t block_row_max,
                                 ReadSchemaSPtr read_schema);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _cursor.update_profile(profile); }

private:
    Status _emit_change(ChangelogOp op, MutableColumns& target_columns, size_t* output_rows);
    Status _append_change_row(MutableColumns& target_columns, changelog::StreamChangeOp output_op,
                              bool use_before) const;

    ChangelogInputCursor _cursor;
    const size_t _block_row_max;
    const ReadSchemaSPtr _read_schema;
    const int32_t _op_ordinal;
    ChangelogPendingRow _pending_row;
};

} // namespace doris
