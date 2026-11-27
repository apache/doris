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
#include "storage/iterator/block_batch_limit.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterator/changelog_iterator_utils.h"
#include "storage/schema.h"

namespace doris {

// Folds consecutive (key, TSO)-ordered changelog rows of each primary key into its minimum net
// change (SKIP, INSERT, DELETE, or UPDATE_BEFORE + UPDATE_AFTER) for MIN_DELTA scans. Only the
// first and last row of each key group are copied into `_group_columns`; intermediate rows are
// skipped via block-level `update_equal_flags`.
class ChangelogMinDeltaBlockIterator final : public BlockIterator {
public:
    ChangelogMinDeltaBlockIterator(std::unique_ptr<BlockIterator> input,
                                   BlockBatchLimit batch_limit, ReadSchemaSPtr read_schema);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override { _cursor.update_profile(profile); }

private:
    Status _fill_block(MutableColumns* target_columns, size_t* output_rows);
    Status _collect_group();
    void _mark_block_same_keys();
    void _set_group_row(size_t row_pos, bool replace_last);
    bool _group_values_unchanged();
    Status _emit_group(MutableColumns* target_columns, size_t row_limit, size_t* output_rows);
    Status _emit_update_pair(MutableColumns* target_columns, size_t row_limit, size_t* output_rows);
    Status _append_insert_row(MutableColumns* columns) const;
    Status _append_delete_row(MutableColumns* columns) const;
    Status _append_update_before_row(MutableColumns* columns) const;
    Status _append_update_after_row(MutableColumns* columns) const;
    size_t _last_group_row() const { return _group_columns[0]->size() - 1; }

    changelog::ChangelogInputCursor _cursor;
    const BlockBatchLimit _batch_limit;
    const ReadSchemaSPtr _read_schema;
    const size_t _num_key_columns;
    const int32_t _op_col_idx;
    const int32_t _tso_col_idx;
    const int32_t _lsn_col_idx;

    MutableColumns _group_columns;
    EqualFlags _same_key;
    uint64_t _same_key_block_id = 0;
    changelog::ChangelogPendingRow _pending_row;
    bool _value_compare_unsupported = false;
};

} // namespace doris
