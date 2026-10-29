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

#include <cstdint>

#include "common/status.h"
#include "core/column/column.h"
#include "storage/iterator/tablet_block_iterator.h"

namespace doris {

class Block;
class RowwiseIterator;
struct OlapReaderStatistics;

// Merge-on-read UNIQUE_KEYS tables without a sequence map: queries, compactions and the
// checksum task.
//
// The rows of the read arrive in key order, the newest version of a key first, and it
// drops the older versions of the same key itself (skip_same). So every row the cursor
// yields is the final row of its key and is copied as is; the cursor only has to stop at
// the Block limits.
//
// Reads that may apply the delete sign (TabletReader::_delete_sign_available: base/full/cold
// compaction and checksum) additionally drop the rows whose __DORIS_DELETE_SIGN__ != 0 from
// every output Block and count them in rows_del_filtered. Queries keep those rows; the FE
// filters them with a predicate.
class UniqueKeyBlockIterator final : public TabletBlockIterator {
public:
    // `delete_sign_ordinal` is the read-schema ordinal of __DORIS_DELETE_SIGN__ when deleted
    // rows must be filtered, -1 to keep them.
    UniqueKeyBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits,
                           int32_t delete_sign_ordinal, OlapReaderStatistics* stats);

    Status init() override { return _cursor.init(); }

    Status next_block(Block* block, bool* eof) override;

private:
    // Remove the rows marked deleted from `block`, which holds `rows` rows.
    Status _filter_deleted_rows(Block* block, size_t rows);

    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
    const int32_t _delete_sign_ordinal;
    OlapReaderStatistics* _stats;
    // Filter column reused across Blocks; only allocated when deleted rows are filtered.
    ColumnPtr _delete_filter_column;
};

} // namespace doris
