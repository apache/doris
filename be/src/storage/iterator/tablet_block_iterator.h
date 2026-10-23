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

#include <glog/logging.h>

#include <cstddef>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/output_block_limits.h"

namespace doris {

class RowwiseIterator;

// The one read mode of a BlockReader: turns the rows of the read, a RowwiseIterator over all
// its rowsets, into the Blocks handed to the caller of TabletReader::next_block_with_aggregation.
//
// There is exactly one implementation per (table model, read mode) pair, and every
// implementation owns only the state its own read needs:
//   DirectBlockIterator            DUP_KEYS, merge-on-write UNIQUE_KEYS queries, direct mode
//   UniqueKeyBlockIterator         merge-on-read UNIQUE_KEYS without a sequence map
//   SequenceMapBlockIterator       merge-on-read UNIQUE_KEYS with a sequence map
//   AggKeyBlockIterator            AGG_KEYS
//   ChangelogMinDeltaBlockIterator changelog MIN_DELTA scans
//   ChangelogDetailBlockIterator   changelog DETAIL scans
//   TopNBlockIterator              ORDER BY key LIMIT n pushed down to the storage layer
// BlockReader::_init_block_iterator chooses one of them for a read.
class TabletBlockIterator {
public:
    virtual ~TabletBlockIterator() = default;

    // Called once, after the rows of the read have been initialized.
    virtual Status init() = 0;

    // Read the next output Block.
    // Return OK and set `*eof` to false when at least one row was appended to `block`.
    // Return OK and set `*eof` to true when no more rows can be read; `block` is then empty,
    // so a caller can stop as soon as `*eof` is set.
    // Return others when an unexpected error happens.
    virtual Status next_block(Block* block, bool* eof) = 0;
};

// Row-at-a-time view of the rows of the read for the iterators that fold rows.
//
// It always holds the one row that has been fetched but not consumed yet, so an iterator can
// stop in the middle of a key group at an output Block boundary and continue with exactly
// that row on the next call. Example with batch size 2 over the rows k1, k1, k2, k2: the
// first next_block() copies k1 and k2 and stops with the cursor on the second k2; the second
// call starts from it. The row is a reference into the iterator's Blocks and stays valid
// until the next advance(), see RowwiseIterator::next_row().
class MergedRowCursor {
public:
    explicit MergedRowCursor(RowwiseIterator* rows) : _rows(rows) {}

    // Position the cursor on the first row; eof() when there is none.
    Status init();

    bool eof() const { return _eof; }

    // The fetched, not yet consumed row. Only valid while !eof().
    const IteratorRowRef& row() const {
        DCHECK(!_eof);
        return _row;
    }

    // Move to the next row. Running out of rows sets eof() and is not an error.
    Status advance();

private:
    RowwiseIterator* _rows;
    IteratorRowRef _row {{}, -1, false};
    bool _eof = false;
};

} // namespace doris
