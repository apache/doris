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
#include "core/column/column.h"

namespace doris {

class VCollectIterator;

// The one read mode of a BlockReader: turns the rows VCollectIterator yields into the Blocks
// handed to the caller of TabletReader::next_block_with_aggregation.
//
// There is exactly one implementation per (table model, read mode) pair, and every
// implementation owns only the state its own read needs:
//   DirectBlockIterator            DUP_KEYS, merge-on-write UNIQUE_KEYS queries, direct mode
//   UniqueKeyBlockIterator         merge-on-read UNIQUE_KEYS without a sequence map
//   SequenceMapBlockIterator       merge-on-read UNIQUE_KEYS with a sequence map
//   AggKeyBlockIterator            AGG_KEYS
//   ChangelogMinDeltaBlockIterator changelog MIN_DELTA scans
//   ChangelogDetailBlockIterator   changelog DETAIL scans
// BlockReader::_init_block_iterator chooses one of them for a read.
class TabletBlockIterator {
public:
    virtual ~TabletBlockIterator() = default;

    // Called once, after VCollectIterator has been built.
    virtual Status init() = 0;

    // Read the next output Block.
    // Return OK and set `*eof` to false when a Block was read.
    // Return OK and set `*eof` to true when no more rows can be read; `block` may still carry
    // the last rows.
    // Return others when an unexpected error happens.
    virtual Status next_block(Block* block, bool* eof) = 0;
};

// Size limits of one output Block, shared by every TabletBlockIterator.
struct OutputBlockLimits {
    // Row ceiling of an output Block.
    size_t max_rows = 0;
    // Byte budget of an output Block. The budget is only consulted every
    // BYTE_BUDGET_CHECK_INTERVAL_ROWS rows because measuring the columns costs a full walk.
    size_t preferred_bytes = 0;

    static constexpr size_t BYTE_BUDGET_CHECK_INTERVAL_ROWS = 64;

    // Whether `columns` holds at least `preferred_bytes`. Always false when adaptive batch
    // sizing is disabled or no budget is configured.
    bool reached_byte_budget(const MutableColumns& columns) const;

    // reached_byte_budget() at the row-count checkpoints only, so the callers all agree on
    // the measuring cadence.
    bool reached_byte_budget_at(size_t rows, const MutableColumns& columns) const {
        return rows % BYTE_BUDGET_CHECK_INTERVAL_ROWS == 0 && reached_byte_budget(columns);
    }
};

// Row-at-a-time view of VCollectIterator for the iterators that merge rows.
//
// It always holds the one merged row that has been fetched but not consumed yet, so an
// iterator can stop in the middle of a key group at an output Block boundary and continue
// with exactly that row on the next call. Example with batch size 2 over the merged rows
// k1, k1, k2, k2: the first next_block() copies k1 and k2 and stops with the cursor on the
// second k2; the second call starts from it.
class MergedRowCursor {
public:
    explicit MergedRowCursor(VCollectIterator* collect_iter) : _collect_iter(collect_iter) {}

    // Position the cursor on the first merged row; eof() when the merge is empty.
    Status init();

    bool eof() const { return _eof; }

    // The fetched, not yet consumed row. Only valid while !eof().
    const IteratorRowRef& row() const {
        DCHECK(!_eof);
        return _row;
    }

    // Move to the next merged row. Running out of rows sets eof() and is not an error.
    Status advance();

private:
    VCollectIterator* _collect_iter;
    IteratorRowRef _row {{}, -1, false};
    bool _eof = false;
};

} // namespace doris
