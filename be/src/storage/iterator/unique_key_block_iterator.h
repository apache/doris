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

#include "common/status.h"
#include "storage/iterator/tablet_block_iterator.h"

namespace doris {

class Block;
class RowwiseIterator;

// Merge-on-read UNIQUE_KEYS tables without a sequence map: queries, compactions and the
// checksum task.
//
// The rows of the read arrive in key order, the newest version of a key first, and it
// drops the older versions of the same key itself (skip_same). So every row the cursor
// yields is the final row of its key and is copied as is; the cursor only has to stop at
// the Block limits. The reads that may apply the delete sign drop the deleted rows in
// BlockReader::next_block_with_aggregation, after the merge.
class UniqueKeyBlockIterator final : public TabletBlockIterator {
public:
    UniqueKeyBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits);

    Status init() override { return _cursor.init(); }

    Status next_block(Block* block, bool* eof) override;

private:
    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
};

} // namespace doris
