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
#include "storage/schema.h"

namespace doris {

class Block;
class VCollectIterator;

// Merge-on-read UNIQUE_KEYS tables with a sequence map, i.e. several sequence columns that
// each govern a group of value columns: queries, compactions and the checksum task.
//
// VCollectIterator yields the rows in key order, the newest rowset version of a key first,
// and flags every older version of the same key with is_same (it must not drop them, since
// an older rowset may still hold the winning value of some sequence group). The newest row
// is the output row of its key; for each older row and each sequence group, the group's
// values and sequence replace those of the output row when the older row's sequence is
// larger. Example with groups {s1 -> v1}, {s2 -> v2}:
//   newest row: s1=5 v1=a s2=1 v2=x
//   older row:  s1=3 v1=b s2=7 v2=y   ->  output: s1=5 v1=a s2=7 v2=y
class SequenceMapBlockIterator final : public TabletBlockIterator {
public:
    SequenceMapBlockIterator(VCollectIterator* collect_iter, OutputBlockLimits limits,
                             ReadSchemaSPtr read_schema, uint64_t* merged_rows);

    Status init() override { return _cursor.init(); }

    Status next_block(Block* block, bool* eof) override;

private:
    // Fold the cursor's row, an older version of the key, into the last row of `columns`.
    void _replace_by_sequence(MutableColumns& columns);

    MergedRowCursor _cursor;
    const OutputBlockLimits _limits;
    const ReadSchemaSPtr _read_schema;
    uint64_t* _merged_rows;
};

} // namespace doris
