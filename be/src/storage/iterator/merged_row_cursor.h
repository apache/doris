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
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/iterator/same_key_flags.h"

namespace doris {

class Block;
class RowwiseIterator;
struct IteratorRowRef;

// Row-at-a-time view of the rows of a read, for the BlockReader read modes that fold the rows
// of a key (AGG_KEYS, a sequence map, changelog scans). It reads the rows a Block at a time into
// a Block of its own and hands them out as IteratorRowRefs into that Block. `is_same` of a row
// is 1 when its key equals the key of the row before it, across Blocks too: the merged rows
// k1 k1 k2 | k2 k3 come out with is_same 0 1 0 | 1 0. A row stays valid until next() moves past
// the last row of its Block, which then is refilled in place.
class MergedRowCursor {
public:
    // Over rows that never repeat a key, the rowsets read one after another: no row is same.
    explicit MergedRowCursor(RowwiseIterator* rows) : _rows(rows) {}
    // Over the rows of a VHeapMergeIterator ordered by `key_ordinals`.
    MergedRowCursor(RowwiseIterator* rows, std::vector<uint32_t> key_ordinals)
            : _rows(rows), _flags(std::in_place, std::move(key_ordinals)) {}

    // Set `*row` to the first row. Return EndOfFile when there is none.
    Status init(IteratorRowRef* row);

    // Set `*row` to the next row. Return EndOfFile when the rows ran out.
    Status next(IteratorRowRef* row);

private:
    // Read the next Block and set `*row` to its first row.
    Status _read_block(IteratorRowRef* row);

    RowwiseIterator* const _rows;
    std::optional<SameKeyFlags> _flags;
    std::shared_ptr<Block> _block;
    // The is_same of every row of `_block`.
    std::vector<uint8_t> _same_key;
    int _row_pos = -1;
};

} // namespace doris
