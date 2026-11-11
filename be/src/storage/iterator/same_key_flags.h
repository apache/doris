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
#include <vector>

#include "core/column/column.h"

namespace doris {
class Block;

// The same-key flags of a key-ordered stream of Blocks, the output of a VHeapMergeIterator,
// worked out from the rows themselves: a row's flag is 1 when its key columns equal those of the
// row before it, the last row of the previous Block for the first row of a Block. Example:
// Blocks k1 k1 k2 | k2 k3 give the flags 0 1 0 | 1 0.
//
// Two values are equal when IColumn::compare_at() finds them equal, the comparison the merge
// ordered the rows by. Like StarRocks' build_tie_for_column(), a key column of a type a key can
// have (integer, decimal, date and time, IP, string; nullable or not) is compared by one loop over
// the Block for its type; any other column is compared by compare_at() row by row.
class SameKeyFlags {
public:
    // `key_ordinals` are the read-schema ordinals the rows are merged by.
    explicit SameKeyFlags(std::vector<uint32_t> key_ordinals)
            : _key_ordinals(std::move(key_ordinals)) {}

    // Set `flags` to one flag per row of `block`, which has rows, and keep the key of its last
    // row for the Block that comes next.
    void compute(const Block& block, std::vector<uint8_t>* flags);

private:
    const std::vector<uint32_t> _key_ordinals;
    // One row: the key columns of the last row seen. Empty before the first Block.
    MutableColumns _last_key;
};

} // namespace doris
