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

#include <vector>

#include "olap/memory/buffer.h"
#include "olap/memory/common.h"

namespace doris {
namespace memory {

// DeltaIndex store all the updated rows' id(rowids) for a ColumnDelta.
// Rowids are sorted and divided into blocks, each 64K rowid space is a
// block. Since each block only have 64K id space, it can be store as uint16_t
// rather than uint32_t to save memory.
class DeltaIndex : public RefCountedThreadSafe<DeltaIndex> {
public:
    static const uint32_t npos = 0xffffffffu;

    DeltaIndex() = default;

    // get memory consumption
    size_t memory() const;

    // find rowid(rid) in the index,
    // return index position if found, else return npos
    uint32_t find_idx(uint32_t rid);

    // get a block's index position range as [start, end)
    void block_range(uint32_t bid, uint32_t* start, uint32_t* end) const {
        if (bid < _block_ends.size()) {
            *start = bid > 0 ? _block_ends[bid - 1] : 0;
            *end = _block_ends[bid];
        } else {
            *start = 0;
            *end = 0;
        }
    }

    // Return true if this index has any rowid belonging to this block
    bool contains_block(uint32_t bid) const {
        if (bid < _block_ends.size()) {
            return (bid > 0 ? _block_ends[bid - 1] : 0) < _block_ends[bid];
        }
        return false;
    }

    Buffer& data() { return _data; }
    const Buffer& data() const { return _data; }
    vector<uint32_t>& block_ends() { return _block_ends; }
    const vector<uint32_t>& block_ends() const { return _block_ends; }

private:
    vector<uint32_t> _block_ends;
    Buffer _data;
    DISALLOW_COPY_AND_ASSIGN(DeltaIndex);
};

} // namespace memory
} // namespace doris
