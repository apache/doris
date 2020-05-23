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

#include "olap/memory/delta_index.h"

namespace doris {
namespace memory {

size_t DeltaIndex::memory() const {
    return _data.bsize() + _block_ends.size() * sizeof(uint32_t);
}

uint32_t DeltaIndex::find_idx(uint32_t rid) {
    if (!_data) {
        return npos;
    }
    uint32_t bid = rid >> 16;
    if (bid >= _block_ends.size()) {
        return npos;
    }
    // TODO: use SIMD
    uint32_t start = bid > 0 ? _block_ends[bid - 1] : 0;
    uint32_t end = _block_ends[bid];
    if (start == end) {
        return npos;
    }
    uint16_t* astart = _data.as<uint16_t>() + start;
    uint16_t* aend = _data.as<uint16_t>() + end;
    uint32_t bidx = rid & 0xffff;
    uint16_t* pos = std::lower_bound(astart, aend, bidx);
    if ((pos != aend) && (*pos == bidx)) {
        return pos - _data.as<uint16_t>();
    } else {
        return npos;
    }
}

} // namespace memory
} // namespace doris
