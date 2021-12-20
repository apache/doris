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

#include "olap/rowset/segment_v2/block_split_bloom_filter.h"

#include "util/debug_util.h"

namespace doris {
namespace segment_v2 {

void BlockSplitBloomFilter::add_hash(uint64_t hash) {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    const uint32_t bucket_index =
            static_cast<uint32_t>(hash >> 32) & (_num_bytes / BYTES_PER_BLOCK - 1);
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate mask for bucket.
    BlockMask block_mask;
    _set_masks(key, block_mask);

    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
        bitset32[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
    }
}

bool BlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) & (_num_bytes / BYTES_PER_BLOCK - 1));
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate masks for bucket.
    BlockMask block_mask;
    _set_masks(key, block_mask);

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if (0 == (bitset32[BITS_SET_PER_BLOCK * bucket_index + i] & block_mask.item[i])) {
            return false;
        }
    }
    return true;
}

} // namespace segment_v2
} // namespace doris
