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

const uint32_t BlockSplitBloomFilter::SALT[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
                                                 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

void BlockSplitBloomFilter::add_hash(uint64_t hash) {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    uint32_t block_size = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(hash >> 32) & (block_size - 1);
    uint32_t key = (uint32_t)hash;

    // Calculate masks for bucket.
    uint32_t masks[8];
    _set_masks(key, masks);
    uint32_t* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        *(block_offset + i) |= masks[i];
    }
}

bool BlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    uint32_t block_size = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(hash >> 32) & (block_size - 1);
    uint32_t key = (uint32_t)hash;
    // Calculate masks for bucket.
    uint32_t masks[BITS_SET_PER_BLOCK];
    _set_masks(key, masks);
    uint32_t* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if ((*(block_offset + i) & masks[i]) == 0) {
            return false;
        }
    }
    return true;
}

} // namespace segment_v2
} // namespace doris
