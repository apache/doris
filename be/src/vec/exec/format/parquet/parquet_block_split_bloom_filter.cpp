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

#include <glog/logging.h>

#include <cstring>

#include "vec/exec/format/parquet/vparquet_column_reader.h"

namespace doris {
namespace vectorized {

// for write
Status ParquetBlockSplitBloomFilter::init(uint64_t filter_size,
                                          segment_v2::HashStrategyPB strategy) {
    if (strategy == XX_HASH_64) {
        _hash_func = [](const void* buf, const int64_t len, const uint64_t seed, void* out) {
            auto h =
                    HashUtil::xxhash64_compat_with_seed(reinterpret_cast<const char*>(buf), len, 0);
            *reinterpret_cast<uint64_t*>(out) = h;
        };
    } else {
        return Status::InvalidArgument("invalid strategy:{}", strategy);
    }
    _num_bytes = filter_size;
    _size = _num_bytes;
    _data = new char[_size];
    memset(_data, 0, _size);
    _has_null = nullptr;
    _is_write = true;
    g_write_bloom_filter_num << 1;
    g_write_bloom_filter_total_bytes << _size;
    g_total_bloom_filter_total_bytes << _size;
    return Status::OK();
}

// for read
// use deep copy to acquire the data
Status ParquetBlockSplitBloomFilter::init(const char* buf, size_t size,
                                          segment_v2::HashStrategyPB strategy) {
    if (size <= 1) {
        return Status::InvalidArgument("invalid size:{}", size);
    }
    DCHECK(size > 1);
    if (strategy == XX_HASH_64) {
        _hash_func = [](const void* buf, const int64_t len, const uint64_t seed, void* out) {
            auto h =
                    HashUtil::xxhash64_compat_with_seed(reinterpret_cast<const char*>(buf), len, 0);
            *reinterpret_cast<uint64_t*>(out) = h;
        };
    } else {
        return Status::InvalidArgument("invalid strategy:{}", strategy);
    }
    if (buf == nullptr) {
        return Status::InvalidArgument("buf is nullptr");
    }

    _data = new char[size];
    memcpy(_data, buf, size);
    _size = size;
    _num_bytes = _size;
    _has_null = nullptr;
    g_read_bloom_filter_num << 1;
    g_read_bloom_filter_total_bytes << _size;
    g_total_bloom_filter_total_bytes << _size;
    return Status::OK();
}

void ParquetBlockSplitBloomFilter::add_bytes(const char* buf, size_t size) {
    DCHECK(buf != nullptr) << "Parquet bloom filter does not track nulls";
    uint64_t code = hash(buf, size);
    add_hash(code);
}

bool ParquetBlockSplitBloomFilter::test_bytes(const char* buf, size_t size) const {
    uint64_t code = hash(buf, size);
    return test_hash(code);
}

void ParquetBlockSplitBloomFilter::set_has_null(bool has_null) {
    DCHECK(!has_null) << "Parquet bloom filter does not track nulls";
}

void ParquetBlockSplitBloomFilter::add_hash(uint64_t hash) {
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) * (_num_bytes / BYTES_PER_BLOCK) >> 32);
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate mask for bucket.
    BlockMask block_mask;
    _set_masks(key, block_mask);

    for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
        bitset32[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
    }
}

bool ParquetBlockSplitBloomFilter::test_hash(uint64_t hash) const {
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) * (_num_bytes / BYTES_PER_BLOCK) >> 32);
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(_data);

    // Calculate masks for bucket.
    BlockMask block_mask;
    _set_masks(key, block_mask);

    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        uint32_t bit_val = bitset32[BITS_SET_PER_BLOCK * bucket_index + i];
        if (0 == (bit_val & block_mask.item[i])) {
            return false;
        }
    }
    return true;
}

} // namespace vectorized
} // namespace doris
