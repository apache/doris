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

#include "format_v2/parquet/reader/native/block_split_bloom_filter.h"

#include <cstring>

#include "util/hash_util.hpp"

namespace doris::format::parquet::native {

namespace {
Status set_hash_strategy(segment_v2::HashStrategyPB strategy,
                         std::function<void(const void*, int64_t, uint64_t, void*)>* hash_func) {
    if (strategy != segment_v2::HashStrategyPB::XX_HASH_64) {
        return Status::InvalidArgument("Invalid Parquet Bloom filter hash strategy {}", strategy);
    }
    *hash_func = [](const void* buf, int64_t len, uint64_t, void* out) {
        *reinterpret_cast<uint64_t*>(out) =
                HashUtil::xxhash64_compat_with_seed(reinterpret_cast<const char*>(buf), len, 0);
    };
    return Status::OK();
}
} // namespace

Status BlockSplitBloomFilter::init(uint64_t filter_size, segment_v2::HashStrategyPB strategy) {
    RETURN_IF_ERROR(set_hash_strategy(strategy, &_hash_func));
    _num_bytes = filter_size;
    _size = _num_bytes;
    _data = new char[_size];
    memset(_data, 0, _size);
    _has_null = nullptr;
    _is_write = true;
    segment_v2::g_write_bloom_filter_num << 1;
    segment_v2::g_write_bloom_filter_total_bytes << _size;
    segment_v2::g_total_bloom_filter_total_bytes << _size;
    return Status::OK();
}

Status BlockSplitBloomFilter::init(const char* buf, size_t size,
                                   segment_v2::HashStrategyPB strategy) {
    if (buf == nullptr || size <= 1) {
        return Status::InvalidArgument("Invalid Parquet Bloom filter buffer of size {}", size);
    }
    RETURN_IF_ERROR(set_hash_strategy(strategy, &_hash_func));
    _data = new char[size];
    memcpy(_data, buf, size);
    _size = size;
    _num_bytes = size;
    _has_null = nullptr;
    segment_v2::g_read_bloom_filter_num << 1;
    segment_v2::g_read_bloom_filter_total_bytes << _size;
    segment_v2::g_total_bloom_filter_total_bytes << _size;
    return Status::OK();
}

void BlockSplitBloomFilter::add_bytes(const char* buf, size_t size) {
    DCHECK(buf != nullptr);
    add_hash(hash(buf, size));
}

bool BlockSplitBloomFilter::test_bytes(const char* buf, size_t size) const {
    return test_hash(hash(buf, size));
}

void BlockSplitBloomFilter::set_has_null(bool has_null) {
    DCHECK(!has_null) << "Parquet Bloom filters do not track nulls";
}

void BlockSplitBloomFilter::set_masks(uint32_t key, BlockMask* block_mask) {
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        block_mask->item[i] = uint32_t {1} << ((key * SALT[i]) >> 27);
    }
}

void BlockSplitBloomFilter::add_hash(uint64_t hash) {
    DCHECK_GE(_num_bytes, BYTES_PER_BLOCK);
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) * (_num_bytes / BYTES_PER_BLOCK) >> 32);
    auto* bitset = reinterpret_cast<uint32_t*>(_data);
    BlockMask block_mask;
    set_masks(static_cast<uint32_t>(hash), &block_mask);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        bitset[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
    }
}

bool BlockSplitBloomFilter::test_hash(uint64_t hash) const {
    const uint32_t bucket_index =
            static_cast<uint32_t>((hash >> 32) * (_num_bytes / BYTES_PER_BLOCK) >> 32);
    const auto* bitset = reinterpret_cast<const uint32_t*>(_data);
    BlockMask block_mask;
    set_masks(static_cast<uint32_t>(hash), &block_mask);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if ((bitset[bucket_index * BITS_SET_PER_BLOCK + i] & block_mask.item[i]) == 0) {
            return false;
        }
    }
    return true;
}

} // namespace doris::format::parquet::native
