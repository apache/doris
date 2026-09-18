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

#include <cstddef>
#include <cstdint>

#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris::format::parquet::native {

// Keep the split-block implementation inside v2; the native scan path must not link through the
// legacy Parquet reader merely to evaluate footer Bloom filters.
class BlockSplitBloomFilter final : public segment_v2::BloomFilter {
public:
    Status init(uint64_t filter_size, segment_v2::HashStrategyPB strategy) override;
    Status init(const char* buf, size_t size, segment_v2::HashStrategyPB strategy) override;
    void add_bytes(const char* buf, size_t size) override;
    bool test_bytes(const char* buf, size_t size) const override;
    void set_has_null(bool has_null) override;
    bool has_null() const override { return false; }
    void add_hash(uint64_t hash) override;
    bool test_hash(uint64_t hash) const override;

private:
    static constexpr int BYTES_PER_BLOCK = 32;
    static constexpr int BITS_SET_PER_BLOCK = 8;
    static constexpr uint32_t SALT[BITS_SET_PER_BLOCK] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                                          0xa2b7289dU, 0x705495c7U, 0x2df1424bU,
                                                          0x9efc4947U, 0x5c6bfb31U};

    struct BlockMask {
        uint32_t item[BITS_SET_PER_BLOCK];
    };

    static void set_masks(uint32_t key, BlockMask* block_mask);
};

} // namespace doris::format::parquet::native
