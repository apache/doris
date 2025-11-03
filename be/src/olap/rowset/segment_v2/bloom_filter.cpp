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

#include "olap/rowset/segment_v2/bloom_filter.h"

#include <gen_cpp/segment_v2.pb.h>
#include <math.h>

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "olap/rowset/segment_v2/block_split_bloom_filter.h"
#include "olap/rowset/segment_v2/ngram_bloom_filter.h"
#include "util/frame_of_reference_coding.h"

namespace doris {
namespace segment_v2 {

Status BloomFilter::create(BloomFilterAlgorithmPB algorithm, std::unique_ptr<BloomFilter>* bf,
                           size_t bf_size) {
    if (algorithm == BLOCK_BLOOM_FILTER) {
        bf->reset(new BlockSplitBloomFilter());
    } else if (algorithm == NGRAM_BLOOM_FILTER) {
        bf->reset(new NGramBloomFilter(bf_size));
    } else {
        return Status::InternalError("invalid bloom filter algorithm:{}", algorithm);
    }
    return Status::OK();
}

uint32_t BloomFilter::used_bits(uint64_t value) {
    return 64 - leading_zeroes(value);
}

uint32_t BloomFilter::optimal_bit_num(uint64_t n, double fpp) {
    // ref parquet bloom_filter branch(BlockSplitBloomFilter.java)
    auto num_bits = uint32_t(-8 * (double)n / log(1 - pow(fpp, 1.0 / 8)));
    uint32_t max_bits = MAXIMUM_BYTES << 3;
    if (num_bits > max_bits) {
        num_bits = max_bits;
    }

    // Get closest power of 2 if bits is not power of 2.
    if ((num_bits & (num_bits - 1)) != 0) {
        num_bits = 1 << used_bits(num_bits);
    }
    if (num_bits < MINIMUM_BYTES << 3) {
        num_bits = MINIMUM_BYTES << 3;
    }
    return num_bits;
}

} // namespace segment_v2
} // namespace doris
