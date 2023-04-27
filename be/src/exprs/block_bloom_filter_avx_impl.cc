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
// This file is copied from
// https://github.com/apache/kudu/blob/master/src/kudu/util/block_bloom_filter_avx2.cc
// and modified by Doris

#ifdef __AVX2__

#include <glog/logging.h>
#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>

#include "exprs/block_bloom_filter.hpp"

namespace doris {

void BlockBloomFilter::bucket_insert_avx2(const uint32_t bucket_idx, const uint32_t hash) noexcept {
    const __m256i mask = make_mark(hash);
    __m256i* const bucket = &(reinterpret_cast<__m256i*>(_directory)[bucket_idx]);
    _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
    // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
    // dont have to save them off before using XMM registers.
    _mm256_zeroupper();
}

void BlockBloomFilter::insert_avx2(const uint32_t hash) noexcept {
    _always_false = false;
    const uint32_t bucket_idx = rehash32to32(hash) & _directory_mask;
    bucket_insert_avx2(bucket_idx, hash);
}

void BlockBloomFilter::or_equal_array_avx2(size_t n, const uint8_t* __restrict__ in,
                                           uint8_t* __restrict__ out) {
    static constexpr size_t kAVXRegisterBytes = sizeof(__m256d);
    static_assert(kAVXRegisterBytes == kBucketByteSize, "Unexpected AVX register bytes");
    DCHECK_EQ(n % kAVXRegisterBytes, 0) << "Invalid Bloom filter directory size";

    const uint8_t* const in_end = in + n;
    for (; in != in_end; (in += kAVXRegisterBytes), (out += kAVXRegisterBytes)) {
        const double* double_in = reinterpret_cast<const double*>(in);
        double* double_out = reinterpret_cast<double*>(out);
        _mm256_storeu_pd(double_out,
                         _mm256_or_pd(_mm256_loadu_pd(double_out), _mm256_loadu_pd(double_in)));
    }
}
} // namespace doris
#endif
