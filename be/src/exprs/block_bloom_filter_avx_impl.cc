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

#ifdef __AVX2__

#include <immintrin.h>

#include "exprs/block_bloom_filter.hpp"

namespace doris {
static inline ATTRIBUTE_ALWAYS_INLINE __attribute__((__target__("avx2"))) __m256i make_mark(
        const uint32_t hash) {
    const __m256i ones = _mm256_set1_epi32(1);
    const __m256i rehash = _mm256_setr_epi32(BLOOM_HASH_CONSTANTS);
    // Load hash into a YMM register, repeated eight times
    __m256i hash_data = _mm256_set1_epi32(hash);
    // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
    // odd constants, then keep the 5 most significant bits from each product.
    hash_data = _mm256_mullo_epi32(rehash, hash_data);
    hash_data = _mm256_srli_epi32(hash_data, 27);
    // Use these 5 bits to shift a single bit to a location in each 32-bit lane
    return _mm256_sllv_epi32(ones, hash_data);
}

void BlockBloomFilter::bucket_insert_avx2(const uint32_t bucket_idx, const uint32_t hash) noexcept {
    const __m256i mask = make_mark(hash);
    __m256i* const bucket = &(reinterpret_cast<__m256i*>(_directory)[bucket_idx]);
    _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
    // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
    // dont have to save them off before using XMM registers.
    _mm256_zeroupper();
}

bool BlockBloomFilter::bucket_find_avx2(const uint32_t bucket_idx, const uint32_t hash) const
        noexcept {
    const __m256i mask = make_mark(hash);
    const __m256i bucket = reinterpret_cast<__m256i*>(_directory)[bucket_idx];
    // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
    // takes the negation of its first argument and ands that with its second argument. In
    // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
    // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
    const bool result = _mm256_testc_si256(bucket, mask);
    _mm256_zeroupper();
    return result;
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