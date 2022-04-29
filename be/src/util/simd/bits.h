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

#include <cstdint>

#ifdef __AVX2__
#include <immintrin.h>
#elif __SSE2__
#include <emmintrin.h>
#endif

namespace doris {
namespace simd {

/// todo(zeno) Compile add avx512 parameter, modify it to bytes64_mask_to_bits64_mask
/// Transform 32-byte mask to 32-bit mask
inline uint32_t bytes32_mask_to_bits32_mask(const uint8_t* data) {
#ifdef __AVX2__
    auto zero32 = _mm256_setzero_si256();
    uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(
            _mm256_cmpgt_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(data)), zero32)));
#elif __SSE2__
    auto zero16 = _mm_setzero_si128();
    uint32_t mask =
            (static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i*>(data)), zero16)))) |
            ((static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                      _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16)), zero16)))
              << 16) &
             0xffff0000);
#else
    uint32_t mask = 0;
    for (size_t i = 0; i < 32; ++i) {
        mask |= static_cast<uint32_t>(1 == *(data + i)) << i;
    }
#endif
    return mask;
}

inline uint32_t bytes32_mask_to_bits32_mask(const bool* data) {
    return bytes32_mask_to_bits32_mask(reinterpret_cast<const uint8_t*>(data));
}

} // namespace simd
} // namespace doris