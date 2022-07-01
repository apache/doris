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

#include <cstdint>

#ifdef __AVX2__
#include <immintrin.h>
#elif __SSE2__
#include <emmintrin.h>
#elif __aarch64__
#include <sse2neon.h>
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
#elif defined(__SSE2__) || defined(__aarch64__)
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
    for (std::size_t i = 0; i < 32; ++i) {
        mask |= static_cast<uint32_t>(1 == *(data + i)) << i;
    }
#endif
    return mask;
}

inline uint32_t bytes32_mask_to_bits32_mask(const bool* data) {
    return bytes32_mask_to_bits32_mask(reinterpret_cast<const uint8_t*>(data));
}

inline size_t count_zero_num(const int8_t* __restrict data, size_t size) {
    size_t num = 0;
    const int8_t* end = data + size;
#if defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    const int8_t* end64 = data + (size / 64 * 64);

    for (; data < end64; data += 64) {
        num += __builtin_popcountll(
                static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(data)), zero16))) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16)), zero16)))
                 << 16u) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 32)), zero16)))
                 << 32u) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 48)), zero16)))
                 << 48u));
    }
#endif
    for (; data < end; ++data) {
        num += (*data == 0);
    }
    return num;
}

inline size_t count_zero_num(const int8_t* __restrict data, const uint8_t* __restrict null_map,
                             size_t size) {
    size_t num = 0;
    const int8_t* end = data + size;
    for (; data < end; ++data, ++null_map) {
        num += ((*data == 0) | *null_map);
    }
    return num;
}

// collect the true flag idx
// example flags:[0,1,0,1]=>idx:[1, 3], return 2
inline uint16_t flags_to_idx(const uint8_t* flags, uint16_t flag_size, uint16_t* idx) {
    uint16_t idx_size = 0;
    uint32_t pos = 0;
    const uint32_t end = pos + flag_size;

#if defined(__SSE2__)
    static constexpr uint16_t SIMD_BYTES = 32;
    const uint32_t end_simd = pos + flag_size / SIMD_BYTES * SIMD_BYTES;

    while (pos < end_simd) {
        auto mask = simd::bytes32_mask_to_bits32_mask(flags + pos);
        if (0 == mask) {
            //pass
        } else if (0xffffffff == mask) {
            for (uint32_t i = 0; i < SIMD_BYTES; i++) {
                idx[idx_size++] = pos + i;
            }
        } else {
            while (mask) {
                const size_t bit_pos = __builtin_ctzll(mask);
                idx[idx_size++] = pos + bit_pos;
                mask = mask & (mask - 1);
            }
        }
        pos += SIMD_BYTES;
    }
#endif
    for (; pos < end; pos++) {
        idx[idx_size] = pos;
        idx_size += flags[pos];
    }
    return idx_size;
}
} // namespace simd
} // namespace doris
