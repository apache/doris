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

#include <bits/types/time_t.h>
#include <glog/logging.h>

#include <type_traits>

#include "common/compiler_util.h"

#if defined(__SSE2__) || defined(__aarch64__)
#define SIMD_AVAILABLE 1
#endif

#ifdef SIMD_AVAILABLE
namespace doris::simd {

/// define max usable word of simd instruction, and include headers(they have hierarchical relationship)
#if defined(__AVX512F__)
#include <immintrin.h>
using INT_WORD = __m512i;

#elif defined(__AVX2__) || defined(__AVX__)
#include <immintrin.h>
using INT_WORD = __m256i;

#elif defined(__SSE4_2__)
#include <nmmintrin.h>
using INT_WORD = __m256i;

#elif defined(__SSE4_1__)
#include <smmintrin.h>
using INT_WORD = __m256i;

#elif defined(__SSE2__)
#include <emmintrin.h>
using INT_WORD = __m128i;

#elif defined(__aarch64__)
#include <sse2neon.h>
using INT_WORD = __m128i;

#endif

constexpr static auto INT_WORD_SIZE = sizeof(INT_WORD);

inline ALWAYS_INLINE auto load_si(const void* __restrict__ src) {
#if defined(__AVX512F__)
    return _mm512_loadu_si512(src);
#elif defined(__AVX2__)
    return _mm256_loadu_si256(reinterpret_cast<const __m256i_u*>(src));
#elif defined(__SSE2__) || defined(__aarch64__)
    return _mm_loadu_si128(reinterpret_cast<__m128i*>(src));
#endif
}

template <typename T>
inline ALWAYS_INLINE void store_si(void* __restrict__ dst, T src) {
#if defined(__AVX512F__)
    _mm512_storeu_si512(dst, src);
#elif defined(__AVX__)
    _mm256_storeu_si256(reinterpret_cast<__m256i_u*>(dst), src);
#elif defined(__SSE2__) || defined(__aarch64__)
    _mm_storeu_si128(reinterpret_cast<__m128i_u*>(dst), src);
#endif
}

inline ALWAYS_INLINE auto fill_with_pi8(char ch) {
#if defined(__AVX512F__)
    return _mm512_set1_epi8(ch);
#elif defined(__AVX__)
    return _mm256_set1_epi8(ch);
#elif defined(__SSE2__) || defined(__aarch64__)
    return _mm_set1_epi8(ch);
#endif
}

template <typename T>
inline ALWAYS_INLINE auto eq_for_epi8(T lhs, T rhs) {
#if defined(__AVX512BW__)
    return _mm512_cmpeq_epi8_mask(lhs, rhs); // 64-bit for 64 result
#elif defined(__AVX__)
    return _mm256_cmpeq_epi8(lhs, rhs); // 256-bit for 32 result
#elif defined(__SSE2__) || defined(__aarch64__)
    return _mm_cmpeq_epi8(lhs, rhs); // 128-bit for 16 result
#endif
}

template <typename T>
inline ALWAYS_INLINE auto movemask_epi8(T a) {
#if defined(__AVX512F__)
    DCHECK(typeid(a) == typeid(__mmask64) || typeid(a) == typeid(__mmask32) ||
           typeid(a) == typeid(__mmask16));
    return a;
#elif defined(__AVX__)
    return _mm256_movemask_epi8(a);     // extract every 8-bit ---> to 32bit ans
#elif defined(__SSE2__) || defined(__aarch64__)
    return _mm_movemask_epi8(a);     // extract every 8-bit ---> to 16bit ans
#endif
}

/// mask is generally extracted to lowbit of result. remove the leading zeros(if any).
template <typename T>
inline ALWAYS_INLINE auto movemask_epi8_to_left(T a) {
#if defined(__AVX512F__)
    DCHECK(typeid(a) == typeid(__mmask64) || typeid(a) == typeid(__mmask32) ||
           typeid(a) == typeid(__mmask16));
    return a;
#elif defined(__AVX__)
    return _mm256_movemask_epi8(a);     // extract every 8-bit ---> to 32bit ans ---> fill int
#elif defined(__SSE2__) || defined(__aarch64__)
    return _mm_movemask_epi8(a)
           << 16; // extract every 8-bit ---> to 16bit ans ---> low 16bit of int ---> move left
#endif
}

template <typename T>
inline ALWAYS_INLINE auto bitwise_not(T a) {
#ifdef __AVX512BW__
    if constexpr (std::is_same_v<T, __mmask64>) { // ull
        return _knot_mask64(a);
    }
    if constexpr (std::is_same_v<T, __mmask32>) { // ul
        return _knot_mask32(a);
    }
#endif
    return ~a;
}

} // namespace doris::simd
#endif //SIMD_AVAILABLE
