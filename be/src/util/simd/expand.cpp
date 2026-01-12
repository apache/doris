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

#include "util/simd/expand.h"

// Use GCC function multi-versioning for runtime CPU dispatch
// Reference: https://gcc.gnu.org/wiki/FunctionMultiVersioning

#if defined(__GNUC__) && defined(__x86_64__)
#include <immintrin.h>

namespace doris::simd::Expand {

// Default implementation for expand_load_selection_i32
__attribute__((target("default"))) static void expand_load_selection_i32(int32_t* dst_data,
                                                                         const int32_t* src_data,
                                                                         const uint8_t* nulls,
                                                                         size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

// AVX512 implementation for expand_load_selection_i32
// Requires AVX512F, AVX512VL, and AVX512BW
__attribute__((target("avx512f,avx512vl,avx512bw"))) static void expand_load_selection_i32(
        int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls, size_t count) {
    size_t cnt = 0;
    size_t i = 0;

    // Process 16 elements at a time using AVX-512
    for (; i + 16 <= count; i += 16) {
        // Load 16 bytes of null flags
        __m128i mask = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&nulls[i]));
        // Compare with zero: nulls[i] == 0 means non-null
        __mmask16 null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);

        // Expand load: load non-null values from src_data into positions specified by mask
        __m512i loaded = _mm512_maskz_expandloadu_epi32(null_mask, &src_data[cnt]);
        // Count number of non-null values
        cnt += _mm_popcnt_u32(null_mask);
        // Store result
        _mm512_storeu_si512(&dst_data[i], loaded);
    }

    // Handle remaining elements
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
}

// Default implementation for expand_load_selection_i64
__attribute__((target("default"))) static void expand_load_selection_i64(int64_t* dst_data,
                                                                         const int64_t* src_data,
                                                                         const uint8_t* nulls,
                                                                         size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

// AVX512 implementation for expand_load_selection_i64
__attribute__((target("avx512f,avx512vl,avx512bw"))) static void expand_load_selection_i64(
        int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls, size_t count) {
    size_t cnt = 0;
    size_t i = 0;

    // Process 16 elements at a time (but 64-bit elements need 2 AVX-512 operations per 16)
    for (; i + 16 <= count; i += 16) {
        // Load 16 bytes of null flags
        __m128i mask = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&nulls[i]));
        __mmask16 null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);

        __m512i loaded;

        // Process low 8 elements
        __mmask8 mask_lo = static_cast<__mmask8>(null_mask);
        loaded = _mm512_maskz_expandloadu_epi64(mask_lo, &src_data[cnt]);
        _mm512_storeu_si512(&dst_data[i], loaded);
        cnt += _mm_popcnt_u32(mask_lo);

        // Process high 8 elements
        __mmask8 mask_hi = static_cast<__mmask8>(null_mask >> 8);
        loaded = _mm512_maskz_expandloadu_epi64(mask_hi, &src_data[cnt]);
        _mm512_storeu_si512(&dst_data[i + 8], loaded);
        cnt += _mm_popcnt_u32(mask_hi);
    }

    // Handle remaining elements
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
}

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_selection_i32(dst_data, src_data, nulls, count);
}

void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_selection_i64(dst_data, src_data, nulls, count);
}

} // namespace doris::simd::Expand

#elif defined(__aarch64__)

// ARM NEON implementation (fallback to branchless for now, can be optimized later)
namespace doris::simd::Expand {

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

} // namespace doris::simd::Expand

#else

// Generic fallback for other architectures
namespace doris::simd::Expand {

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                      size_t count) {
    expand_load_branchless(dst_data, src_data, nulls, count);
}

} // namespace doris::simd::Expand

#endif
