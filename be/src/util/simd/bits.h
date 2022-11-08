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
#include <cstring>
#include <vector>

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

// TODO: compare with different SIMD implements
template <class T>
inline static size_t find_byte(const std::vector<T>& vec, size_t start, T byte) {
    if (start >= vec.size()) {
        return start;
    }
    const void* p = std::memchr((const void*)(vec.data() + start), byte, vec.size() - start);
    if (p == nullptr) {
        return vec.size();
    }
    return (T*)p - vec.data();
}

template <typename T>
inline bool contain_byte(const T* __restrict data, const size_t length, const signed char byte) {
    return nullptr != std::memchr(reinterpret_cast<const void*>(data), byte, length);
}

inline size_t find_one(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 1);
}

inline size_t find_zero(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 0);
}

} // namespace simd
} // namespace doris
