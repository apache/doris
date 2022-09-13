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

#include "vec/columns/column.h"
#include "vec/common/pod_array.h"

namespace doris::simd {

static constexpr size_t UINT64_MAX_MASK = 0xFFFFFFFFFFFFFFFF;

inline uint64_t bytes64_mask_to_bits64_not_mask(const int8_t* bytes64) {
#ifdef __AVX2__
    const auto zero32 = _mm256_setzero_si256();
    uint64_t res =
            static_cast<uint64_t>(
                    _mm256_movemask_epi8(_mm256_cmpeq_epi8(
                            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes64)),
                            zero32)) &
                    0xffffffff) |
            (static_cast<uint64_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(
                     _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes64 + 32)), zero32)))
             << 32);
#elif __SSE2__
    static const __m128i zero16 = _mm_setzero_si128();
    uint64_t res =
            static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i*>(bytes64)), zero16))) |
            (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(bytes64 + 16)), zero16)))
             << 16) |
            (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(bytes64 + 32)), zero16)))
             << 32) |
            (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(bytes64 + 48)), zero16)))
             << 48);
#else
    uint64_t res = 0;
    for (std::size_t i = 0; i < 64; ++i) {
        res |= static_cast<uint64_t>(0 == *(bytes64 + i)) << i;
    }
#endif
    return res;
}

inline uint64_t bytes64_mask_to_bits64_mask(const uint8_t* data) {
    return ~bytes64_mask_to_bits64_not_mask(reinterpret_cast<const int8_t*>(data));
}

inline uint64_t bytes64_mask_to_bits64_mask(const bool* data) {
    return ~bytes64_mask_to_bits64_not_mask(reinterpret_cast<const int8_t*>(data));
}

inline std::size_t count_zero_num(const int8_t* data, std::size_t size) {
    std::size_t num = 0;
    const int8_t* end = data + size;
#if defined(__POPCNT__)
    const int8_t* end64 = data + (size / 64 * 64);
    for (; data < end64; data += 64) {
        num += __builtin_popcountll(bytes64_mask_to_bits64_not_mask(data));
    }
#endif
    for (; data < end; ++data) {
        num += (*data == 0);
    }
    return num;
}

inline std::size_t count_zero_num(const int8_t* data, const uint8_t* null_map, std::size_t size) {
    std::size_t num = 0;
    const int8_t* end = data + size;
    for (; data < end; ++data, ++null_map) {
        num += ((*data == 0) | *null_map);
    }
    return num;
}

/**
 * Counts how many bytes of `data` are not equal zero.
 * NOTE: In theory, `data` should only contain zeros and ones.
 */
inline std::size_t count_not_zero(const int8_t* data, std::size_t size) {
    return size - count_zero_num(data, size);
}

inline std::size_t count_not_zero(std::vector<uint8_t>& vec) {
    return count_not_zero(reinterpret_cast<const int8_t*>(vec.data()), vec.size());
}

inline std::size_t count_not_zero(const vectorized::IColumn::Filter& filter) {
    return count_not_zero(reinterpret_cast<const int8_t*>(filter.data()), filter.size());
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

inline size_t find_nonzero(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 1);
}

inline size_t find_zero(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 0);
}

} // namespace doris::simd
