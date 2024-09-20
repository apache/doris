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

#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "util/sse_util.hpp"

namespace doris {
namespace simd {

consteval auto bits_mask_length() {
#if defined(__ARM_NEON) && defined(__aarch64__)
    return 16;
#else
    return 32;
#endif
}

#if defined(__ARM_NEON) && defined(__aarch64__)
inline uint64_t get_nibble_mask(uint8x16_t values) {
    // It produces 4-bit out of each byte, alternating between the high 4-bits and low 4-bits of the 16-byte vector.
    // Given that the comparison operators give a 16-byte result of 0x00 or 0xff, the result is close to being a PMOVMSKB,
    // the only difference is that every matching bit is repeated 4 times and is a 64-bit integer.
    // https://community.arm.com/arm-community-blogs/b/infrastructure-solutions-blog/posts/porting-x86-vector-bitmask-optimizations-to-arm-neon?CommentId=af187ac6-ae00-4e4d-bbf0-e142187aa92e
    return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(values), 4)), 0);
}
/*
Input 16 bytes of data and convert it into a 64-bit integer, where one bit appears 4 times.
Compare with bytes32_mask_to_bits32_mask, a u8 array with a length of 32
  std::vector<uint8_t> vec = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0};

bytes32_mask_to_bits32_mask   0100 0000 0000 0000,1101 0000 0000 0011


                            (1101 0000 0000 0011)
bytes16_mask_to_bits64_mask   1111 1111 0000 1111,0000 0000 0000 0000,0000 0000 0000 0000,0000 0000 1111 1111
                            (0100 0000 0000 0000)
                              0000 1111 0000 0000,0000 0000 0000 0000,0000 0000 0000 0000,0000 0000 0000 0000
*/

inline uint64_t bytes16_mask_to_bits64_mask(const uint8_t* data) {
    const uint8x16_t vfilter = vld1q_u8(data);
    return get_nibble_mask(vmvnq_u8(vceqzq_u8(vfilter)));
}
#endif

inline uint32_t bytes32_mask_to_bits32_mask(const uint8_t* data) {
#ifdef __AVX2__
    auto zero32 = _mm256_setzero_si256();
    uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(
            _mm256_cmpgt_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(data)), zero32)));
#elif defined(__SSE2__)
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

inline auto bytes_mask_to_bits_mask(const uint8_t* data) {
#if defined(__ARM_NEON) && defined(__aarch64__)
    return bytes16_mask_to_bits64_mask(data);
#else
    return bytes32_mask_to_bits32_mask(data);
#endif
}

inline constexpr auto bits_mask_all() {
#if defined(__ARM_NEON) && defined(__aarch64__)
    return 0xffff'ffff'ffff'ffffULL;
#else
    return 0xffffffff;
#endif
}

template <typename Func>
void iterate_through_bits_mask(Func func, decltype(bytes_mask_to_bits_mask(nullptr)) mask) {
#if defined(__ARM_NEON) && defined(__aarch64__)
    mask &= 0x8888'8888'8888'8888ULL;
    while (mask) {
        const auto index = __builtin_ctzll(mask) >> 2;
        func(index);
        mask &= mask - 1;
    }

#else
    while (mask) {
        const auto bit_pos = __builtin_ctzll(mask);
        func(bit_pos);
        mask = mask & (mask - 1);
    }
#endif
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
#if defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    const int8_t* end64 = data + (size / 64 * 64);

    for (; data < end64; data += 64) {
        num += __builtin_popcountll(
                static_cast<uint64_t>(_mm_movemask_epi8(_mm_or_si128(
                        _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(data)),
                                       zero16),
                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(null_map))))) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_or_si128(
                         _mm_cmpeq_epi8(
                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16)),
                                 zero16),
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(null_map + 16)))))
                 << 16u) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_or_si128(
                         _mm_cmpeq_epi8(
                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 32)),
                                 zero16),
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(null_map + 32)))))
                 << 32u) |
                (static_cast<uint64_t>(_mm_movemask_epi8(_mm_or_si128(
                         _mm_cmpeq_epi8(
                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 48)),
                                 zero16),
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(null_map + 48)))))
                 << 48u));
    }
#endif
    for (; data < end; ++data, ++null_map) {
        num += ((*data == 0) | *null_map);
    }
    return num;
}

// TODO: compare with different SIMD implements
template <class T>
static size_t find_byte(const std::vector<T>& vec, size_t start, T byte) {
    if (start >= vec.size()) {
        return start;
    }
    const void* p = std::memchr((const void*)(vec.data() + start), byte, vec.size() - start);
    if (p == nullptr) {
        return vec.size();
    }
    return (T*)p - vec.data();
}

template <class T>
static size_t find_byte(const T* data, size_t start, size_t end, T byte) {
    if (start >= end) {
        return start;
    }
    const void* p = std::memchr((const void*)(data + start), byte, end - start);
    if (p == nullptr) {
        return end;
    }
    return (T*)p - data;
}

template <typename T>
bool contain_byte(const T* __restrict data, const size_t length, const signed char byte) {
    return nullptr != std::memchr(reinterpret_cast<const void*>(data), byte, length);
}

inline size_t find_one(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 1);
}

inline size_t find_one(const uint8_t* data, size_t start, size_t end) {
    return find_byte<uint8_t>(data, start, end, 1);
}

inline size_t find_zero(const std::vector<uint8_t>& vec, size_t start) {
    return find_byte<uint8_t>(vec, start, 0);
}

} // namespace simd
} // namespace doris
