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

#ifdef __AVX2__
#include <immintrin.h>

#include "gutil/macros.h"
#endif
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>

#include "util/simd/lower_upper_impl.h"
#include "util/sse_util.hpp"
#include "vec/common/string_ref.h"

namespace doris {

static constexpr std::array<uint8, 256> UTF8_BYTE_LENGTH = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6};

inline uint8_t get_utf8_byte_length(uint8_t character) {
    return UTF8_BYTE_LENGTH[character];
}

// copy from https://github.com/lemire/fastvalidate-utf-8/blob/master/include/simdasciicheck.h
// The function returns true (1) if all chars passed in src are
// 7-bit values (0x00..0x7F). Otherwise, it returns false (0).
inline bool validate_ascii_fast(const char* src, size_t len) {
    size_t i = 0;
    __m128i has_error = _mm_setzero_si128();
    if (len >= 16) {
        for (; i <= len - 16; i += 16) {
            __m128i current_bytes = _mm_loadu_si128((const __m128i*)(src + i));
            has_error = _mm_or_si128(has_error, current_bytes);
        }
    }
    int error_mask = _mm_movemask_epi8(has_error);

    char tail_has_error = 0;
    for (; i < len; i++) {
        tail_has_error |= src[i];
    }
    error_mask |= (tail_has_error & 0x80);

    return !error_mask;
}

#ifdef __AVX2__
#include <x86intrin.h>
// The function returns true (1) if all chars passed in src are
// 7-bit values (0x00..0x7F). Otherwise, it returns false (0).
inline bool validate_ascii_fast_avx(const char* src, size_t len) {
    size_t i = 0;
    __m256i has_error = _mm256_setzero_si256();
    if (len >= 32) {
        for (; i <= len - 32; i += 32) {
            __m256i current_bytes = _mm256_loadu_si256((const __m256i*)(src + i));
            has_error = _mm256_or_si256(has_error, current_bytes);
        }
    }
    int error_mask = _mm256_movemask_epi8(has_error);

    char tail_has_error = 0;
    for (; i < len; i++) {
        tail_has_error |= src[i];
    }
    error_mask |= (tail_has_error & 0x80);

    return !error_mask;
}
#endif

namespace simd {

class VStringFunctions {
public:
#if defined(__SSE2__) || defined(__aarch64__)
    /// n equals to 16 chars length
    static constexpr auto REGISTER_SIZE = sizeof(__m128i);
#endif

    template <bool trim_single>
    static inline const unsigned char* rtrim(const unsigned char* begin, const unsigned char* end,
                                             const StringRef& remove_str) {
        if (remove_str.size == 0) {
            return end;
        }
        const auto* p = end;

        if constexpr (trim_single) {
            const auto ch = remove_str.data[0];
#if defined(__AVX2__)
            constexpr auto AVX2_BYTES = sizeof(__m256i);
            const auto size = end - begin;
            const auto* const avx2_begin = end - size / AVX2_BYTES * AVX2_BYTES;
            const auto spaces = _mm256_set1_epi8(ch);
            for (p = end - AVX2_BYTES; p >= avx2_begin; p -= AVX2_BYTES) {
                uint32_t masks = _mm256_movemask_epi8(
                        _mm256_cmpeq_epi8(_mm256_loadu_si256((__m256i*)p), spaces));
                if ((~masks)) {
                    break;
                }
            }
            p += AVX2_BYTES;
#endif
            for (; (p - 1) >= begin && *(p - 1) == ch; p--) {
            }
            return p;
        }

        const auto remove_size = remove_str.size;
        const auto* const remove_data = remove_str.data;
        while (p - begin >= remove_size) {
            if (memcmp(p - remove_size, remove_data, remove_size) == 0) {
                p -= remove_str.size;
            } else {
                break;
            }
        }
        return p;
    }

    template <bool trim_single>
    static inline const unsigned char* ltrim(const unsigned char* begin, const unsigned char* end,
                                             const StringRef& remove_str) {
        if (remove_str.size == 0) {
            return begin;
        }
        const auto* p = begin;

        if constexpr (trim_single) {
            const auto ch = remove_str.data[0];
#if defined(__AVX2__)
            constexpr auto AVX2_BYTES = sizeof(__m256i);
            const auto size = end - begin;
            const auto* const avx2_end = begin + size / AVX2_BYTES * AVX2_BYTES;
            const auto spaces = _mm256_set1_epi8(ch);
            for (; p < avx2_end; p += AVX2_BYTES) {
                uint32_t masks = _mm256_movemask_epi8(
                        _mm256_cmpeq_epi8(_mm256_loadu_si256((__m256i*)p), spaces));
                if ((~masks)) {
                    break;
                }
            }
#endif
            for (; p < end && *p == ch; ++p) {
            }
            return p;
        }

        const auto remove_size = remove_str.size;
        const auto* const remove_data = remove_str.data;
        while (end - p >= remove_size) {
            if (memcmp(p, remove_data, remove_size) == 0) {
                p += remove_str.size;
            } else {
                break;
            }
        }
        return p;
    }

    // Iterate a UTF-8 string without exceeding a given length n.
    // The function returns two values:
    // the first represents the byte length traversed, and the second represents the char length traversed.
    static inline std::pair<size_t, size_t> iterate_utf8_with_limit_length(const char* begin,
                                                                           const char* end,
                                                                           size_t n) {
        const char* p = begin;
        int char_size = 0;

        size_t i = 0;
        for (; i < n && p < end; ++i, p += char_size) {
            char_size = UTF8_BYTE_LENGTH[static_cast<uint8_t>(*p)];
        }

        return {p - begin, i};
    }

    // Gcc will do auto simd in this function
    static bool is_ascii(const StringRef& str) {
#ifdef __AVX2__
        return validate_ascii_fast_avx(str.data, str.size);
#endif
        return validate_ascii_fast(str.data, str.size);
    }

    static void reverse(const StringRef& str, StringRef dst) {
        if (is_ascii(str)) {
            int64_t begin = 0;
            int64_t end = str.size;
            int64_t result_end = dst.size - 1;

            // auto SIMD here
            auto* __restrict l = const_cast<char*>(dst.data);
            auto* __restrict r = str.data;
            for (; begin < end; ++begin, --result_end) {
                l[result_end] = r[begin];
            }
        } else {
            char* dst_data = const_cast<char*>(dst.data);
            for (size_t i = 0, char_size = 0; i < str.size; i += char_size) {
                char_size = UTF8_BYTE_LENGTH[(unsigned char)(str.data)[i]];
                // there exists occasion where the last character is an illegal UTF-8 one which returns
                // a char_size larger than the actual space, which would cause offset execeeding the buffer range
                // for example, consider str.size=4, i = 3, then the last char returns char_size 2, then
                // the str.data + offset would exceed the buffer range
                size_t offset = i + char_size;
                if (offset > str.size) {
                    offset = str.size;
                }
                std::copy(str.data + i, str.data + offset, dst_data + str.size - offset);
            }
        }
    }

    static void hex_encode(const unsigned char* src_str, size_t length, char* dst_str) {
        static constexpr auto hex_table = "0123456789ABCDEF";
        auto src_str_end = src_str + length;

#if defined(__SSE2__) || defined(__aarch64__)
        constexpr auto step = sizeof(uint64);
        if (src_str + step < src_str_end) {
            const auto hex_map = _mm_loadu_si128(reinterpret_cast<const __m128i*>(hex_table));
            const auto mask_map = _mm_set1_epi8(0x0F);

            do {
                auto data = _mm_loadu_si64(src_str);
                auto hex_loc =
                        _mm_and_si128(_mm_unpacklo_epi8(_mm_srli_epi64(data, 4), data), mask_map);
                _mm_storeu_si128(reinterpret_cast<__m128i*>(dst_str),
                                 _mm_shuffle_epi8(hex_map, hex_loc));

                src_str += step;
                dst_str += step * 2;
            } while (src_str + step < src_str_end);
        }
#endif
        char res[2];
        // hex(str) str length is n, result must be 2 * n length
        for (; src_str < src_str_end; src_str += 1, dst_str += 2) {
            // low 4 bits
            *(res + 1) = hex_table[src_str[0] & 0x0F];
            // high 4 bits
            *res = hex_table[(src_str[0] >> 4)];
            std::copy(res, res + 2, dst_str);
        }
    }

    static void to_lower(const uint8_t* src, int64_t len, uint8_t* dst) {
        if (len <= 0) {
            return;
        }
        LowerUpperImpl<'A', 'Z'> lowerUpper;
        lowerUpper.transfer(src, src + len, dst);
    }

    static void to_upper(const uint8_t* src, int64_t len, uint8_t* dst) {
        if (len <= 0) {
            return;
        }
        LowerUpperImpl<'a', 'z'> lowerUpper;
        lowerUpper.transfer(src, src + len, dst);
    }

    static inline size_t get_char_len(const char* src, size_t len, std::vector<size_t>& str_index) {
        size_t char_len = 0;
        for (size_t i = 0, char_size = 0; i < len; i += char_size) {
            char_size = UTF8_BYTE_LENGTH[(unsigned char)src[i]];
            str_index.push_back(i);
            ++char_len;
        }
        return char_len;
    }

    // utf8-encoding:
    // - 1-byte: 0xxx_xxxx;
    // - 2-byte: 110x_xxxx 10xx_xxxx;
    // - 3-byte: 1110_xxxx 10xx_xxxx 10xx_xxxx;
    // - 4-byte: 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx.
    // Counting utf8 chars in a byte string is equivalent to counting first byte of utf chars, that
    // is to say, counting bytes which do not match 10xx_xxxx pattern.
    // All 0xxx_xxxx, 110x_xxxx, 1110_xxxx and 1111_0xxx are greater than 1011_1111 when use int8_t arithmetic,
    // so just count bytes greater than 1011_1111 in a byte string as the result of utf8_length.
    static inline size_t get_char_len(const char* src, size_t len) {
        size_t char_len = 0;
        const char* p = src;
        const char* end = p + len;
#if defined(__SSE2__) || defined(__aarch64__)
        constexpr auto bytes_sse2 = sizeof(__m128i);
        const auto src_end_sse2 = p + (len & ~(bytes_sse2 - 1));
        // threshold = 1011_1111
        const auto threshold = _mm_set1_epi8(0xBF);
        for (; p < src_end_sse2; p += bytes_sse2) {
            char_len += __builtin_popcount(_mm_movemask_epi8(_mm_cmpgt_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i*>(p)), threshold)));
        }
#endif
        // process remaining bytes the number of which not exceed bytes_sse2 at the
        // tail of string, one by one.
        for (; p < end; ++p) {
            char_len += static_cast<int8_t>(*p) > static_cast<int8_t>(0xBF);
        }
        return char_len;
    }
};
} // namespace simd
} // namespace doris
