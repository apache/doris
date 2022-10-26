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

#include <unistd.h>

#include <cstdint>

#ifdef __aarch64__
#include <sse2neon.h>
#endif

#include "runtime/string_value.hpp"
#include "util/simd/lower_upper_impl.h"

namespace doris {

static size_t get_utf8_byte_length(unsigned char byte) {
    size_t char_size = 0;
    if (byte >= 0xFC) {
        char_size = 6;
    } else if (byte >= 0xF8) {
        char_size = 5;
    } else if (byte >= 0xF0) {
        char_size = 4;
    } else if (byte >= 0xE0) {
        char_size = 3;
    } else if (byte >= 0xC0) {
        char_size = 2;
    } else {
        char_size = 1;
    }
    return char_size;
}

namespace simd {

class VStringFunctions {
public:
#if defined(__SSE2__) || defined(__aarch64__)
    /// n equals to 16 chars length
    static constexpr auto REGISTER_SIZE = sizeof(__m128i);
#endif
public:
    static StringVal rtrim(const StringVal& str) {
        if (str.is_null || str.len == 0) {
            return str;
        }
        auto begin = 0;
        auto end = str.len - 1;
#if defined(__SSE2__) || defined(__aarch64__)
        char blank = ' ';
        const auto pattern = _mm_set1_epi8(blank);
        while (end - begin + 1 >= REGISTER_SIZE) {
            const auto v_haystack = _mm_loadu_si128(
                    reinterpret_cast<const __m128i*>(str.ptr + end + 1 - REGISTER_SIZE));
            const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);
            const auto mask = _mm_movemask_epi8(v_against_pattern);
            int offset = __builtin_clz(~(mask << REGISTER_SIZE));
            /// means not found
            if (offset == 0) {
                return StringVal(str.ptr + begin, end - begin + 1);
            } else {
                end -= offset;
            }
        }
#endif
        while (end >= begin && str.ptr[end] == ' ') {
            --end;
        }
        if (end < 0) {
            return StringVal("");
        }
        return StringVal(str.ptr + begin, end - begin + 1);
    }

    static StringVal ltrim(const StringVal& str) {
        if (str.is_null || str.len == 0) {
            return str;
        }
        auto begin = 0;
        auto end = str.len - 1;
#if defined(__SSE2__) || defined(__aarch64__)
        char blank = ' ';
        const auto pattern = _mm_set1_epi8(blank);
        while (end - begin + 1 >= REGISTER_SIZE) {
            const auto v_haystack =
                    _mm_loadu_si128(reinterpret_cast<const __m128i*>(str.ptr + begin));
            const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);
            const auto mask = _mm_movemask_epi8(v_against_pattern) ^ 0xffff;
            /// zero means not found
            if (mask == 0) {
                begin += REGISTER_SIZE;
            } else {
                const auto offset = __builtin_ctz(mask);
                begin += offset;
                return StringVal(str.ptr + begin, end - begin + 1);
            }
        }
#endif
        while (begin <= end && str.ptr[begin] == ' ') {
            ++begin;
        }
        return StringVal(str.ptr + begin, end - begin + 1);
    }

    static StringVal trim(const StringVal& str) {
        if (str.is_null || str.len == 0) {
            return str;
        }
        return rtrim(ltrim(str));
    }

    // Gcc will do auto simd in this function
    static bool is_ascii(const StringVal& str) {
        char or_code = 0;
        for (size_t i = 0; i < str.len; i++) {
            or_code |= str.ptr[i];
        }
        return !(or_code & 0x80);
    }

    static void reverse(const StringVal& str, StringVal dst) {
        if (is_ascii(str)) {
            int64_t begin = 0;
            int64_t end = str.len;
            int64_t result_end = dst.len - 1;

            // auto SIMD here
            auto* __restrict l = dst.ptr;
            auto* __restrict r = str.ptr;
            for (; begin < end; ++begin, --result_end) {
                l[result_end] = r[begin];
            }
        } else {
            for (size_t i = 0, char_size = 0; i < str.len; i += char_size) {
                char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
                std::copy(str.ptr + i, str.ptr + i + char_size, dst.ptr + str.len - i - char_size);
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
};
} // namespace simd
} // namespace doris
