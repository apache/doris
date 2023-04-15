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

#include <array>
#include <cstddef>
#include <cstdint>

#include "util/simd/lower_upper_impl.h"
#include "util/simd/simd.h"
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

namespace simd {

class VStringFunctions {
public:
    static StringRef rtrim(const StringRef& str) {
        if (str.size == 0) {
            return str;
        }
        auto begin = 0;
        int64_t end = str.size - 1;
#ifdef SIMD_AVAILABLE
        char blank = ' ';
        const auto pattern = fill_with_pi8(blank);
        while (end - begin + 1 >= INT_WORD_SIZE) {
            const auto v_haystack = load_si(str.data + end + 1 - INT_WORD_SIZE);
            const auto v_against_pattern = eq_for_epi8(v_haystack, pattern);
            int offset = __builtin_clz(bitwise_not(movemask_epi8(
                    v_against_pattern))); // count leading zeros will get original equal number

            if (offset == 0) { // means not found
                return StringRef(str.data + begin, end - begin + 1);
            } else {
                end -= offset;
            }
        }
#endif
        while (end >= begin && str.data[end] == ' ') {
            --end;
        }
        if (end < 0) {
            return StringRef("");
        }
        return StringRef(str.data + begin, end - begin + 1);
    }

    static StringRef ltrim(const StringRef& str) {
        if (str.size == 0) {
            return str;
        }
        auto begin = 0;
        auto end = str.size - 1;
#ifdef SIMD_AVAILABLE
        char blank = ' ';
        const auto pattern = fill_with_pi8(blank);
        while (end - begin + 1 >= INT_WORD_SIZE) {
            const auto v_haystack = load_si(str.data + begin);
            const auto v_against_pattern = eq_for_epi8(v_haystack, pattern);
            const auto mask = bitwise_not(movemask_epi8(v_against_pattern));
            /// zero means not found
            if (mask == 0) {
                begin += INT_WORD_SIZE;
            } else {
                const auto offset = __builtin_ctz(mask);
                begin += offset;
                return StringRef(str.data + begin, end - begin + 1);
            }
        }
#endif
        while (begin <= end && str.data[begin] == ' ') {
            ++begin;
        }
        return StringRef(str.data + begin, end - begin + 1);
    }

    static StringRef trim(const StringRef& str) {
        if (str.size == 0) {
            return str;
        }
        return rtrim(ltrim(str));
    }

    // Gcc will do auto simd in this function
    static bool is_ascii(const StringRef& str) {
        char or_code = 0;
        for (size_t i = 0; i < str.size; i++) {
            or_code |= str.data[i];
        }
        return !(or_code & 0x80);
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
};
} // namespace simd
} // namespace doris
