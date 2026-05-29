// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace doris::segment_v2::inverted_index {

// SIMD-accelerated ASCII alnum-run scanner with optional in-place
// lowercasing. Shared between `BasicTokenizer::cut` (general
// analyzer path) and V4's `inverted_index_writer.cpp` fast
// tokenize+append fused loop (skips the CLucene Token wrapping
// virtual `next()` per token).
//
// Scan `[s + start, s + length)` for the end of the ASCII-alnum
// run starting at `start`. Stops at the first non-ASCII byte (the
// scalar fallback in the caller handles UTF-8 then). When
// `Lowercase` is true, lowercases each ASCII letter in-place
// during the scan.
//
// Returns the index of the first non-alnum or non-ASCII byte
// (i.e. the run's exclusive end). Processes 32 bytes per AVX2
// iteration; tail loop is scalar.
//
// Char class derivation (all signed-int8 comparisons; the ASCII
// alnum set sits in the positive int8 range):
//   digit:  '0' <= c && c <= '9'
//   upper:  'A' <= c && c <= 'Z'
//   lower:  'a' <= c && c <= 'z'
//   alnum:  digit | upper | lower
//   ascii:  (c & 0x80) == 0
template <bool Lowercase>
[[gnu::always_inline]] inline int32_t scan_ascii_alnum_run(uint8_t* s, int32_t start,
                                                           int32_t length) {
#if defined(__AVX2__)
    const __m256i v_high_bit = _mm256_set1_epi8(static_cast<char>(0x80));
    const __m256i v_zero_m1 = _mm256_set1_epi8('0' - 1);
    const __m256i v_nine_p1 = _mm256_set1_epi8('9' + 1);
    const __m256i v_upper_a_m1 = _mm256_set1_epi8('A' - 1);
    const __m256i v_upper_z_p1 = _mm256_set1_epi8('Z' + 1);
    const __m256i v_lower_a_m1 = _mm256_set1_epi8('a' - 1);
    const __m256i v_lower_z_p1 = _mm256_set1_epi8('z' + 1);
    const __m256i v_lower_off = _mm256_set1_epi8(0x20);

    int32_t i = start;
    while (i + 32 <= length) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(s + i));
        const int high_mask = _mm256_movemask_epi8(_mm256_and_si256(v, v_high_bit));
        if (high_mask != 0) {
            break;
        }
        const __m256i is_digit =
                _mm256_and_si256(_mm256_cmpgt_epi8(v, v_zero_m1), _mm256_cmpgt_epi8(v_nine_p1, v));
        const __m256i is_upper = _mm256_and_si256(_mm256_cmpgt_epi8(v, v_upper_a_m1),
                                                  _mm256_cmpgt_epi8(v_upper_z_p1, v));
        const __m256i is_lower = _mm256_and_si256(_mm256_cmpgt_epi8(v, v_lower_a_m1),
                                                  _mm256_cmpgt_epi8(v_lower_z_p1, v));
        const __m256i is_alnum = _mm256_or_si256(_mm256_or_si256(is_digit, is_upper), is_lower);
        const uint32_t alnum_mask = static_cast<uint32_t>(_mm256_movemask_epi8(is_alnum));
        if (alnum_mask != 0xFFFFFFFFU) {
            const int32_t first_non_alnum = static_cast<int32_t>(__builtin_ctz(~alnum_mask));
            if constexpr (Lowercase) {
                // Lowercase only the run's bytes [i, i + first_non_alnum). The
                // remaining bytes of this 32-byte window belong to the separator
                // and the next token, so a full-width store would mutate data
                // outside the run being scanned — do a scalar pass over just the
                // run instead.
                for (int32_t j = i; j < i + first_non_alnum; ++j) {
                    if (static_cast<uint8_t>(s[j] - 'A') <= 25U) {
                        s[j] = static_cast<uint8_t>(s[j] | 0x20U);
                    }
                }
            }
            return i + first_non_alnum;
        }
        if constexpr (Lowercase) {
            const __m256i add = _mm256_and_si256(is_upper, v_lower_off);
            v = _mm256_add_epi8(v, add);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(s + i), v);
        }
        i += 32;
    }
    for (; i < length; ++i) {
        const uint8_t c = s[i];
        if ((c & 0x80U) != 0U) {
            return i;
        }
        const bool is_a = (static_cast<uint8_t>(c - '0') <= 9U) ||
                          (static_cast<uint8_t>(c - 'A') <= 25U) ||
                          (static_cast<uint8_t>(c - 'a') <= 25U);
        if (!is_a) {
            return i;
        }
        if constexpr (Lowercase) {
            if (static_cast<uint8_t>(c - 'A') <= 25U) {
                s[i] = static_cast<uint8_t>(c | 0x20U);
            }
        }
    }
    return length;
#else
    int32_t i = start;
    for (; i < length; ++i) {
        const uint8_t c = s[i];
        if ((c & 0x80U) != 0U) {
            return i;
        }
        const bool is_a = (static_cast<uint8_t>(c - '0') <= 9U) ||
                          (static_cast<uint8_t>(c - 'A') <= 25U) ||
                          (static_cast<uint8_t>(c - 'a') <= 25U);
        if (!is_a) {
            return i;
        }
        if constexpr (Lowercase) {
            if (static_cast<uint8_t>(c - 'A') <= 25U) {
                s[i] = static_cast<uint8_t>(c | 0x20U);
            }
        }
    }
    return length;
#endif
}

} // namespace doris::segment_v2::inverted_index
