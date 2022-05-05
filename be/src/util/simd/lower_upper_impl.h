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
#ifdef __SSE2__
#include <emmintrin.h>
#endif
#include <stdint.h>
#include <iostream>
#include <string>

// the code refer: https://clickhouse.tech/codebrowser/html_report//ClickHouse/src/Functions/LowerUpperImpl.h.html
// Doris only handle one character at a time, this function use SIMD to more characters at a time
namespace doris::simd {

template <char not_case_lower_bound, char not_case_upper_bound>
class LowerUpperImpl {
public:
    static void transfer(const uint8_t* src, const uint8_t* src_end, uint8_t* dst) {
        const auto flip_case_mask = 'A' ^ 'a';

#ifdef __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto src_end_sse = src_end - (src_end - src) % bytes_sse;

        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse) {
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
            const auto is_not_case = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound),
                                                   _mm_cmplt_epi8(chars, v_not_case_upper_bound));
            const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);
            const auto cased_chars = _mm_xor_si128(chars, xor_mask);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), cased_chars);
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};
} // namespace doris::simd
