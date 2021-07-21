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

#ifndef BE_V_STRING_FUNCTIONS_H
#define BE_V_STRING_FUNCTIONS_H

#include <stdint.h>
#include <unistd.h>
#include "runtime/string_value.hpp"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __SSE4_1__
#include <smmintrin.h>
#endif

namespace doris {
class VStringFunctions {

#ifdef __SSE2__
    /// n equals to 16 chars length
    static constexpr auto n = sizeof(__m128i);
#endif
public:

    static StringVal rtrim(const StringVal& str) {
        if (str.is_null) {
            return StringVal::null();
        }
        if (str.len == 0) {
            return str;
        }
        auto begin = 0;
        auto end = str.len - 1;
#ifdef __SSE2__
        char blank = ' ';
        const auto pattern =  _mm_set1_epi8(blank);
        while (end - begin + 1 >= n) {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(str.ptr + end + 1 - n));
            const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);
            const auto mask = _mm_movemask_epi8(v_against_pattern);
            const auto offset = __builtin_ctz(mask);
            /// means not found
            if (mask == 0)
            {
                return StringVal(str.ptr + begin, end - begin + 1);
            } else {
                end -= n - offset;
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
        if (str.is_null) {
            return StringVal::null();
        }
        if (str.len == 0) {
            return str;
        }
        auto begin = 0;
        auto end = str.len - 1;
#ifdef __SSE2__
        char blank = ' ';
        const auto pattern =  _mm_set1_epi8(blank);
        while (end - begin + 1 >= n) {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(str.ptr + begin));
            const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);
            const auto mask = _mm_movemask_epi8(v_against_pattern);
            const auto offset = __builtin_ctz(mask ^ 0xffff);
            /// means not found
            if (offset == 0)
            {
                return StringVal(str.ptr + begin, end - begin + 1);
            } else if (offset > n) {
                begin += n;
            } else {
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
};
}

#endif //BE_V_STRING_FUNCTIONS_H
