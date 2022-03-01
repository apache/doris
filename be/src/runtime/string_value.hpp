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

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_STRING_VALUE_INLINE_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_STRING_VALUE_INLINE_H

#include <cstring>

#include "runtime/string_value.h"
#include "util/cpu_info.h"
#include "vec/common/string_ref.h"
#ifdef __SSE4_2__
#include "util/sse_util.hpp"
#endif

namespace doris {

// Compare two strings using sse4.2 intrinsics if they are available. This code assumes
// that the trivial cases are already handled (i.e. one string is empty).
// Returns:
//   < 0 if s1 < s2
//   0 if s1 == s2
//   > 0 if s1 > s2
// The SSE code path is just under 2x faster than the non-sse code path.
//   - s1/n1: ptr/len for the first string
//   - s2/n2: ptr/len for the second string
//   - len: min(n1, n2) - this can be more cheaply passed in by the caller
static inline int string_compare(const char* s1, int64_t n1, const char* s2, int64_t n2,
                                 int64_t len) {
    DCHECK_EQ(len, std::min(n1, n2));
#ifdef __SSE4_2__
    while (len >= sse_util::CHARS_PER_128_BIT_REGISTER) {
        __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
        __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
        int chars_match =
                _mm_cmpestri(xmm0, sse_util::CHARS_PER_128_BIT_REGISTER, xmm1,
                             sse_util::CHARS_PER_128_BIT_REGISTER, sse_util::STRCMP_MODE);
        if (chars_match != sse_util::CHARS_PER_128_BIT_REGISTER) {
            return (unsigned char)s1[chars_match] - (unsigned char)s2[chars_match];
        }
        len -= sse_util::CHARS_PER_128_BIT_REGISTER;
        s1 += sse_util::CHARS_PER_128_BIT_REGISTER;
        s2 += sse_util::CHARS_PER_128_BIT_REGISTER;
    }
#endif
    unsigned char u1, u2;
    while (len-- > 0) {
        u1 = (unsigned char)*s1++;
        u2 = (unsigned char)*s2++;
        if (u1 != u2) return u1 - u2;
        if (u1 == '\0') return n1 - n2;
    }

    return n1 - n2;
}

inline int StringValue::compare(const StringValue& other) const {
    int l = std::min(len, other.len);

    if (l == 0) {
        if (len == other.len) {
            return 0;
        } else if (len == 0) {
            return -1;
        } else {
            DCHECK_EQ(other.len, 0);
            return 1;
        }
    }

    return string_compare(this->ptr, this->len, other.ptr, other.len, l);
}

inline bool StringValue::eq(const StringValue& other) const {
    if (this->len != other.len) {
        return false;
    }
#if defined(__SSE2__)
    return memequalSSE2Wide(this->ptr, other.ptr, this->len);
#endif

    return string_compare(this->ptr, this->len, other.ptr, other.len, this->len) == 0;
}

inline StringValue StringValue::substring(int start_pos) const {
    return StringValue(ptr + start_pos, len - start_pos);
}

inline StringValue StringValue::substring(int start_pos, int new_len) const {
    return StringValue(ptr + start_pos, (new_len < 0) ? (len - start_pos) : new_len);
}

inline StringValue StringValue::trim() const {
    // Remove leading and trailing spaces.
    int32_t begin = 0;

    while (begin < len && ptr[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && ptr[end] == ' ') {
        --end;
    }

    return StringValue(ptr + begin, end - begin + 1);
}

} // namespace doris

#endif
