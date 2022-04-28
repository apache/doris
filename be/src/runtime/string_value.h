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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/string-value.h
// and modified by Doris

#ifndef DORIS_BE_RUNTIME_STRING_VALUE_H
#define DORIS_BE_RUNTIME_STRING_VALUE_H

#include <string.h>

#include "udf/udf.h"
#include "util/hash_util.hpp"
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
        int chars_match = _mm_cmpestri(xmm0, sse_util::CHARS_PER_128_BIT_REGISTER, xmm1,
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

// The format of a string-typed slot.
// The returned StringValue of all functions that return StringValue
// shares its buffer the parent.
struct StringValue {
    const static char MIN_CHAR;
    const static char MAX_CHAR;

    static const int MAX_LENGTH = (1 << 30);
    // TODO: change ptr to an offset relative to a contiguous memory block,
    // so that we can send row batches between nodes without having to swizzle
    // pointers
    // NOTE: This struct should keep the same memory layout with Slice, otherwise
    // it will lead to BE crash.
    // TODO(zc): we should unify this struct with Slice some day.
    char* ptr;
    size_t len;

    StringValue(char* ptr, int len) : ptr(ptr), len(len) {}
    StringValue(const char* ptr, int len) : ptr(const_cast<char*>(ptr)), len(len) {}
    StringValue() : ptr(nullptr), len(0) {}

    /// Construct a StringValue from 's'.  's' must be valid for as long as
    /// this object is valid.
    explicit StringValue(const std::string& s) : ptr(const_cast<char*>(s.c_str())), len(s.size()) {
        DCHECK_LE(len, MAX_LENGTH);
    }

    void replace(char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }

    // Byte-by-byte comparison. Returns:
    // this < other: -1
    // this == other: 0
    // this > other: 1
    int compare(const StringValue& other) const {
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

    // ==
    bool eq(const StringValue& other) const {
        if (this->len != other.len) {
            return false;
        }

#if defined(__SSE2__)
        return memequalSSE2Wide(this->ptr, other.ptr, this->len);
#endif

        return string_compare(this->ptr, this->len, other.ptr, other.len, this->len) == 0;
    }

    bool operator==(const StringValue& other) const { return eq(other); }
    // !=
    bool ne(const StringValue& other) const { return !eq(other); }
    // <=
    bool le(const StringValue& other) const { return compare(other) <= 0; }
    // >=
    bool ge(const StringValue& other) const { return compare(other) >= 0; }
    // <
    bool lt(const StringValue& other) const { return compare(other) < 0; }
    // >
    bool gt(const StringValue& other) const { return compare(other) > 0; }

    bool operator!=(const StringValue& other) const { return ne(other); }

    bool operator<=(const StringValue& other) const { return le(other); }

    bool operator>=(const StringValue& other) const { return ge(other); }

    bool operator<(const StringValue& other) const { return lt(other); }

    bool operator>(const StringValue& other) const { return gt(other); }

    std::string debug_string() const;

    std::string to_string() const;

    // Returns the substring starting at start_pos until the end of string.
    StringValue substring(int start_pos) const;

    // Returns the substring starting at start_pos with given length.
    // If new_len < 0 then the substring from start_pos to end of string is returned.
    StringValue substring(int start_pos, int new_len) const;

    // Trims leading and trailing spaces.
    StringValue trim() const;

    void to_string_val(doris_udf::StringVal* sv) const {
        *sv = doris_udf::StringVal(reinterpret_cast<uint8_t*>(ptr), len);
    }

    static StringValue from_string_val(const doris_udf::StringVal& sv) {
        return StringValue(reinterpret_cast<char*>(sv.ptr), sv.len);
    }

    static StringValue min_string_val();

    static StringValue max_string_val();

    struct Comparator {
        bool operator()(const StringValue& a, const StringValue& b) const {
            return a.compare(b) < 0;
        }
    };

    struct HashOfStringValue {
        size_t operator()(const StringValue& v) const { return HashUtil::hash(v.ptr, v.len, 0); }
    };
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const StringValue& v) {
    return HashUtil::hash(v.ptr, v.len, 0);
}

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

std::size_t operator-(const StringValue& v1, const StringValue& v2);

} // namespace doris

#endif
