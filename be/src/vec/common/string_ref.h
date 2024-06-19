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
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/StringRef.h
// and modified by Doris

#pragma once

// IWYU pragma: no_include <crc32intrin.h>
#include <glog/logging.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "gutil/hash/city.h"
#include "util/hash_util.hpp"
#include "util/slice.h"
#include "util/sse_util.hpp"
#include "vec/common/unaligned.h"
#include "vec/core/types.h"

namespace doris {

/// unnamed namespace packaging simd-style equality compare functions.
namespace {

#if defined(__SSE2__) || defined(__aarch64__)

/** Compare strings for equality.
  * The approach is controversial and does not win in all cases.
  * For more information, see hash_map_string_2.cpp
  */

inline bool compareSSE2(const char* p1, const char* p2) {
    return 0xFFFF ==
           _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                            _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))));
}

inline bool compareSSE2x4(const char* p1, const char* p2) {
    return 0xFFFF ==
           _mm_movemask_epi8(_mm_and_si128(
                   _mm_and_si128(
                           _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                          _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))),
                           _mm_cmpeq_epi8(
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 1),
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 1))),
                   _mm_and_si128(
                           _mm_cmpeq_epi8(
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 2),
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 2)),
                           _mm_cmpeq_epi8(
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 3),
                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 3)))));
}

inline bool memequalSSE2Wide(const char* p1, const char* p2, size_t size) {
    /** The order of branches and the trick with overlapping comparisons
      * are the same as in memcpy implementation.
      * See the comments in
      * https://github.com/ClickHouse/ClickHouse/blob/master/base/glibc-compatibility/memcpy/memcpy.h
      */

    if (size <= 16) {
        if (size >= 8) {
            /// Chunks of [8,16] bytes.
            return unaligned_load<uint64_t>(p1) == unaligned_load<uint64_t>(p2) &&
                   unaligned_load<uint64_t>(p1 + size - 8) ==
                           unaligned_load<uint64_t>(p2 + size - 8);
        } else if (size >= 4) {
            /// Chunks of [4,7] bytes.
            return unaligned_load<uint32_t>(p1) == unaligned_load<uint32_t>(p2) &&
                   unaligned_load<uint32_t>(p1 + size - 4) ==
                           unaligned_load<uint32_t>(p2 + size - 4);
        } else if (size >= 2) {
            /// Chunks of [2,3] bytes.
            return unaligned_load<uint16_t>(p1) == unaligned_load<uint16_t>(p2) &&
                   unaligned_load<uint16_t>(p1 + size - 2) ==
                           unaligned_load<uint16_t>(p2 + size - 2);
        } else if (size >= 1) {
            /// A single byte.
            return *p1 == *p2;
        }
        return true;
    }

    while (size >= 64) {
        if (compareSSE2x4(p1, p2)) {
            p1 += 64;
            p2 += 64;
            size -= 64;
        } else {
            return false;
        }
    }

    switch (size / 16) {
    case 3:
        if (!compareSSE2(p1 + 32, p2 + 32)) {
            return false;
        }
        [[fallthrough]];
    case 2:
        if (!compareSSE2(p1 + 16, p2 + 16)) {
            return false;
        }
        [[fallthrough]];
    case 1:
        if (!compareSSE2(p1, p2)) {
            return false;
        }
    }

    return compareSSE2(p1 + size - 16, p2 + size - 16);
}

#endif

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
PURE inline int string_compare(const char* s1, int64_t n1, const char* s2, int64_t n2,
                               int64_t len) {
    DCHECK_EQ(len, std::min(n1, n2));
#if defined(__SSE4_2__) || defined(__aarch64__)
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
        if (u1 != u2) {
            return u1 - u2;
        }
    }

    return n1 - n2;
}

} // unnamed namespace

/// The thing to avoid creating strings to find substrings in the hash table.
/// User should make sure data source is const.
/// maybe considering rewrite it with std::span / std::basic_string_view is meaningful.
struct StringRef {
    // FIXME: opening member accessing really damages.
    const char* data = nullptr;
    size_t size = 0;

    StringRef() = default;
    StringRef(const char* data_, size_t size_) : data(data_), size(size_) {}
    StringRef(const unsigned char* data_, size_t size_)
            : StringRef(reinterpret_cast<const char*>(data_), size_) {}

    /// Make this copy constructor explicit to prevent inadvertently constructing a StringRef from a temporary std::string variable.
    explicit StringRef(const std::string& s) : data(s.data()), size(s.size()) {}
    explicit StringRef(const char* str) : data(str), size(strlen(str)) {}

    std::string to_string() const { return std::string(data, size); }
    std::string debug_string() const { return to_string(); }
    std::string_view to_string_view() const { return {data, size}; }
    Slice to_slice() const { return {data, size}; }

    // this is just for show, e.g. print data to error log, to avoid print large string.
    std::string to_prefix(size_t length) const { return std::string(data, std::min(length, size)); }

    explicit operator std::string() const { return to_string(); }
    operator std::string_view() const { return std::string_view {data, size}; }

    StringRef substring(int start_pos, int new_len) const {
        return {data + start_pos, (new_len < 0) ? (size - start_pos) : new_len};
    }

    StringRef substring(int start_pos) const { return substring(start_pos, size - start_pos); }

    const char* begin() const { return data; }
    const char* end() const { return data + size; }
    // there's no border check in functions below. That's same with STL.
    char front() const { return *data; }
    char back() const { return *(data + size - 1); }

    // Trims leading and trailing spaces.
    StringRef trim() const;

    bool empty() const { return size == 0; }

    // support for type_limit
    static constexpr char MIN_CHAR = 0;
    static constexpr char MAX_CHAR = char(
            UCHAR_MAX); // We will convert char to uchar and compare, so we define max_char to unsigned char max.
    static StringRef min_string_val();
    static StringRef max_string_val();

    bool start_with(char) const;
    bool end_with(char) const;
    bool start_with(const StringRef& search_string) const;
    bool end_with(const StringRef& search_string) const;

    // Byte-by-byte comparison. Returns:
    // this < other: -1
    // this == other: 0
    // this > other: 1
    int compare(const StringRef& other) const {
        int l = std::min(size, other.size);

        if (l == 0) {
            if (size == other.size) {
                return 0;
            } else if (size == 0) {
                return -1;
            } else {
                DCHECK_EQ(other.size, 0);
                return 1;
            }
        }

        // string_compare doesn't have sign result
        int cmp_result = string_compare(this->data, this->size, other.data, other.size, l);
        return (cmp_result > 0) - (cmp_result < 0);
    }

    void replace(const char* ptr, int len) {
        this->data = ptr;
        this->size = len;
    }

    // Find the first position char of appear, return -1 if not found
    size_t find_first_of(char c) const;

    // ==
    bool eq(const StringRef& other) const {
        return (size == other.size) && (memcmp(data, other.data, size) == 0);
    }

    bool operator==(const StringRef& other) const { return eq(other); }
    // !=
    bool ne(const StringRef& other) const { return !eq(other); }
    // <=
    bool le(const StringRef& other) const { return compare(other) <= 0; }
    // >=
    bool ge(const StringRef& other) const { return compare(other) >= 0; }
    // <
    bool lt(const StringRef& other) const { return compare(other) < 0; }
    // >
    bool gt(const StringRef& other) const { return compare(other) > 0; }

    bool operator!=(const StringRef& other) const { return ne(other); }

    bool operator<=(const StringRef& other) const { return le(other); }

    bool operator>=(const StringRef& other) const { return ge(other); }

    bool operator<(const StringRef& other) const { return lt(other); }

    bool operator>(const StringRef& other) const { return gt(other); }

    struct Comparator {
        bool operator()(const StringRef& a, const StringRef& b) const { return a.compare(b) < 0; }
    };
}; // class StringRef

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const StringRef& v) {
    return HashUtil::hash(v.data, v.size, 0);
}

using StringRefs = std::vector<StringRef>;

#if defined(__SSE4_2__) || defined(__aarch64__)

/// Parts are taken from CityHash.

inline doris::vectorized::UInt64 hash_len16(doris::vectorized::UInt64 u,
                                            doris::vectorized::UInt64 v) {
    return util_hash::HashLen16(u, v);
}

inline doris::vectorized::UInt64 shift_mix(doris::vectorized::UInt64 val) {
    return val ^ (val >> 47);
}

inline doris::vectorized::UInt64 rotate_by_at_least1(doris::vectorized::UInt64 val, int shift) {
    return (val >> shift) | (val << (64 - shift));
}

inline size_t hash_less_than8(const char* data, size_t size) {
    static constexpr doris::vectorized::UInt64 k2 = 0x9ae16a3b2f90404fULL;
    static constexpr doris::vectorized::UInt64 k3 = 0xc949d7c7509e6557ULL;

    if (size >= 4) {
        doris::vectorized::UInt64 a = unaligned_load<uint32_t>(data);
        return hash_len16(size + (a << 3), unaligned_load<uint32_t>(data + size - 4));
    }

    if (size > 0) {
        uint8_t a = data[0];
        uint8_t b = data[size >> 1];
        uint8_t c = data[size - 1];
        uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
        uint32_t z = size + (static_cast<uint32_t>(c) << 2);
        return shift_mix(y * k2 ^ z * k3) * k2;
    }

    return k2;
}

inline size_t hash_less_than16(const char* data, size_t size) {
    if (size > 8) {
        auto a = unaligned_load<doris::vectorized::UInt64>(data);
        auto b = unaligned_load<doris::vectorized::UInt64>(data + size - 8);
        return hash_len16(a, rotate_by_at_least1(b + size, size)) ^ b;
    }

    return hash_less_than8(data, size);
}

inline size_t crc32_hash(const char* pos, size_t size) {
    if (size == 0) {
        return 0;
    }

    if (size < 8) {
        return hash_less_than8(pos, size);
    }

    const char* end = pos + size;
    size_t res = -1ULL;

    do {
        auto word = unaligned_load<doris::vectorized::UInt64>(pos);
        res = _mm_crc32_u64(res, word);

        pos += 8;
    } while (pos + 8 < end);

    auto word =
            unaligned_load<doris::vectorized::UInt64>(end - 8); /// I'm not sure if this is normal.
    res = _mm_crc32_u64(res, word);

    return res;
}

inline size_t crc32_hash(const std::string str) {
    return crc32_hash(str.data(), str.size());
}

struct CRC32Hash {
    size_t operator()(const StringRef& x) const { return crc32_hash(x.data, x.size); }
};

struct StringRefHash : CRC32Hash {};

#else

struct CRC32Hash {
    size_t operator()(StringRef /* x */) const {
        throw std::logic_error {"Not implemented CRC32Hash without SSE"};
    }
};

struct StringRefHash : StringRefHash64 {};

#endif // end of hash functions

inline std::ostream& operator<<(std::ostream& os, const StringRef& str) {
    return os << str.to_string();
}
} // namespace doris

namespace ZeroTraits {
inline bool check(const doris::StringRef& x) {
    return 0 == x.size;
}
inline void set(doris::StringRef& x) {
    x.size = 0;
}
} // namespace ZeroTraits

template <>
struct std::hash<doris::StringRef> : public doris::StringRefHash {};
