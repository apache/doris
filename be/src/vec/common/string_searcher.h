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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/StringSearcher.h
// and modified by Doris

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "util/sse_util.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/string_utils/string_utils.h"

namespace doris {

// namespace ErrorCodes
// {
//     extern const int BAD_ARGUMENTS;
// }

/** Variants for searching a substring in a string.
  */

class StringSearcherBase {
public:
    bool force_fallback = false;
#if defined(__SSE2__) || defined(__aarch64__)
protected:
    static constexpr auto n = sizeof(__m128i);
    const int page_size = sysconf(_SC_PAGESIZE); //::getPageSize();

    bool page_safe(const void* const ptr) const {
        return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - n;
    }
#endif
};

/// Performs case-sensitive and case-insensitive search of UTF-8 strings
template <bool CaseSensitive, bool ASCII>
class StringSearcher;

/// Case-sensitive searcher (both ASCII and UTF-8)
template <bool ASCII>
class StringSearcher<true, ASCII> : public StringSearcherBase {
private:
    /// string to be searched for
    const uint8_t* const needle;
    const uint8_t* const needle_end;
    /// first character in `needle`
    uint8_t first {};

#if defined(__SSE4_1__) || defined(__aarch64__)
    uint8_t second {};
    /// vector filled `first` or `second` for determining leftmost position of the first and second symbols
    __m128i first_pattern;
    __m128i second_pattern;
    /// vector of first 16 characters of `needle`
    __m128i cache = _mm_setzero_si128();
    int cachemask {};
#endif

public:
    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    StringSearcher(const CharT* needle_, const size_t needle_size)
            : needle {reinterpret_cast<const uint8_t*>(needle_)},
              needle_end {needle + needle_size} {
        if (0 == needle_size) return;

        first = *needle;

#if defined(__SSE4_1__) || defined(__aarch64__)
        first_pattern = _mm_set1_epi8(first);
        if (needle + 1 < needle_end) {
            second = *(needle + 1);
            second_pattern = _mm_set1_epi8(second);
        }
        const auto* needle_pos = needle;

        //for (const auto i : collections::range(0, n))
        for (size_t i = 0; i < n; i++) {
            cache = _mm_srli_si128(cache, 1);

            if (needle_pos != needle_end) {
                cache = _mm_insert_epi8(cache, *needle_pos, n - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, size_t haystack_size) const {
        // cast to unsigned int8 to be consitent with needle type
        // ensure unsigned type compare
        return reinterpret_cast<const CharT*>(
                _search(reinterpret_cast<const uint8_t*>(haystack), haystack_size));
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const CharT* haystack_end) const {
        // cast to unsigned int8 to be consitent with needle type
        // ensure unsigned type compare
        return reinterpret_cast<const CharT*>(
                _search(reinterpret_cast<const uint8_t*>(haystack),
                        reinterpret_cast<const uint8_t*>(haystack_end)));
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compare(const CharT* haystack, const CharT* haystack_end, CharT* pos) const {
        // cast to unsigned int8 to be consitent with needle type
        // ensure unsigned type compare
        return _compare(reinterpret_cast<const uint8_t*>(haystack),
                        reinterpret_cast<const uint8_t*>(haystack_end),
                        reinterpret_cast<const uint8_t*>(pos));
    }

private:
    ALWAYS_INLINE bool _compare(uint8_t* /*haystack*/, uint8_t* /*haystack_end*/,
                                uint8_t* pos) const {
#if defined(__SSE4_1__) || defined(__aarch64__)
        if (needle_end - needle > n && page_safe(pos)) {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
            const auto v_against_cache = _mm_cmpeq_epi8(v_haystack, cache);
            const auto mask = _mm_movemask_epi8(v_against_cache);

            if (0xffff == cachemask) {
                if (mask == cachemask) {
                    pos += n;
                    const auto* needle_pos = needle + n;

                    while (needle_pos < needle_end && *pos == *needle_pos) ++pos, ++needle_pos;

                    if (needle_pos == needle_end) return true;
                }
            } else if ((mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == first) {
            ++pos;
            const auto* needle_pos = needle + 1;

            while (needle_pos < needle_end && *pos == *needle_pos) ++pos, ++needle_pos;

            if (needle_pos == needle_end) return true;
        }

        return false;
    }

    const uint8_t* _search(const uint8_t* haystack, const uint8_t* haystack_end) const {
        if (needle == needle_end) return haystack;

        const auto needle_size = needle_end - needle;
#if defined(__SSE4_1__) || defined(__aarch64__)
        /// Here is the quick path when needle_size is 1.
        if (needle_size == 1) {
            while (haystack < haystack_end) {
                if (haystack + n <= haystack_end && page_safe(haystack)) {
                    const auto v_haystack =
                            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                    const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, first_pattern);
                    const auto mask = _mm_movemask_epi8(v_against_pattern);
                    if (mask == 0) {
                        haystack += n;
                        continue;
                    }

                    const auto offset = __builtin_ctz(mask);
                    haystack += offset;

                    return haystack;
                }

                if (haystack == haystack_end) {
                    return haystack_end;
                }

                if (*haystack == first) {
                    return haystack;
                }
                ++haystack;
            }
            return haystack_end;
        }
#endif

        while (haystack < haystack_end && haystack_end - haystack >= needle_size) {
#if defined(__SSE4_1__) || defined(__aarch64__)
            if ((haystack + 1 + n) <= haystack_end && page_safe(haystack)) {
                /// find first and second characters
                const auto v_haystack_block_first =
                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                const auto v_haystack_block_second =
                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack + 1));

                const auto v_against_pattern_first =
                        _mm_cmpeq_epi8(v_haystack_block_first, first_pattern);
                const auto v_against_pattern_second =
                        _mm_cmpeq_epi8(v_haystack_block_second, second_pattern);

                const auto mask = _mm_movemask_epi8(
                        _mm_and_si128(v_against_pattern_first, v_against_pattern_second));
                /// first and second characters not present in 16 octets starting at `haystack`
                if (mask == 0) {
                    haystack += n;
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack + n <= haystack_end && page_safe(haystack)) {
                    /// check for first 16 octets
                    const auto v_haystack_offset =
                            _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                    const auto v_against_cache = _mm_cmpeq_epi8(v_haystack_offset, cache);
                    const auto mask_offset = _mm_movemask_epi8(v_against_cache);

                    if (0xffff == cachemask) {
                        if (mask_offset == cachemask) {
                            const auto* haystack_pos = haystack + n;
                            const auto* needle_pos = needle + n;

                            while (haystack_pos < haystack_end && needle_pos < needle_end &&
                                   *haystack_pos == *needle_pos)
                                ++haystack_pos, ++needle_pos;

                            if (needle_pos == needle_end) return haystack;
                        }
                    } else if ((mask_offset & cachemask) == cachemask)
                        return haystack;

                    ++haystack;
                    continue;
                }
            }
#endif

            if (haystack == haystack_end) return haystack_end;

            if (*haystack == first) {
                const auto* haystack_pos = haystack + 1;
                const auto* needle_pos = needle + 1;

                while (haystack_pos < haystack_end && needle_pos < needle_end &&
                       *haystack_pos == *needle_pos)
                    ++haystack_pos, ++needle_pos;

                if (needle_pos == needle_end) return haystack;
            }

            ++haystack;
        }

        return haystack_end;
    }

    const uint8_t* _search(const uint8_t* haystack, const size_t haystack_size) const {
        return _search(haystack, haystack + haystack_size);
    }
};

// Searches for needle surrounded by token-separators.
// Separators are anything inside ASCII (0-128) and not alphanum.
// Any value outside of basic ASCII (>=128) is considered a non-separator symbol, hence UTF-8 strings
// should work just fine. But any Unicode whitespace is not considered a token separtor.
template <typename StringSearcher>
class TokenSearcher : public StringSearcherBase {
    StringSearcher searcher;
    size_t needle_size;

public:
    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    TokenSearcher(const CharT* needle_, const size_t needle_size_)
            : searcher {needle_, needle_size_}, needle_size(needle_size_) {
        if (std::any_of(needle_, needle_ + needle_size_, isTokenSeparator)) {
            //throw Exception{"Needle must not contain whitespace or separator characters", ErrorCodes::BAD_ARGUMENTS};
        }
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compare(const CharT* haystack, const CharT* haystack_end,
                               const CharT* pos) const {
        // use searcher only if pos is in the beginning of token and pos + searcher.needle_size is end of token.
        if (isToken(haystack, haystack_end, pos))
            return searcher.compare(haystack, haystack_end, pos);

        return false;
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const CharT* const haystack_end) const {
        // use searcher.search(), then verify that returned value is a token
        // if it is not, skip it and re-run

        const auto* pos = haystack;
        while (pos < haystack_end) {
            pos = searcher.search(pos, haystack_end);
            if (pos == haystack_end || isToken(haystack, haystack_end, pos)) return pos;

            // assuming that heendle does not contain any token separators.
            pos += needle_size;
        }
        return haystack_end;
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const size_t haystack_size) const {
        return search(haystack, haystack + haystack_size);
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool isToken(const CharT* haystack, const CharT* const haystack_end,
                               const CharT* p) const {
        return (p == haystack || isTokenSeparator(*(p - 1))) &&
               (p + needle_size >= haystack_end || isTokenSeparator(*(p + needle_size)));
    }

    ALWAYS_INLINE static bool isTokenSeparator(const uint8_t c) {
        return !(is_alpha_numeric_ascii(c) || !is_ascii(c));
    }
};

using ASCIICaseSensitiveStringSearcher = StringSearcher<true, true>;
// using ASCIICaseInsensitiveStringSearcher = StringSearcher<false, true>;
using UTF8CaseSensitiveStringSearcher = StringSearcher<true, false>;
// using UTF8CaseInsensitiveStringSearcher = StringSearcher<false, false>;
using ASCIICaseSensitiveTokenSearcher = TokenSearcher<ASCIICaseSensitiveStringSearcher>;
// using ASCIICaseInsensitiveTokenSearcher = TokenSearcher<ASCIICaseInsensitiveStringSearcher>;

/** Uses functions from libc.
  * It makes sense to use only with short haystacks when cheap initialization is required.
  * There is no option for case-insensitive search for UTF-8 strings.
  * It is required that strings are zero-terminated.
  */

struct LibCASCIICaseSensitiveStringSearcher : public StringSearcherBase {
    const char* const needle;

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    LibCASCIICaseSensitiveStringSearcher(const CharT* const needle_, const size_t /* needle_size */)
            : needle(reinterpret_cast<const char*>(needle_)) {}

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const CharT* const haystack_end) const {
        const auto* res = strstr(reinterpret_cast<const char*>(haystack),
                                 reinterpret_cast<const char*>(needle));
        if (!res) return haystack_end;
        return reinterpret_cast<const CharT*>(res);
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const size_t haystack_size) const {
        return search(haystack, haystack + haystack_size);
    }
};

struct LibCASCIICaseInsensitiveStringSearcher : public StringSearcherBase {
    const char* const needle;

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    LibCASCIICaseInsensitiveStringSearcher(const CharT* const needle_,
                                           const size_t /* needle_size */)
            : needle(reinterpret_cast<const char*>(needle_)) {}

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const CharT* const haystack_end) const {
        const auto* res = strcasestr(reinterpret_cast<const char*>(haystack),
                                     reinterpret_cast<const char*>(needle));
        if (!res) return haystack_end;
        return reinterpret_cast<const CharT*>(res);
    }

    template <typename CharT>
    // requires (sizeof(CharT) == 1)
    const CharT* search(const CharT* haystack, const size_t haystack_size) const {
        return search(haystack, haystack + haystack_size);
    }
};
} // namespace doris
