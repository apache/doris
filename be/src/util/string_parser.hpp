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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/string-parser.hpp
// and modified by Doris

#pragma once

#include <fast_float/fast_float.h>
#include <fast_float/parse_number.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdlib>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "vec/common/int_exp.h"
#include "vec/common/string_utils/string_utils.h"
#include "vec/core/extended_types.h"
#include "vec/data_types/number_traits.h"

namespace doris {
namespace vectorized {
template <DecimalNativeTypeConcept T>
struct Decimal;
} // namespace vectorized

// they rely on the template parameter `IS_STRICT`. in strict mode, it will set error code and otherwise it will not.
#ifndef SET_PARAMS_RET_FALSE_IFN
#define SET_PARAMS_RET_FALSE_IFN(stmt, ...)                           \
    do {                                                              \
        if (!(stmt)) [[unlikely]] {                                   \
            if constexpr (IsStrict) {                                 \
                params.status = Status::InvalidArgument(__VA_ARGS__); \
            }                                                         \
            return false;                                             \
        }                                                             \
    } while (false)
#endif

#ifndef SET_PARAMS_RET_FALSE_FROM_EXCEPTION
#define SET_PARAMS_RET_FALSE_FROM_EXCEPTION(stmt) \
    do {                                          \
        try {                                     \
            { stmt; }                             \
        } catch (const doris::Exception& e) {     \
            if constexpr (IsStrict) {             \
                params.status = e.to_status();    \
            }                                     \
            return false;                         \
        }                                         \
    } while (false)
#endif

// skip leading and trailing ascii whitespaces,
// return the pointer to the first non-whitespace char,
// and update the len to the new length, which does not include
// leading and trailing whitespaces
template <typename T>
inline const char* skip_ascii_whitespaces(const char* s, T& len) {
    while (len > 0 && is_whitespace_ascii(*s)) {
        ++s;
        --len;
    }

    while (len > 0 && is_whitespace_ascii(s[len - 1])) {
        --len;
    }

    return s;
}

template <bool (*Pred)(char)>
bool range_suite(const char* s, const char* end) {
    return std::ranges::all_of(s, end, Pred);
}

inline auto is_digit_range = range_suite<is_numeric_ascii>;
inline auto is_space_range = range_suite<is_whitespace_ascii>;

// combine in_bound and range_suite is ok. won't lead to duplicated calculation.
inline bool in_bound(const char* s, const char* end, size_t offset) {
    if (s + offset >= end) [[unlikely]] {
        return false;
    }
    return true;
}

// LEN = 0 means any length(include zero). LEN = 1 means only one character. so on. LEN = -x means x or more.
// if need result, use StringRef{origin_s, s} outside
template <int LEN, bool (*Pred)(char)>
bool skip_qualified_char(const char*& s, const char* end) {
    if constexpr (LEN == 0) {
        // Consume any length of characters that match the predicate.
        while (s != end && Pred(*s)) {
            ++s;
        }
    } else if constexpr (LEN > 0) {
        // Consume exactly LEN characters that match the predicate.
        for (int i = 0; i < LEN; ++i, ++s) {
            if (s == end || !Pred(*s)) [[unlikely]] {
                return false;
            }
        }
    } else { // LEN < 0
        // Consume at least -LEN characters that match the predicate.
        int count = 0;
        while (s != end && Pred(*s)) {
            ++s;
            ++count;
        }
        if (count < -LEN) [[unlikely]] {
            return false;
        }
    }
    return true;
}

inline auto skip_any_whitespace = skip_qualified_char<0, is_whitespace_ascii>;
inline auto skip_any_digit = skip_qualified_char<0, is_numeric_ascii>;
inline auto skip_tz_name_part = skip_qualified_char<-1, is_not_whitespace_ascii>;
inline auto skip_one_slash = skip_qualified_char<1, is_slash_ascii>;
inline auto skip_one_non_alnum = skip_qualified_char<1, is_non_alnum>;

inline bool is_delimiter(char c) {
    return c == ' ' || c == 'T';
}
inline auto consume_one_delimiter = skip_qualified_char<1, is_delimiter>;

inline bool is_bar(char c) {
    return c == '-';
}
inline auto consume_one_bar = skip_qualified_char<1, is_bar>;

inline bool is_colon(char c) {
    return c == ':';
}
inline auto consume_one_colon = skip_qualified_char<1, is_colon>;

// only consume a string of digit, not include sign.
// when has MAX_LEN > 0, do greedy match but at most MAX_LEN.
// LEN = 0 means any length, otherwise(must > 0) it means exactly LEN digits.
template <typename T, int LEN = 0, int MAX_LEN = -1>
bool consume_digit(const char*& s, const char* end, T& out) {
    static_assert(LEN >= 0);
    if constexpr (MAX_LEN > 0) {
        out = 0;
        for (int i = 0; i < MAX_LEN; ++i, ++s) {
            if (s == end || !is_numeric_ascii(*s)) {
                if (i < LEN) [[unlikely]] {
                    return false;
                }
                break; // stop consuming if we have consumed enough digits.
            }
            out = out * 10 + (*s - '0');
        }
    } else if constexpr (LEN == 0) {
        // Consume any length of digits.
        out = 0;
        while (s != end && is_numeric_ascii(*s)) {
            out = out * 10 + (*s - '0');
            ++s;
        }
    } else if constexpr (LEN > 0) {
        // Consume exactly LEN digits.
        out = 0;
        for (int i = 0; i < LEN; ++i, ++s) {
            if (s == end || !is_numeric_ascii(*s)) [[unlikely]] {
                return false;
            }
            out = out * 10 + (*s - '0');
        }
    }
    return true;
}

// specialized version for 2 digits, which is used very often in date/time parsing.
template <>
inline bool consume_digit<uint32_t, 2, -1>(const char*& s, const char* end, uint32_t& out) {
    out = 0;
    if (s == end || s + 1 == end || !is_numeric_ascii(*s) || !is_numeric_ascii(*(s + 1)))
            [[unlikely]] {
        return false;
    }
    out = (s[0] - '0') * 10 + (s[1] - '0');
    s += 2; // consume 2 digits
    return true;
}

// specialized version for 1 or 2 digits, which is used very often in date/time parsing.
template <>
inline bool consume_digit<uint32_t, 1, 2>(const char*& s, const char* end, uint32_t& out) {
    out = 0;
    if (s == end || !is_numeric_ascii(*s)) [[unlikely]] {
        return false;
    } else if (s + 1 != end && is_numeric_ascii(*(s + 1))) {
        // consume 2 digits
        out = (*s - '0') * 10 + (*(s + 1) - '0');
        s += 2;
    } else {
        // consume 1 digit
        out = *s - '0';
        ++s;
    }
    return true;
}

template <bool (*Pred)(char)>
uint32_t count_valid_length(const char* s, const char* end) {
    DCHECK(s <= end) << "s: " << s << ", end: " << end;
    uint32_t count = 0;
    while (s != end && Pred(*s)) {
        ++count;
        ++s;
    }
    return count;
}

inline auto count_digits = count_valid_length<is_numeric_ascii>;

inline PURE std::string combine_tz_offset(char sign, uint32_t hour_offset, uint32_t minute_offset) {
    std::string result(6, '0');
    result[0] = sign;
    result[1] = '0' + (hour_offset / 10);
    result[2] = '0' + (hour_offset % 10);
    result[3] = ':';
    result[4] = '0' + (minute_offset / 10);
    result[5] = '0' + (minute_offset % 10);
    DCHECK_EQ(result.size(), 6);
    return result;
}

// Utility functions for doing atoi/atof on non-null terminated strings.  On micro benchmarks,
// this is significantly faster than libc (atoi/strtol and atof/strtod).
//
// Strings with leading and trailing whitespaces are accepted.
// Branching is heavily optimized for the non-whitespace successful case.
// All the StringTo* functions first parse the input string assuming it has no leading whitespace.
// If that first attempt was unsuccessful, these functions retry the parsing after removing
// whitespace. Therefore, strings with whitespace take a perf hit on branch mis-prediction.
//
// For overflows, we are following the mysql behavior, to cap values at the max/min value for that
// data type.  This is different from hive, which returns NULL for overflow slots for int types
// and inf/-inf for float types.
//
// Things we tried that did not work:
//  - lookup table for converting character to digit
// Improvements (TODO):
//  - Validate input using _simd_compare_ranges
//  - Since we know the length, we can parallelize this: i.e. result = 100*s[0] + 10*s[1] + s[2]
class StringParser {
public:
    enum ParseResult { PARSE_SUCCESS = 0, PARSE_FAILURE, PARSE_OVERFLOW, PARSE_UNDERFLOW };

    template <typename T>
    static T numeric_limits(bool negative) {
        if constexpr (std::is_same_v<T, __int128>) {
            return negative ? MIN_INT128 : MAX_INT128;
        } else {
            return negative ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
        }
    }

    template <typename T>
    static T get_scale_multiplier(int scale) {
        static_assert(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                              std::is_same_v<T, __int128> || std::is_same_v<T, wide::Int256>,
                      "You can only instantiate as int32_t, int64_t, __int128.");
        if constexpr (std::is_same_v<T, int32_t>) {
            return common::exp10_i32(scale);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return common::exp10_i64(scale);
        } else if constexpr (std::is_same_v<T, __int128>) {
            return common::exp10_i128(scale);
        } else if constexpr (std::is_same_v<T, wide::Int256>) {
            return common::exp10_i256(scale);
        }
    }

    // This is considerably faster than glibc's implementation (25x).
    // Assumes s represents a decimal number.
    template <typename T, bool enable_strict_mode = false>
    static inline T string_to_int(const char* __restrict s, size_t len, ParseResult* result) {
        s = skip_ascii_whitespaces(s, len);
        return string_to_int_internal<T, enable_strict_mode>(s, len, result);
    }

    // This is considerably faster than glibc's implementation.
    // In the case of overflow, the max/min value for the data type will be returned.
    // Assumes s represents a decimal number.
    template <typename T>
    static inline T string_to_unsigned_int(const char* __restrict s, int len, ParseResult* result) {
        s = skip_ascii_whitespaces(s, len);
        return string_to_unsigned_int_internal<T>(s, len, result);
    }

    // Convert a string s representing a number in given base into a decimal number.
    template <typename T>
    static inline T string_to_int(const char* __restrict s, int64_t len, int base,
                                  ParseResult* result) {
        s = skip_ascii_whitespaces(s, len);
        return string_to_int_internal<T>(s, len, base, result);
    }

    template <typename T>
    static inline T string_to_float(const char* __restrict s, size_t len, ParseResult* result) {
        s = skip_ascii_whitespaces(s, len);
        return string_to_float_internal<T>(s, len, result);
    }

    // Parses a string for 'true' or 'false', case insensitive.
    static inline bool string_to_bool(const char* __restrict s, size_t len, ParseResult* result) {
        s = skip_ascii_whitespaces(s, len);
        return string_to_bool_internal(s, len, result);
    }

    template <PrimitiveType P>
    static typename PrimitiveTypeTraits<P>::CppType::NativeType string_to_decimal(
            const char* __restrict s, size_t len, int type_precision, int type_scale,
            ParseResult* result);

    template <typename T>
    static Status split_string_to_map(const std::string& base, const T element_separator,
                                      const T key_value_separator,
                                      std::map<std::string, std::string>* result) {
        int key_pos = 0;
        int key_end;
        int val_pos;
        int val_end;

        while ((key_end = base.find(key_value_separator, key_pos)) != std::string::npos) {
            if ((val_pos = base.find_first_not_of(key_value_separator, key_end)) ==
                std::string::npos) {
                break;
            }
            if ((val_end = base.find(element_separator, val_pos)) == std::string::npos) {
                val_end = base.size();
            }
            result->insert(std::make_pair(base.substr(key_pos, key_end - key_pos),
                                          base.substr(val_pos, val_end - val_pos)));
            key_pos = val_end;
            if (key_pos != std::string::npos) {
                ++key_pos;
            }
        }

        return Status::OK();
    }

    // This is considerably faster than glibc's implementation.
    // In the case of overflow, the max/min value for the data type will be returned.
    // Assumes s represents a decimal number.
    // Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
    template <typename T, bool enable_strict_mode = false>
    static inline T string_to_int_internal(const char* __restrict s, int len, ParseResult* result);

    // This is considerably faster than glibc's implementation.
    // In the case of overflow, the max/min value for the data type will be returned.
    // Assumes s represents a decimal number.
    // Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
    template <typename T>
    static inline T string_to_unsigned_int_internal(const char* __restrict s, int len,
                                                    ParseResult* result);

    // Convert a string s representing a number in given base into a decimal number.
    // Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
    template <typename T>
    static inline T string_to_int_internal(const char* __restrict s, int64_t len, int base,
                                           ParseResult* result);

    // Converts an ascii string to an integer of type T assuming it cannot overflow
    // and the number is positive.
    // Leading whitespace is not allowed. Trailing whitespace will be skipped.
    template <typename T, bool enable_strict_mode = false>
    static inline T string_to_int_no_overflow(const char* __restrict s, int len,
                                              ParseResult* result);

    // zero length, or at least one legal digit. at most consume MAX_LEN digits and stop. or stop when next
    // char is not a digit.
    template <typename T>
    static inline T string_to_uint_greedy_no_overflow(const char* __restrict s, int max_len,
                                                      ParseResult* result);

    // This is considerably faster than glibc's implementation (>100x why???)
    // No special case handling needs to be done for overflows, the floating point spec
    // already does it and will cap the values to -inf/inf
    // To avoid inaccurate conversions this function falls back to strtod for
    // scientific notation.
    // Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
    // TODO: Investigate using intrinsics to speed up the slow strtod path.
    template <typename T>
    static inline T string_to_float_internal(const char* __restrict s, int len,
                                             ParseResult* result);

    // parses a string for 'true' or 'false', case insensitive
    // Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
    static inline bool string_to_bool_internal(const char* __restrict s, int len,
                                               ParseResult* result);

    // Returns true if s only contains whitespace.
    static inline bool is_all_whitespace(const char* __restrict s, int len) {
        for (int i = 0; i < len; ++i) {
            if (!LIKELY(is_whitespace_ascii(s[i]))) {
                return false;
            }
        }
        return true;
    }

    // For strings like "3.0", "3.123", and "3.", can parse them as 3.
    static inline bool is_float_suffix(const char* __restrict s, int len) {
        return (s[0] == '.' && is_all_digit(s + 1, len - 1));
    }

    static inline bool is_all_digit(const char* __restrict s, int len) {
        for (int i = 0; i < len; ++i) {
            if (!LIKELY(s[i] >= '0' && s[i] <= '9')) {
                return false;
            }
        }
        return true;
    }
}; // end of class StringParser

template <typename T, bool enable_strict_mode>
T StringParser::string_to_int_internal(const char* __restrict s, int len, ParseResult* result) {
    if (UNLIKELY(len <= 0)) {
        *result = PARSE_FAILURE;
        return 0;
    }

    using UnsignedT = MakeUnsignedT<T>;
    UnsignedT val = 0;
    UnsignedT max_val = StringParser::numeric_limits<T>(false);
    bool negative = false;
    int i = 0;
    switch (*s) {
    case '-':
        negative = true;
        max_val += 1;
        [[fallthrough]];
    case '+':
        ++i;
        // only one '+'/'-' char, so could return failure directly
        if (UNLIKELY(len == 1)) {
            *result = PARSE_FAILURE;
            return 0;
        }
    }

    // This is the fast path where the string cannot overflow.
    if (LIKELY(len - i < vectorized::NumberTraits::max_ascii_len<T>())) {
        val = string_to_int_no_overflow<UnsignedT, enable_strict_mode>(s + i, len - i, result);
        return static_cast<T>(negative ? -val : val);
    }

    const T max_div_10 = max_val / 10;
    const T max_mod_10 = max_val % 10;

    int first = i;
    for (; i < len; ++i) {
        if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
            T digit = s[i] - '0';
            // This is a tricky check to see if adding this digit will cause an overflow.
            if (UNLIKELY(val > (max_div_10 - (digit > max_mod_10)))) {
                *result = PARSE_OVERFLOW;
                return negative ? -max_val : max_val;
            }
            val = val * 10 + digit;
        } else {
            if constexpr (enable_strict_mode) {
                if ((UNLIKELY(i == first || !is_all_whitespace(s + i, len - i)))) {
                    // Reject the string because the remaining chars are not all whitespace
                    *result = PARSE_FAILURE;
                    return 0;
                }
            } else {
                if ((UNLIKELY(i == first || (!is_all_whitespace(s + i, len - i) &&
                                             !is_float_suffix(s + i, len - i))))) {
                    // Reject the string because either the first char was not a digit,
                    // or the remaining chars are not all whitespace
                    *result = PARSE_FAILURE;
                    return 0;
                }
            }
            // Returning here is slightly faster than breaking the loop.
            *result = PARSE_SUCCESS;
            return static_cast<T>(negative ? -val : val);
        }
    }
    *result = PARSE_SUCCESS;
    return static_cast<T>(negative ? -val : val);
}

template <typename T>
T StringParser::string_to_unsigned_int_internal(const char* __restrict s, int len,
                                                ParseResult* result) {
    if (UNLIKELY(len <= 0)) {
        *result = PARSE_FAILURE;
        return 0;
    }

    T val = 0;
    T max_val = std::numeric_limits<T>::max();
    int i = 0;

    using signedT = MakeSignedT<T>;
    // This is the fast path where the string cannot overflow.
    if (LIKELY(len - i < vectorized::NumberTraits::max_ascii_len<signedT>())) {
        val = string_to_int_no_overflow<T>(s + i, len - i, result);
        return val;
    }

    const T max_div_10 = max_val / 10;
    const T max_mod_10 = max_val % 10;

    int first = i;
    for (; i < len; ++i) {
        if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
            T digit = s[i] - '0';
            // This is a tricky check to see if adding this digit will cause an overflow.
            if (UNLIKELY(val > (max_div_10 - (digit > max_mod_10)))) {
                *result = PARSE_OVERFLOW;
                return max_val;
            }
            val = val * 10 + digit;
        } else {
            if ((UNLIKELY(i == first || !is_all_whitespace(s + i, len - i)))) {
                // Reject the string because either the first char was not a digit,
                // or the remaining chars are not all whitespace
                *result = PARSE_FAILURE;
                return 0;
            }
            // Returning here is slightly faster than breaking the loop.
            *result = PARSE_SUCCESS;
            return val;
        }
    }
    *result = PARSE_SUCCESS;
    return val;
}

template <typename T>
T StringParser::string_to_int_internal(const char* __restrict s, int64_t len, int base,
                                       ParseResult* result) {
    using UnsignedT = MakeUnsignedT<T>;
    UnsignedT val = 0;
    UnsignedT max_val = StringParser::numeric_limits<T>(false);
    bool negative = false;
    if (UNLIKELY(len <= 0)) {
        *result = PARSE_FAILURE;
        return 0;
    }
    int i = 0;
    switch (*s) {
    case '-':
        negative = true;
        max_val = StringParser::numeric_limits<T>(false) + 1;
        [[fallthrough]];
    case '+':
        i = 1;
    }

    const T max_div_base = max_val / base;
    const T max_mod_base = max_val % base;

    int first = i;
    for (; i < len; ++i) {
        T digit;
        if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
            digit = s[i] - '0';
        } else if (s[i] >= 'a' && s[i] <= 'z') {
            digit = (s[i] - 'a' + 10);
        } else if (s[i] >= 'A' && s[i] <= 'Z') {
            digit = (s[i] - 'A' + 10);
        } else {
            if ((UNLIKELY(i == first || !is_all_whitespace(s + i, len - i)))) {
                // Reject the string because either the first char was not an alpha/digit,
                // or the remaining chars are not all whitespace
                *result = PARSE_FAILURE;
                return 0;
            }
            // skip trailing whitespace.
            break;
        }

        // Bail, if we encounter a digit that is not available in base.
        if (digit >= base) {
            break;
        }

        // This is a tricky check to see if adding this digit will cause an overflow.
        if (UNLIKELY(val > (max_div_base - (digit > max_mod_base)))) {
            *result = PARSE_OVERFLOW;
            return static_cast<T>(negative ? -max_val : max_val);
        }
        val = val * base + digit;
    }
    *result = PARSE_SUCCESS;
    return static_cast<T>(negative ? -val : val);
}

template <typename T, bool enable_strict_mode>
T StringParser::string_to_int_no_overflow(const char* __restrict s, int len, ParseResult* result) {
    T val = 0;
    if (UNLIKELY(len == 0)) {
        *result = PARSE_SUCCESS;
        return val;
    }
    // Factor out the first char for error handling speeds up the loop.
    if (LIKELY(s[0] >= '0' && s[0] <= '9')) {
        val = s[0] - '0';
    } else {
        *result = PARSE_FAILURE;
        return 0;
    }
    for (int i = 1; i < len; ++i) {
        if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
            T digit = s[i] - '0';
            val = val * 10 + digit;
        } else {
            if constexpr (enable_strict_mode) {
                if (UNLIKELY(!is_all_whitespace(s + i, len - i))) {
                    *result = PARSE_FAILURE;
                    return 0;
                }
            } else {
                if ((UNLIKELY(!is_all_whitespace(s + i, len - i) &&
                              !is_float_suffix(s + i, len - i)))) {
                    *result = PARSE_FAILURE;
                    return 0;
                }
            }
            *result = PARSE_SUCCESS;
            return val;
        }
    }
    *result = PARSE_SUCCESS;
    return val;
}

// at least the first char(if any) must be a digit.
template <typename T>
T StringParser::string_to_uint_greedy_no_overflow(const char* __restrict s, int max_len,
                                                  ParseResult* result) {
    T val = 0;
    if (max_len == 0) [[unlikely]] {
        *result = PARSE_SUCCESS;
        return val;
    }
    // Factor out the first char for error handling speeds up the loop.
    if (is_numeric_ascii(s[0])) [[likely]] {
        val = s[0] - '0';
    } else {
        *result = PARSE_FAILURE;
        return 0;
    }
    for (int i = 1; i < max_len; ++i) {
        if (is_numeric_ascii(s[i])) [[likely]] {
            T digit = s[i] - '0';
            val = val * 10 + digit;
        } else {
            // 123abc, return 123
            *result = PARSE_SUCCESS;
            return val;
        }
    }
    *result = PARSE_SUCCESS;
    return val;
}

template <typename T>
T StringParser::string_to_float_internal(const char* __restrict s, int len, ParseResult* result) {
    int i = 0;
    // skip leading spaces
    for (; i < len; ++i) {
        if (!is_whitespace_ascii(s[i])) {
            break;
        }
    }

    // skip back spaces
    int j = len - 1;
    for (; j >= i; j--) {
        if (!is_whitespace_ascii(s[j])) {
            break;
        }
    }

    // skip leading '+', from_chars can handle '-'
    if (i < len && s[i] == '+') {
        i++;
        // ++ or +- are not valid, but the first + is already skipped,
        // if don't check here, from_chars will succeed.
        //
        // New version of fast_float supports a new flag called 'chars_format::allow_leading_plus'
        // which may avoid this extra check here.
        // e.g.:
        // fast_float::chars_format format =
        //         fast_float::chars_format::general | fast_float::chars_format::allow_leading_plus;
        // auto res = fast_float::from_chars(s + i, s + j + 1, val, format);
        if (i < len && (s[i] == '+' || s[i] == '-')) {
            *result = PARSE_FAILURE;
            return 0;
        }
    }
    if (UNLIKELY(i > j)) {
        *result = PARSE_FAILURE;
        return 0;
    }

    // Use double here to not lose precision while accumulating the result
    double val = 0;
    auto res = fast_float::from_chars(s + i, s + j + 1, val);

    if (res.ptr == s + j + 1) {
        *result = PARSE_SUCCESS;
        return val;
    } else {
        *result = PARSE_FAILURE;
    }
    return 0;
}

inline bool StringParser::string_to_bool_internal(const char* __restrict s, int len,
                                                  ParseResult* result) {
    *result = PARSE_SUCCESS;

    if (len == 1) {
        if (s[0] == '1' || s[0] == 't' || s[0] == 'T') {
            return true;
        }
        if (s[0] == '0' || s[0] == 'f' || s[0] == 'F') {
            return false;
        }
        *result = PARSE_FAILURE;
        return false;
    }

    if (len == 2) {
        if ((s[0] == 'o' || s[0] == 'O') && (s[1] == 'n' || s[1] == 'N')) {
            return true;
        }
        if ((s[0] == 'n' || s[0] == 'N') && (s[1] == 'o' || s[1] == 'O')) {
            return false;
        }
    }

    if (len == 3) {
        if ((s[0] == 'y' || s[0] == 'Y') && (s[1] == 'e' || s[1] == 'E') &&
            (s[2] == 's' || s[2] == 'S')) {
            return true;
        }
        if ((s[0] == 'o' || s[0] == 'O') && (s[1] == 'f' || s[1] == 'F') &&
            (s[2] == 'f' || s[2] == 'F')) {
            return false;
        }
    }

    if (len == 4 && (s[0] == 't' || s[0] == 'T') && (s[1] == 'r' || s[1] == 'R') &&
        (s[2] == 'u' || s[2] == 'U') && (s[3] == 'e' || s[3] == 'E')) {
        return true;
    }

    if (len == 5 && (s[0] == 'f' || s[0] == 'F') && (s[1] == 'a' || s[1] == 'A') &&
        (s[2] == 'l' || s[2] == 'L') && (s[3] == 's' || s[3] == 'S') &&
        (s[4] == 'e' || s[4] == 'E')) {
        return false;
    }

    // No valid boolean value found
    *result = PARSE_FAILURE;
    return false;
}

} // end namespace doris
