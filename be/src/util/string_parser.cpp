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

#include "util/string_parser.hpp"

#include <limits>

#include "vec/core/extended_types.h"
#include "vec/core/types.h"
namespace doris {
#include "common/compile_check_avoid_begin.h"
// Supported decimal number format:
// <decimal> ::= <whitespace>* <value> <whitespace>*
//
// <whitespace> ::= " " | "\t" | "\n" | "\r" | "\f" | "\v"
//
// <value> ::= <sign>? <significand> <exponent>?
//
// <sign> ::= "+" | "-"
//
// <significand> ::= <digits> "." <digits> | <digits> | <digits> "." | "." <digits>
//
// <digits> ::= <digit>+
//
// <digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
//
// <exponent> ::= <e_marker> <sign>? <digits>
//
// <e_marker> ::= "e" | "E"
//
// Parsing algorithm:
// 1. Trim spaces and the sign, then normalize the significand by skipping leading zeros and an
//    optional leading dot. During this scan, count digits that belong to the original integral
//    part (`int_part_count`) and remember where the significand ends (`end_digit_index`).
// 2. Parse the optional exponent. Scientific notation is handled by moving the decimal point:
//    `result_int_part_digit_count = int_part_count + exponent`. For example, "12.34e-1" has
//    int_part_count=2 and exponent=-1, so the result has one integral digit: "1.234".
// 3. Build the result in scaled-integer form: first collect the integral digits up to the shifted
//    decimal point, then collect up to `type_scale` fractional digits, padding with zeros when the
//    input has fewer fractional digits than the target scale.
// 4. If there are extra fractional digits, round half up using the first discarded digit. Finally,
//    check the integral digit count against `type_precision - type_scale` and return the signed
//    scaled integer value.
template <PrimitiveType P>
typename PrimitiveTypeTraits<P>::CppType::NativeType StringParser::string_to_decimal(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result) {
    using T = typename PrimitiveTypeTraits<P>::CppType::NativeType;
    static_assert(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                          std::is_same_v<T, __int128> || std::is_same_v<T, wide::Int256>,
                  "Cast string to decimal only support target type int32_t, int64_t, __int128 or "
                  "wide::Int256.");

    // Parse in two logical coordinate systems:
    // 1. `s[0, end_digit_index)` is the normalized significand after trimming spaces, sign and
    //    leading zeros. If the original value starts with '.', the dot is also skipped so
    //    ".14E+3" is parsed as significand "14" with exponent 3.
    // 2. `result_int_part_digit_count = int_part_count + exponent` is the decimal point position
    //    after applying scientific notation. For example, "1.4E+2" has int_part_count=1,
    //    exponent=2, result_int_part_digit_count=3, so "14" becomes integer 140.
    // `digit_index` always indexes the normalized significand string, which may still contain a
    // dot for inputs like "1.4E+2"; loops that build numbers skip that dot explicitly.
    // Ignore leading and trailing spaces.
    s = skip_ascii_whitespaces(s, len);

    bool is_negative = false;
    if (len > 0) {
        switch (*s) {
        case '-':
            is_negative = true;
            [[fallthrough]];
        case '+':
            ++s;
            --len;
        }
    }
    // Ignore leading zeros.
    bool found_value = false;
    while (len > 0 && UNLIKELY(*s == '0')) {
        found_value = true;
        ++s;
        --len;
    }

    int found_dot = 0;
    if (len > 0 && *s == '.') {
        found_dot = 1;
        ++s;
        --len;
    }
    int int_part_count = 0;
    int i = 0;
    for (; i != len; ++i) {
        const char& c = s[i];
        if (LIKELY('0' <= c && c <= '9')) {
            found_value = true;
            if (!found_dot) {
                ++int_part_count;
            }
        } else if (c == '.') {
            if (found_dot) {
                *result = StringParser::PARSE_FAILURE;
                return 0;
            }
            found_dot = 1;
        } else {
            break;
        }
    }
    if (!found_value) {
        // '', '.'
        *result = StringParser::PARSE_FAILURE;
        return 0;
    }
    // Parse exponent if any. Keep `end_digit_index` before consuming 'e/E' so later digit counts
    // ignore exponent syntax. For "1.4E+2", end_digit_index points just after "1.4", not after
    // "E+2".
    int64_t exponent = 0;
    auto end_digit_index = i;
    if (i != len) {
        bool negative_exponent = false;
        if (s[i] == 'e' || s[i] == 'E') {
            ++i;
            if (i != len) {
                switch (s[i]) {
                case '-':
                    negative_exponent = true;
                    [[fallthrough]];
                case '+':
                    ++i;
                }
            }
            if (i == len) {
                // '123e', '123e+', '123e-'
                *result = StringParser::PARSE_FAILURE;
                return 0;
            }
            for (; i != len; ++i) {
                const char& c = s[i];
                if (LIKELY('0' <= c && c <= '9')) {
                    exponent = exponent * 10 + (c - '0');
                    // max string len is config::string_type_length_soft_limit_bytes,
                    // whose max value is std::numeric_limits<int32_t>::max() - 4,
                    // just check overflow of int32_t to simplify the logic
                    // For edge cases like 0.{2147483647 zeros}e+2147483647
                    if (exponent > std::numeric_limits<int32_t>::max()) {
                        *result = StringParser::PARSE_OVERFLOW;
                        return 0;
                    }
                } else {
                    // '123e12abc', '123e1.2'
                    *result = StringParser::PARSE_FAILURE;
                    return 0;
                }
            }
            if (negative_exponent) {
                exponent = -exponent;
            }
        } else {
            *result = StringParser::PARSE_FAILURE;
            return 0;
        }
    }
    // TODO: check limit values of exponent and add UT
    // max string len is config::string_type_length_soft_limit_bytes,
    // whose max value is std::numeric_limits<int32_t>::max() - 4,
    // so int_part_count will be in range of int32_t,
    // and int_part_count + exponent will be in range of int64_t
    int64_t tmp_result_int_part_digit_count = int_part_count + exponent;
    if (tmp_result_int_part_digit_count > std::numeric_limits<int>::max() ||
        tmp_result_int_part_digit_count < std::numeric_limits<int>::min()) {
        *result = is_negative ? StringParser::PARSE_UNDERFLOW : StringParser::PARSE_OVERFLOW;
        return 0;
    }
    int result_int_part_digit_count = tmp_result_int_part_digit_count;
    T int_part_number = 0;
    T frac_part_number = 0;
    int actual_frac_part_count = 0;
    int digit_index = 0;
    if (result_int_part_digit_count >= 0) {
        // `max_index` is the raw significand index where integer-part digits stop. Add one extra
        // raw character only when crossing an in-buffer dot, e.g. "1.4E+2" must scan "1.4" to
        // collect three integer digits after the exponent shift. It is capped by end_digit_index
        // because missing digits are appended later by multiplying with powers of 10.
        int max_index = std::min(found_dot ? (result_int_part_digit_count +
                                              ((int_part_count > 0 && exponent > 0) ? 1 : 0))
                                           : result_int_part_digit_count,
                                 end_digit_index);
        max_index = (max_index == std::numeric_limits<int>::min() ? end_digit_index : max_index);
        // skip zero number
        for (; digit_index != max_index && s[digit_index] == '0'; ++digit_index) {
        }
        // test 0.00, .00, 0.{00...}e2147483647
        // 0.00000e2147483647
        if (digit_index != max_index &&
            (result_int_part_digit_count - digit_index > type_precision - type_scale)) {
            *result = is_negative ? StringParser::PARSE_UNDERFLOW : StringParser::PARSE_OVERFLOW;
            return 0;
        }
        // get int part number
        for (; digit_index != max_index; ++digit_index) {
            if (UNLIKELY(s[digit_index] == '.')) {
                continue;
            }
            int_part_number = int_part_number * 10 + (s[digit_index] - '0');
        }
        // Count only significand digits, not exponent syntax. If the exponent moves the decimal
        // point past all available significant digits, append zeros by scaling the integer part:
        // "1.4E+2" scans integer 14, total_significant_digit_count=2, then multiplies by 10.
        auto total_significant_digit_count =
                end_digit_index - ((found_dot && int_part_count > 0) ? 1 : 0);
        if (result_int_part_digit_count > total_significant_digit_count) {
            int_part_number *= get_scale_multiplier<T>(result_int_part_digit_count -
                                                       total_significant_digit_count);
        }
    } else {
        // leading zeros of fraction part
        actual_frac_part_count = -result_int_part_digit_count;
    }
    // get fraction part number
    for (; digit_index != end_digit_index && actual_frac_part_count < type_scale; ++digit_index) {
        if (UNLIKELY(s[digit_index] == '.')) {
            continue;
        }
        frac_part_number = frac_part_number * 10 + (s[digit_index] - '0');
        ++actual_frac_part_count;
    }
    auto type_scale_multiplier = get_scale_multiplier<T>(type_scale);
    // Round only when the next parsed significand digit is exactly the first discarded fractional
    // digit. If `actual_frac_part_count` is already greater than type_scale, the missing positions
    // are implicit zeros from a negative exponent, so "5e-17" to scale 15 must stay 0 instead of
    // rounding up.
    if (actual_frac_part_count == type_scale && digit_index != end_digit_index) {
        if (UNLIKELY(s[digit_index] == '.')) {
            ++digit_index;
        }
        if (digit_index != end_digit_index) {
            // example: test 1.5 -> decimal(1, 0)
            if (s[digit_index] >= '5') {
                ++frac_part_number;
                if (frac_part_number == type_scale_multiplier) {
                    frac_part_number = 0;
                    ++int_part_number;
                }
            }
        }
    } else {
        if (actual_frac_part_count < type_scale) {
            frac_part_number *= get_scale_multiplier<T>(type_scale - actual_frac_part_count);
        }
    }
    if (int_part_number >= get_scale_multiplier<T>(type_precision - type_scale)) {
        *result = is_negative ? StringParser::PARSE_UNDERFLOW : StringParser::PARSE_OVERFLOW;
        return 0;
    }

    T value = int_part_number * type_scale_multiplier + frac_part_number;
    *result = StringParser::PARSE_SUCCESS;
    return is_negative ? T(-value) : T(value);
}

template vectorized::Int32 StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMAL32>(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result);
template vectorized::Int64 StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMAL64>(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result);
template vectorized::Int128 StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMAL128I>(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result);
template vectorized::Int128 StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMALV2>(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result);
template wide::Int256 StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMAL256>(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result);
} // end namespace doris
#include "common/compile_check_avoid_end.h"