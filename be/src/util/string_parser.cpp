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
template <PrimitiveType P>
typename PrimitiveTypeTraits<P>::CppType::NativeType StringParser::string_to_decimal(
        const char* __restrict s, size_t len, int type_precision, int type_scale,
        ParseResult* result) {
    using T = typename PrimitiveTypeTraits<P>::CppType::NativeType;
    static_assert(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                          std::is_same_v<T, __int128> || std::is_same_v<T, wide::Int256>,
                  "Cast string to decimal only support target type int32_t, int64_t, __int128 or "
                  "wide::Int256.");
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
    std::vector<unsigned char> digits;
    if (len > 0) {
        digits.resize(len);
    }
    int total_digit_count = 0;
    int i = 0;
    for (; i != len; ++i) {
        const char& c = s[i];
        if (LIKELY('0' <= c && c <= '9')) {
            found_value = true;
            digits[total_digit_count++] = c - '0';
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
    // parse exponent if any
    int64_t exponent = 0;
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
    T int_part_number = 0;
    T frac_part_number = 0;
    // TODO: check limit values of exponent and add UT
    // max string len is config::string_type_length_soft_limit_bytes,
    // whose max value is std::numeric_limits<int32_t>::max() - 4,
    // so int_part_count will be in range of int32_t,
    // and int_part_count + exponent will be in range of int64_t
    int64_t tmp_actual_int_part_count = int_part_count + exponent;
    if (tmp_actual_int_part_count > std::numeric_limits<int>::max() ||
        tmp_actual_int_part_count < std::numeric_limits<int>::min()) {
        *result = StringParser::PARSE_OVERFLOW;
        return 0;
    }
    int actual_int_part_count = tmp_actual_int_part_count;
    int actual_frac_part_count = 0;
    int digit_index = 0;
    if (actual_int_part_count >= 0) {
        int max_index = std::min(actual_int_part_count, total_digit_count);
        // skip zero number
        for (; digit_index != max_index && digits[digit_index] == 0; ++digit_index) {
        }
        // test 0.00, .00, 0.{00...}e2147483647
        // 0.00000e2147483647
        if (max_index - digit_index > type_precision - type_scale) {
            *result = is_negative ? StringParser::PARSE_UNDERFLOW : StringParser::PARSE_OVERFLOW;
            return 0;
        }
        // get int part number
        for (; digit_index != max_index; ++digit_index) {
            int_part_number = int_part_number * 10 + digits[digit_index];
        }
        if (digit_index != actual_int_part_count) {
            int_part_number *= get_scale_multiplier<T>(actual_int_part_count - digit_index);
        }
    } else {
        // leading zeros of fraction part
        actual_frac_part_count = -actual_int_part_count;
    }
    // get fraction part number
    for (; digit_index != total_digit_count && actual_frac_part_count < type_scale;
         ++digit_index, ++actual_frac_part_count) {
        frac_part_number = frac_part_number * 10 + digits[digit_index];
    }
    auto type_scale_multiplier = get_scale_multiplier<T>(type_scale);
    // there are still extra fraction digits left, check rounding
    if (digit_index != total_digit_count) {
        // example: test 1.5 -> decimal(1, 0)
        if (digits[digit_index] >= 5) {
            ++frac_part_number;
            if (frac_part_number == type_scale_multiplier) {
                frac_part_number = 0;
                ++int_part_number;
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