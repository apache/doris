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

#include <sys/types.h>

#include <type_traits>

#include "cast_base.h"
#include "runtime/primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h" // IWYU pragma: keep
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)

template <bool IsStrict>
[[nodiscard]] [[maybe_unused]] static bool init_microsecond(int64_t frac_input,
                                                            uint32_t frac_length,
                                                            TimeValue::TimeType& val,
                                                            uint32_t target_scale,
                                                            CastParameters& params) {
    if (frac_length > 0) {
        int sign = 1;
        // time type accept negative input.
        if (frac_input < 0) {
            frac_input = -frac_input;
            sign = -1;
        }

        // align to `target_scale` digits
        auto in_scale_part =
                (frac_length > target_scale)
                        ? (uint32_t)(frac_input / common::exp10_i64(frac_length - target_scale))
                        : (uint32_t)(frac_input * common::exp10_i64(target_scale - frac_length));

        if (frac_length > target_scale) { // to_scale is up to 6
            // round off to at most `to_scale` digits
            auto digit_next =
                    (uint32_t)(frac_input / common::exp10_i64(frac_length - target_scale - 1)) % 10;
            if (digit_next >= 5) {
                in_scale_part++;
                DCHECK(in_scale_part <= 1000000);
                if (in_scale_part == common::exp10_i32(target_scale)) {
                    // overflow, round up to next second
                    val += sign * TimeValue::ONE_SECOND_MICROSECONDS;
                    SET_PARAMS_RET_FALSE_IFN(val <= TimeValue::MAX_TIME,
                                             "time overflow when rounding up");
                    in_scale_part = 0;
                }
            }
        }
        val = TimeValue::init_microsecond(
                val, sign * in_scale_part * common::exp10_i32(6 - (int)target_scale));
    }
    return true;
}

/**
 * For the functions:
 * function name `strict_mode` or `non_strict_mode`: follow the RULES of which mode
 * template parameter `IsStrict`: whether it is RUNNING IN strict mode or not.
 * return value: whether the cast is successful or not.
 * `params.status`: set error code ONLY IN STRICT MODE.
 */
struct CastToTimeV2 {
    // may be slow
    template <typename T>
    static inline bool from_integer(T int_val, TimeValue::TimeType& val, CastParameters& params) {
        if (params.is_strict) {
            return from_integer<true>(int_val, val, params);
        } else {
            return from_integer<false>(int_val, val, params);
        }
    }

    // same behaviour in both strict and non-strict mode
    template <bool IsStrict, typename T>
    static inline bool from_integer(T int_val, TimeValue::TimeType& val, CastParameters& params);

    // may be slow
    template <typename T>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, TimeValue::TimeType& val, uint32_t to_scale,
                                  CastParameters& params) {
        if (params.is_strict) {
            return from_float<true>(float_value, val, to_scale, params);
        } else {
            return from_float<false>(float_value, val, to_scale, params);
        }
    }

    template <bool IsStrict, typename T>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, TimeValue::TimeType& val, uint32_t to_scale,
                                  CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(float_value > (double)std::numeric_limits<int64_t>::min() &&
                                         !std::isnan(float_value) && !std::isinf(float_value) &&
                                         float_value < (double)std::numeric_limits<int64_t>::max(),
                                 "invalid float value for time: {}", float_value);

        auto int_part = static_cast<int64_t>(float_value);
        if (!from_integer<IsStrict>(int_part, val, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }

        int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
        if (!init_microsecond<IsStrict>(ms_part_7, 7, val, to_scale, params)) {
            return false; // status set in init_microsecond
        }
        return true;
    }

    // may be slow
    template <typename T>
    static inline bool from_decimal(const T& int_part, const T& frac_part,
                                    const int64_t& decimal_scale, TimeValue::TimeType& res,
                                    uint32_t to_scale, CastParameters& params) {
        if (params.is_strict) {
            return from_decimal<true>(int_part, frac_part, decimal_scale, res, to_scale, params);
        } else {
            return from_decimal<false>(int_part, frac_part, decimal_scale, res, to_scale, params);
        }
    }

    template <bool IsStrict, typename T>
    static inline bool from_decimal(const T& int_part, const T& frac_part,
                                    const int64_t& decimal_scale, TimeValue::TimeType& res,
                                    uint32_t to_scale, CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(int_part <= std::numeric_limits<int64_t>::max() &&
                                         int_part >= std::numeric_limits<int64_t>::min(),
                                 "invalid decimal value for time: {}.{}", int_part, frac_part);

        if (!from_integer<IsStrict>(int_part, res, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }

        if (!init_microsecond<IsStrict>((int64_t)frac_part, (uint32_t)decimal_scale, res, to_scale,
                                        params)) {
            return false; // status set in init_microsecond
        }
        return true;
    }

    // may be slow
    static inline bool from_string(const StringRef& str, TimeValue::TimeType& res,
                                   const cctz::time_zone* local_time_zone, uint32_t to_scale,
                                   CastParameters& params) {
        if (params.is_strict) {
            return from_string_strict_mode<true>(str, res, local_time_zone, to_scale, params);
        } else {
            return from_string_non_strict_mode(str, res, local_time_zone, to_scale, params);
        }
    }

    // this code follow rules of strict mode, but whether it RUNNING IN strict mode or not depends on the `IsStrict`
    // parameter. if it's false, we dont set error code for performance and we dont need.
    template <bool IsStrict>
    static inline bool from_string_strict_mode(const StringRef& str, TimeValue::TimeType& res,
                                               const cctz::time_zone* local_time_zone,
                                               uint32_t to_scale, CastParameters& params);

    static inline bool from_string_non_strict_mode(const StringRef& str, TimeValue::TimeType& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   uint32_t to_scale, CastParameters& params) {
        return CastToTimeV2::from_string_strict_mode<false>(str, res, local_time_zone, to_scale,
                                                            params) ||
               CastToTimeV2::from_string_non_strict_mode_impl(str, res, local_time_zone, to_scale,
                                                              params);
    }

    static inline bool from_string_non_strict_mode_impl(const StringRef& str,
                                                        TimeValue::TimeType& res,
                                                        const cctz::time_zone* local_time_zone,
                                                        uint32_t to_scale, CastParameters& params);
};

template <bool IsStrict, typename T>
inline bool CastToTimeV2::from_integer(T input, TimeValue::TimeType& val, CastParameters& params) {
    // T maybe int128 then bigger than int64_t. so we must check before cast
    SET_PARAMS_RET_FALSE_IFN(input <= std::numeric_limits<int64_t>::max() &&
                                     input >= std::numeric_limits<int64_t>::min(),
                             "invalid int value for time: {}", input);
    auto int_val = static_cast<int64_t>(input);
    int length = common::count_digits_fast(int_val);

    SET_PARAMS_RET_FALSE_IFN(length >= 1 && length <= 7, "invalid digits for time: {}", int_val);

    bool negative = int_val < 0;
    uint64_t uint_val = negative ? -int_val : int_val;

    int hour = int(uint_val / 10000);
    int minute = (uint_val / 100) % 100;
    int second = uint_val % 100;
    SET_PARAMS_RET_FALSE_FROM_EXCEPTION(
            val = TimeValue::make_time<true>(hour, minute, second, 0, negative));
    SET_PARAMS_RET_FALSE_IFN(TimeValue::valid(val), "invalid time value: {}:{}:{}", hour, minute,
                             second);
    return true;
}

/**
<time> ::= ("+" | "-")? (<colon-format> | <numeric-format>)

<colon-format> ::= <hour> ":" <minute> (":" <second> (<microsecond>)?)?
<hour> ::= <digit>+
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<numeric-format> ::= <digit>+ (<microsecond>)?

<microsecond> ::= "." <digit>*

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
*/
template <bool IsStrict>
inline bool CastToTimeV2::from_string_strict_mode(const StringRef& str, TimeValue::TimeType& res,
                                                  const cctz::time_zone* local_time_zone,
                                                  uint32_t to_scale, CastParameters& params) {
    const char* ptr = str.data;
    const char* end = ptr + str.size;

    // No whitespace skipping in strict mode
    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty time string");

    // check sign
    bool negative = false;
    if (*ptr == '+') {
        ++ptr;
    } else if (*ptr == '-') {
        negative = true;
        ++ptr;
    }

    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty time value after sign");

    // Two possible formats: colon-format or numeric-format
    uint32_t hour = 0, minute = 0, second = 0;
    uint32_t microsecond = 0;

    // Check if we have colon format by looking ahead
    const char* temp = ptr;
    static_cast<void>(skip_any_digit(temp, end));
    bool colon_format = (temp < end && *temp == ':');

    if (colon_format) {
        // Parse hour
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr != end, "no digits in hour part");

        StringParser::ParseResult success;
        hour = StringParser::string_to_int_internal<uint32_t, true>(start, (int)(ptr - start),
                                                                    &success);
        SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                 "invalid hour part in time string '{}'", std::string {start, ptr});

        // Check and consume colon
        SET_PARAMS_RET_FALSE_IFN(in_bound(ptr, end, 0), "expected ':' after hour but got nothing");
        SET_PARAMS_RET_FALSE_IFN(*ptr == ':', "expected ':' after hour but got {}", *ptr);
        ++ptr;

        // Parse minute (1 or 2 digits)
        SET_PARAMS_RET_FALSE_IFN((consume_digit<uint32_t, 1, 2>(ptr, end, minute)),
                                 "failed to consume 1 or 2 digits for minute, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);

        // Check if we have seconds
        if (ptr < end && *ptr == ':') {
            ++ptr;

            // Parse second (1 or 2 digits)
            SET_PARAMS_RET_FALSE_IFN((consume_digit<uint32_t, 1, 2>(ptr, end, second)),
                                     "failed to consume 1 or 2 digits for second, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);

            // Check if we have microseconds
            if (ptr < end && *ptr == '.') {
                ++ptr;

                const auto* ms_start = ptr;
                static_cast<void>(skip_any_digit(ptr, end));
                auto length = ptr - ms_start;

                if (length > 0) {
                    auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                            ms_start, std::min<int>((int)length, to_scale), &success);
                    SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                             "invalid fractional part in time string '{}'",
                                             std::string {start, ptr});

                    if (length > to_scale) { // to_scale is up to 6
                        // round off to at most `to_scale` digits
                        if (*(ms_start + to_scale) - '0' >= 5) {
                            frac_literal++;
                            DCHECK(frac_literal <= 1000000);
                            if (frac_literal == common::exp10_i32(to_scale)) {
                                // overflow, round up to next second
                                second++;
                                if (second == 60) {
                                    second = 0;
                                    minute++;
                                    if (minute == 60) {
                                        minute = 0;
                                        hour++;
                                    }
                                }
                                frac_literal = 0;
                            }
                        }
                        microsecond = frac_literal * common::exp10_i32(6 - (int)to_scale);
                    } else {
                        microsecond = frac_literal * common::exp10_i32(6 - (int)length);
                    }
                }
            }
        }
    } else {
        // numeric-format
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr != start, "no digits in numeric time format");

        StringParser::ParseResult success;
        auto numeric_value = StringParser::string_to_int_internal<uint32_t, true>(
                start, (int)(ptr - start), &success);
        SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                 "invalid numeric time format '{}'", std::string {start, ptr});

        // Convert the number to HHMMSS format
        if (numeric_value < 100) { // 1 or 2 digits
            // SS or S format
            second = numeric_value;
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        } else if (numeric_value < 10000) { // 3 or 4 digits
            // MMSS format
            minute = numeric_value / 100;
            second = numeric_value % 100;
            SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        } else { // 5 or more digits
            // HHMMSS format
            hour = numeric_value / 10000;
            minute = (numeric_value / 100) % 100;
            second = numeric_value % 100;
            SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        }

        // Check if we have microseconds
        if (ptr < end && *ptr == '.') {
            ++ptr;

            const auto* ms_start = ptr;
            static_cast<void>(skip_any_digit(ptr, end));
            auto length = ptr - ms_start;

            if (length > 0) {
                auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        ms_start, std::min<int>((int)length, to_scale), &success);
                SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                         "invalid fractional part in time string '{}'",
                                         std::string {start, ptr});

                if (length > to_scale) { // to_scale is up to 6
                    // round off to at most `to_scale` digits
                    if (*(ms_start + to_scale) - '0' >= 5) {
                        frac_literal++;
                        DCHECK(frac_literal <= 1000000);
                        if (frac_literal == common::exp10_i32(to_scale)) {
                            // overflow, round up to next second
                            second++;
                            if (second == 60) {
                                second = 0;
                                minute++;
                                if (minute == 60) {
                                    minute = 0;
                                    hour++;
                                }
                            }
                            frac_literal = 0;
                        }
                    }
                    microsecond = frac_literal * common::exp10_i32(6 - (int)to_scale);
                } else {
                    microsecond = frac_literal * common::exp10_i32(6 - (int)length);
                }
            }
        }
    }

    // No trailing characters allowed in strict mode
    SET_PARAMS_RET_FALSE_IFN(ptr == end, "invalid time string '{}', extra characters after parsing",
                             std::string {ptr, end});

    // Convert to TimeValue's internal storage format (microseconds since 00:00:00)
    SET_PARAMS_RET_FALSE_FROM_EXCEPTION(
            res = TimeValue::make_time<true>(hour, minute, second, microsecond, negative));
    SET_PARAMS_RET_FALSE_IFN(TimeValue::valid(res), "invalid time value: {}:{}:{}.{}", hour, minute,
                             second, microsecond);
    return true;
}

/**
<time> ::= <whitespace>* ("+" | "-")? (<colon-format> | <numeric-format>) <whitespace>*

<colon-format> ::= <hour> ":" <minute> (":" <second> (<microsecond>)?)?
<hour> ::= <digit>+
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<numeric-format> ::= <digit>+ (<microsecond>)?

<microsecond> ::= "." <digit>*

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
<whitespace> ::= " " | "\t" | "\n" | "\r" | "\v" | "\f"
*/
inline bool CastToTimeV2::from_string_non_strict_mode_impl(const StringRef& str,
                                                           TimeValue::TimeType& res,
                                                           const cctz::time_zone* local_time_zone,
                                                           uint32_t to_scale,
                                                           CastParameters& params) {
    constexpr bool IsStrict = false;
    const char* ptr = str.data;
    const char* end = ptr + str.size;

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty time string");

    // check sign
    bool negative = false;
    if (*ptr == '+') {
        ++ptr;
    } else if (*ptr == '-') {
        negative = true;
        ++ptr;
    }

    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty time value after sign");

    // Two possible formats: colon-format or numeric-format
    uint32_t hour = 0, minute = 0, second = 0;
    uint32_t microsecond = 0;

    // Check if we have colon format by looking ahead
    const char* temp = ptr;
    static_cast<void>(skip_any_digit(temp, end));
    bool colon_format = (temp < end && *temp == ':');

    if (colon_format) {
        // Parse hour
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr != start, "no digits in hour part");

        StringParser::ParseResult success;
        hour = StringParser::string_to_int_internal<uint32_t, true>(start, (int)(ptr - start),
                                                                    &success);
        SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                 "invalid hour part in time string '{}'", std::string {start, ptr});

        // Check and consume colon
        PROPAGATE_FALSE(in_bound(ptr, end, 0));
        SET_PARAMS_RET_FALSE_IFN(*ptr == ':', "expected ':' after hour but got {}", *ptr);
        ++ptr;

        // Parse minute (1 or 2 digits)
        PROPAGATE_FALSE((consume_digit<uint32_t, 1, 2>(ptr, end, minute)));
        SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);

        // Check if we have seconds
        if (ptr < end && *ptr == ':') {
            ++ptr;

            // Parse second (1 or 2 digits)
            PROPAGATE_FALSE((consume_digit<uint32_t, 1, 2>(ptr, end, second)));
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);

            // Check if we have microseconds
            if (ptr < end && *ptr == '.') {
                ++ptr;

                const auto* ms_start = ptr;
                static_cast<void>(skip_any_digit(ptr, end));
                auto length = ptr - ms_start;

                if (length > 0) {
                    auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                            ms_start, std::min<int>((int)length, to_scale), &success);
                    SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                             "invalid fractional part in time string '{}'",
                                             std::string {start, ptr});

                    if (length > to_scale) { // to_scale is up to 6
                        // round off to at most `to_scale` digits
                        if (*(ms_start + to_scale) - '0' >= 5) {
                            frac_literal++;
                            DCHECK(frac_literal <= 1000000);
                            if (frac_literal == common::exp10_i32(to_scale)) {
                                // overflow, round up to next second
                                second++;
                                if (second == 60) {
                                    second = 0;
                                    minute++;
                                    if (minute == 60) {
                                        minute = 0;
                                        hour++;
                                    }
                                }
                                frac_literal = 0;
                            }
                        }
                        microsecond = frac_literal * common::exp10_i32(6 - (int)to_scale);
                    } else {
                        microsecond = frac_literal * common::exp10_i32(6 - (int)length);
                    }
                }
            }
        }
    } else {
        // numeric-format
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr != start, "no digits in numeric time format");

        StringParser::ParseResult success;
        auto numeric_value = StringParser::string_to_int_internal<uint32_t, true>(
                start, (int)(ptr - start), &success);
        SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                 "invalid numeric time format '{}'", std::string {start, ptr});

        // Convert the number to HHMMSS format
        if (numeric_value < 100) { // 1 or 2 digits
            // SS or S format
            second = numeric_value;
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        } else if (numeric_value < 10000) { // 3 or 4 digits
            // MMSS format
            minute = numeric_value / 100;
            second = numeric_value % 100;
            SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        } else { // 5 or more digits
            // HHMMSS format
            hour = numeric_value / 10000;
            minute = (numeric_value / 100) % 100;
            second = numeric_value % 100;
            SET_PARAMS_RET_FALSE_IFN(minute < 60, "invalid minute {}", minute);
            SET_PARAMS_RET_FALSE_IFN(second < 60, "invalid second {}", second);
        }

        // Check if we have microseconds
        if (ptr < end && *ptr == '.') {
            ++ptr;

            const auto* ms_start = ptr;
            static_cast<void>(skip_any_digit(ptr, end));
            auto length = ptr - ms_start;

            if (length > 0) {
                auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        ms_start, std::min<int>((int)length, to_scale), &success);
                SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                         "invalid fractional part in time string '{}'",
                                         std::string {start, ptr});

                if (length > to_scale) { // to_scale is up to 6
                    // round off to at most `to_scale` digits
                    if (*(ms_start + to_scale) - '0' >= 5) {
                        frac_literal++;
                        DCHECK(frac_literal <= 1000000);
                        if (frac_literal == common::exp10_i32(to_scale)) {
                            // overflow, round up to next second
                            second++;
                            if (second == 60) {
                                second = 0;
                                minute++;
                                if (minute == 60) {
                                    minute = 0;
                                    hour++;
                                }
                            }
                            frac_literal = 0;
                        }
                    }
                    microsecond = frac_literal * common::exp10_i32(6 - (int)to_scale);
                } else {
                    microsecond = frac_literal * common::exp10_i32(6 - (int)length);
                }
            }
        }
    }

    // Skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr == end, "invalid time string '{}', extra characters after parsing",
                             std::string {ptr, end});

    // Convert to TimeValue's internal storage format (microseconds since 00:00:00)
    SET_PARAMS_RET_FALSE_FROM_EXCEPTION(
            res = TimeValue::make_time<true>(hour, minute, second, microsecond, negative));
    SET_PARAMS_RET_FALSE_IFN(TimeValue::valid(res), "invalid time value: {}:{}:{}.{}", hour, minute,
                             second, microsecond);
    return true;
}

// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)
#include "common/compile_check_end.h"
} // namespace doris::vectorized
