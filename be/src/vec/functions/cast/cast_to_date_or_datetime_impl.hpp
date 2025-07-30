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
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h" // IWYU pragma: keep
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/datelike_serde_common.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)

template <bool IsDatetime>
inline static void cast_to_type(VecDateTimeValue& res) {
    if constexpr (IsDatetime) {
        res.to_datetime();
    } else {
        res.cast_to_date();
    }
}

template <bool IsStrict>
[[nodiscard]] inline static bool microsecond_carry_on(int64_t frac_part, uint32_t float_scale,
                                                      VecDateTimeValue& val,
                                                      CastParameters& params) {
    if (val.type() == TimeType::TIME_DATE) {
        // for date, we just ignore the fractional part
        return true;
    }
    // normalize the fractional part to microseconds(6 digits)
    if (float_scale > 0) {
        if (float_scale > 6) {
            int ms = int(frac_part / common::exp10_i64(float_scale - 6));
            // if scale > 6, we need to round the fractional part
            int digit7 = frac_part % common::exp10_i32(float_scale - 6) /
                         common::exp10_i32(float_scale - 7);
            if (digit7 >= 5) {
                ms++;
                DCHECK(ms <= 1000000);
                if (ms == 1000000) {
                    // overflow, round up to next second
                    SET_PARAMS_RET_FALSE_IFN(val.date_add_interval<TimeUnit::SECOND>(
                                                     TimeInterval {TimeUnit::SECOND, 1, false}),
                                             "datetime overflow when rounding up to next second");
                    ms = 0;
                }
            }
        }
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
// for datev1 or datetimev1
struct CastToDateOrDatetime {
    // may be slow
    template <typename T, bool IsDatetime>
    static inline bool from_integer(T int_val, VecDateTimeValue& val, CastParameters& params) {
        if (params.is_strict) {
            return from_integer<true, IsDatetime>(int_val, val, params);
        } else {
            return from_integer<false, IsDatetime>(int_val, val, params);
        }
    }

    // same behaviour in both strict and non-strict mode
    template <bool IsStrict, bool IsDatetime, typename T>
    static inline bool from_integer(T int_val, VecDateTimeValue& val, CastParameters& params);

    // may be slow
    template <typename T, bool IsDatetime>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, VecDateTimeValue& val, uint32_t to_scale,
                                  CastParameters& params) {
        if (params.is_strict) {
            return from_float<true, IsDatetime>(float_value, val, to_scale, params);
        } else {
            return from_float<false, IsDatetime>(float_value, val, to_scale, params);
        }
    }

    template <bool IsStrict, bool IsDatetime, typename T>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, VecDateTimeValue& val, CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(float_value > 0 && !std::isnan(float_value) &&
                                         !std::isinf(float_value) &&
                                         float_value < (double)std::numeric_limits<int64_t>::max(),
                                 "invalid float value for date/datetime: {}", float_value);

        auto int_part = static_cast<int64_t>(float_value);
        if (!from_integer<IsStrict, IsDatetime>(int_part, val, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }

        if constexpr (IsDatetime) {
            int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
            if (!microsecond_carry_on<IsStrict>(ms_part_7, 7, val, params)) [[unlikely]] {
                return false; // status set in microsecond_carry_on
            }
        }
        return true;
    }

    // may be slow
    template <typename T>
    static inline bool from_decimal(const T& int_part, const T& frac_part,
                                    const int64_t& decimal_scale, TimeValue::TimeType& res,
                                    CastParameters& params) {
        if (params.is_strict) {
            return from_decimal<true>(int_part, frac_part, decimal_scale, res, params);
        } else {
            return from_decimal<false>(int_part, frac_part, decimal_scale, res, params);
        }
    }

    template <bool IsStrict, bool IsDatetime, typename T>
    static inline bool from_decimal(const T& int_part, const T& frac_part,
                                    const int64_t& decimal_scale, VecDateTimeValue& res,
                                    CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(int_part <= std::numeric_limits<int64_t>::max() && int_part >= 1,
                                 "invalid decimal value for date/datetime: {}.{}", int_part,
                                 frac_part);

        if (!from_integer<IsStrict, IsDatetime>(int_part, res, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }

        if constexpr (IsDatetime) {
            if (!microsecond_carry_on<IsStrict>((int64_t)frac_part, (uint32_t)decimal_scale, res,
                                                params)) [[unlikely]] {
                return false; // status set in microsecond_carry_on
            }
        }
        return true;
    }

    // may be slow
    template <bool IsDatetime>
    static inline bool from_string(const StringRef& str, VecDateTimeValue& res,
                                   const cctz::time_zone* local_time_zone, CastParameters& params) {
        if (params.is_strict) {
            return from_string_strict_mode<true, IsDatetime>(str, res, local_time_zone, params);
        } else {
            return from_string_non_strict_mode<IsDatetime>(str, res, local_time_zone, params);
        }
    }

    // this code follow rules of strict mode, but whether it RUNNING IN strict mode or not depends on the `IsStrict`
    // parameter. if it's false, we dont set error code for performance and we dont need.
    template <bool IsStrict, bool IsDatetime>
    static inline bool from_string_strict_mode(const StringRef& str, VecDateTimeValue& res,
                                               const cctz::time_zone* local_time_zone,
                                               CastParameters& params);

    template <bool IsDatetime>
    static inline bool from_string_non_strict_mode(const StringRef& str, VecDateTimeValue& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   CastParameters& params) {
        return CastToDateOrDatetime::from_string_strict_mode<false, IsDatetime>(
                       str, res, local_time_zone, params) ||
               CastToDateOrDatetime::from_string_non_strict_mode_impl<IsDatetime>(
                       str, res, local_time_zone, params);
    }

    template <bool IsDatetime>
    static inline bool from_string_non_strict_mode_impl(const StringRef& str, VecDateTimeValue& res,
                                                        const cctz::time_zone* local_time_zone,
                                                        CastParameters& params);
};

template <bool IsStrict, bool IsDatetime, typename T>
inline bool CastToDateOrDatetime::from_integer(T input, VecDateTimeValue& val,
                                               CastParameters& params) {
    // T maybe int128 then bigger than int64_t. so we must check before cast
    SET_PARAMS_RET_FALSE_IFN(input <= std::numeric_limits<int64_t>::max() && input > 0,
                             "invalid int value for date/datetime: {}", input);
    auto int_val = static_cast<int64_t>(input);
    int length = common::count_digits_fast(int_val);
    if (length == 3 || length == 4) {
        val.unchecked_set_time_unit<TimeUnit::YEAR>(2000);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::MONTH>((uint32_t)int_val / 100),
                                 "invalid month {}", int_val / 100);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::DAY>(int_val % 100), "invalid day {}",
                                 int_val % 100);
    } else if (length == 5) {
        SET_PARAMS_RET_FALSE_IFN(
                val.set_time_unit<TimeUnit::YEAR>(2000 + (uint32_t)int_val / 10000),
                "invalid year {}", 2000 + int_val / 10000);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::MONTH>(int_val % 10000 / 100),
                                 "invalid month {}", int_val % 10000 / 100);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::DAY>(int_val % 100), "invalid day {}",
                                 int_val % 100);
    } else if (length == 6) {
        uint32_t year = (uint32_t)int_val / 10000;
        if (year < 70) {
            year += 2000;
        } else {
            year += 1900;
        }
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::YEAR>(year), "invalid year {}", year);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::MONTH>(int_val % 10000 / 100),
                                 "invalid month {}", int_val % 10000 / 100);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::DAY>(int_val % 100), "invalid day {}",
                                 int_val % 100);
    } else if (length == 8) {
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::YEAR>((uint32_t)int_val / 10000),
                                 "invalid year {}", int_val / 10000);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::MONTH>(int_val % 10000 / 100),
                                 "invalid month {}", int_val % 10000 / 100);
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::DAY>(int_val % 100), "invalid day {}",
                                 int_val % 100);
    } else if (length == 14) {
        SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::YEAR>(int_val / common::exp10_i64(10)),
                                 "invalid year {}", int_val / common::exp10_i64(10));
        SET_PARAMS_RET_FALSE_IFN(
                val.set_time_unit<TimeUnit::MONTH>((int_val / common::exp10_i32(8)) % 100),
                "invalid month {}", (int_val / common::exp10_i32(8)) % 100);
        SET_PARAMS_RET_FALSE_IFN(
                val.set_time_unit<TimeUnit::DAY>((int_val / common::exp10_i32(6)) % 100),
                "invalid day {}", (int_val / common::exp10_i32(6)) % 100);
        if (val.type() == TimeType::TIME_DATETIME) {
            SET_PARAMS_RET_FALSE_IFN(
                    val.set_time_unit<TimeUnit::HOUR>((int_val / common::exp10_i32(4)) % 100),
                    "invalid hour {}", (int_val / common::exp10_i32(4)) % 100);
            SET_PARAMS_RET_FALSE_IFN(
                    val.set_time_unit<TimeUnit::MINUTE>((int_val / common::exp10_i32(2)) % 100),
                    "invalid minute {}", (int_val / common::exp10_i32(2)) % 100);
            SET_PARAMS_RET_FALSE_IFN(val.set_time_unit<TimeUnit::SECOND>(int_val % 100),
                                     "invalid second {}", int_val % 100);
        }
    } else [[unlikely]] {
        if constexpr (IsStrict) {
            params.status =
                    Status::InvalidArgument("invalid digits for date/datetime: {}", int_val);
        }
        return false;
    }

    cast_to_type<IsDatetime>(val);
    return true;
}

/**
<datetime>       ::= <date> (("T" | " ") <time> <whitespace>* <offset>?)?

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<date>           ::= <year> "-" <month1> "-" <day1>
                   | <year> <month2> <day2>

<year>           ::= <digit>{2} | <digit>{4} ; 1970 为界
<month1>         ::= <digit>{1,2}            ; 01–12
<day1>           ::= <digit>{1,2}            ; 01–28/29/30/31 视月份而定

<month2>         ::= <digit>{2}              ; 01–12
<day2>           ::= <digit>{2}              ; 01–28/29/30/31 视月份而定

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<time>           ::= <hour1> (":" <minute1> (":" <second1> <fraction>?)?)?
                   | <hour2> (<minute2> (<second2> <fraction>?)?)?

<hour1>           ::= <digit>{1,2}      ; 00–23
<minute1>         ::= <digit>{1,2}      ; 00–59
<second1>         ::= <digit>{1,2}      ; 00–59

<hour2>           ::= <digit>{2}        ; 00–23
<minute2>         ::= <digit>{2}        ; 00–59
<second2>         ::= <digit>{2}        ; 00–59

<fraction>        ::= "." <digit>*

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<offset>         ::= ( "+" | "-" ) <hour-offset> [ ":"? <minute-offset> ]
                   | <tz-name>

<tz-name>        ::= <short-tz> | <long-tz> 

<short-tz>       ::= "CST" | "UTC" | "GMT" | "ZULU" | "Z"   ; 忽略大小写
<long-tz>        ::= <area> "/" <location>                  ; e.g. America/New_York

<hour-offset>    ::= <digit>{1,2}      ; 0–14
<minute-offset>  ::= <digit>{2}        ; 00/30/45

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<digit>          ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<area>           ::= <alpha>+
<location>       ::= (<alpha> | "_")+
<alpha>          ::= "A" | … | "Z" | "a" | … | "z"
<whitespace>     ::= " " | "\t" | "\n" | "\r" | "\v" | "\f"
*/
template <bool IsStrict, bool IsDatetime>
inline bool CastToDateOrDatetime::from_string_strict_mode(const StringRef& str,
                                                          VecDateTimeValue& res,
                                                          const cctz::time_zone* local_time_zone,
                                                          CastParameters& params) {
    const char* ptr = str.data;
    const char* end = ptr + str.size;

    uint32_t part[4];
    bool has_second = false;

    // special `date` and `time` part format: 14-length digits string. parse it as YYYYMMDDHHMMSS
    if (assert_within_bound(ptr, end, 13) && is_digit_range(ptr, ptr + 14)) {
        // if the string is all digits, treat it as a date in YYYYMMDD format.
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 4>(ptr, end, part[0])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
            SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::YEAR>(part[0]),
                                     "invalid year {}", part[0]);
            SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                     "invalid month {}", part[1]);
            SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                     "invalid day {}", part[2]);
        }

        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::HOUR>(part[0]),
                                 "invalid hour {}", part[0]);
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                 "invalid minute {}", part[1]);
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                 "invalid second {}", part[2]);
        has_second = true;
        if (ptr == end) {
            // no fraction or timezone part, just return.
            return true;
        }
        goto FRAC;
    }

    // date part
    SET_PARAMS_RET_FALSE_IF_ERR(assert_within_bound(ptr, end, 5));
    if (is_digit_range(ptr, ptr + 5)) {
        // no delimiter here.
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (assert_within_bound(ptr, end, 0) && is_numeric_ascii(*ptr)) {
            // 4 digits year
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[3])));
            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.template set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(part[2]),
                                         "invalid month {}", part[2]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(part[3]),
                                         "invalid day {}", part[3]);
            }
        } else {
            if (!try_convert_set_zero_date(res, complete_4digit_year(part[0]), part[1], part[2])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.template set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                         "invalid month {}", part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                         "invalid day {}", part[2]);
            }
        }
    } else {
        // has delimiter here.
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        SET_PARAMS_RET_FALSE_IF_ERR(assert_within_bound(ptr, end, 0));
        if (*ptr == '-') {
            // 2 digits year
            ++ptr; // consume one bar
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_one_bar(ptr, end)));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));

            if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.template set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                         "invalid month {}", part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                         "invalid day {}", part[2]);
            }
        } else {
            // 4 digits year
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[1])));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_one_bar(ptr, end)));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_one_bar(ptr, end)));
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[3])));

            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.template set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(part[2]),
                                         "invalid month {}", part[2]);
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(part[3]),
                                         "invalid day {}", part[3]);
            }
        }
    }

    if (ptr == end) {
        // no time part, just return.
        cast_to_type<IsDatetime>(res);
        return true;
    }

    SET_PARAMS_RET_FALSE_IF_ERR(consume_one_delimiter(ptr, end));

    // time part.
    // hour
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[0])));
    SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                             part[0]);
    SET_PARAMS_RET_FALSE_IF_ERR(assert_within_bound(ptr, end, 0));
    if (*ptr == ':') {
        // with hour:minute:second
        if (consume_one_colon(ptr, end)) { // minute
            SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                     "invalid minute {}", part[1]);
            if (consume_one_colon(ptr, end)) { // second
                has_second = true;
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                         "invalid second {}", part[2]);
            } else {
                res.template unchecked_set_time_unit<TimeUnit::SECOND>(0);
            }
        } else {
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>(0);
            res.template unchecked_set_time_unit<TimeUnit::SECOND>(0);
        }
    } else {
        // no ':'
        if (consume_digit<UInt32, 2>(ptr, end, part[1])) {
            // has minute
            SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                     "invalid minute {}", part[1]);
            if (consume_digit<UInt32, 2>(ptr, end, part[2])) {
                // has second
                has_second = true;
                SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                         "invalid second {}", part[2]);
            }
        }
    }

FRAC:
    // fractional part
    if (has_second && assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        [[maybe_unused]] auto length = ptr - start;

        if constexpr (IsDatetime) {
            if (length > 0) {
                StringParser::ParseResult success;
                auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        start, std::min<int>((int)length, 6), &success);
                SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                         "invalid fractional part in datetime string '{}'",
                                         std::string {start, ptr});
                // differ to datetimev2, we only need to process carrying on which caused by carrying on to digit 6-th.
                if (length > 6) {
                    // round off to at most 6 digits
                    if (auto remainder = *(start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            res.template date_add_interval<TimeUnit::SECOND>(
                                    TimeInterval {TimeUnit::SECOND, 1, false});
                            ms = 0;
                        }
                    }
                }
            }
        }
    }
    static_cast<void>(skip_any_digit(ptr, end));

    static_cast<void>(skip_any_whitespace(ptr, end));

    // timezone part
    if (ptr != end) {
        cctz::time_zone parsed_tz {};
        if (*ptr == '+' || *ptr == '-') {
            // offset
            const char sign = *ptr;
            ++ptr;
            part[1] = 0;

            uint32_t length = count_digits(ptr, end);
            // hour
            if (length == 1 || length == 3) {
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1>(ptr, end, part[0])));
            } else {
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[0])));
            }
            SET_PARAMS_RET_FALSE_IFN(part[0] <= 14, "invalid hour offset {}", part[0]);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, part[1])));
                SET_PARAMS_RET_FALSE_IFN((part[1] == 0 || part[1] == 30 || part[1] == 45),
                                         "invalid minute offset {}", part[1]);
            }
            SET_PARAMS_RET_FALSE_IFN(part[0] != 14 || part[1] == 0, "invalid timezone offset '{}'",
                                     combine_tz_offset(sign, part[0], part[1]));

            SET_PARAMS_RET_FALSE_IFN(TimezoneUtils::find_cctz_time_zone(
                                             combine_tz_offset(sign, part[0], part[1]), parsed_tz),
                                     "invalid timezone offset '{}'",
                                     combine_tz_offset(sign, part[0], part[1]));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            SET_PARAMS_RET_FALSE_IF_ERR(skip_tz_name_part(ptr, end));

            SET_PARAMS_RET_FALSE_IFN(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        if constexpr (IsDatetime) {
            // convert tz
            cctz::civil_second cs {res.year(), res.month(),  res.day(),
                                   res.hour(), res.minute(), res.second()};

            auto given = cctz::convert(cs, parsed_tz);
            auto local = cctz::convert(given, *local_time_zone);
            res.template unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
            res.template unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
            res.template unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
            res.template unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
            res.template unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());
        }

        static_cast<void>(skip_any_whitespace(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr == end,
                                 "invalid datetime string '{}', extra characters after timezone",
                                 std::string {ptr, end});
    }
    cast_to_type<IsDatetime>(res);
    return true;
}

/**
<datetime> ::= <whitespace>* <date> (<delimiter> <time> <whitespace>* <timezone>?)? <whitespace>*

<date> ::= <year> <separator> <month> <separator> <day>
<time> ::= <hour> <separator> <minute> <separator> <second> [<fraction>]

<year> ::= <digit>{4} | <digit>{2}
<month> ::= <digit>{1,2}
<day> ::= <digit>{1,2}
<hour> ::= <digit>{1,2}
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<separator> ::= ^(<digit> | <alpha>)
<delimiter> ::= " " | "T"

<fraction> ::= "." <digit>*

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<offset>         ::= ( "+" | "-" ) <hour-offset> [ ":"? <minute-offset> ]
                   | <tz-name>

<tz-name>        ::= <short-tz> | <long-tz> 

<short-tz>       ::= "CST" | "UTC" | "GMT" | "ZULU" | "Z"   ; 忽略大小写
<long-tz>        ::= <area> "/" <location>                  ; e.g. America/New_York

<hour-offset>    ::= <digit>{1,2}      ; 0–14
<minute-offset>  ::= <digit>{2}        ; 00/30/45

<area>           ::= <alpha>+
<location>       ::= (<alpha> | "_")+

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<whitespace> ::= " " | "\t" | "\n" | "\r" | "\v" | "\f"

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<alpha>          ::= "A" | … | "Z" | "a" | … | "z"
*/
template <bool IsDatetime>
inline bool CastToDateOrDatetime::from_string_non_strict_mode_impl(
        const StringRef& str, VecDateTimeValue& res, const cctz::time_zone* local_time_zone,
        CastParameters& params) {
    constexpr bool IsStrict = false;
    const char* ptr = str.data;
    const char* end = ptr + str.size;

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty date/datetime string");

    // date part
    uint32_t year, month, day;

    // read year
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, year)));
    if (is_digit_range(ptr, ptr + 1)) {
        // continue by digit, it must be a 4-digit year
        uint32_t year2;
        SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, year2)));
        year = year * 100 + year2;
    } else {
        // otherwise, it must be a 2-digit year
        if (year < 100) {
            // Convert 2-digit year based on 1970 boundary
            year += (year >= 70) ? 1900 : 2000;
        }
    }

    // check for separator
    SET_PARAMS_RET_FALSE_IF_ERR(skip_one_non_alnum(ptr, end));

    // read month
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, month)));

    // check for separator
    SET_PARAMS_RET_FALSE_IF_ERR(skip_one_non_alnum(ptr, end));

    // read day
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, day)));

    if (!try_convert_set_zero_date(res, year, month, day)) {
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::YEAR>(year),
                                 "invalid year {}", year);
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MONTH>(month),
                                 "invalid month {}", month);
        SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::DAY>(day), "invalid day {}",
                                 day);
    }

    if (ptr == end) {
        // no time part, just return.
        cast_to_type<IsDatetime>(res);
        return true;
    }

    SET_PARAMS_RET_FALSE_IF_ERR(consume_one_delimiter(ptr, end));

    // time part
    uint32_t hour, minute, second;

    // hour
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, hour)));
    SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::HOUR>(hour), "invalid hour {}",
                             hour);

    // check for separator
    SET_PARAMS_RET_FALSE_IF_ERR(skip_one_non_alnum(ptr, end));

    // minute
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, minute)));
    SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::MINUTE>(minute),
                             "invalid minute {}", minute);

    // check for separator
    SET_PARAMS_RET_FALSE_IF_ERR(skip_one_non_alnum(ptr, end));

    // second
    SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1, 2>(ptr, end, second)));
    SET_PARAMS_RET_FALSE_IFN(res.template set_time_unit<TimeUnit::SECOND>(second),
                             "invalid second {}", second);

    // fractional part
    if (assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        [[maybe_unused]] auto length = ptr - start;

        if constexpr (IsDatetime) {
            if (length > 0) {
                StringParser::ParseResult success;
                auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        start, std::min<int>((int)length, 6), &success);
                SET_PARAMS_RET_FALSE_IFN(success == StringParser::PARSE_SUCCESS,
                                         "invalid fractional part in datetime string '{}'",
                                         std::string {start, ptr});
                // differ to datetimev2, we only need to process carrying on which caused by carrying on to digit 6-th.
                if (length > 6) {
                    // round off to at most 6 digits
                    if (auto remainder = *(start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            res.template date_add_interval<TimeUnit::SECOND>(
                                    TimeInterval {TimeUnit::SECOND, 1, false});
                            ms = 0;
                        }
                    }
                }
            }
        }
    }

    // skip any whitespace after time
    static_cast<void>(skip_any_whitespace(ptr, end));

    // timezone part (if any)
    if (ptr != end) {
        cctz::time_zone parsed_tz {};
        if (*ptr == '+' || *ptr == '-') {
            // offset
            const char sign = *ptr;
            ++ptr;
            uint32_t hour_offset, minute_offset = 0;

            uint32_t length = count_digits(ptr, end);
            // hour
            if (length == 1 || length == 3) {
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 1>(ptr, end, hour_offset)));
            } else {
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, hour_offset)));
            }
            SET_PARAMS_RET_FALSE_IFN(hour_offset <= 14, "invalid hour offset {}", hour_offset);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                SET_PARAMS_RET_FALSE_IF_ERR((consume_digit<UInt32, 2>(ptr, end, minute_offset)));
                SET_PARAMS_RET_FALSE_IFN(
                        (minute_offset == 0 || minute_offset == 30 || minute_offset == 45),
                        "invalid minute offset {}", minute_offset);
            }
            SET_PARAMS_RET_FALSE_IFN(hour_offset != 14 || minute_offset == 0,
                                     "invalid timezone offset '{}'",
                                     combine_tz_offset(sign, hour_offset, minute_offset));

            SET_PARAMS_RET_FALSE_IFN(
                    TimezoneUtils::find_cctz_time_zone(
                            combine_tz_offset(sign, hour_offset, minute_offset), parsed_tz),
                    "invalid timezone offset '{}'",
                    combine_tz_offset(sign, hour_offset, minute_offset));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            SET_PARAMS_RET_FALSE_IF_ERR(skip_tz_name_part(ptr, end));

            SET_PARAMS_RET_FALSE_IFN(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        if constexpr (IsDatetime) {
            // convert tz
            cctz::civil_second cs {res.year(), res.month(),  res.day(),
                                   res.hour(), res.minute(), res.second()};

            auto given = cctz::convert(cs, parsed_tz);
            auto local = cctz::convert(given, *local_time_zone);
            res.template unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
            res.template unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
            res.template unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
            res.template unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
            res.template unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());
        }
    }

    // skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr == end,
                             "invalid datetime string '{}', extra characters after parsing",
                             std::string {ptr, end});

    cast_to_type<IsDatetime>(res);
    return true;
}

// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)
#include "common/compile_check_end.h"
} // namespace doris::vectorized
