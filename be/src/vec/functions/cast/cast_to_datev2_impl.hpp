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

#include "cast_base.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "util/asan_util.h"
#include "util/string_parser.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h" // IWYU pragma: keep
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/datelike_serde_common.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
/**
 * For the functions:
 * function name `strict_mode` or `non_strict_mode`: follow the RULES of which mode
 * template parameter `IsStrict`: whether it is RUNNING IN strict mode or not.
 * return value: whether the cast is successful or not.
 * `params.status`: set error code ONLY IN STRICT MODE.
 */
struct CastToDateV2 {
    // may be slow
    template <typename T>
    static inline bool from_integer(T int_val, DateV2Value<DateV2ValueType>& val,
                                    CastParameters& params) {
        if (params.is_strict) {
            return from_integer<true>(int_val, val, params);
        } else {
            return from_integer<false>(int_val, val, params);
        }
    }

    // same behaviour in both strict and non-strict mode
    template <bool IsStrict, typename T>
    static inline bool from_integer(T int_val, DateV2Value<DateV2ValueType>& val,
                                    CastParameters& params);

    // may be slow
    template <typename T>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, DateV2Value<DateV2ValueType>& val,
                                  CastParameters& params) {
        if (params.is_strict) {
            return from_float<true>(float_value, val, params);
        } else {
            return from_float<false>(float_value, val, params);
        }
    }

    template <bool IsStrict, typename T>
        requires std::is_floating_point_v<T>
    static inline bool from_float(T float_value, DateV2Value<DateV2ValueType>& val,
                                  CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(float_value > 0 && !std::isnan(float_value) &&
                                         !std::isinf(float_value) &&
                                         float_value < (double)std::numeric_limits<int64_t>::max(),
                                 "invalid float value for datev2: {}", float_value);

        auto int_part = static_cast<int64_t>(float_value);
        if (!from_integer<IsStrict>(int_part, val, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }
        return true;
    }

    // may be slow
    template <typename T>
    static inline bool from_decimal(const T& int_part, const int64_t& decimal_scale,
                                    DateV2Value<DateV2ValueType>& res, CastParameters& params) {
        if (params.is_strict) {
            return from_decimal<true>(int_part, decimal_scale, res, params);
        } else {
            return from_decimal<false>(int_part, decimal_scale, res, params);
        }
    }

    template <bool IsStrict, typename T>
    static inline bool from_decimal(const T& int_part, const int64_t& decimal_scale,
                                    DateV2Value<DateV2ValueType>& res, CastParameters& params) {
        SET_PARAMS_RET_FALSE_IFN(int_part <= std::numeric_limits<int64_t>::max() && int_part >= 1,
                                 "invalid decimal value for datev2: {}.xxx", int_part);

        if (!from_integer<IsStrict>(int_part, res, params)) {
            // if IsStrict, error code has been set in from_integer
            return false;
        }
        return true;
    }

    // may be slow
    static inline bool from_string(const StringRef& str, DateV2Value<DateV2ValueType>& res,
                                   const cctz::time_zone* local_time_zone, CastParameters& params) {
        if (params.is_strict) {
            return from_string_strict_mode<true>(str, res, local_time_zone, params);
        } else {
            return from_string_non_strict_mode(str, res, local_time_zone, params);
        }
    }

    // this code follow rules of strict mode, but whether it RUNNING IN strict mode or not depends on the `IsStrict`
    // parameter. if it's false, we dont set error code for performance and we dont need.
    template <bool IsStrict>
    static inline bool from_string_strict_mode(const StringRef& str,
                                               DateV2Value<DateV2ValueType>& res,
                                               const cctz::time_zone* local_time_zone,
                                               CastParameters& params);

    static inline bool from_string_non_strict_mode(const StringRef& str,
                                                   DateV2Value<DateV2ValueType>& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   CastParameters& params) {
        return CastToDateV2::from_string_strict_mode<false>(str, res, local_time_zone, params) ||
               CastToDateV2::from_string_non_strict_mode_impl(str, res, local_time_zone, params);
    }

    static inline bool from_string_non_strict_mode_impl(const StringRef& str,
                                                        DateV2Value<DateV2ValueType>& res,
                                                        const cctz::time_zone* local_time_zone,
                                                        CastParameters& params);
};

template <bool IsStrict, typename T>
inline bool CastToDateV2::from_integer(T input, DateV2Value<DateV2ValueType>& val,
                                       CastParameters& params) {
    // T maybe int128 then bigger than int64_t. so we must check before cast
    SET_PARAMS_RET_FALSE_IFN(input <= std::numeric_limits<int64_t>::max() && input > 0,
                             "invalid int value for datev2: {}", input);
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
    } else [[unlikely]] {
        if constexpr (IsStrict) {
            params.status = Status::InvalidArgument("invalid digits for datev2: {}", int_val);
        }
    }
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
template <bool IsStrict>
inline bool CastToDateV2::from_string_strict_mode(const StringRef& str,
                                                  DateV2Value<DateV2ValueType>& res,
                                                  const cctz::time_zone* local_time_zone,
                                                  CastParameters& params) {
    const char* ptr = str.data;
    const char* end = ptr + str.size;
    AsanPoisonDefer defer(end, 1);

    uint32_t part[4];
    bool has_second = false;

    // special `date` and `time` part format: 14-length digits string. parse it as YYYYMMDDHHMMSS
    if (ptr + 13 < end && is_digit_range(ptr, ptr + 14)) {
        // if the string is all digits, treat it as a date in YYYYMMDD format.
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 4>(ptr, end, part[0])),
                                 "failed to consume 4 digits for year, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[1])),
                                 "failed to consume 2 digits for month, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[2])),
                                 "failed to consume 2 digits for day, got {}",
                                 std::string {ptr, end});
        if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
            SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::YEAR>(part[0]), "invalid year {}",
                                     part[0]);
            SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                     "invalid month {}", part[1]);
            SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(part[2]), "invalid day {}",
                                     part[2]);
        }

        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[0])),
                                 "failed to consume 2 digits for hour, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[1])),
                                 "failed to consume 2 digits for minute, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[2])),
                                 "failed to consume 2 digits for second, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                                 part[0]);
        SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::MINUTE>(part[1]), "invalid minute {}",
                                 part[1]);
        SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::SECOND>(part[2]), "invalid second {}",
                                 part[2]);
        has_second = true;
        if (ptr == end) {
            // no fraction or timezone part, just return.
            return true;
        }
        goto FRAC;
    }

    // date part
    SET_PARAMS_RET_FALSE_IFN(in_bound(ptr, end, 5), "too short date part, got '{}'",
                             std::string {ptr, end});
    if (is_digit_range(ptr, ptr + 5)) {
        // no delimiter here.
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[0])),
                                 "failed to consume 2 digits for year, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[1])),
                                 "failed to consume 2 digits for year/month, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[2])),
                                 "failed to consume 2 digits for month/day, got {}",
                                 std::string {ptr, end});
        if (ptr < end && is_numeric_ascii(*ptr)) {
            // 4 digits year
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[3])),
                                     "failed to consume 2 digits for day, got {}",
                                     std::string {ptr, end});
            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                                         "invalid year {}", part[0] * 100 + part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                         "invalid month {}", part[2]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(part[3]),
                                         "invalid day {}", part[3]);
            }
        } else {
            if (!try_convert_set_zero_date(res, complete_4digit_year(part[0]), part[1], part[2])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                         "invalid month {}", part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(part[2]),
                                         "invalid day {}", part[2]);
            }
        }
    } else {
        // has delimiter here.
        SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[0])),
                                 "failed to consume 2 digits for year, got {}",
                                 std::string {ptr, end});
        SET_PARAMS_RET_FALSE_IFN(in_bound(ptr, end, 0), "too short date part, got '{}'",
                                 std::string {ptr, end});
        if (*ptr == '-') {
            // 2 digits year
            ++ptr; // consume one bar
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[1])),
                                     "failed to consume 1 or 2 digits for month, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_one_bar(ptr, end)),
                                     "failed to consume one bar after month, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[2])),
                                     "failed to consume 1 or 2 digits for day, got {}",
                                     std::string {ptr, end});

            if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
                SET_PARAMS_RET_FALSE_IFN(
                        res.set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                         "invalid month {}", part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(part[2]),
                                         "invalid day {}", part[2]);
            }
        } else {
            // 4 digits year
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[1])),
                                     "failed to consume 4 digits for year, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_one_bar(ptr, end)),
                                     "failed to consume one bar after year, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[2])),
                                     "failed to consume 1 or 2 digits for month, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_one_bar(ptr, end)),
                                     "failed to consume one bar after month, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[3])),
                                     "failed to consume 1 or 2 digits for day, got {}",
                                     std::string {ptr, end});

            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                                         "invalid year {}", part[0] * 100 + part[1]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                         "invalid month {}", part[2]);
                SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(part[3]),
                                         "invalid day {}", part[3]);
            }
        }
    }

    if (ptr == end) {
        // no time part, just return.
        return true;
    }

    SET_PARAMS_RET_FALSE_IFN(consume_one_delimiter(ptr, end),
                             "failed to consume one delimiter after date, got {}",
                             std::string {ptr, end});

    // time part.
    // hour
    SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[0])),
                             "failed to consume 1 or 2 digits for hour, got {}",
                             std::string {ptr, end});
    SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                             part[0]);
    if (ptr == end) {
        // no minute part, just return.
        return true;
    }
    if (*ptr == ':') {
        // with hour:minute:second
        if (consume_one_colon(ptr, end)) { // minute
            SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[1])),
                                     "failed to consume 1 or 2 digits for minute, got {}",
                                     std::string {ptr, end});
            SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::MINUTE>(part[1]),
                                     "invalid minute {}", part[1]);
            if (consume_one_colon(ptr, end)) { // second
                has_second = true;
                SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1, 2>(ptr, end, part[2])),
                                         "failed to consume 1 or 2 digits for second, got {}",
                                         std::string {ptr, end});
                SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::SECOND>(part[2]),
                                         "invalid second {}", part[2]);
            }
        }
    } else {
        // no ':'
        if (in_bound(ptr, end, 1) && is_digit_range(ptr, ptr + 2)) {
            part[1] = (ptr[0] - '0') * 10 + ptr[1] - '0';
            // has minute
            SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::MINUTE>(part[1]),
                                     "invalid minute {}", part[1]);
            ptr += 2;
            if (in_bound(ptr, end, 1) && is_digit_range(ptr, ptr + 2)) {
                part[2] = (ptr[0] - '0') * 10 + ptr[1] - '0';
                // has second
                has_second = true;
                SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::SECOND>(part[2]),
                                         "invalid second {}", part[2]);
                ptr += 2;
            }
        }
    }

FRAC:
    // fractional part
    if (has_second && ptr < end && *ptr == '.') {
        ++ptr;
        static_cast<void>(skip_any_digit(ptr, end));
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
                SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 1>(ptr, end, part[0])),
                                         "invalid hour offset '{}'", std::string {ptr, end});
            } else {
                SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[0])),
                                         "invalid hour offset '{}'", std::string {ptr, end});
            }
            SET_PARAMS_RET_FALSE_IFN(part[0] <= 14, "invalid hour offset '{}'", part[0]);
            if (ptr < end) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                SET_PARAMS_RET_FALSE_IFN((consume_digit<UInt32, 2>(ptr, end, part[1])),
                                         "invalid minute offset '{}'", std::string {ptr, end});
                SET_PARAMS_RET_FALSE_IFN((part[1] == 0 || part[1] == 30 || part[1] == 45),
                                         "invalid minute offset '{}'", part[1]);
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
            SET_PARAMS_RET_FALSE_IFN(skip_tz_name_part(ptr, end), "invalid timezone name '{}'",
                                     std::string {start, ptr});

            SET_PARAMS_RET_FALSE_IFN(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }

        static_cast<void>(skip_any_whitespace(ptr, end));
        SET_PARAMS_RET_FALSE_IFN(ptr == end,
                                 "invalid date string '{}', extra characters after timezone",
                                 std::string {ptr, end});
    }
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
inline bool CastToDateV2::from_string_non_strict_mode_impl(const StringRef& str,
                                                           DateV2Value<DateV2ValueType>& res,
                                                           const cctz::time_zone* local_time_zone,
                                                           CastParameters& params) {
    constexpr bool IsStrict = false;
    const char* ptr = str.data;
    const char* end = ptr + str.size;
    AsanPoisonDefer defer(end, 1);

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr != end, "empty date string");

    // date part
    uint32_t year, month, day;

    // read year
    PROPAGATE_FALSE((consume_digit<UInt32, 2>(ptr, end, year)));
    if (is_digit_range(ptr, ptr + 1)) {
        // continue by digit, it must be a 4-digit year
        uint32_t year2;
        PROPAGATE_FALSE((consume_digit<UInt32, 2>(ptr, end, year2)));
        year = year * 100 + year2;
    } else {
        // otherwise, it must be a 2-digit year
        if (year < 100) {
            // Convert 2-digit year based on 1970 boundary
            year += (year >= 70) ? 1900 : 2000;
        }
    }

    // check for separator
    PROPAGATE_FALSE(skip_one_non_alnum(ptr, end));

    // read month
    PROPAGATE_FALSE((consume_digit<UInt32, 1, 2>(ptr, end, month)));

    // check for separator
    PROPAGATE_FALSE(skip_one_non_alnum(ptr, end));

    // read day
    PROPAGATE_FALSE((consume_digit<UInt32, 1, 2>(ptr, end, day)));

    if (!try_convert_set_zero_date(res, year, month, day)) {
        SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::YEAR>(year), "invalid year {}", year);
        SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::MONTH>(month), "invalid month {}",
                                 month);
        SET_PARAMS_RET_FALSE_IFN(res.set_time_unit<TimeUnit::DAY>(day), "invalid day {}", day);
    }

    if (is_space_range(ptr, end)) {
        // no time part, just return.
        return true;
    }

    PROPAGATE_FALSE(consume_one_delimiter(ptr, end));

    // time part
    uint32_t hour, minute, second;

    // hour
    PROPAGATE_FALSE((consume_digit<UInt32, 1, 2>(ptr, end, hour)));
    SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::HOUR>(hour), "invalid hour {}", hour);

    // check for separator
    PROPAGATE_FALSE(skip_one_non_alnum(ptr, end));

    // minute
    PROPAGATE_FALSE((consume_digit<UInt32, 1, 2>(ptr, end, minute)));
    SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::MINUTE>(minute), "invalid minute {}",
                             minute);

    // check for separator
    PROPAGATE_FALSE(skip_one_non_alnum(ptr, end));

    // second
    PROPAGATE_FALSE((consume_digit<UInt32, 1, 2>(ptr, end, second)));
    SET_PARAMS_RET_FALSE_IFN(res.test_time_unit<TimeUnit::SECOND>(second), "invalid second {}",
                             second);

    // fractional part
    if (ptr < end && *ptr == '.') {
        ++ptr;
        static_cast<void>(skip_any_digit(ptr, end));
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
                PROPAGATE_FALSE((consume_digit<UInt32, 1>(ptr, end, hour_offset)));
            } else {
                PROPAGATE_FALSE((consume_digit<UInt32, 2>(ptr, end, hour_offset)));
            }
            SET_PARAMS_RET_FALSE_IFN(hour_offset <= 14, "invalid hour offset {}", hour_offset);
            if (ptr < end) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                PROPAGATE_FALSE((consume_digit<UInt32, 2>(ptr, end, minute_offset)));
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
            PROPAGATE_FALSE(skip_tz_name_part(ptr, end));

            SET_PARAMS_RET_FALSE_IFN(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
    }

    // skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    SET_PARAMS_RET_FALSE_IFN(ptr == end, "invalid date string '{}', extra characters after parsing",
                             std::string {ptr, end});

    return true;
}

// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)
#include "common/compile_check_end.h"
} // namespace doris::vectorized
