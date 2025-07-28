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

#include "data_type_datetimev2_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include <algorithm>
#include <chrono> // IWYU pragma: keep
#include <cmath>
#include <cstdint>

#include "common/status.h"
#include "datelike_serde_common.hpp"
#include "runtime/primitive_type.h"
#include "util/string_parser.hpp"
#include "vec/columns/column_const.h"
#include "vec/common/int_exp.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

enum {
    DIVISOR_FOR_SECOND = 1,
    DIVISOR_FOR_MILLI = 1000,
    DIVISOR_FOR_MICRO = 1000000,
    DIVISOR_FOR_NANO = 1000000000
};

namespace doris::vectorized {
static const int64_t micro_to_nano_second = 1000;
#include "common/compile_check_begin.h"

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeDateTimeV2SerDe::from_string_batch(const ColumnString& col_str,
                                                  ColumnNullable& col_res,
                                                  const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        DateV2Value<DateTimeV2ValueType> res;
        //TODO: maybe we can statistics the failure rate of strict mode. if high, directly try non-strict mode.
        Status st = _from_string_strict_mode(str, res, options.timezone);
        if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            st = _from_string(str, res, options.timezone);
        }
        if (st.ok()) {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            // if still invalid, set null
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            // some internal error
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res);

    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_element(i);
        DateV2Value<DateTimeV2ValueType> res;
        RETURN_IF_ERROR(_from_string_strict_mode(str, res, options.timezone));
        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
    }
    return Status::OK();
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
Status DataTypeDateTimeV2SerDe::_from_string(const std::string& str,
                                             DateV2Value<DateTimeV2ValueType>& res,
                                             const cctz::time_zone* local_time_zone) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    if (ptr == end) {
        return Status::InvalidArgument("empty datetime string");
    }

    // date part
    uint32_t year, month, day;

    // read year
    RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, year)));
    if (is_digit_range(ptr, ptr + 1)) {
        // continue by digit, it must be a 4-digit year
        uint32_t year2;
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, year2)));
        year = year * 100 + year2;
    } else {
        // otherwise, it must be a 2-digit year
        if (year < 100) {
            // Convert 2-digit year based on 1970 boundary
            year += (year >= 70) ? 1900 : 2000;
        }
    }

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // read month
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, month)));

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // read day
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, day)));

    if (!try_convert_set_zero_date(res, year, month, day)) {
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(year), "invalid year {}", year);
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(month), "invalid month {}",
                                  month);
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(day), "invalid day {}", day);
    }

    if (ptr == end) {
        // no time part, just return.
        res.unchecked_set_time_unit<TimeUnit::HOUR>(0);
        res.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
        res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
        return Status::OK();
    }

    RETURN_IF_ERROR(consume_one_delimiter(ptr, end));

    // time part
    uint32_t hour, minute, second;

    // hour
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, hour)));
    RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::HOUR>(hour), "invalid hour {}", hour);

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // minute
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, minute)));
    RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(minute), "invalid minute {}",
                              minute);

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // second
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, second)));
    RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(second), "invalid second {}",
                              second);

    // fractional part
    if (assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        auto length = ptr - start;

        if (length > 0) {
            StringParser::ParseResult success;
            auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                    start, std::min<int>((int)length, _scale), &success);
            RETURN_INVALID_ARG_IF_NOT(success == StringParser::PARSE_SUCCESS,
                                      "invalid fractional part in datetime string '{}'",
                                      std::string {start, ptr});

            if (length > _scale) { // _scale is up to 6
                // round off to at most `_scale` digits
                if (*(start + _scale) - '0' >= 5) {
                    frac_literal++;
                    DCHECK(frac_literal <= 1000000);
                    if (frac_literal == common::exp10_i32(_scale)) {
                        // overflow, round up to next second
                        RETURN_INVALID_ARG_IF_NOT(
                                res.date_add_interval<TimeUnit::SECOND>(
                                        TimeInterval {TimeUnit::SECOND, 1, false}),
                                "datetime overflow when rounding up to next second");
                        frac_literal = 0;
                    }
                }
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                        (int32_t)frac_literal * common::exp10_i32(6 - (int)_scale));
            } else { // length <= _scale
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                        (int32_t)frac_literal * common::exp10_i32(6 - (int)length));
            }
        } else {
            res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
        }
    } else {
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
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
                RETURN_IF_ERROR((consume_digit<UInt32, 1>(ptr, end, hour_offset)));
            } else {
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, hour_offset)));
            }
            RETURN_INVALID_ARG_IF_NOT(hour_offset <= 14, "invalid hour offset {}", hour_offset);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, minute_offset)));
                RETURN_INVALID_ARG_IF_NOT(
                        (minute_offset == 0 || minute_offset == 30 || minute_offset == 45),
                        "invalid minute offset {}", minute_offset);
            }
            if (hour_offset == 14 && minute_offset > 0) [[unlikely]] {
                return Status::InvalidArgument("invalid timezone offset '{}'",
                                               combine_tz_offset(sign, hour_offset, minute_offset));
            }

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(
                            combine_tz_offset(sign, hour_offset, minute_offset), parsed_tz),
                    "invalid timezone offset '{}'",
                    combine_tz_offset(sign, hour_offset, minute_offset));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            RETURN_IF_ERROR(skip_tz_name_part(ptr, end));

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }

        // convert tz
        cctz::civil_second cs {res.year(), res.month(),  res.day(),
                               res.hour(), res.minute(), res.second()};

        auto given = cctz::convert(cs, parsed_tz);
        auto local = cctz::convert(given, *local_time_zone);
        res.unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
        res.unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
        res.unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
        res.unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
        res.unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
        res.unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());
    }

    // skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    RETURN_INVALID_ARG_IF_NOT(ptr == end,
                              "invalid datetime string '{}', extra characters after parsing",
                              std::string {ptr, end});

    return Status::OK();
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
Status DataTypeDateTimeV2SerDe::_from_string_strict_mode(
        const std::string& str, DateV2Value<DateTimeV2ValueType>& res,
        const cctz::time_zone* local_time_zone) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    uint32_t part[4];
    bool has_second = false;

    // special `date` and `time` part format: 14-length digits string. parse it as YYYYMMDDHHMMSS
    if (assert_within_bound(ptr, end, 13) && is_digit_range(ptr, ptr + 14)) {
        // if the string is all digits, treat it as a date in YYYYMMDD format.
        RETURN_IF_ERROR((consume_digit<UInt32, 4>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(part[0]), "invalid year {}",
                                      part[0]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                      "invalid month {}", part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[2]), "invalid day {}",
                                      part[2]);
        }

        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                                  part[0]);
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(part[1]), "invalid minute {}",
                                  part[1]);
        RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(part[2]), "invalid second {}",
                                  part[2]);
        has_second = true;
        if (ptr == end) {
            // no fraction or timezone part, just return.
            return Status::OK();
        }
        goto FRAC;
    }

    // date part
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 5));
    if (is_digit_range(ptr, ptr + 5)) {
        // no delimiter here.
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (is_numeric_ascii(*ptr)) {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[3])));
            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                          "invalid month {}", part[2]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[3]),
                                          "invalid day {}", part[3]);
            }
        } else {
            if (!try_convert_set_zero_date(res, complete_4digit_year(part[0]), part[1], part[2])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                          "invalid month {}", part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[2]),
                                          "invalid day {}", part[2]);
            }
        }
    } else {
        // has delimiter here.
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
        if (*ptr == '-') {
            // 2 digits year
            ++ptr; // consume one bar
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));

            if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                          "invalid month {}", part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[2]),
                                          "invalid day {}", part[2]);
            }
        } else {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[3])));

            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                          "invalid month {}", part[2]);
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[3]),
                                          "invalid day {}", part[3]);
            }
        }
    }

    if (ptr == end) {
        // no time part, just return.
        res.unchecked_set_time_unit<TimeUnit::HOUR>(0);
        res.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
        res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
        return Status::OK();
    }

    RETURN_IF_ERROR(consume_one_delimiter(ptr, end));

    // time part.
    // hour
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[0])));
    RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                              part[0]);
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
    if (*ptr == ':') {
        // with hour:minute:second
        if (consume_one_colon(ptr, end)) { // minute
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_one_colon(ptr, end)) { // second
                has_second = true;
                RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(part[2]),
                                          "invalid second {}", part[2]);
            } else {
                res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
            }
        } else {
            res.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
            res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
        }
    } else {
        // no ':'
        if (consume_digit<UInt32, 2>(ptr, end, part[1])) {
            // has minute
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_digit<UInt32, 2>(ptr, end, part[2])) {
                // has second
                has_second = true;
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(part[2]),
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
        auto length = ptr - start;

        if (length > 0) {
            StringParser::ParseResult success;
            auto frac_literal = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                    start, std::min<int>((int)length, _scale), &success);
            RETURN_INVALID_ARG_IF_NOT(success == StringParser::PARSE_SUCCESS,
                                      "invalid fractional part in datetime string '{}'",
                                      std::string {start, ptr});

            if (length > _scale) { // _scale is up to 6
                // round off to at most `_scale` digits
                if (*(start + _scale) - '0' >= 5) {
                    frac_literal++;
                    DCHECK(frac_literal <= 1000000);
                    if (frac_literal == common::exp10_i32(_scale)) {
                        // overflow, round up to next second
                        RETURN_INVALID_ARG_IF_NOT(
                                res.date_add_interval<TimeUnit::SECOND>(
                                        TimeInterval {TimeUnit::SECOND, 1, false}),
                                "datetime overflow when rounding up to next second");
                        frac_literal = 0;
                    }
                }
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                        (int32_t)frac_literal * common::exp10_i32(6 - (int)_scale));
            } else { // length <= _scale
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                        (int32_t)frac_literal * common::exp10_i32(6 - (int)length));
            }
        }
    } else {
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
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
                RETURN_IF_ERROR((consume_digit<UInt32, 1>(ptr, end, part[0])));
            } else {
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
            }
            RETURN_INVALID_ARG_IF_NOT(part[0] <= 14, "invalid hour offset {}", part[0]);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
                RETURN_INVALID_ARG_IF_NOT((part[1] == 0 || part[1] == 30 || part[1] == 45),
                                          "invalid minute offset {}", part[1]);
            }
            if (part[0] == 14 && part[1] > 0) [[unlikely]] {
                return Status::InvalidArgument("invalid timezone offset '{}'",
                                               combine_tz_offset(sign, part[0], part[1]));
            }

            RETURN_INVALID_ARG_IF_NOT(TimezoneUtils::find_cctz_time_zone(
                                              combine_tz_offset(sign, part[0], part[1]), parsed_tz),
                                      "invalid timezone offset '{}'",
                                      combine_tz_offset(sign, part[0], part[1]));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            RETURN_IF_ERROR(skip_tz_name_part(ptr, end));

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        // convert tz
        cctz::civil_second cs {res.year(), res.month(),  res.day(),
                               res.hour(), res.minute(), res.second()};

        auto given = cctz::convert(cs, parsed_tz);
        auto local = cctz::convert(given, *local_time_zone);
        res.unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
        res.unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
        res.unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
        res.unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
        res.unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
        res.unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());

        static_cast<void>(skip_any_whitespace(ptr, end));
        RETURN_INVALID_ARG_IF_NOT(ptr == end,
                                  "invalid datetime string '{}', extra characters after timezone",
                                  std::string {ptr, end});
    }
    return Status::OK();
}

static Status from_int(uint64_t uint_val, int length, DateV2Value<DateTimeV2ValueType>& val) {
    if (length == 3 || length == 4) {
        val.unchecked_set_time_unit<TimeUnit::YEAR>(2000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>((uint32_t)uint_val / 100),
                                  "invalid month {}", uint_val / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 5) {
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::YEAR>(2000 + (uint32_t)uint_val / 10000),
                "invalid year {}", 2000 + uint_val / 10000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 6) {
        uint32_t year = (uint32_t)uint_val / 10000;
        if (year < 70) {
            year += 2000;
        } else {
            year += 1900;
        }
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::YEAR>(year), "invalid year {}", year);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 8) {
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::YEAR>((uint32_t)uint_val / 10000),
                                  "invalid year {}", uint_val / 10000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 14) {
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::YEAR>(uint_val / common::exp10_i64(10)),
                "invalid year {}", uint_val / common::exp10_i64(10));
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::MONTH>((uint_val / common::exp10_i32(8)) % 100),
                "invalid month {}", (uint_val / common::exp10_i32(8)) % 100);
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::DAY>((uint_val / common::exp10_i32(6)) % 100),
                "invalid day {}", (uint_val / common::exp10_i32(6)) % 100);
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::HOUR>((uint_val / common::exp10_i32(4)) % 100),
                "invalid hour {}", (uint_val / common::exp10_i32(4)) % 100);
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::MINUTE>((uint_val / common::exp10_i32(2)) % 100),
                "invalid minute {}", (uint_val / common::exp10_i32(2)) % 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::SECOND>(uint_val % 100),
                                  "invalid second {}", uint_val % 100);
    } else [[unlikely]] {
        return Status::InvalidArgument("invalid digits for datetimev2: {}", uint_val);
    }
    return Status::OK();
}

[[nodiscard]] static bool init_microsecond(int64_t frac_input, uint32_t frac_length,
                                           DateV2Value<DateTimeV2ValueType>& val,
                                           uint32_t target_scale) {
    if (frac_length > 0) {
        // align to `target_scale` digits
        auto in_scale_part =
                (frac_length > target_scale)
                        ? (uint32_t)(frac_input / common::exp10_i64(frac_length - target_scale))
                        : (uint32_t)(frac_input * common::exp10_i64(target_scale - frac_length));

        if (frac_length > target_scale) { // _scale is up to 6
            // round off to at most `_scale` digits
            auto digit_next =
                    (uint32_t)(frac_input / common::exp10_i64(frac_length - target_scale - 1)) % 10;
            if (digit_next >= 5) {
                in_scale_part++;
                DCHECK(in_scale_part <= 1000000);
                if (in_scale_part == common::exp10_i32(target_scale)) {
                    // overflow, round up to next second
                    RETURN_INVALID_ARG_IF_NOT(val.date_add_interval<TimeUnit::SECOND>(
                                                      TimeInterval {TimeUnit::SECOND, 1, false}),
                                              "datetime overflow when rounding up to next second");
                    in_scale_part = 0;
                }
            }
        }
        val.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                (uint32_t)in_scale_part * common::exp10_i32(6 - (int)target_scale));
    }
    return true;
}

template <typename IntDataType>
Status DataTypeDateTimeV2SerDe::from_int_batch(const IntDataType::ColumnType& int_col,
                                               ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        if (int_col.get_element(i) > std::numeric_limits<int64_t>::max() ||
            int_col.get_element(i) < std::numeric_limits<int64_t>::min()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            continue;
        }
        auto int_val = (int64_t)int_col.get_element(i);
        if (int_val <= 0) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            continue;
        }
        int length = common::count_digits_fast(int_val);

        DateV2Value<DateTimeV2ValueType> val;
        if (auto st = from_int(int_val, length, val); st.ok()) [[likely]] {
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
            col_nullmap.get_data()[i] = false;
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                           IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        if (int_col.get_element(i) > std::numeric_limits<int64_t>::max() ||
            int_col.get_element(i) < std::numeric_limits<int64_t>::min()) {
            return Status::InvalidArgument("invalid int value for time: {}",
                                           int_col.get_element(i));
        }
        auto int_val = (int64_t)int_col.get_element(i);
        if (int_val <= 0) {
            return Status::InvalidArgument("invalid int value for datetimev2: {}", int_val);
        }
        int length = common::count_digits_fast(int_val);

        DateV2Value<DateTimeV2ValueType> val;
        RETURN_IF_ERROR(from_int(int_val, length, val));
        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateTimeV2SerDe::from_float_batch(const FloatDataType::ColumnType& float_col,
                                                 ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (float_value <= 0 || std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            continue;
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        DateV2Value<DateTimeV2ValueType> val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
            if (init_microsecond(ms_part_7, 7, val, _scale)) [[likely]] {
                col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
                col_nullmap.get_data()[i] = false;
            } else {
                col_nullmap.get_data()[i] = true;
                col_data.get_data()[i] =
                        binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            }
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch(
        const FloatDataType::ColumnType& float_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (float_value <= 0 || std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            return Status::InvalidArgument("invalid float value for datetimev2: {}", float_value);
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        DateV2Value<DateTimeV2ValueType> val;
        RETURN_IF_ERROR(from_int(int_part, length, val));

        int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
        if (!init_microsecond(ms_part_7, 7, val, _scale)) [[unlikely]] {
            return Status::InvalidArgument("datetime overflow after carry on microsecond {}",
                                           float_value);
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateTimeV2SerDe::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                                   ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        if (decimal_col.get_intergral_part(i) > std::numeric_limits<int64_t>::max() ||
            decimal_col.get_intergral_part(i) < std::numeric_limits<int64_t>::min()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            continue;
        }
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        if (int_part <= 0) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            continue;
        }
        int length = common::count_digits_fast(int_part);

        DateV2Value<DateTimeV2ValueType> val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            if (init_microsecond((int64_t)decimal_col.get_fractional_part(i),
                                 decimal_col.get_scale(), val, _scale)) [[likely]] {
                col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
                col_nullmap.get_data()[i] = false;
            } else {
                col_nullmap.get_data()[i] = true;
                col_data.get_data()[i] =
                        binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
            }
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(target_col);
    col_data.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        if (decimal_col.get_intergral_part(i) > std::numeric_limits<int64_t>::max() ||
            decimal_col.get_intergral_part(i) < std::numeric_limits<int64_t>::min()) {
            return Status::InvalidArgument("invalid intergral value for time: {}",
                                           decimal_col.get_element(i));
        }
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        if (int_part <= 0) {
            return Status::InvalidArgument("invalid decimal integral part for datetimev2: {}",
                                           int_part);
        }
        int length = common::count_digits_fast(int_part);

        DateV2Value<DateTimeV2ValueType> val;
        RETURN_IF_ERROR(from_int(int_part, length, val));
        if (!init_microsecond((int64_t)decimal_col.get_fractional_part(i), decimal_col.get_scale(),
                              val, _scale)) [[unlikely]] {
            return Status::InvalidArgument("datetime overflow after carry on microsecond {}",
                                           decimal_col.get_fractional_part(i));
        }

        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(val);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                         int64_t end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                           BufferWritable& bw,
                                                           FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (_nesting_level > 1) {
        bw.write('"');
    }
    UInt64 int_val =
            assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                    row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    bw.write(buf, pos - buf - 1);

    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}
Status DataTypeDateTimeV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(column);
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    UInt64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size);
        !read_datetime_v2_text_impl<UInt64>(val, rb, _scale)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column,
                                                      const NullMap* null_map,
                                                      arrow::ArrayBuilder* array_builder,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnDateTimeV2&>(column).get_data();
    auto& timestamp_builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    std::shared_ptr<arrow::TimestampType> timestamp_type =
            std::static_pointer_cast<arrow::TimestampType>(array_builder->type());
    const std::string& timezone = timestamp_type->timezone();
    const cctz::time_zone& real_ctz = timezone.empty() ? cctz::utc_time_zone() : ctz;
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            int64_t timestamp = 0;
            DateV2Value<DateTimeV2ValueType> datetime_val =
                    binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[i]);
            datetime_val.unix_timestamp(&timestamp, real_ctz);

            if (_scale > 3) {
                uint32_t microsecond = datetime_val.microsecond();
                timestamp = (timestamp * 1000000) + microsecond;
            } else if (_scale > 0) {
                uint32_t millisecond = datetime_val.microsecond() / 1000;
                timestamp = (timestamp * 1000) + millisecond;
            }
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.Append(timestamp), column.get_name(),
                                             array_builder->type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::read_column_from_arrow(IColumn& column,
                                                       const arrow::Array* arrow_array,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnDateTimeV2&>(column).get_data();
    int64_t divisor = 1;
    if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: {
            divisor = DIVISOR_FOR_SECOND;
            break;
        }
        case arrow::TimeUnit::type::MILLI: {
            divisor = DIVISOR_FOR_MILLI;
            break;
        }
        case arrow::TimeUnit::type::MICRO: {
            divisor = DIVISOR_FOR_MICRO;
            break;
        }
        case arrow::TimeUnit::type::NANO: {
            divisor = DIVISOR_FOR_NANO;
            break;
        }
        default: {
            LOG(WARNING) << "not support convert to datetimev2 from time_unit:" << type->unit();
            return Status::InvalidArgument("not support convert to datetimev2 from time_unit: {}",
                                           type->unit());
        }
        }
        for (auto value_i = start; value_i < end; ++value_i) {
            auto utc_epoch = static_cast<UInt64>(concrete_array->Value(value_i));

            DateV2Value<DateTimeV2ValueType> v;
            // convert second
            v.from_unixtime(utc_epoch / divisor, ctz);
            // get rest time
            // add 0 on the right to make it 6 digits. DateTimeV2Value microsecond is 6 digits,
            // the scale decides to keep the first few digits, so the valid digits should be kept at the front.
            // "2022-01-01 11:11:11.111", utc_epoch = 1641035471111, divisor = 1000, set_microsecond(111000)
            v.set_microsecond((utc_epoch % divisor) * DIVISOR_FOR_MICRO / divisor);
            col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
        }
    } else {
        LOG(WARNING) << "not support convert to datetimev2 from arrow type:"
                     << arrow_array->type()->id();
        return Status::InternalError("not support convert to datetimev2 from arrow type: {}",
                                     arrow_array->type()->id());
    }
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateTimeV2ValueType> date_val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val, _scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int64_t start, int64_t end,
                                                    vectorized::Arena& arena) const {
    const auto& col_data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::TimestampVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int64_t timestamp = 0;
        DateV2Value<DateTimeV2ValueType> datetime_val =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[row_id]);
        if (!datetime_val.unix_timestamp(&timestamp, timezone)) {
            return Status::InternalError("get unix timestamp error.");
        }

        cur_batch->data[row_id] = timestamp;
        cur_batch->nanoseconds[row_id] = datetime_val.microsecond() * micro_to_nano_second;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                      uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = assert_cast<ColumnDateTimeV2&>(column);
    auto sz = col.size();
    UInt64 val = col.get_element(sz - 1);
    col.insert_many_vals(val, times);
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

} // namespace doris::vectorized
