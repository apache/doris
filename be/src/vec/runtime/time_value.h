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

#include <cctz/time_zone.h>

#include <cmath>
#include <cstdint>
#include <string>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
#include "util/string_parser.hpp"

namespace doris {
#include "common/compile_check_begin.h"

/// TODO:  Due to the "Time type is not supported for OLAP table" issue, a lot of basic content is missing.It will be supplemented later.
// now we do check only when print.
class TimeValue {
public:
    constexpr static int64_t ONE_SECOND_MICROSECONDS = 1000000;
    constexpr static int64_t ONE_MINUTE_MICROSECONDS = 60 * ONE_SECOND_MICROSECONDS;
    constexpr static int64_t ONE_HOUR_MICROSECONDS = 60 * ONE_MINUTE_MICROSECONDS;
    constexpr static int64_t ONE_MINUTE_SECONDS = 60;
    constexpr static int64_t ONE_HOUR_SECONDS = 60 * ONE_MINUTE_SECONDS;
    constexpr static uint32_t MICROS_SCALE = 6;
    constexpr static int64_t MAX_TIME = 3020399LL * ONE_SECOND_MICROSECONDS;

    /// TODO: Why is the time type stored as double? Can we directly use int64 and remove the time limit?
    using TimeType = typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType; // double
    using ColumnTimeV2 = typename PrimitiveTypeTraits<TYPE_TIMEV2>::ColumnType;

    static int64_t round_time(TimeType value, uint32_t scale) {
        int64_t time = value;
        DCHECK(scale <= MICROS_SCALE);
        int64_t factor = std::pow(10, 6 - scale);
        int64_t roundedValue = (time >= 0) ? (time + factor / 2) / factor * factor
                                           : (time - factor / 2) / factor * factor;
        return roundedValue;
    }

    // Construct time based on hour/minute/second/microsecond
    template <bool CHECK = false>
    static TimeType make_time(int32_t hour, int32_t minute, int32_t second, int32_t microsecond = 0,
                              bool negative = false) {
        if constexpr (CHECK) {
            if (std::abs(hour) >= 838 || std::abs(minute) >= 60 || std::abs(second) >= 60 ||
                std::abs(microsecond) >= 1000000) [[unlikely]] {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid time value: hour={}, minute={}, second={}, microsecond={}",
                                hour, minute, second, microsecond);
            }
        }
        DCHECK(hour >= 0 && minute >= 0 && second >= 0 && microsecond >= 0)
                << "Hour, minute, second and microsecond must be non-negative but got " << hour
                << ":" << minute << ":" << second << "." << microsecond;
        int64_t value = ((int64_t)hour * ONE_HOUR_MICROSECONDS) +
                        ((int64_t)minute * ONE_MINUTE_MICROSECONDS) +
                        (second * ONE_SECOND_MICROSECONDS) + microsecond;
        return static_cast<TimeType>(negative ? -value : value);
    }

    // if time is negative, ms should be negative too.
    static TimeType init_microsecond(TimeType time, int32_t microsecond) {
        if (std::abs(microsecond) >= 1000000) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Microsecond must be in the range [0, 999999]");
        }
        DCHECK(std::signbit(time) == std::signbit(microsecond) || time || microsecond)
                << "Time and microsecond must have the same sign but got " << time << " and "
                << microsecond;

        return static_cast<TimeType>(time + microsecond);
    }

    static std::string to_string(TimeType time, int scale) {
        return timev2_to_buffer_from_double(time, scale);
    }

    // Return the hour/minute/second part of the time, ignoring the sign
    /*
        select cast(-121314 as time); -> -12:13:14
        select hour(cast(-121314 as time)),minute(cast(-121314 as time)),second(cast(-121314 as time)); -> 12	13	14 
    */
    static int32_t hour(TimeType time) {
        return (int32_t)std::abs(static_cast<int64_t>(limit_with_bound(time) / ONE_HOUR_MICROSECONDS));
    }

    static int32_t minute(TimeType time) {
        return (int32_t)std::abs((static_cast<int64_t>(limit_with_bound(time)) % ONE_HOUR_MICROSECONDS) /
                                             ONE_MINUTE_MICROSECONDS);
    }

    static int32_t second(TimeType time) {
        return (int32_t)std::abs((static_cast<int64_t>(limit_with_bound(time)) / ONE_SECOND_MICROSECONDS) %
                                             ONE_MINUTE_SECONDS);
    }

    static int32_t microsecond(TimeType time) {
        return (int32_t)std::abs(static_cast<int64_t>(limit_with_bound(time)) % ONE_SECOND_MICROSECONDS);
    }

    static int8_t sign(TimeType time) { return (time < 0) ? -1 : 1; }

    // Construct time based on seconds
    static TimeType from_seconds_with_limit(int32_t sec) {
        return limit_with_bound((TimeType)sec * ONE_SECOND_MICROSECONDS);
    }

    // Cast from string
    // Some examples of conversions.
    // '300' -> 00:03:00 '20:23' ->  20:23:00 '20:23:24' -> 20:23:24
    template <typename T>
    static bool try_parse_time(char* s, size_t len, T& x, const cctz::time_zone& local_time_zone) {
        if (try_as_time(s, len, x)) {
            return true;
        } else {
            // For example, "2013-01-01 01:02:03" can be parsed as datetime
            if (DateV2Value<doris::DateTimeV2ValueType> dv {};
                dv.from_date_str(s, (int)len, local_time_zone)) {
                // can be parse as a datetime
                x = TimeValue::make_time(dv.hour(), dv.minute(), dv.second());
                return true;
            }
            return false;
        }
    }

    template <typename T>
    static bool try_as_time(const char* s, size_t len, T& x) {
        const char* first_char = s;
        const char* end_char = s + len;
        int64_t hour = 0, minute = 0, second = 0;
        // For a valid time string, its format is hh:mm:ss
        // Where hh can be negative, mm/ss must be positive
        // For example: -12:13:14 and 12:13:14 are valid, but 12:-30:14 is invalid
        auto parse_from_str_to_int = [](const char* begin, size_t len, auto& num) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto int_value = StringParser::string_to_int<int64_t>(begin, (int)len, &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return false;
            }
            num = int_value;
            return true;
        };

        auto parse_from_str_to_uint = [](const char* begin, size_t len, auto& num) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto int_value =
                    StringParser::string_to_unsigned_int<uint64_t>(begin, (int)len, &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return false;
            }
            num = int_value;
            return true;
        };
        if (char* first_colon {nullptr};
            (first_colon = (char*)memchr(first_char, ':', len)) != nullptr) {
            if (char* second_colon {nullptr};
                (second_colon = (char*)memchr(first_colon + 1, ':', end_char - first_colon - 1)) !=
                nullptr) {
                // find two colons
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    // hour failed
                    return false;
                }
                // parse minute
                if (!parse_from_str_to_uint(first_colon + 1, second_colon - first_colon - 1,
                                            minute)) {
                    return false;
                }
                // parse second
                if (!parse_from_str_to_uint(second_colon + 1, end_char - second_colon - 1,
                                            second)) {
                    return false;
                }
            } else {
                // find one colon
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    return false;
                }
                // parse minute
                if (!parse_from_str_to_uint(first_colon + 1, end_char - first_colon - 1, minute)) {
                    return false;
                }
            }
        } else {
            // no colon, so try to parse as a number
            // This is to be compatible with MySQL behavior. If it is not a valid time string, it will try to parse it as a number and convert it to a time type
            // select cast("31214" as time); ==> 03:12:14
            int64_t from {};
            if (!parse_from_str_to_int(first_char, len, from)) {
                return false;
            }
            return try_parse_time(from, x);
        }
        // minute and second must be < 60
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = TimeValue::make_time(hour, minute, second, hour < 0);
        return true;
    }

    // Cast from number
    template <typename T>
    static bool try_parse_time(T from_other, TimeType& x) {
        constexpr static int64_t MAX_INT_TIME = 8385959;
        if (from_other > MAX_INT_TIME || from_other < -MAX_INT_TIME) {
            return false;
        }
        bool negative = from_other < 0;
        int64_t from = std::abs((int64_t)from_other);
        auto seconds = int32_t(from / 100);
        int32_t hour = 0, minute = 0, second = 0;
        second = int32_t(from - 100 * seconds);
        from /= 100;
        seconds = int32_t(from / 100);
        minute = int32_t(from - 100 * seconds);
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = TimeValue::make_time(hour, minute, second, negative);
        return true;
    }

    // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
    // the time value between '-838:59:59' and '838:59:59'
    static TimeType limit_with_bound(TimeType time) {
        // cast(-4562632 as time)
        // -456:26:32
        // hour(cast(-4562632 as time))
        // 456
        // second(cast(-4562632 as time))
        // 32
        if (time > MAX_TIME) {
            return MAX_TIME;
        }
        if (time < -MAX_TIME) {
            return -MAX_TIME;
        }
        return time;
    }

    static bool valid(double time) { return time <= MAX_TIME && time >= -MAX_TIME; }
};

} // namespace doris

#include "common/compile_check_end.h"
