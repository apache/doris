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
#include "vec/runtime/vdatetime_value.h"

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
    constexpr static int64_t MAX_TIME = 838 * ONE_HOUR_MICROSECONDS + 59 * ONE_MINUTE_MICROSECONDS +
                                        59 * ONE_SECOND_MICROSECONDS; // 838:59:59.000000

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
    static TimeType make_time(int64_t hour, int64_t minute, int64_t second, int64_t microsecond = 0,
                              bool negative = false) {
        if constexpr (CHECK) {
            // the max time value is 838:59:59.999999
            if (std::abs(hour) > 838 || std::abs(minute) >= 60 || std::abs(second) >= 60 ||
                std::abs(microsecond) >= 1000000) [[unlikely]] {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid time value: hour={}, minute={}, second={}, microsecond={}",
                                hour, minute, second, microsecond);
            }
        }
        DCHECK(hour >= 0 && minute >= 0 && second >= 0 && microsecond >= 0)
                << "Hour, minute, second and microsecond must be non-negative but got " << hour
                << ":" << minute << ":" << second << "." << microsecond;
        int64_t value = (hour * ONE_HOUR_MICROSECONDS) + (minute * ONE_MINUTE_MICROSECONDS) +
                        (second * ONE_SECOND_MICROSECONDS) + microsecond;
        return static_cast<TimeType>(negative ? -value : value);
    }

    // if time is negative, ms should be negative too. in existing scenario, we ensure microsecond's bound by caller.
    static TimeType init_microsecond(TimeType time, int32_t microsecond) {
        DCHECK(std::signbit(time) == std::signbit(microsecond) || !time || !microsecond)
                << "Time and microsecond must have the same sign but got " << time << " and "
                << microsecond;

        return static_cast<TimeType>(time + microsecond);
    }

    // in existing scenario, we ensure microsecond's bound by caller.
    // ATTN: only for positive input.
    static TimeType reset_microsecond(TimeType time, int32_t microsecond) {
        DCHECK(time >= 0 && microsecond >= 0)
                << "Time and microsecond must be non-negative but got " << time << " and "
                << microsecond;

        return static_cast<TimeType>(time - ((int64_t)time % ONE_SECOND_MICROSECONDS) +
                                     microsecond);
    }

    static std::string to_string(TimeType time, int scale) {
        return timev2_to_buffer_from_double(time, scale);
    }

    /// Return the hour/minute/second part of the time, ignoring the sign
    static int32_t hour(TimeType time) {
        return (int32_t)std::abs(
                static_cast<int64_t>(limit_with_bound(time) / ONE_HOUR_MICROSECONDS));
    }

    static int32_t minute(TimeType time) {
        return (int32_t)std::abs(
                (static_cast<int64_t>(limit_with_bound(time)) % ONE_HOUR_MICROSECONDS) /
                ONE_MINUTE_MICROSECONDS);
    }

    static int32_t second(TimeType time) {
        return (int32_t)std::abs(
                (static_cast<int64_t>(limit_with_bound(time)) / ONE_SECOND_MICROSECONDS) %
                ONE_MINUTE_SECONDS);
    }

    static int32_t microsecond(TimeType time) {
        return (int32_t)std::abs(static_cast<int64_t>(limit_with_bound(time)) %
                                 ONE_SECOND_MICROSECONDS);
    }

    static int8_t sign(TimeType time) { return (time < 0) ? -1 : 1; }

    // Construct time based on seconds
    static TimeType from_seconds_with_limit(int32_t sec) {
        return limit_with_bound((TimeType)sec * ONE_SECOND_MICROSECONDS);
    }

    static TimeType from_double_with_limit(double sec) {
        return limit_with_bound((TimeType)(sec * ONE_SECOND_MICROSECONDS));
    }

    // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
    // the time value between '-838:59:59' and '838:59:59'
    static TimeType limit_with_bound(TimeType time) {
        if (time > MAX_TIME) {
            return MAX_TIME;
        }
        if (time < -MAX_TIME) {
            return -MAX_TIME;
        }
        return time;
    }

    static bool valid(double time) { return time <= MAX_TIME && time >= -MAX_TIME; }

    static bool to_format_string_conservative(const char* format, size_t len, char* to,
                                              size_t max_valid_length, TimeType time) {
        // If time is negative, we here only add a '-' to the begining of res
        // This behavior is consistent with MySQL
        if (time < 0) {
            memcpy(to, "-", 1);
            ++to;
            time = -time;
        }

        int32_t hour = TimeValue::hour(time);
        int32_t minute = TimeValue::minute(time);
        int32_t second = TimeValue::second(time);
        int32_t microsecond = TimeValue::microsecond(time);

        char* const begin = to;
        char buf[64];
        char* pos = nullptr;
        char* cursor = buf;
        const char* ptr = format;
        const char* end = format + len;
        char ch = '\0';

        while (ptr < end) {
            if (to - begin + SAFE_FORMAT_STRING_MARGIN > max_valid_length) [[unlikely]] {
                return false;
            }
            if (*ptr != '%' || (ptr + 1) == end) {
                *to++ = *ptr++;
                continue;
            }
            ptr++;
            switch (ch = *ptr++) {
            case 'H':
                // Hour (00..838 for TIME type, with at least 2 digits)
                if (hour < 100) {
                    to = write_two_digits_to_string(hour, to);
                } else {
                    pos = int_to_str(hour, cursor);
                    to = append_with_prefix(cursor, static_cast<int>(pos - cursor), '0', 2, to);
                }
                break;
            case 'h':
            case 'I':
                // Hour (01..12)
                to = write_two_digits_to_string((hour % 24 + 11) % 12 + 1, to);
                break;
            case 'i':
                // Minutes, numeric (00..59)
                to = write_two_digits_to_string(minute, to);
                break;
            case 'k':
                // Hour (0..23) without leading zero
                pos = int_to_str(hour, cursor);
                to = append_with_prefix(cursor, static_cast<int>(pos - cursor), '0', 1, to);
                break;
            case 'l':
                // Hour (1..12) without leading zero
                pos = int_to_str((hour % 24 + 11) % 12 + 1, cursor);
                to = append_with_prefix(cursor, static_cast<int>(pos - cursor), '0', 1, to);
                break;
            case 's':
            case 'S':
                // Seconds (00..59)
                to = write_two_digits_to_string(second, to);
                break;
            case 'f':
                // Microseconds (000000..999999)
                pos = int_to_str(microsecond, cursor);
                to = append_with_prefix(cursor, static_cast<int>(pos - cursor), '0', 6, to);
                break;
            case 'p': {
                // AM or PM
                if (hour % 24 >= 12) {
                    to = append_string("PM", to);
                } else {
                    to = append_string("AM", to);
                }
                break;
            }
            case 'r': {
                // Time, 12-hour (hh:mm:ss followed by AM or PM)
                int32_t hour_12 = (hour + 11) % 12 + 1;
                *to++ = (char)('0' + (hour_12 / 10));
                *to++ = (char)('0' + (hour_12 % 10));
                *to++ = ':';
                *to++ = (char)('0' + (minute / 10));
                *to++ = (char)('0' + (minute % 10));
                *to++ = ':';
                *to++ = (char)('0' + (second / 10));
                *to++ = (char)('0' + (second % 10));
                if (hour % 24 >= 12) {
                    to = append_string(" PM", to);
                } else {
                    to = append_string(" AM", to);
                }
                break;
            }
            case 'T': {
                // Time, 24-hour (hh:mm:ss or hhh:mm:ss for TIME type)
                if (hour < 100) {
                    *to++ = (char)('0' + (hour / 10));
                    *to++ = (char)('0' + (hour % 10));
                } else {
                    // For hours >= 100, convert to string with at least 2 digits
                    pos = int_to_str(hour, cursor);
                    to = append_with_prefix(cursor, static_cast<int>(pos - cursor), '0', 2, to);
                }
                *to++ = ':';
                *to++ = (char)('0' + (minute / 10));
                *to++ = (char)('0' + (minute % 10));
                *to++ = ':';
                *to++ = (char)('0' + (second / 10));
                *to++ = (char)('0' + (second % 10));
                break;
            }
            case '%':
                *to++ = '%';
                break;
            case 'Y':
                // Year, 4 digits - 4 zeros
                to = append_string("0000", to);
                break;
            case 'y':
            case 'm':
            case 'd':
                // Year (2 digits), Month, Day - insert 2 zeros
                to = write_two_digits_to_string(0, to);
                break;
            case 'c':
            case 'e':
                // Month (0..12) or Day without leading zero - insert 1 zero
                to = append_string("0", to);
                break;
            case 'M':
            case 'W':
            case 'j':
            case 'D':
            case 'U':
            case 'u':
            case 'V':
            case 'v':
            case 'x':
            case 'X':
            case 'w':
                // These specifiers are not supported for TIME type
                return false;
            default:
                *to++ = ch;
                break;
            }
        }
        *to++ = '\0';
        return true;
    }

private:
    static constexpr char digits100[201] =
            "00010203040506070809"
            "10111213141516171819"
            "20212223242526272829"
            "30313233343536373839"
            "40414243444546474849"
            "50515253545556575859"
            "60616263646566676869"
            "70717273747576777879"
            "80818283848586878889"
            "90919293949596979899";

    static char* int_to_str(uint64_t val, char* to) {
        char buf[64];
        char* ptr = buf;
        // Use do/while for 0 value
        do {
            *ptr++ = '0' + (val % 10);
            val /= 10;
        } while (val);

        while (ptr > buf) {
            *to++ = *--ptr;
        }
        return to;
    }

    static char* append_string(const char* from, char* to) {
        while (*from) {
            *to++ = *from++;
        }
        return to;
    }

    static char* append_with_prefix(const char* str, int str_len, char prefix, int target_len,
                                    char* to) {
        // full_len is the lower bound. if less, use prefix to pad. if greater, accept all.
        int diff = target_len - str_len;
        // use prefix to pad
        while (diff-- > 0) {
            *to++ = prefix;
        }

        memcpy(to, str, str_len);
        return to + str_len;
    }

    static char* write_two_digits_to_string(int number, char* dst) {
        memcpy(dst, &digits100[number * 2], 2);
        return dst + 2;
    }

    static bool is_date_related_specifier(char spec) {
        switch (spec) {
        case 'Y':
        case 'y':
        case 'M':
        case 'm':
        case 'b':
        case 'c':
        case 'd':
        case 'D':
        case 'e':
        case 'j':
        case 'U':
            return true;
        default:
            return false;
        }
    }
};
} // namespace doris
#include "common/compile_check_end.h"
