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

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
#include "util/faststring.h"
#include "util/string_parser.hpp"
#include "vec/data_types/data_type_time.h"

namespace doris {
#include "common/compile_check_begin.h"

/// TODO:  Due to the "Time type is not supported for OLAP table" issue, a lot of basic content is missing.It will be supplemented later.
class TimeValue {
public:
    constexpr static int64_t ONE_SECOND_MICROSECONDS = 1000000;
    constexpr static int64_t ONE_MINUTE_MICROSECONDS = 60 * ONE_SECOND_MICROSECONDS;
    constexpr static int64_t ONE_HOUR_MICROSECONDS = 60 * ONE_MINUTE_MICROSECONDS;
    constexpr static int64_t ONE_MINUTE_SECONDS = 60;
    constexpr static int64_t ONE_HOUR_SECONDS = 60 * ONE_MINUTE_SECONDS;
    constexpr static uint32_t MICROS_SCALE = 6;
    constexpr static int64_t MAX_TIME = 3020399LL * 1000 * 1000;

    using TimeType = typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType; // double
    using ColumnTimeV2 = vectorized::DataTypeTimeV2::ColumnType;

    static int64_t round_time(TimeType value, uint32_t scale) {
        int64_t time = value;
        DCHECK(scale <= MICROS_SCALE);
        int64_t factor = std::pow(10, 6 - scale);
        int64_t roundedValue = (time >= 0) ? (time + factor / 2) / factor * factor
                                           : (time - factor / 2) / factor * factor;
        return roundedValue;
    }

    // Construct time based on hour/minute/second/microsecond, ignoring the sign
    /// TODO: Maybe we need to ensure that this function always receives positive numbers
    static TimeType make_time(int64_t hour, int64_t minute, int64_t second,
                              int64_t microsecond = 0) {
        int64_t value = (hour * ONE_HOUR_MICROSECONDS) + (minute * ONE_MINUTE_MICROSECONDS) +
                        (second * ONE_SECOND_MICROSECONDS) + microsecond;
        return static_cast<TimeType>(value);
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
        return std::abs(static_cast<int32_t>(check_over_max_time(time) / ONE_HOUR_MICROSECONDS));
    }

    static int32_t minute(TimeType time) {
        return std::abs(static_cast<int32_t>((check_over_max_time(time) % ONE_HOUR_MICROSECONDS) /
                                             ONE_MINUTE_MICROSECONDS));
    }

    static int32_t second(TimeType time) {
        return std::abs(static_cast<int32_t>((check_over_max_time(time) / ONE_SECOND_MICROSECONDS) %
                                             ONE_MINUTE_SECONDS));
    }

    // Construct time based on seconds
    static TimeType from_second(int64_t sec) {
        return static_cast<TimeType>(sec * ONE_SECOND_MICROSECONDS);
    }

    static bool timev2_to_double_from_str(const char* str, double& v, int scale = 6) {
        // like fe/fe-core/src/main/java/org/apache/doris/analysis/TimeV2Literal.java and
        // fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/literal/TimeV2Literal.java
        // init function to parse str
        std::string s(str);
        bool neg;
        int64_t hour, minute, second, microsecond;
        if (s[0] == '-') {
            neg = true;
            s = s.substr(1);
        } else if (s[0] == '+') {
            neg = false;
            s = s.substr(1);
        }
        if (s.find(':') == std::string::npos) {
            std::string tail;
            if (auto idx = s.find('.'); idx != std::string::npos) {
                tail = s.substr(idx);
                s = s.substr(0, idx);
            }
            auto len = s.length();
            if (len == 1) {
                s = "00:00:0" + s;
            } else if (len == 2) {
                s = "00:00:" + s;
            } else if (len == 3) {
                s = std::string("00:0") + s[0] + ":" + s.substr(1);
            } else if (len == 4) {
                s = "00:" + s.substr(0, 2) + ":" + s.substr(2);
            } else {
                s = s.substr(0, len - 4) + ":" + s.substr(len - 4, len - 2) + ":" +
                    s.substr(len - 2);
            }
            if (neg) {
                s = '-' + s;
            }
            s = s + tail;
        }
        if (!std::all_of(s.begin(), s.end(),
                         [](char c) { return c == '.' || isdigit(c) || c == ':'; })) {
            return false;
        }
        if (s.find(':') == s.rfind(':')) {
            s += ":00";
        }
        auto p1 = s.find(':');
        auto p2 = s.rfind(':');
        hour = std::stoi(s.substr(0, p1));
        minute = std::stoi(s.substr(p1 + 1, p2 - p1));

        // if the part is 60.000 it will cause judge feed execute error
        if (s.substr(p2 + 1).starts_with("60")) {
            return false;
        }
        double sec_part = std::stod(s.substr(p2 + 1));
        sec_part = sec_part * double(long(pow(10, scale)));
        sec_part = round(sec_part);
        sec_part = double((long)sec_part * (long)pow(10, 6 - scale));
        second = int64_t(sec_part) / 1000000;

        if (scale != 0) {
            microsecond = int64_t(sec_part) % 1000000;
            if (second == 60) {
                minute += 1;
                second -= 60;
                if (minute == 60) {
                    hour += 1;
                    minute -= 60;
                }
            }
        } else {
            microsecond = 0;
        }
        v = make_time(hour, minute, second, microsecond);
        return check_over_max_time(v);
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
        x = TimeValue::make_time_with_negative(hour < 0, hour, minute, second);
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
        auto seconds = int64(from / 100);
        int64 hour = 0, minute = 0, second = 0;
        second = int64(from - 100 * seconds);
        from /= 100;
        seconds = int64(from / 100);
        minute = int64(from - 100 * seconds);
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = TimeValue::make_time_with_negative(negative, hour, minute, second);
        return true;
    }

private:
    // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
    // the time value between '-838:59:59' and '838:59:59'
    /// TODO: Why is the time type stored as double? Can we directly use int64 and remove the time limit?
    static int64_t check_over_max_time(double time) {
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
        return static_cast<int64_t>(time);
    }

    static TimeType make_time_with_negative(bool negative, int64_t hour, int64_t minute,
                                            int64_t second, int64_t microsecond = 0) {
        return (negative ? -1 : 1) *
               TimeValue::make_time(std::abs(hour), std::abs(minute), std::abs(second));
    }
};

} // namespace doris

#include "common/compile_check_end.h"
