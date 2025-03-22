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

#include <string>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
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

    using TimeType = typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType;
    using ColumnTime = vectorized::DataTypeTimeV2::ColumnType;

    // refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
    // the time value between '-838:59:59' and '838:59:59'
    /// TODO: Why is the time type stored as double? Can we directly use int64 and remove the time limit?
    static int64_t check_over_max_time(double time) {
        const static int64_t max_time = 3020399LL * 1000 * 1000;
        // cast(-4562632 as time)
        // -456:26:32
        // hour(cast(-4562632 as time))
        // 456
        // second(cast(-4562632 as time))
        // 32
        if (time > max_time || time < -max_time) {
            return max_time;
        }
        return static_cast<int64_t>(time);
    }

    static TimeType make_time(int64_t hour, int64_t minute, int64_t second,
                              int64_t microsecond = 0) {
        int64_t value = hour * ONE_HOUR_MICROSECONDS + minute * ONE_MINUTE_MICROSECONDS +
                        second * ONE_SECOND_MICROSECONDS + microsecond;
        return static_cast<TimeType>(value);
    }

    static std::string to_string(TimeType time, int scale) {
        return timev2_to_buffer_from_double(time, scale);
    }
    static int32_t hour(TimeType time) {
        return static_cast<int32_t>(check_over_max_time(time) / ONE_HOUR_MICROSECONDS);
    }

    static int32_t minute(TimeType time) {
        return (check_over_max_time(time) % ONE_HOUR_MICROSECONDS) / ONE_MINUTE_MICROSECONDS;
    }

    static int32_t second(TimeType time) {
        return (check_over_max_time(time) / ONE_SECOND_MICROSECONDS) % ONE_MINUTE_SECONDS;
    }

    static TimeType from_second(int64_t sec) {
        return static_cast<TimeType>(sec * ONE_SECOND_MICROSECONDS);
    }

    // Cast from string
    // Some examples of conversions.
    // '300' -> 00:03:00 '20:23' ->  20:23:00 '20:23:24' -> 20:23:24

    constexpr static std::string_view date_time_format = "%Y-%m-%d %H:%i:%s";
    template <typename T>
    static bool try_parse_time(char* s, size_t len, T& x) {
        if (try_as_time(s, len, x)) {
            return true;
        } else {
            if (DateV2Value<doris::DateTimeV2ValueType> dv {};
                dv.from_date_format_str(date_time_format.data(), date_time_format.size(), s, len)) {
                // can be parse as a datetime
                x = TimeValue::make_time(dv.hour(), dv.minute(), dv.second());
                return true;
            }
            return false;
        }
    }

    template <typename T>
    static bool try_as_time(char* s, size_t len, T& x) {
        char* first_char = s;
        char* end_char = s + len;
        int64_t hour = 0, minute = 0, second = 0;
        auto parse_from_str_to_int = [](char* begin, size_t len, auto& num) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto int_value = StringParser::string_to_int<int64_t>(begin, (int)len, &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return false;
            }
            num = int_value;
            return true;
        };

        auto parse_from_str_to_uint = [](char* begin, size_t len, auto& num) {
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
                // find two colon
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    // hour  failed
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
            // no colon ,so try to parse as a number
            int64_t from {};
            if (!parse_from_str_to_int(first_char, len, from)) {
                return false;
            }
            return try_parse_time(from, x);
        }
        // minute second must be < 60
        if (minute >= 60 || second >= 60) {
            return false;
        }
        if (hour < 0) {
            hour = -hour;
            // cast('-1:02:03' as time); --> -01:02:03
            x = -TimeValue::make_time(hour, minute, second);
        } else {
            x = TimeValue::make_time(hour, minute, second);
        }
        return true;
    }
    // Cast from number
    template <typename T, typename S>
    //requires {std::is_arithmetic_v<T> && std::is_arithmetic_v<S>}
    static bool try_parse_time(T from_other, S& x) {
        int64_t from = (int64_t)from_other;
        int64 seconds = int64(from / 100);
        int64 hour = 0, minute = 0, second = 0;
        second = int64(from - 100 * seconds);
        from /= 100;
        seconds = int64(from / 100);
        minute = int64(from - 100 * seconds);
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = TimeValue::make_time(hour, minute, second);
        return true;
    }
    template <typename S>
    static bool try_parse_time(__int128 from_128, S& x) {
        int64_t from = from_128 % (int64)(1000000000000);
        int64 seconds = from / 100;
        int64 hour = 0, minute = 0, second = 0;
        second = from - 100 * seconds;
        from /= 100;
        seconds = from / 100;
        minute = from - 100 * seconds;
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = TimeValue::make_time(hour, minute, second);
        return true;
    }
};

} // namespace doris

#include "common/compile_check_end.h"
