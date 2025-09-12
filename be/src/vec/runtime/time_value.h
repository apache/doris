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
                                        59 * ONE_SECOND_MICROSECONDS + 999999; // 838:59:59.999999

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

    // if time is negative, ms should be negative too.
    static TimeType init_microsecond(TimeType time, int32_t microsecond) {
        if (std::abs(microsecond) >= 1000000) [[unlikely]] {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Microsecond must be in the range [0, 999999]");
        }
        DCHECK(std::signbit(time) == std::signbit(microsecond) || !time || !microsecond)
                << "Time and microsecond must have the same sign but got " << time << " and "
                << microsecond;

        return static_cast<TimeType>(time + microsecond);
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
};
} // namespace doris
#include "common/compile_check_end.h"
