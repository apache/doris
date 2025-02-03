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
};

} // namespace doris

#include "common/compile_check_end.h"
