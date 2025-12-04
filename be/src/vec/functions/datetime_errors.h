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
#include <string_view>

#include "common/exception.h"
#include "common/status.h"
#include "util/binary_cast.hpp"
#include "vec/core/types.h"

namespace doris::vectorized {
// Convert a native datelike value to printable string using DateValueType::to_string
// Note: DateValueType must support to_string(char*) -> char*
//       NativeT is the corresponding FieldType of the DataType
template <typename DateValueType, typename NativeT>
inline std::string datelike_to_string(NativeT native) {
    char buf[40];
    auto value = binary_cast<NativeT, DateValueType>(native);
    char* end = value.to_string(buf);
    // minus 1 to skip trailing '\0'
    return std::string(buf, end - 1);
}

// Throw for operations with one datelike argument
template <typename DateValueType, typename NativeT>
[[noreturn]] inline void throw_out_of_bound_one_date(const char* op, NativeT arg0) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {} out of range", op,
                    datelike_to_string<DateValueType>(arg0));
}

// Throw for operations with a datelike and an integer (e.g. period)
template <typename DateValueType, typename NativeT>
[[noreturn]] inline void throw_out_of_bound_date_int(const char* op, NativeT arg0, Int32 delta) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {} out of range", op,
                    datelike_to_string<DateValueType>(arg0), delta);
}

// Throw for operations with a single integer argument (e.g., from_days daynr)
[[noreturn]] inline void throw_out_of_bound_int(const char* op, int64_t value) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {} out of range", op, value);
}

// Throw for operations with two integer arguments (e.g., makedate(year, day))
[[noreturn]] inline void throw_out_of_bound_two_ints(const char* op, int64_t a, int64_t b) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {} out of range", op, a, b);
}

// for convert_tz
template <typename DateValueType, typename NativeT>
[[noreturn]] inline void throw_out_of_bound_convert_tz(NativeT arg0, std::string_view from_name,
                                                       std::string_view to_name) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Cannot convert {} from {} to {}",
                    datelike_to_string<DateValueType>(arg0), from_name, to_name);
}

// Throw for operations with a datelike, an integer and an origin datelike
// (e.g. time_round(datetime, period, origin))
template <typename DateValueType, typename NativeT>
[[noreturn]] inline void throw_out_of_bound_int_date(const char* op, NativeT arg0, Int32 delta,
                                                     NativeT origin) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {}, {} out of range", op,
                    datelike_to_string<DateValueType>(arg0), delta,
                    datelike_to_string<DateValueType>(origin));
}

// Throw for operations with two datelike arguments
// (e.g. time_round(datetime, origin))
template <typename DateValueType, typename NativeT>
[[noreturn]] inline void throw_out_of_bound_date_date(const char* op, NativeT arg0, NativeT arg1) {
    throw Exception(ErrorCode::OUT_OF_BOUND, "Operation {} of {}, {} out of range", op,
                    datelike_to_string<DateValueType>(arg0),
                    datelike_to_string<DateValueType>(arg1));
}

// Throw for operations with an invalid string argument (e.g., from_iso8601_date)
[[noreturn]] inline void throw_invalid_string(const char* op, std::string_view s) {
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} of {} is invalid", op, s);
}

// Throw for operations with two invalid string arguments (e.g., unix_timestamp with format)
[[noreturn]] inline void throw_invalid_strings(const char* op, std::string_view s0,
                                               std::string_view s1) {
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} of {}, {} is invalid", op, s0, s1);
}

// Helper to get time unit name for error messages
inline const char* get_time_unit_name(TimeUnit unit) {
    switch (unit) {
    case TimeUnit::YEAR:
        return "year_add";
    case TimeUnit::QUARTER:
        return "quarter_add";
    case TimeUnit::MONTH:
        return "month_add";
    case TimeUnit::WEEK:
        return "week_add";
    case TimeUnit::DAY:
        return "day_add";
    case TimeUnit::HOUR:
        return "hour_add";
    case TimeUnit::MINUTE:
        return "minute_add";
    case TimeUnit::SECOND:
        return "second_add";
    case TimeUnit::MILLISECOND:
        return "millisecond_add";
    case TimeUnit::MICROSECOND:
        return "microsecond_add";
    default:
        return "date_add";
    }
}
} // namespace doris::vectorized
