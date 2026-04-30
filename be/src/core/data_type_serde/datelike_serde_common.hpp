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

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common/config.h"
#include "core/value/vdatetime_value.h"

namespace doris {

template <typename T>
inline constexpr bool can_fast_parse_fixed_canonical_time = false;

template <>
inline constexpr bool can_fast_parse_fixed_canonical_time<VecDateTimeValue> = true;

template <>
inline constexpr bool can_fast_parse_fixed_canonical_time<DateV2Value<DateTimeV2ValueType>> = true;

template <>
inline constexpr bool can_fast_parse_fixed_canonical_time<DateV2Value<DateV2ValueType>> = true;

enum class DatelikeFastParseResult : uint8_t {
    FAIL,
    // A canonical YYYY-MM-DD prefix matched. Callers still need the full parser when
    // there is remaining suffix that the fast path did not consume.
    DATE_ONLY,
    // A canonical YYYY-MM-DD HH:MM:SS prefix matched.
    DATE_TIME,
};

inline PURE bool is_fixed_two_digit_ascii(const char* ptr) {
    return static_cast<unsigned>(ptr[0] - '0') < 10 && static_cast<unsigned>(ptr[1] - '0') < 10;
}

inline PURE bool is_fixed_four_digit_ascii(const char* ptr) {
    return is_fixed_two_digit_ascii(ptr) && is_fixed_two_digit_ascii(ptr + 2);
}

inline PURE uint32_t parse_fixed_two_digit_ascii(const char* ptr) {
    return (ptr[0] - '0') * 10 + (ptr[1] - '0');
}

inline PURE uint32_t parse_fixed_four_digit_ascii(const char* ptr) {
    return parse_fixed_two_digit_ascii(ptr) * 100 + parse_fixed_two_digit_ascii(ptr + 2);
}

template <typename T>
inline DatelikeFastParseResult try_parse_fixed_canonical_datelike_prefix(const char* ptr,
                                                                         size_t size, T& res) {
    if (size < 10 || ptr[4] != '-' || ptr[7] != '-' || !is_fixed_four_digit_ascii(ptr) ||
        !is_fixed_two_digit_ascii(ptr + 5) || !is_fixed_two_digit_ascii(ptr + 8)) {
        return DatelikeFastParseResult::FAIL;
    }

    const uint32_t year = parse_fixed_four_digit_ascii(ptr);
    const uint32_t month = parse_fixed_two_digit_ascii(ptr + 5);
    const uint32_t day = parse_fixed_two_digit_ascii(ptr + 8);
    if (!try_convert_set_zero_date(res, year, month, day)) {
        if (!res.template set_time_unit<TimeUnit::YEAR>(year) ||
            !res.template set_time_unit<TimeUnit::MONTH>(month) ||
            !res.template set_time_unit<TimeUnit::DAY>(day)) {
            return DatelikeFastParseResult::FAIL;
        }
    }

    if (size == 10) {
        return DatelikeFastParseResult::DATE_ONLY;
    }

    if constexpr (!can_fast_parse_fixed_canonical_time<T>) {
        return DatelikeFastParseResult::DATE_ONLY;
    }

    if (size < 19 || (ptr[10] != ' ' && ptr[10] != 'T') || ptr[13] != ':' || ptr[16] != ':' ||
        !is_fixed_two_digit_ascii(ptr + 11) || !is_fixed_two_digit_ascii(ptr + 14) ||
        !is_fixed_two_digit_ascii(ptr + 17)) {
        return DatelikeFastParseResult::DATE_ONLY;
    }

    const uint32_t hour = parse_fixed_two_digit_ascii(ptr + 11);
    const uint32_t minute = parse_fixed_two_digit_ascii(ptr + 14);
    const uint32_t second = parse_fixed_two_digit_ascii(ptr + 17);
    if constexpr (std::is_same_v<T, DateV2Value<DateV2ValueType>>) {
        // DATEV2 ignores the time fields, but validating a canonical HH:MM:SS prefix here
        // still lets strict callers continue from the suffix without reparsing the prefix.
        if (!res.template test_time_unit<TimeUnit::HOUR>(hour) ||
            !res.template test_time_unit<TimeUnit::MINUTE>(minute) ||
            !res.template test_time_unit<TimeUnit::SECOND>(second)) {
            return DatelikeFastParseResult::FAIL;
        }
    } else {
        if (!res.template set_time_unit<TimeUnit::HOUR>(hour) ||
            !res.template set_time_unit<TimeUnit::MINUTE>(minute) ||
            !res.template set_time_unit<TimeUnit::SECOND>(second)) {
            return DatelikeFastParseResult::FAIL;
        }
    }
    return DatelikeFastParseResult::DATE_TIME;
}

inline PURE uint32_t complete_4digit_year(uint32_t year) {
    if (year < 70) {
        return year + 2000; // 00-69 -> 2000-2069
    } else {
        return year + 1900; // 70-99 -> 1970-1999
    }
}

// return true if we set the date value in this way.
template <typename T>
inline bool try_convert_set_zero_date(T& date_val, uint32_t year, uint32_t month, uint32_t day) {
    if (config::allow_zero_date && year == 0 && month == 0 && day == 0) {
        date_val.template unchecked_set_time_unit<TimeUnit::YEAR>(0);
        date_val.template unchecked_set_time_unit<TimeUnit::MONTH>(1);
        date_val.template unchecked_set_time_unit<TimeUnit::DAY>(1);
        return true;
    }
    return false;
}
} // namespace doris
