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

#include <compare>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <type_traits>

#include "core/value/vdatetime_value.h"

namespace doris {

// TIMESTAMP_NS is represented by a signed Int64 count of nanoseconds from the Unix epoch. The
// complete Int64 domain is valid, giving the exact range
// [1677-09-21 00:12:43.145224192, 2262-04-11 23:47:16.854775807]. The SQL type is timezone-naive:
// UTC below is only a deterministic bridge between the integer and civil calendar fields, and
// must not apply the session time zone.
//
// Keep this class an Int64-sized trivially-copyable value. Columns, storage encodings, hashing,
// and SIMD paths rely on being able to move its bytes exactly like an Int64.
class TimeStampNsValue {
public:
    static constexpr int FRACTIONAL_DIGITS = 9;
    static constexpr int64_t NANOS_PER_SECOND = 1000000000;
    static constexpr int64_t NANOS_PER_MILLISECOND = 1000000;
    static constexpr int64_t NANOS_PER_MICROSECOND = 1000;
    static constexpr int64_t SECONDS_PER_DAY = 86400;
    static constexpr int64_t NANOS_PER_DAY = SECONDS_PER_DAY * NANOS_PER_SECOND;
    static constexpr int64_t UNIX_EPOCH_DAYNR = 719528;

    constexpr TimeStampNsValue() = default;
    explicit constexpr TimeStampNsValue(int64_t epoch_nanos) : _epoch_nanos(epoch_nanos) {}

    // This is the raw physical key, not the packed civil layout used by DATETIMEV2.
    constexpr int64_t to_date_int_val() const { return _epoch_nanos; }
    constexpr int64_t epoch_nanos() const { return _epoch_nanos; }

    // Split epoch nanoseconds using floor division so the fractional component is always
    // non-negative. For example, -1ns becomes second -1 plus nanosecond 999,999,999 rather than
    // second 0 plus a negative fraction.
    int64_t epoch_seconds() const {
        int64_t seconds = _epoch_nanos / NANOS_PER_SECOND;
        if (_epoch_nanos % NANOS_PER_SECOND < 0) {
            --seconds;
        }
        return seconds;
    }

    uint32_t nanosecond() const {
        int64_t nanos = _epoch_nanos % NANOS_PER_SECOND;
        if (nanos < 0) {
            nanos += NANOS_PER_SECOND;
        }
        return static_cast<uint32_t>(nanos);
    }

    uint32_t microsecond() const { return nanosecond() / 1000; }
    uint16_t nanosecond_remainder() const { return static_cast<uint16_t>(nanosecond() % 1000); }

    // DATEV2 and DATETIMEV2 are used only as civil-calendar adapters. DATETIMEV2 carries the first
    // six fractional digits; callers must preserve nanosecond_remainder() separately when the
    // operation should not discard the final three digits.
    DateV2Value<DateV2ValueType> to_date() const;
    DateV2Value<DateTimeV2ValueType> to_datetime() const;
    bool from_datetime(const DateV2Value<DateTimeV2ValueType>& value,
                       uint16_t nanosecond_remainder = 0);

    // Calendar-only fields use the packed DATEV2 value as an adapter. Epoch-derived fields stay on
    // the Int64 representation so extraction and difference functions do not pay for a
    // civil-calendar conversion.
    uint16_t year() const { return to_date().year(); }
    uint8_t month() const { return to_date().month(); }
    uint8_t day() const { return to_date().day(); }
    uint8_t hour() const { return static_cast<uint8_t>(time_part_to_seconds() / 3600); }
    uint8_t minute() const { return static_cast<uint8_t>(time_part_to_seconds() / 60 % 60); }
    uint8_t second() const { return static_cast<uint8_t>(time_part_to_seconds() % 60); }
    int quarter() const { return (month() - 1) / 3 + 1; }
    int64_t daynr() const { return UNIX_EPOCH_DAYNR + epoch_days(); }
    uint16_t year_of_week() const { return to_date().year_of_week(); }
    uint8_t week(uint8_t mode) const { return to_date().week(mode); }
    uint32_t year_week(uint8_t mode) const { return to_date().year_week(mode); }
    int day_of_year() const { return to_date().day_of_year(); }
    int day_of_week() const { return (weekday() + 1) % 7 + 1; }
    uint8_t weekday() const { return calc_weekday(daynr(), false); }
    int64_t time_part_to_nanosecond() const {
        int64_t nanos = _epoch_nanos % NANOS_PER_DAY;
        if (nanos < 0) {
            nanos += NANOS_PER_DAY;
        }
        return nanos;
    }
    int64_t time_part_to_seconds() const { return time_part_to_nanosecond() / NANOS_PER_SECOND; }
    int64_t time_part_to_microsecond() const {
        return time_part_to_nanosecond() / NANOS_PER_MICROSECOND;
    }

    template <typename RHS>
    int64_t time_part_diff_in_ms(const RHS& rhs) const {
        return time_part_to_microsecond() - rhs.time_part_to_microsecond();
    }

    // This intentionally compares integral civil seconds and ignores fractional seconds. It is
    // the contract used by sequence_match's second-based pattern conditions.
    template <typename RHS>
    int64_t datetime_diff_in_seconds(const RHS& rhs) const {
        if constexpr (std::is_same_v<std::remove_cvref_t<RHS>, TimeStampNsValue>) {
            return epoch_seconds() - rhs.epoch_seconds();
        } else {
            return (daynr() - rhs.daynr()) * SECOND_PER_HOUR * HOUR_PER_DAY +
                   time_part_to_seconds() - rhs.time_part_to_seconds();
        }
    }

    template <typename RHS>
    int64_t datetime_diff_in_microseconds(const RHS& rhs) const {
        if constexpr (std::is_same_v<std::remove_cvref_t<RHS>, TimeStampNsValue>) {
            return static_cast<int64_t>((static_cast<__int128>(_epoch_nanos) - rhs._epoch_nanos) /
                                        NANOS_PER_MICROSECOND);
        }
        return (daynr() - rhs.daynr()) * HOUR_PER_DAY * SECOND_PER_HOUR * MS_PER_SECOND +
               time_part_diff_in_ms(rhs);
    }

    template <typename RHS>
    int32_t date_diff_in_days(const RHS& rhs) const {
        return static_cast<int32_t>(daynr() - rhs.daynr());
    }

    int32_t date_diff_in_days_round_to_zero_by_time(const auto& rhs) const {
        int32_t days = date_diff_in_days(rhs);
        const int64_t time_diff = time_part_diff_in_ms(rhs);
        if (days > 0 && time_diff < 0) {
            --days;
        } else if (days < 0 && time_diff > 0) {
            ++days;
        }
        return days;
    }

    const char* day_name_with_locale(const char* const* day_names) const {
        return to_date().day_name_with_locale(day_names);
    }
    const char* month_name_with_locale(const char* const* month_names) const {
        return to_date().month_name_with_locale(month_names);
    }
    bool to_format_string_conservative(const char* format, size_t len, char* to,
                                       size_t max_valid_length) const {
        return to_datetime().to_format_string_conservative(format, len, to, max_valid_length,
                                                           nanosecond());
    }

    bool is_valid_date() const { return true; }

    // Calendar arithmetic preserves all nine fractional digits. The packed adapter carries the
    // leading microseconds and the explicit remainder carries the final three nanoseconds.
    template <TimeUnit unit>
    bool date_add_interval(const TimeInterval& interval) {
        auto value = to_datetime();
        const uint16_t remainder = nanosecond_remainder();
        if (!value.template date_add_interval<unit>(interval)) {
            return false;
        }
        return from_datetime(value, remainder);
    }

    // Truncation deliberately discards every field below unit. Conversion back detects the two
    // civil boundary days whose midnight lies outside the signed epoch-nanosecond range.
    template <TimeUnit unit>
    bool datetime_trunc() {
        auto value = to_datetime();
        if (!value.template datetime_trunc<unit>()) {
            return false;
        }
        return from_datetime(value);
    }

    // TIMESTAMP_NS has no configurable scale. Formatting always emits all nine fractional digits,
    // including trailing zeros, so every output path exposes the same fixed-width representation.
    int32_t to_buffer(char* buffer) const;
    char* to_string(char* buffer) const {
        const int32_t length = to_buffer(buffer);
        buffer[length] = '\0';
        return buffer + length + 1;
    }
    std::string to_string() const {
        char buffer[40];
        const int32_t length = to_buffer(buffer);
        return {buffer, static_cast<size_t>(length)};
    }

    auto operator<=>(const TimeStampNsValue&) const = default;

    TimeStampNsValue& operator+=(int64_t seconds) {
        int64_t delta = 0;
        DORIS_CHECK(!__builtin_mul_overflow(seconds, NANOS_PER_SECOND, &delta));
        DORIS_CHECK(!__builtin_add_overflow(_epoch_nanos, delta, &_epoch_nanos));
        return *this;
    }

    TimeStampNsValue& operator-=(int64_t seconds) {
        DORIS_CHECK_NE(seconds, std::numeric_limits<int64_t>::min());
        return *this += -seconds;
    }

    uint32_t hash(int seed) const {
        return HashUtil::hash(&_epoch_nanos, sizeof(_epoch_nanos), seed);
    }

private:
    int64_t epoch_days() const {
        int64_t days = _epoch_nanos / NANOS_PER_DAY;
        if (_epoch_nanos % NANOS_PER_DAY < 0) {
            --days;
        }
        return days;
    }

    int64_t _epoch_nanos = 0;
};

static_assert(sizeof(TimeStampNsValue) == sizeof(int64_t));
static_assert(std::is_trivially_copyable_v<TimeStampNsValue>);

inline __int128 datetime_diff_in_nanoseconds(const DateV2Value<DateTimeV2ValueType>& datetime1,
                                             uint32_t nanosecond1,
                                             const DateV2Value<DateTimeV2ValueType>& datetime2,
                                             uint32_t nanosecond2) {
    const __int128 day_difference = datetime2.daynr() - datetime1.daynr();
    const int64_t second_difference =
            datetime2.time_part_to_seconds() - datetime1.time_part_to_seconds();
    return (day_difference * HOUR_PER_DAY * SECOND_PER_HOUR + second_difference) *
                   TimeStampNsValue::NANOS_PER_SECOND +
           static_cast<int64_t>(nanosecond2) - nanosecond1;
}

// Whole-unit differences follow DATETIMEV2 semantics: calendar units compare civil fields while
// elapsed units use the exact civil nanosecond difference and truncate toward zero.
template <TimeUnit UNIT>
int64_t datetime_diff_with_nanoseconds(const DateV2Value<DateTimeV2ValueType>& datetime1,
                                       uint32_t nanosecond1,
                                       const DateV2Value<DateTimeV2ValueType>& datetime2,
                                       uint32_t nanosecond2) {
    if constexpr (UNIT == YEAR || UNIT == QUARTER || UNIT == MONTH || UNIT == WEEK || UNIT == DAY) {
        const auto time_key = [](const DateV2Value<DateTimeV2ValueType>& value, uint32_t nanosecond,
                                 bool include_month) {
            int64_t result = include_month ? value.month() : 0;
            result = result * 32 + value.day();
            result = result * 24 + value.hour();
            result = result * 60 + value.minute();
            result = result * 60 + value.second();
            return result * TimeStampNsValue::NANOS_PER_SECOND + nanosecond;
        };
        if constexpr (UNIT == YEAR) {
            int year = datetime2.year() - datetime1.year();
            const int64_t remainder1 = time_key(datetime1, nanosecond1, true);
            const int64_t remainder2 = time_key(datetime2, nanosecond2, true);
            if (year > 0) {
                year -= remainder2 < remainder1;
            } else if (year < 0) {
                year += remainder2 > remainder1;
            }
            return year;
        } else if constexpr (UNIT == QUARTER || UNIT == MONTH) {
            int month = (datetime2.year() - datetime1.year()) * 12 +
                        (datetime2.month() - datetime1.month());
            const int64_t remainder1 = time_key(datetime1, nanosecond1, false);
            const int64_t remainder2 = time_key(datetime2, nanosecond2, false);
            if (month > 0) {
                month -= remainder2 < remainder1;
            } else if (month < 0) {
                month += remainder2 > remainder1;
            }
            return UNIT == QUARTER ? month / 3 : month;
        } else {
            int64_t day = datetime2.daynr() - datetime1.daynr();
            const int64_t time1 =
                    datetime1.time_part_to_seconds() * TimeStampNsValue::NANOS_PER_SECOND +
                    nanosecond1;
            const int64_t time2 =
                    datetime2.time_part_to_seconds() * TimeStampNsValue::NANOS_PER_SECOND +
                    nanosecond2;
            if (day > 0) {
                day -= time2 < time1;
            } else if (day < 0) {
                day += time2 > time1;
            }
            return UNIT == WEEK ? day / 7 : day;
        }
    } else {
        static_assert(UNIT == HOUR || UNIT == MINUTE || UNIT == SECOND || UNIT == MILLISECOND ||
                              UNIT == MICROSECOND,
                      "Unsupported TimeUnit for TIMESTAMP_NS datetime_diff");
        constexpr int64_t divisor = [] {
            if constexpr (UNIT == HOUR) {
                return 3600 * TimeStampNsValue::NANOS_PER_SECOND;
            } else if constexpr (UNIT == MINUTE) {
                return 60 * TimeStampNsValue::NANOS_PER_SECOND;
            } else if constexpr (UNIT == SECOND) {
                return TimeStampNsValue::NANOS_PER_SECOND;
            } else if constexpr (UNIT == MILLISECOND) {
                return TimeStampNsValue::NANOS_PER_MILLISECOND;
            } else {
                return TimeStampNsValue::NANOS_PER_MICROSECOND;
            }
        }();
        return static_cast<int64_t>(
                datetime_diff_in_nanoseconds(datetime1, nanosecond1, datetime2, nanosecond2) /
                divisor);
    }
}

template <TimeUnit UNIT>
int64_t datetime_diff(const TimeStampNsValue& ts_value1, const TimeStampNsValue& ts_value2) {
    return datetime_diff_with_nanoseconds<UNIT>(ts_value1.to_datetime(), ts_value1.nanosecond(),
                                                ts_value2.to_datetime(), ts_value2.nanosecond());
}

template <TimeUnit UNIT>
int64_t datetime_diff(const TimeStampNsValue& ts_value1,
                      const DateV2Value<DateTimeV2ValueType>& ts_value2) {
    return datetime_diff_with_nanoseconds<UNIT>(ts_value1.to_datetime(), ts_value1.nanosecond(),
                                                ts_value2, ts_value2.microsecond() * 1000);
}

template <TimeUnit UNIT>
int64_t datetime_diff(const DateV2Value<DateTimeV2ValueType>& ts_value1,
                      const TimeStampNsValue& ts_value2) {
    return datetime_diff_with_nanoseconds<UNIT>(ts_value1, ts_value1.microsecond() * 1000,
                                                ts_value2.to_datetime(), ts_value2.nanosecond());
}

} // namespace doris

template <>
struct std::hash<doris::TimeStampNsValue> {
    size_t operator()(const doris::TimeStampNsValue& value) const {
        return std::hash<int64_t> {}(value.epoch_nanos());
    }
};
